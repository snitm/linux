/*
 * Copyright (C) 2012 Red Hat. All rights reserved.
 *
 * This file is released under the GPL.
 */

#include "dm.h"
#include "dm-bio-prison.h"
#include "dm-cache-metadata.h"
#include "dm-cache-policy-internal.h"

#include <asm/div64.h>

#include <linux/blkdev.h>
#include <linux/dm-io.h>
#include <linux/dm-kcopyd.h>
#include <linux/init.h>
#include <linux/list.h>
#include <linux/mempool.h>
#include <linux/module.h>
#include <linux/slab.h>

#define DM_MSG_PREFIX "cache"
#define DAEMON "cached"

/*
 * FIXME: we must commit after every migration in order to guarantee crash
 * safety.  Thin works because we never recycle a data block within a
 * transaction.
 */

/*----------------------------------------------------------------*/

/*
 * Glossary:
 *
 * oblock; index of an origin block
 * cblock; index of a cache block
 * migration; movement of a block between the origin and cache device, either direction
 * promotion; movement of a block from origin to cache
 * demotion; movement of a block from cache to origin
 */

/*----------------------------------------------------------------*/

static unsigned long *alloc_and_set_bitset(unsigned nr_entries)
{
	size_t s = sizeof(unsigned long) * dm_div_up(nr_entries, BITS_PER_LONG);
	unsigned long *r = vzalloc(s);
	if (r)
		memset(r, ~0, s);

	return r;
}

static void free_bitset(unsigned long *bits)
{
	vfree(bits);
}

/*----------------------------------------------------------------*/

#define BLOCK_SIZE_MIN 64
#define PRISON_CELLS 1024
#define ENDIO_HOOK_POOL_SIZE 1024
#define MIGRATION_POOL_SIZE 128
#define COMMIT_PERIOD HZ
#define MIGRATION_COUNT_WINDOW 10

/*
 * The cache runs in 3 modes.  Ordered in degraded order for comparisons.
 * FIXME: factor out to common code to share with dm-thin
 */
enum cache_mode {
	CM_WRITE,		/* metadata may be changed */
	CM_READ_ONLY,		/* metadata may not be changed */
	CM_FAIL,		/* all I/O fails */
};

struct cache_features {
	enum cache_mode mode;
};

struct cache_c;
struct dm_cache_migration;
typedef void (*process_bio_fn)(struct cache_c *c, struct bio *bio);
typedef void (*process_migration_fn)(struct dm_cache_migration *mg);

struct cache {
	struct list_head list;
	struct dm_target *ti;	/* Only set if a cache target is bound */

	struct mapped_device *cache_md;
	struct block_device *md_dev;
	struct dm_cache_metadata *cmd;

	process_bio_fn process_bio;
	process_bio_fn process_discard;
	process_bio_fn process_flush;

	process_migration_fn issue_copy;
	process_migration_fn complete_migration;

	/*
	 * Size of the origin device in _complete_ blocks and native sectors.
	 */
	dm_block_t origin_blocks;
	sector_t origin_sectors;

	/*
	 * Size of the cache device in blocks.
	 */
	dm_block_t cache_size;

	/*
	 * Fields for converting from sectors to blocks.
	 */
	sector_t sectors_per_block;
	sector_t offset_mask;
	unsigned int block_shift;

	spinlock_t lock;
	struct bio_list deferred_bios;
	struct bio_list deferred_flush_bios;
	struct list_head quiesced_migrations;
	struct list_head completed_migrations;
	wait_queue_head_t migration_wait;
	atomic_t nr_migrations;

	struct cache_features cf;

	/*
	 * cache_size entries, dirty if set
	 */
	unsigned long *dirty_bitset;

	/*
	 * origin_blocks entries, discarded if set.
	 * FIXME: This is too big
	 */
	unsigned long *discard_bitset;

	struct dm_kcopyd_client *copier;
	struct workqueue_struct *wq;
	struct work_struct worker;

	struct delayed_work waker;
	unsigned long last_commit_jiffies;

	struct dm_bio_prison *prison;
	struct dm_deferred_set *all_io_ds;

	mempool_t *endio_hook_pool;
	mempool_t *migration_pool;
	struct dm_cache_migration *next_migration;

	bool need_tick_bio:1;
	bool quiescing:1;

	unsigned ref_count;
	struct dm_cache_policy *policy;

	atomic_t read_hit;
	atomic_t read_miss;
	atomic_t write_hit;
	atomic_t write_miss;
	atomic_t demotion;
	atomic_t promotion;
	atomic_t copies_avoided;
	atomic_t cache_cell_clash;
};

struct cache_c {
	struct dm_target *ti;
	struct cache *cache;

	/*
	 * Metadata is written to this device.
	 */
	struct dm_dev *metadata_dev;

	/*
	 * The slower of the two data devices.  Typically a spindle.
	 */
	struct dm_dev *origin_dev;

	/*
	 * The faster of the two data devices.  Typically an SSD.
	 */
	struct dm_dev *cache_dev;

	struct dm_target_callbacks callbacks;

	struct cache_features requested_cf; /* Features requested during table load */
	struct cache_features adjusted_cf;  /* Features used after adjusting for constituent devices */
};

/*----------------------------------------------------------------*/

/*
 * A global list of caches that uses a struct mapped_device as a key.
 * FIXME: factor out to common code and share with dm_thin_pool_table
 */
static struct dm_cache_table {
	struct mutex mutex;
	struct list_head caches;
} dm_cache_table;

static void cache_table_init(void)
{
	mutex_init(&dm_cache_table.mutex);
	INIT_LIST_HEAD(&dm_cache_table.caches);
}

static void __cache_table_insert(struct cache *cache)
{
	BUG_ON(!mutex_is_locked(&dm_cache_table.mutex));
	list_add(&cache->list, &dm_cache_table.caches);
}

static void __cache_table_remove(struct cache *cache)
{
	BUG_ON(!mutex_is_locked(&dm_cache_table.mutex));
	list_del(&cache->list);
}

static struct cache *__cache_table_lookup(struct mapped_device *md)
{
	struct cache *cache = NULL, *tmp;

	BUG_ON(!mutex_is_locked(&dm_cache_table.mutex));

	list_for_each_entry(tmp, &dm_cache_table.caches, list) {
		if (tmp->cache_md == md) {
			cache = tmp;
			break;
		}
	}

	return cache;
}

static struct cache *__cache_table_lookup_metadata_dev(struct block_device *md_dev)
{
	struct cache *cache = NULL, *tmp;

	BUG_ON(!mutex_is_locked(&dm_cache_table.mutex));

	list_for_each_entry(tmp, &dm_cache_table.caches, list) {
		if (tmp->md_dev == md_dev) {
			cache = tmp;
			break;
		}
	}

	return cache;
}

/*----------------------------------------------------------------*/

struct dm_cache_endio_hook {
	struct cache_c *c;
	struct dm_deferred_entry *all_io_entry;
	unsigned req_nr:2;
	bool tick:1;
};

struct dm_cache_migration {
	bool err:1;
	bool demote:1;
	bool promote:1;

	struct list_head list;
	struct cache_c *c;

	unsigned long start_jiffies;
	dm_block_t old_oblock;
	dm_block_t new_oblock;
	dm_block_t cblock;

	struct dm_bio_prison_cell *old_ocell;
	struct dm_bio_prison_cell *new_ocell;
};

static void build_key(dm_block_t block, struct dm_cell_key *key)
{
	key->virtual = 0;
	key->dev = 0;
	key->block = block;
}

static void wake_worker(struct cache *cache)
{
	queue_work(cache->wq, &cache->worker);
}

/*----------------------------------------------------------------*/

/*
 * The discard bitset is accessed from both the worker thread and the
 * cache_map function, we need to protect it.
 */
static void set_discard(struct cache *cache, dm_block_t b)
{
	unsigned long flags;

	spin_lock_irqsave(&cache->lock, flags);
	set_bit(b, cache->discard_bitset);
	spin_unlock_irqrestore(&cache->lock, flags);
}

static void clear_discard(struct cache *cache, dm_block_t b)
{
	unsigned long flags;

	spin_lock_irqsave(&cache->lock, flags);
	clear_bit(b, cache->discard_bitset);
	spin_unlock_irqrestore(&cache->lock, flags);
}

static bool is_discarded(struct cache *cache, dm_block_t b)
{
	int r;
	unsigned long flags;

	spin_lock_irqsave(&cache->lock, flags);
	r = test_bit(b, cache->discard_bitset);
	spin_unlock_irqrestore(&cache->lock, flags);

	return r;
}

/*----------------------------------------------------------------
 * Remapping
 *--------------------------------------------------------------*/
static void remap_to_origin(struct cache_c *c, struct bio *bio)
{
	bio->bi_bdev = c->origin_dev->bdev;
}

static void remap_to_cache(struct cache_c *c, struct bio *bio,
			   dm_block_t cblock)
{
	struct cache *cache = c->cache;

	bio->bi_bdev = c->cache_dev->bdev;
	bio->bi_sector = (cblock << cache->block_shift) + (bio->bi_sector & cache->offset_mask);
}

static void check_if_tick_bio_needed(struct cache_c *c, struct bio *bio)
{
	unsigned long flags;
	struct dm_cache_endio_hook *h = dm_get_mapinfo(bio)->ptr;
	struct cache *cache = c->cache;

	spin_lock_irqsave(&cache->lock, flags);
	if (cache->need_tick_bio &&
	    !(bio->bi_rw & (REQ_FUA | REQ_FLUSH | REQ_DISCARD))) {
		h->tick = true;
		cache->need_tick_bio = false;
	}
	spin_unlock_irqrestore(&cache->lock, flags);
}

static void remap_to_origin_dirty(struct cache_c *c, struct bio *bio,
				  dm_block_t oblock)
{
	check_if_tick_bio_needed(c, bio);
	remap_to_origin(c, bio);
	if (bio_data_dir(bio) == WRITE)
		clear_discard(c->cache, oblock);
}

static void remap_to_cache_dirty(struct cache_c *c, struct bio *bio,
				 dm_block_t oblock, dm_block_t cblock)
{
	struct cache *cache = c->cache;

	remap_to_cache(c, bio, cblock);
	if (bio_data_dir(bio) == WRITE) {
		set_bit(cblock, cache->dirty_bitset);
		clear_discard(cache, oblock);
	}
}

static dm_block_t get_bio_block(struct cache_c *c, struct bio *bio)
{
	return bio->bi_sector >> c->cache->block_shift;
}

static int bio_triggers_commit(struct cache_c *c, struct bio *bio)
{
	return (bio->bi_rw & (REQ_FLUSH | REQ_FUA)) &&
		dm_cache_changed_this_transaction(c->cache->cmd);
}

static void issue(struct cache_c *c, struct bio *bio)
{
	struct cache *cache = c->cache;
	unsigned long flags;

	if (!bio_triggers_commit(c, bio)) {
		generic_make_request(bio);
		return;
	}

	/*
	 * Complete bio with an error if earlier I/O caused changes to
	 * the metadata that can't be committed e.g, due to I/O errors
	 * on the metadata device.
	 */
	if (dm_cache_aborted_changes(cache->cmd)) {
		bio_io_error(bio);
		return;
	}

	/*
	 * Batch together any bios that trigger commits and then issue a
	 * single commit for them in process_deferred_flush_bios().
	 */
	spin_lock_irqsave(&cache->lock, flags);
	bio_list_add(&cache->deferred_flush_bios, bio);
	spin_unlock_irqrestore(&cache->lock, flags);
}

/*----------------------------------------------------------------
 * Migration processing
 *
 * Migration covers moving data from the origin device to the cache, or
 * vice versa.
 *--------------------------------------------------------------*/
static int ensure_next_migration(struct cache *cache)
{
	if (cache->next_migration)
		return 0;

	cache->next_migration = mempool_alloc(cache->migration_pool, GFP_ATOMIC);
	return cache->next_migration ? 0 : -ENOMEM;
}

static struct dm_cache_migration *get_next_migration(struct cache *cache)
{
	struct dm_cache_migration *r = cache->next_migration;

	BUG_ON(!r);
	cache->next_migration = NULL;

	return r;
}

static void free_migration(struct cache *cache, struct dm_cache_migration *mg)
{
	mempool_free(mg, cache->migration_pool);
}

static void inc_nr_migrations(struct cache *cache)
{
	atomic_inc(&cache->nr_migrations);
	wake_up(&cache->migration_wait); /* FIXME: why is there a wakeup here? */
}

static void dec_nr_migrations(struct cache *cache)
{
	atomic_dec(&cache->nr_migrations);
	wake_up(&cache->migration_wait);
}

static void __cell_defer(struct cache *cache, struct dm_bio_prison_cell *cell,
			 bool holder)
{
	if (holder)
		dm_cell_release(cell, &cache->deferred_bios);
	else
		dm_cell_release_no_holder(cell, &cache->deferred_bios);
}

static void cell_defer(struct cache *cache, struct dm_bio_prison_cell *cell,
		       bool holder)
{
	unsigned long flags;

	spin_lock_irqsave(&cache->lock, flags);
	__cell_defer(cache, cell, holder);
	spin_unlock_irqrestore(&cache->lock, flags);

	wake_worker(cache);
}

static void cleanup_migration(struct cache *cache, struct dm_cache_migration *mg)
{
	free_migration(cache, mg);
	dec_nr_migrations(cache);
}

static void migration_failure(struct cache *cache, struct dm_cache_migration *mg)
{
	if (mg->demote) {
		DMWARN("demotion failed; couldn't copy block");
		policy_force_mapping(cache->policy, mg->new_oblock, mg->old_oblock);

		cell_defer(cache, mg->old_ocell, mg->promote ? 0 : 1);
		if (mg->promote)
			cell_defer(cache, mg->new_ocell, 1);
	} else {
		DMWARN("promotion failed; couldn't copy block");
		policy_remove_mapping(cache->policy, mg->new_oblock);
		cell_defer(cache, mg->new_ocell, 1);
	}

	cleanup_migration(cache, mg);
}

static void migration_success(struct cache *cache, struct dm_cache_migration *mg)
{
	unsigned long flags;

	if (mg->demote) {
		cell_defer(cache, mg->old_ocell, mg->promote ? 0 : 1);

		if (dm_cache_remove_mapping(cache->cmd, mg->cblock)) {
			DMWARN("demotion failed; couldn't update on disk metadata");
			policy_force_mapping(cache->policy, mg->new_oblock,
					     mg->old_oblock);
			if (mg->promote)
				cell_defer(cache, mg->new_ocell, 1);
			cleanup_migration(cache, mg);
			return;
		}

		if (mg->promote) {
			mg->demote = false;

			spin_lock_irqsave(&cache->lock, flags);
			list_add_tail(&mg->list, &cache->quiesced_migrations);
			spin_unlock_irqrestore(&cache->lock, flags);
		} else
			cleanup_migration(cache, mg);

	} else {
		cell_defer(cache, mg->new_ocell, 1);

		if (dm_cache_insert_mapping(cache->cmd, mg->cblock, mg->new_oblock)) {
			DMWARN_LIMIT("promotion failed; couldn't update on disk metadata");
			policy_remove_mapping(cache->policy, mg->new_oblock);
		}

		clear_bit(mg->cblock, cache->dirty_bitset);
		cleanup_migration(cache, mg);
	}
}

static void copy_complete(int read_err, unsigned long write_err, void *context)
{
	unsigned long flags;
	struct dm_cache_migration *mg = (struct dm_cache_migration *) context;
	struct cache *cache = mg->c->cache;

	if (read_err || write_err)
		mg->err = true;

	spin_lock_irqsave(&cache->lock, flags);
	list_add_tail(&mg->list, &cache->completed_migrations);
	spin_unlock_irqrestore(&cache->lock, flags);

	wake_worker(cache);
}

static void issue_copy_real(struct cache_c *c, struct dm_cache_migration *mg)
{
	struct cache *cache = c->cache;
	struct dm_io_region o_region, c_region;
	int r;

	o_region.bdev = c->origin_dev->bdev;
	o_region.count = cache->sectors_per_block;

	c_region.bdev = c->cache_dev->bdev;
	c_region.sector = mg->cblock * cache->sectors_per_block;
	c_region.count = cache->sectors_per_block;

	if (mg->demote) {
		/* demote */
		o_region.sector = mg->old_oblock * cache->sectors_per_block;
		r = dm_kcopyd_copy(cache->copier, &c_region, 1, &o_region, 0, copy_complete, mg);
	} else {
		/* promote */
		o_region.sector = mg->new_oblock * cache->sectors_per_block;
		r = dm_kcopyd_copy(cache->copier, &o_region, 1, &c_region, 0, copy_complete, mg);
	}

	if (r < 0)
		migration_failure(cache, mg);
}

static void avoid_copy(struct cache *cache, struct dm_cache_migration *mg)
{
	atomic_inc(&cache->copies_avoided);
	migration_success(cache, mg);
}

static void issue_copy(struct dm_cache_migration *mg)
{
	struct cache_c *c = mg->c;
	struct cache *cache = c->cache;
	bool avoid;

	if (mg->demote)
		avoid = !test_bit(mg->cblock, cache->dirty_bitset) ||
			is_discarded(cache, mg->old_oblock);
	else
		avoid = is_discarded(cache, mg->new_oblock);

	avoid ? avoid_copy(c->cache, mg) : issue_copy_real(c, mg);
}

static void complete_migration(struct dm_cache_migration *mg)
{
	struct cache *cache = mg->c->cache;

	if (mg->err)
		migration_failure(cache, mg);
	else
		migration_success(cache, mg);
}

static void process_migrations(struct cache *cache, struct list_head *head,
			       void (*fn)(struct dm_cache_migration *))
{
	unsigned long flags;
	struct list_head list;
	struct dm_cache_migration *mg, *tmp;

	INIT_LIST_HEAD(&list);
	spin_lock_irqsave(&cache->lock, flags);
	list_splice_init(head, &list);
	spin_unlock_irqrestore(&cache->lock, flags);

	list_for_each_entry_safe(mg, tmp, &list, list)
		fn(mg);
}

static void __queue_quiesced_migration(struct cache *cache,
				       struct dm_cache_migration *mg)
{
	list_add_tail(&mg->list, &cache->quiesced_migrations);
}

static void queue_quiesced_migration(struct cache *cache,
				     struct dm_cache_migration *mg)
{
	unsigned long flags;

	spin_lock_irqsave(&cache->lock, flags);
	__queue_quiesced_migration(cache, mg);
	spin_unlock_irqrestore(&cache->lock, flags);

	wake_worker(cache);
}

static void queue_quiesced_migrations(struct cache *cache, struct list_head *work)
{
	unsigned long flags;
	struct dm_cache_migration *mg, *tmp;

	spin_lock_irqsave(&cache->lock, flags);
	list_for_each_entry_safe(mg, tmp, work, list)
		__queue_quiesced_migration(cache, mg);
	spin_unlock_irqrestore(&cache->lock, flags);

	wake_worker(cache);
}

static void check_for_quiesced_migrations(struct cache *cache,
					  struct dm_cache_endio_hook *h)
{
	struct list_head work;

	if (!h->all_io_entry)
		return;

	INIT_LIST_HEAD(&work);
	if (h->all_io_entry)
		dm_deferred_entry_dec(h->all_io_entry, &work);

	if (!list_empty(&work))
		queue_quiesced_migrations(cache, &work);
}

static void quiesce_migration(struct cache *cache, struct dm_cache_migration *mg)
{
	if (!dm_deferred_set_add_work(cache->all_io_ds, &mg->list))
		queue_quiesced_migration(cache, mg);
}

static void promote(struct cache_c *c, dm_block_t oblock, dm_block_t cblock,
		    struct dm_bio_prison_cell *cell)
{
	struct cache *cache = c->cache;
	struct dm_cache_migration *mg = get_next_migration(cache);

	mg->err = false;
	mg->demote = false;
	mg->promote = true;
	mg->c = c;
	mg->new_oblock = oblock;
	mg->cblock = cblock;
	mg->old_ocell = NULL;
	mg->new_ocell = cell;
	mg->start_jiffies = jiffies;

	inc_nr_migrations(cache);
	quiesce_migration(cache, mg);
}

static void writeback_then_promote(struct cache_c *c,
				   dm_block_t old_oblock,
				   dm_block_t new_oblock,
				   dm_block_t cblock,
				   struct dm_bio_prison_cell *old_ocell,
				   struct dm_bio_prison_cell *new_ocell)
{
	struct cache *cache = c->cache;
	struct dm_cache_migration *mg = get_next_migration(cache);

	mg->err = false;
	mg->demote = true;
	mg->promote = true;
	mg->c = c;
	mg->old_oblock = old_oblock;
	mg->new_oblock = new_oblock;
	mg->cblock = cblock;
	mg->old_ocell = old_ocell;
	mg->new_ocell = new_ocell;
	mg->start_jiffies = jiffies;

	inc_nr_migrations(cache);
	quiesce_migration(cache, mg);
}

/*----------------------------------------------------------------
 * bio processing
 *--------------------------------------------------------------*/
static void defer_bio(struct cache_c *c, struct bio *bio)
{
	unsigned long flags;
	struct cache *cache = c->cache;	

	spin_lock_irqsave(&cache->lock, flags);
	bio_list_add(&cache->deferred_bios, bio);
	spin_unlock_irqrestore(&cache->lock, flags);

	wake_worker(cache);
}

static void process_flush(struct cache_c *c, struct bio *bio)
{
	struct dm_cache_endio_hook *h = dm_get_mapinfo(bio)->ptr;

	BUG_ON(bio->bi_size);
	if (h->req_nr == 0)
		remap_to_origin(c, bio);
	else
		remap_to_cache(c, bio, 0);

	issue(c, bio);
}

#if 0
static bool covers_block(struct cache_c *c, struct bio *bio)
{
	return !(bio->bi_sector & c->offset_mask) &&
		(bio->bi_size == (c->sectors_per_block << SECTOR_SHIFT));
}
#endif

/*
 * People generally discard large parts of a device, eg, the whole device
 * when formatting.  Splitting these large discards up into cache block
 * sized ios and then quiescing (always neccessary for discard) takes too
 * long.
 *
 * We keep it simple, and allow any size of discard to come in, and just
 * mark off blocks on the discard bitset.  No passdown occurs!
 *
 * To implement passdown we need to change the bio_prison such that a cell
 * can have a key that spans many blocks.  This change is planned for
 * thin-provisioning.
 */
static void process_discard(struct cache_c *c, struct bio *bio)
{
	struct cache *cache = c->cache;
	dm_block_t start_block = dm_div_up(bio->bi_sector, cache->sectors_per_block);
	dm_block_t end_block = bio->bi_sector + bio_sectors(bio);
	dm_block_t b;

	do_div(end_block, cache->sectors_per_block);

	for (b = start_block; b < end_block; b++)
		set_discard(cache, b);

	bio_endio(bio, 0);
}

static void process_bio(struct cache_c *c, struct bio *bio)
{
	int r;
	bool release_cell = true;
	struct cache *cache = c->cache;
	struct dm_cell_key key;	
	dm_block_t block = get_bio_block(c, bio);
	struct dm_bio_prison_cell *old_ocell, *new_ocell;
	struct policy_result lookup_result;
	struct dm_cache_endio_hook *h = dm_get_mapinfo(bio)->ptr;
	bool discarded_block = is_discarded(cache, block);
	bool can_migrate = true;

	/*
	 * Check to see if that block is currently migrating.
	 */
	build_key(block, &key);
	r = dm_bio_detain(cache->prison, &key, bio, &new_ocell);
	if (r > 0)
		return;

	policy_map(cache->policy, block, can_migrate, discarded_block,
		   bio, &lookup_result);
	switch (lookup_result.op) {
	case POLICY_HIT:
		atomic_inc(bio_data_dir(bio) == READ ? &cache->read_hit : &cache->write_hit);
		h->all_io_entry = dm_deferred_entry_inc(cache->all_io_ds);
		remap_to_cache_dirty(c, bio, block, lookup_result.cblock);
		issue(c, bio);
		break;

	case POLICY_MISS:
		atomic_inc(bio_data_dir(bio) == READ ? &cache->read_miss : &cache->write_miss);
		h->all_io_entry = dm_deferred_entry_inc(cache->all_io_ds);
		remap_to_origin_dirty(c, bio, block);
		issue(c, bio);
		break;

	case POLICY_NEW:
		atomic_inc(&cache->promotion);
		promote(c, block, lookup_result.cblock, new_ocell);
		release_cell = false;
		break;

	case POLICY_REPLACE:
		build_key(lookup_result.old_oblock, &key);
		r = dm_bio_detain(cache->prison, &key, bio, &old_ocell);
		if (r > 0) {
			/*
			 * We have to be careful to avoid lock inversion of
			 * the cells.  So we back off, and wait for the
			 * old_ocell to become free.
			 */
			policy_force_mapping(cache->policy, block,
					     lookup_result.old_oblock);
			atomic_inc(&cache->cache_cell_clash);
			break;
		}
		atomic_inc(&cache->demotion);
		atomic_inc(&cache->promotion);

		writeback_then_promote(c, lookup_result.old_oblock, block,
				       lookup_result.cblock,
				       old_ocell, new_ocell);
		release_cell = false;
		break;
	}

	if (release_cell)
		cell_defer(cache, new_ocell, 0);
}

static void process_bio_read_only(struct cache_c *c, struct bio *bio)
{
	/* FIXME: implement */
	bio_io_error(bio);
}

static void process_bio_fail(struct cache_c *c, struct bio *bio)
{
	bio_io_error(bio);
}

/*----------------------------------------------------------------*/

/* FIXME: factor out to common code to share with dm-thin */

static enum cache_mode get_cache_mode(struct cache *cache)
{
	return cache->cf.mode;
}

/* FIXME: could use different name, so as not to be confused with wb and wt */
static void set_cache_mode(struct cache *cache, enum cache_mode mode)
{
	int r;

	cache->cf.mode = mode;

	switch (mode) {
	case CM_FAIL:
		DMERR("switching cache to failure mode");
		cache->process_bio = process_bio_fail;
		cache->process_discard = process_bio_fail;
		cache->process_flush = process_bio_fail;
		cache->issue_copy = issue_copy; /* FIXME */
		cache->complete_migration = complete_migration; /* FIXME */
		break;

	case CM_READ_ONLY:
		DMERR("switching cache to read-only mode");
		r = dm_cache_abort_metadata(cache->cmd);
		if (r) {
			DMERR("aborting transaction failed");
			set_cache_mode(cache, CM_FAIL);
		} else {
			dm_cache_metadata_read_only(cache->cmd);
			cache->process_bio = process_bio_read_only;
			cache->process_discard = process_discard;
			cache->process_flush = process_bio_read_only;
			cache->issue_copy = issue_copy; /* FIXME */
			cache->complete_migration = complete_migration; /* FIXME */
		}
		break;

	case CM_WRITE:
		cache->process_bio = process_bio;
		cache->process_discard = process_discard;
		cache->process_flush = process_flush;
		cache->issue_copy = issue_copy;
		cache->complete_migration = complete_migration;
		break;
	}
}

static int commit(struct cache *cache, unsigned *sb_flags)
{
	int r;

	if (!sb_flags)
		r = dm_cache_commit(cache->cmd);
	else
		r = dm_cache_commit_with_flags(cache->cmd, sb_flags);
	if (r)
		DMERR("commit failed, error = %d", r);

	return r;
}

/*
 * A non-zero return indicates read_only or fail_io mode.
 * Many callers don't care about the return value.
 */
static int commit_or_fallback(struct cache *cache, unsigned *sb_flags)
{
	int r;

	if (get_cache_mode(cache) != CM_WRITE)
		return -EINVAL;

	r = commit(cache, sb_flags);
	if (r)
		set_cache_mode(cache, CM_READ_ONLY);

	return r;
}

/*----------------------------------------------------------------*/

static int need_commit_due_to_time(struct cache *cache)
{
	return jiffies < cache->last_commit_jiffies ||
	       jiffies > cache->last_commit_jiffies + COMMIT_PERIOD;
}

static void process_deferred_bios(struct cache *cache)
{
	unsigned long flags;
	struct bio *bio;
	struct bio_list bios;

	bio_list_init(&bios);

	spin_lock_irqsave(&cache->lock, flags);
	bio_list_merge(&bios, &cache->deferred_bios);
	bio_list_init(&cache->deferred_bios);
	spin_unlock_irqrestore(&cache->lock, flags);

	while ((bio = bio_list_pop(&bios))) {
		struct dm_cache_endio_hook *h = dm_get_mapinfo(bio)->ptr;
		struct cache_c *c = h->c;

		/*
		 * If we've got no free migration structs, and processing
		 * this bio might require one, we pause until there are some
		 * prepared mappings to process.
		 */
		if (ensure_next_migration(cache)) {
			spin_lock_irqsave(&cache->lock, flags);
			bio_list_merge(&cache->deferred_bios, &bios);
			spin_unlock_irqrestore(&cache->lock, flags);

			break;
		}

		if (bio->bi_rw & REQ_FLUSH)
			cache->process_flush(c, bio);

		else if (bio->bi_rw & REQ_DISCARD)
			cache->process_discard(c, bio);

		else
			cache->process_bio(c, bio);
	}
}

static void process_deferred_flush_bios(struct cache *cache)
{
	unsigned long flags;
	struct bio *bio;
	struct bio_list bios;

	/*
	 * If there are any deferred flush bios, we must commit
	 * the metadata before issuing them.
	 */
	bio_list_init(&bios);
	spin_lock_irqsave(&cache->lock, flags);
	bio_list_merge(&bios, &cache->deferred_flush_bios);
	bio_list_init(&cache->deferred_flush_bios);
	spin_unlock_irqrestore(&cache->lock, flags);

	if (bio_list_empty(&bios) && !need_commit_due_to_time(cache))
		return;

	if (commit_or_fallback(cache, NULL)) {
		while ((bio = bio_list_pop(&bios)))
			bio_io_error(bio);
		return;
	}
	cache->last_commit_jiffies = jiffies;

	while ((bio = bio_list_pop(&bios)))
		generic_make_request(bio);
}

/*----------------------------------------------------------------
 * Main worker loop
 *--------------------------------------------------------------*/
static void start_quiescing(struct cache *cache)
{
	unsigned long flags;

	spin_lock_irqsave(&cache->lock, flags);
	cache->quiescing = true;
	spin_unlock_irqrestore(&cache->lock, flags);
}

static void stop_quiescing(struct cache *cache)
{
	unsigned long flags;

	spin_lock_irqsave(&cache->lock, flags);
	cache->quiescing = false;
	spin_unlock_irqrestore(&cache->lock, flags);
}

static bool is_quiescing(struct cache *cache)
{
	int r;
	unsigned long flags;

	spin_lock_irqsave(&cache->lock, flags);
	r = cache->quiescing;
	spin_unlock_irqrestore(&cache->lock, flags);

	return r;
}

static void wait_for_migrations(struct cache *cache)
{
	wait_event(cache->migration_wait, atomic_read(&cache->nr_migrations) == 0);
}

static void stop_worker(struct cache *cache)
{
	cancel_delayed_work(&cache->waker);
	flush_workqueue(cache->wq);
}

static void requeue_deferred_io(struct cache *cache)
{
	struct bio *bio;
	struct bio_list bios;

	bio_list_init(&bios);
	bio_list_merge(&bios, &cache->deferred_bios);
	bio_list_init(&cache->deferred_bios);

	while ((bio = bio_list_pop(&bios)))
		bio_endio(bio, DM_ENDIO_REQUEUE);
}

static int more_work(struct cache *cache)
{
	if (is_quiescing(cache))
		return !list_empty(&cache->quiesced_migrations) ||
			!list_empty(&cache->completed_migrations);
	else
		return !bio_list_empty(&cache->deferred_bios) ||
			!bio_list_empty(&cache->deferred_flush_bios) ||
			!list_empty(&cache->quiesced_migrations) ||
			!list_empty(&cache->completed_migrations);
}

static void do_worker(struct work_struct *ws)
{
	struct cache *cache = container_of(ws, struct cache, worker);

	do {
		if (is_quiescing(cache)) {
			process_migrations(cache, &cache->quiesced_migrations, cache->issue_copy);
			process_migrations(cache, &cache->completed_migrations, cache->complete_migration);
		} else {
			process_deferred_bios(cache);
			process_migrations(cache, &cache->quiesced_migrations, cache->issue_copy);
			process_migrations(cache, &cache->completed_migrations, cache->complete_migration);
			process_deferred_flush_bios(cache);
		}

	} while (more_work(cache));
}

/*
 * We want to commit periodically so that not too much
 * unwritten metadata builds up.
 */
static void do_waker(struct work_struct *ws)
{
	struct cache *cache = container_of(to_delayed_work(ws), struct cache, waker);
	wake_worker(cache);
	queue_delayed_work(cache->wq, &cache->waker, COMMIT_PERIOD);
}

/*----------------------------------------------------------------
 * Binding of control targets to a cache object
 *--------------------------------------------------------------*/
static int bind_control_target(struct cache *cache, struct dm_target *ti)
{
	struct cache_c *c = ti->private;

	/*
	 * We want to make sure that degraded caches are never upgraded.
	 */
	enum cache_mode old_mode = cache->cf.mode;
	enum cache_mode new_mode = c->adjusted_cf.mode;

	if (old_mode > new_mode)
		new_mode = old_mode;

	cache->ti = ti;
	cache->cf = c->adjusted_cf;

	set_cache_mode(cache, new_mode);

	return 0;
}

static void unbind_control_target(struct cache *cache, struct dm_target *ti)
{
	if (cache->ti == ti)
		cache->ti = NULL;
}

/*----------------------------------------------------------------
 * Cache creation
 *--------------------------------------------------------------*/
/* Initialize cache features. */
static void cache_features_init(struct cache_features *cf)
{
	cf->mode = CM_WRITE;
}

static int load_mapping(void *context, dm_block_t oblock, dm_block_t cblock)
{
	struct cache *cache = context;
	return policy_load_mapping(cache->policy, oblock, cblock);
}

static void __cache_destroy(struct cache *cache)
{
	__cache_table_remove(cache);

	if (dm_cache_metadata_close(cache->cmd) < 0)
		DMWARN("%s: dm_cache_metadata_close() failed.", __func__);

	free_bitset(cache->dirty_bitset);
	free_bitset(cache->discard_bitset);

	dm_bio_prison_destroy(cache->prison);
	dm_kcopyd_client_destroy(cache->copier);

	if (cache->wq)
		destroy_workqueue(cache->wq);

	if (cache->next_migration)
		mempool_free(cache->next_migration, cache->migration_pool);
	mempool_destroy(cache->migration_pool);
	mempool_destroy(cache->endio_hook_pool);
	dm_deferred_set_destroy(cache->all_io_ds);
	dm_cache_policy_destroy(cache->policy);
	kfree(cache);
}

static struct kmem_cache *_migration_cache;
static struct kmem_cache *_endio_hook_cache;

static struct cache *cache_create(struct mapped_device *cache_md,
				  struct block_device *metadata_dev,
				  sector_t block_size, sector_t origin_sectors,
				  sector_t cache_sectors, const char *policy_name,
				  int read_only, char **error)
{
	int r;
	void *err_p = ERR_PTR(-ENOMEM);
	struct cache *cache;
	struct dm_cache_metadata *cmd;
	bool format_device = read_only ? false : true;

	cmd = dm_cache_metadata_open(metadata_dev, block_size, format_device);
	if (IS_ERR(cmd)) {
		*error = "Error creating metadata object";
		return (struct cache *)cmd;
	}

	cache = kmalloc(sizeof(*cache), GFP_KERNEL);
	if (!cache) {
		*error = "Error allocating memory for cache";
		goto bad_cache;
	}

	cache->cmd = cmd;
	cache_features_init(&cache->cf);

	cache->origin_blocks = origin_sectors;
	do_div(cache->origin_blocks, block_size);
	cache->sectors_per_block = block_size;
	cache->offset_mask = block_size - 1;
	cache->block_shift = ffs(block_size) - 1;
	cache->cache_size = cache_sectors >> cache->block_shift;

	spin_lock_init(&cache->lock);
	bio_list_init(&cache->deferred_bios);
	bio_list_init(&cache->deferred_flush_bios);
	INIT_LIST_HEAD(&cache->quiesced_migrations);
	INIT_LIST_HEAD(&cache->completed_migrations);
	atomic_set(&cache->nr_migrations, 0);
	init_waitqueue_head(&cache->migration_wait);

	r = dm_cache_resize(cmd, cache->cache_size);
	if (r) {
		*error = "Couldn't resize cache metadata";
		err_p = ERR_PTR(r);
		goto bad_alloc_dirty_bitset;
	}

	cache->dirty_bitset = alloc_and_set_bitset(cache->cache_size);
	if (!cache->dirty_bitset) {
		*error = "Couldn't allocate dirty_bitset";
		goto bad_alloc_dirty_bitset;
	}

	cache->discard_bitset = alloc_and_set_bitset(cache->origin_blocks);
	if (!cache->discard_bitset) {
		*error = "Couldn't allocate discard bitset";
		goto bad_alloc_discard_bitset;
	}

	cache->copier = dm_kcopyd_client_create();
	if (IS_ERR(cache->copier)) {
		*error = "Couldn't create kcopyd client";
		err_p = ERR_PTR(r);
		goto bad_kcopyd_client;
	}

	cache->wq = alloc_ordered_workqueue(DAEMON, WQ_MEM_RECLAIM);
	if (!cache->wq) {
		*error = "Couldn't create workqueue for metadata object";
		goto bad_wq;
	}
	INIT_WORK(&cache->worker, do_worker);
	INIT_DELAYED_WORK(&cache->waker, do_waker);

	cache->prison = dm_bio_prison_create(PRISON_CELLS);
	if (!cache->prison) {
		*error = "Couldn't create bio prison";
		goto bad_prison;
	}

	cache->all_io_ds = dm_deferred_set_create();
	if (!cache->all_io_ds) {
		*error = "Couldn't create all_io deferred set";
		goto bad_deferred_set;
	}

	cache->endio_hook_pool = mempool_create_slab_pool(ENDIO_HOOK_POOL_SIZE,
							  _endio_hook_cache);
	if (!cache->endio_hook_pool) {
		*error = "Error creating cache's endio_hook mempool";
		goto bad_endio_hook_pool;
	}

	cache->migration_pool = mempool_create_slab_pool(MIGRATION_POOL_SIZE,
							 _migration_cache);
	if (!cache->migration_pool) {
		*error = "Error creating cache's endio_hook mempool";
		goto bad_migration_pool;
	}

	cache->policy = dm_cache_policy_create(policy_name, cache->cache_size,
					       cache->origin_sectors, block_size);
	if (!cache->policy) {
		*error = "Error creating cache's policy";
		goto bad_cache_policy;
	}

	atomic_set(&cache->read_hit, 0);
	atomic_set(&cache->read_miss, 0);
	atomic_set(&cache->write_hit, 0);
	atomic_set(&cache->write_miss, 0);
	atomic_set(&cache->demotion, 0);
	atomic_set(&cache->promotion, 0);
	atomic_set(&cache->copies_avoided, 0);
	atomic_set(&cache->cache_cell_clash, 0);

	r = dm_cache_load_mappings(cache->cmd, load_mapping, cache);
	if (r) {
		*error = "Couldn't load cache mappings";
		goto bad_load_mappings;
	}

	cache->ref_count = 1;
	cache->quiescing = false;
	cache->last_commit_jiffies = jiffies;
	cache->cache_md = cache_md;
	cache->md_dev = metadata_dev;
	__cache_table_insert(cache);

	return cache;

bad_load_mappings:
	dm_cache_policy_destroy(cache->policy);
bad_cache_policy:
	mempool_destroy(cache->migration_pool);
bad_migration_pool:
	mempool_destroy(cache->endio_hook_pool);
bad_endio_hook_pool:
	dm_deferred_set_destroy(cache->all_io_ds);
bad_deferred_set:
	dm_bio_prison_destroy(cache->prison);
bad_prison:
	destroy_workqueue(cache->wq);
bad_wq:
	dm_kcopyd_client_destroy(cache->copier);
bad_kcopyd_client:
	free_bitset(cache->discard_bitset);
bad_alloc_discard_bitset:
	free_bitset(cache->dirty_bitset);
bad_alloc_dirty_bitset:
	kfree(cache);
bad_cache:
	if (dm_cache_metadata_close(cmd))
		DMWARN("%s: dm_cache_metadata_close() failed.", __func__);

	return err_p;
}

static void __cache_inc(struct cache *cache)
{
	BUG_ON(!mutex_is_locked(&dm_cache_table.mutex));
	cache->ref_count++;
}

static void __cache_dec(struct cache *cache)
{
	BUG_ON(!mutex_is_locked(&dm_cache_table.mutex));
	BUG_ON(!cache->ref_count);
	if (!--cache->ref_count)
		__cache_destroy(cache);
}

static struct cache *__cache_find(struct mapped_device *cache_md,
				  struct block_device *metadata_dev,
				  sector_t block_size, sector_t origin_sectors,
				  sector_t cache_sectors, const char *policy_name,
				  int read_only, char **error, int *created)
{
	struct cache *cache = __cache_table_lookup_metadata_dev(metadata_dev);

	if (cache) {
		if (cache->cache_md != cache_md) {
			*error = "metadata device already in use by a cache";
			return ERR_PTR(-EBUSY);
		}
		__cache_inc(cache);

	} else {
		cache = __cache_table_lookup(cache_md);
		if (cache) {
			if (cache->md_dev != metadata_dev) {
				*error = "different cache cannot replace a cache";
				return ERR_PTR(-EINVAL);
			}
			__cache_inc(cache);

		} else {
			cache = cache_create(cache_md, metadata_dev, block_size,
					     origin_sectors, cache_sectors,
					     policy_name, read_only, error);
			*created = 1;
		}
	}

	return cache;
}

/*----------------------------------------------------------------
 * Target methods
 *--------------------------------------------------------------*/
static void cache_dtr(struct dm_target *ti)
{
	struct cache_c *c = ti->private;
	struct cache *cache = c->cache;

	pr_alert("dm-cache statistics:\n");
	pr_alert("read hits:\t%u\n", (unsigned) atomic_read(&cache->read_hit));
	pr_alert("read misses:\t%u\n", (unsigned) atomic_read(&cache->read_miss));
	pr_alert("write hits:\t%u\n", (unsigned) atomic_read(&cache->write_hit));
	pr_alert("write misses:\t%u\n", (unsigned) atomic_read(&cache->write_miss));
	pr_alert("demotions:\t%u\n", (unsigned) atomic_read(&cache->demotion));
	pr_alert("promotions:\t%u\n", (unsigned) atomic_read(&cache->promotion));
	pr_alert("copies avoided:\t%u\n", (unsigned) atomic_read(&cache->copies_avoided));
	pr_alert("cache cell clashs:\t%u\n", (unsigned) atomic_read(&cache->cache_cell_clash));

	mutex_lock(&dm_cache_table.mutex);

	unbind_control_target(cache, ti);
	__cache_dec(cache);
	dm_put_device(ti, c->metadata_dev);
	dm_put_device(ti, c->origin_dev);
	dm_put_device(ti, c->cache_dev);
	kfree(c);

	mutex_unlock(&dm_cache_table.mutex);
}

static sector_t get_dev_size(struct dm_dev *dev)
{
	return i_size_read(dev->bdev->bd_inode) >> SECTOR_SHIFT;
}

static int is_congested(struct dm_dev *dev, int bdi_bits)
{
	struct request_queue *q = bdev_get_queue(dev->bdev);
	return bdi_congested(&q->backing_dev_info, bdi_bits);
}

static int cache_is_congested(struct dm_target_callbacks *cb, int bdi_bits)
{
	struct cache_c *c = container_of(cb, struct cache_c, callbacks);

	return is_congested(c->origin_dev, bdi_bits) ||
		is_congested(c->cache_dev, bdi_bits);
}

static int parse_cache_features(struct dm_arg_set *as, struct cache_features *cf,
				struct dm_target *ti)
{
	int r;
	unsigned argc;
	const char *arg_name;

	static struct dm_arg _args[] = {
		{0, 1, "Invalid number of cache feature arguments"},
	};

	/*
	 * No feature arguments supplied.
	 */
	if (!as->argc)
		return 0;

	r = dm_read_arg_group(_args, as, &argc, &ti->error);
	if (r)
		return -EINVAL;

	while (argc && !r) {
		arg_name = dm_shift_arg(as);
		argc--;

		if (!strcasecmp(arg_name, "read_only"))
			cf->mode = CM_READ_ONLY;

		else {
			ti->error = "Unrecognised cache feature requested";
			r = -EINVAL;
			break;
		}
	}

	return r;
}

/*
 * Construct a hierarchical storage device mapping:
 *
 * cache <metadata dev> <origin dev> <cache dev> <block size> <policy>
 *	 [<#feature args> [<arg>]*]
 *
 * metadata dev	   : fast device holding the persistent metadata
 * origin dev	   : slow device holding original data blocks
 * cache dev	   : fast device holding cached data blocks
 * block size	   : cache unit size in sectors
 * policy	   : the replacement policy to use
 *
 * Optional feature arguments are:
 *	 read_only : don't allow any changes to the cache
 */
static int cache_ctr(struct dm_target *ti, unsigned argc, char **argv)
{
	int r, cache_created = 0;
	struct cache_c *c;
	struct cache *cache;
	struct cache_features cf;
	struct dm_arg_set as;
	sector_t block_size, origin_sectors, cache_sectors;
	struct dm_dev *metadata_dev, *origin_dev, *cache_dev;
	const char *policy_name;

	/*
	 * FIXME Remove validation from scope of lock.
	 */
	mutex_lock(&dm_cache_table.mutex);

	if (argc < 5) {
		ti->error = "Invalid argument count";
		r = -EINVAL;
		goto bad_unlock;
	}
	as.argc = argc;
	as.argv = argv;

	if (kstrtoul(argv[3], 10, &block_size) || !block_size ||
	    block_size < BLOCK_SIZE_MIN ||
	    !is_power_of_2(block_size)) {
		ti->error = "Invalid data block size";
		r = -EINVAL;
		goto bad_unlock;
	}

	policy_name = argv[4];

	r = dm_get_device(ti, argv[0], FMODE_READ | FMODE_WRITE, &metadata_dev);
	if (r) {
		ti->error = "Error opening metadata device";
		goto bad_unlock;
	}

	r = dm_get_device(ti, argv[1], FMODE_READ | FMODE_WRITE, &origin_dev);
	if (r) {
		ti->error = "Error opening origin device";
		goto bad_origin;
	}

	r = dm_get_device(ti, argv[2], FMODE_READ | FMODE_WRITE, &cache_dev);
	if (r) {
		ti->error = "Error opening cache device";
		goto bad_cache;
	}
	cache_sectors = get_dev_size(cache_dev);

	origin_sectors = get_dev_size(origin_dev);
	if (ti->len > origin_sectors) {
		ti->error = "Device size larger than cached device";
		goto bad;
	}

	/*
	 * Set default cache features.
	 */
	cache_features_init(&cf);

	dm_consume_args(&as, 5);
	r = parse_cache_features(&as, &cf, ti);
	if (r)
		goto bad;

	c = kzalloc(sizeof(*c), GFP_KERNEL);
	if (!c) {
		ti->error = "Error allocating cache context";
		r = -ENOMEM;
		goto bad;
	}

	cache = __cache_find(dm_table_get_md(ti->table), metadata_dev->bdev,
			     block_size, origin_sectors, cache_sectors, policy_name,
			     cf.mode == CM_READ_ONLY, &ti->error, &cache_created);
	if (IS_ERR(cache)) {
		r = PTR_ERR(cache);
		goto bad_free_c;
	}

	if (dm_set_target_max_io_len(ti, cache->sectors_per_block))
		goto bad_max_io_len;

	c->cache = cache;
	c->ti = ti;
	c->metadata_dev = metadata_dev;
	c->origin_dev = origin_dev;
	c->cache_dev = cache_dev;
	c->adjusted_cf = c->requested_cf = cf;
	ti->num_flush_requests = 2;
	ti->flush_supported = true;

	/* FIXME: make discard support configurable */
	ti->num_discard_requests = 1;
	ti->discards_supported = true;

	ti->private = c;

	c->callbacks.congested_fn = cache_is_congested;
	dm_table_add_target_callbacks(ti->table, &c->callbacks);

	mutex_unlock(&dm_cache_table.mutex);

	return 0;

bad_max_io_len:
	__cache_dec(c->cache);
bad_free_c:
	kfree(c);
bad:
	dm_put_device(ti, cache_dev);
bad_cache:
	dm_put_device(ti, origin_dev);
bad_origin:
	dm_put_device(ti, metadata_dev);
bad_unlock:
	mutex_unlock(&dm_cache_table.mutex);

	return r;
}

static struct dm_cache_endio_hook *cache_hook_bio(struct cache_c *c, struct bio *bio,
						  unsigned req_nr)
{
	struct cache *cache = c->cache;
	struct dm_cache_endio_hook *h = mempool_alloc(cache->endio_hook_pool, GFP_NOIO);

	h->c = c;
	h->all_io_entry = NULL;
	h->tick = false;
	h->req_nr = req_nr;

	return h;
}

static int cache_map(struct dm_target *ti, struct bio *bio,
		     union map_info *map_context)
{
	struct cache_c *c = ti->private;
	struct cache *cache = c->cache;

	int r;
	struct dm_cell_key key;
	dm_block_t block = get_bio_block(c, bio);
	bool can_migrate = false;
	bool discarded_block;
	struct dm_bio_prison_cell *cell;
	struct policy_result lookup_result;
	struct dm_cache_endio_hook *h;

	if (block > cache->origin_blocks) {
		/*
		 * This can only occur if the io goes to a partial block at
		 * the end of the origin device.  We don't cache these.
		 * Just remap to the origin and carry on.
		 */
		remap_to_origin(c, bio);
		return DM_MAPIO_REMAPPED;
	}

	h = map_context->ptr = cache_hook_bio(c, bio, map_context->target_request_nr);

	if (bio->bi_rw & (REQ_FLUSH | REQ_FUA | REQ_DISCARD)) {
		defer_bio(c, bio);
		return DM_MAPIO_SUBMITTED;
	}

	/*
	 * Check to see if that block is currently migrating.
	 */
	build_key(block, &key);
	r = dm_bio_detain(cache->prison, &key, bio, &cell);
	if (r > 0)
		return DM_MAPIO_SUBMITTED;

	discarded_block = is_discarded(cache, block);

	r = policy_map(cache->policy, block, can_migrate, discarded_block,
		       bio, &lookup_result);
	if (r == -EWOULDBLOCK) {
		cell_defer(cache, cell, true);
		return DM_MAPIO_SUBMITTED;
	}

	if (r)
		BUG();

	switch (lookup_result.op) {
	case POLICY_HIT:
		atomic_inc(bio_data_dir(bio) == READ ? &cache->read_hit : &cache->write_hit);
		h->all_io_entry = dm_deferred_entry_inc(cache->all_io_ds);
		remap_to_cache_dirty(c, bio, block, lookup_result.cblock);
		cell_defer(cache, cell, false);
		break;

	case POLICY_MISS:
		atomic_inc(bio_data_dir(bio) == READ ? &cache->read_miss : &cache->write_miss);
		h->all_io_entry = dm_deferred_entry_inc(cache->all_io_ds);
		remap_to_origin_dirty(c, bio, block);
		cell_defer(cache, cell, false);
		break;

	default:
		pr_alert("illegal value: %u\n", (unsigned) lookup_result.op);
		BUG();
	}

	return DM_MAPIO_REMAPPED;
}

static int cache_end_io(struct dm_target *ti, struct bio *bio,
			int error, union map_info *info)
{
	struct cache_c *c = ti->private;
	unsigned long flags;
	struct dm_cache_endio_hook *h = info->ptr;
	struct cache *cache = c->cache;

	if (h->tick) {
		policy_tick(cache->policy);

		spin_lock_irqsave(&cache->lock, flags);
		cache->need_tick_bio = true;
		spin_unlock_irqrestore(&cache->lock, flags);
	}

	check_for_quiesced_migrations(cache, h);
	mempool_free(h, cache->endio_hook_pool);
	return 0;
}

static void cache_postsuspend(struct dm_target *ti)
{
	int r;
	unsigned sb_flags;
	struct cache_c *c = ti->private;
	struct cache *cache = c->cache;

	if (get_cache_mode(cache) == CM_WRITE) {
		r = dm_cache_read_superblock_flags(cache->cmd, &sb_flags);
		if (r) {
			DMERR("could not read superblock flags during suspend");
			return;
		}

		sb_flags &= ~CACHE_DIRTY;
		r = commit_or_fallback(cache, &sb_flags);
		if (r)
			DMERR("could not clear dirty flag in metadata superblock");
	}

	start_quiescing(cache);
	wait_for_migrations(cache);
	stop_worker(cache);
	requeue_deferred_io(cache);
	stop_quiescing(cache);
}

static int cache_preresume(struct dm_target *ti)
{
	int r;
	struct cache_c *c = ti->private;
	struct cache *cache = c->cache;

	/*
	 * Take control of the cache object.
	 */
	r = bind_control_target(cache, ti);
	if (r)
		return r;

	return 0;
}

static void cache_resume(struct dm_target *ti)
{
	int r;
	unsigned sb_flags;
	struct cache_c *c = ti->private;
	struct cache *cache = c->cache;

	cache->need_tick_bio = true;
	do_waker(&cache->waker.work);

	r = dm_cache_read_superblock_flags(cache->cmd, &sb_flags);
	if (r) {
		DMERR("could not read superblock flags during resume");
		return;
	}
	if (sb_flags & CACHE_DIRTY)
		/* FIXME: perform cache policy recovery */
		DMERR("cache metadata was not written cleanly during previous shutdown");

	if (get_cache_mode(cache) == CM_WRITE) {
		sb_flags &= CACHE_DIRTY;
		r = commit_or_fallback(cache, &sb_flags);
		if (r)
			DMERR("could not set dirty flag in metadata superblock");
	}
}

static void emit_flags(struct cache_features *cf, char *result,
		       unsigned sz, unsigned maxlen)
{
	unsigned count = (cf->mode == CM_READ_ONLY);
	DMEMIT("%u ", count);

	if (cf->mode == CM_READ_ONLY)
		DMEMIT("read_only ");
}

static int cache_status(struct dm_target *ti, status_type_t type,
			unsigned status_flags, char *result, unsigned maxlen)
{
	int r;
	ssize_t sz = 0;
	dm_block_t nr_free_blocks_metadata;
	dm_block_t nr_blocks_metadata;
	char buf[BDEVNAME_SIZE];
	struct cache_c *c = ti->private;
	struct cache *cache = c->cache;
	dm_block_t residency;

	switch (type) {
	case STATUSTYPE_INFO:
		/* Commit to ensure statistics aren't out-of-date */
		if (!(status_flags & DM_STATUS_NOFLUSH_FLAG) && !dm_suspended(ti))
			(void) commit_or_fallback(cache, NULL);

		r = dm_cache_get_free_metadata_block_count(cache->cmd,
							   &nr_free_blocks_metadata);
		if (r)
			return r;

		r = dm_cache_get_metadata_dev_size(cache->cmd, &nr_blocks_metadata);
		if (r)
			return r;

		residency = policy_residency(cache->policy);

		DMEMIT("%llu/%llu %u %u %u %u %u %u %llu %u ",
		       (unsigned long long)(nr_blocks_metadata - nr_free_blocks_metadata),
		       (unsigned long long)nr_blocks_metadata,
		       (unsigned) atomic_read(&cache->read_hit),
		       (unsigned) atomic_read(&cache->read_miss),
		       (unsigned) atomic_read(&cache->write_hit),
		       (unsigned) atomic_read(&cache->write_miss),
		       (unsigned) atomic_read(&cache->demotion),
		       (unsigned) atomic_read(&cache->promotion),
		       (unsigned long long) residency,
		       (unsigned) atomic_read(&cache->cache_cell_clash));

		if (cache->cf.mode == CM_READ_ONLY)
			DMEMIT("ro ");
		else
			DMEMIT("rw ");

		break;

	case STATUSTYPE_TABLE:
		format_dev_t(buf, c->metadata_dev->bdev->bd_dev);
		DMEMIT("%s ", buf);
		format_dev_t(buf, c->origin_dev->bdev->bd_dev);
		DMEMIT("%s ", buf);
		format_dev_t(buf, c->cache_dev->bdev->bd_dev);
		DMEMIT("%s ", buf);
		DMEMIT("%llu ", (unsigned long long) cache->sectors_per_block);
		DMEMIT("%s ", dm_cache_policy_get_name(cache->policy));
		emit_flags(&c->requested_cf, result, sz, maxlen);
		break;
	}

	return 0;
}

static int cache_iterate_devices(struct dm_target *ti,
				 iterate_devices_callout_fn fn, void *data)
{
	int r = 0;
	struct cache_c *c = ti->private;

	r = fn(ti, c->cache_dev, 0, get_dev_size(c->cache_dev), data);
	if (!r)
		r = fn(ti, c->origin_dev, 0, ti->len, data);

	return r;
}

static int cache_bvec_merge(struct dm_target *ti,
			  struct bvec_merge_data *bvm,
			  struct bio_vec *biovec, int max_size)
{
	struct cache_c *c = ti->private;
	struct request_queue *q = bdev_get_queue(c->origin_dev->bdev);

	if (!q->merge_bvec_fn)
		return max_size;

	bvm->bi_bdev = c->origin_dev->bdev;
	return min(max_size, q->merge_bvec_fn(q, bvm, biovec));
}

/* FIXME: fix discard settings like was done for thinp */
static void set_discard_limits(struct cache_c *c, struct queue_limits *limits)
{
	struct cache *cache = c->cache;

	/*
	 * FIXME: these limits may be incompatible with the cache's data device
	 */
	limits->max_discard_sectors = cache->sectors_per_block * 1024;

	/*
	 * This is just a hint, and not enforced.  We have to cope with
	 * bios that cover a block partially.  A discard that spans a block
	 * boundary is not sent to this target.
	 */
	limits->discard_granularity = cache->sectors_per_block << SECTOR_SHIFT;
	limits->discard_zeroes_data = 0;
}

static void cache_io_hints(struct dm_target *ti, struct queue_limits *limits)
{
	struct cache_c *c = ti->private;
	struct cache *cache = c->cache;

	blk_limits_io_min(limits, 0);
	blk_limits_io_opt(limits, cache->sectors_per_block << SECTOR_SHIFT);
	set_discard_limits(c, limits);
}

/*----------------------------------------------------------------*/

static struct target_type cache_target = {
	.name = "cache",
	.version = {1, 0, 0},
	.module = THIS_MODULE,
	.ctr = cache_ctr,
	.dtr = cache_dtr,
	.map = cache_map,
	.end_io = cache_end_io,
	.postsuspend = cache_postsuspend,
	.preresume = cache_preresume,
	.resume = cache_resume,
	.status = cache_status,
	.iterate_devices = cache_iterate_devices,
	.merge = cache_bvec_merge,
	.io_hints = cache_io_hints,
};

static int __init dm_cache_init(void)
{
	int r;

	cache_table_init();

	r = dm_register_target(&cache_target);
	if (r)
		return r;

	r = -ENOMEM;

	_migration_cache = KMEM_CACHE(dm_cache_migration, 0);
	if (!_migration_cache)
		goto bad_migration_cache;

	_endio_hook_cache = KMEM_CACHE(dm_cache_endio_hook, 0);
	if (!_endio_hook_cache)
		goto bad_endio_hook_cache;

	return 0;

bad_endio_hook_cache:
	kmem_cache_destroy(_migration_cache);
bad_migration_cache:
	dm_unregister_target(&cache_target);

	return r;
}

static void dm_cache_exit(void)
{
	dm_unregister_target(&cache_target);

	kmem_cache_destroy(_migration_cache);
	kmem_cache_destroy(_endio_hook_cache);
}

module_init(dm_cache_init);
module_exit(dm_cache_exit);

MODULE_DESCRIPTION(DM_NAME " cache target");
MODULE_AUTHOR("Joe Thornber <ejt@redhat.com>");
MODULE_LICENSE("GPL");
