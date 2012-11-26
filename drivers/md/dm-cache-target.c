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

/*----------------------------------------------------------------*/

/*
 * Glossary:
 *
 * oblock: index of an origin block
 * cblock: index of a cache block
 * promotion: movement of a block from origin to cache
 * demotion: movement of a block from cache to origin
 * migration: movement of a block between the origin and cache device,
 *	      either direction
 */

/*----------------------------------------------------------------*/

static size_t bitset_size_in_bytes(unsigned nr_entries)
{
	return sizeof(unsigned long) * dm_div_up(nr_entries, BITS_PER_LONG);
}

static unsigned long *alloc_bitset(unsigned nr_entries)
{
	size_t s = bitset_size_in_bytes(nr_entries);
	return vzalloc(s);
}

static void clear_bitset(void *bitset, unsigned nr_entries)
{
	size_t s = bitset_size_in_bytes(nr_entries);
	memset(bitset, 0, s);
}

static void free_bitset(unsigned long *bits)
{
	vfree(bits);
}

/*----------------------------------------------------------------*/

#define PRISON_CELLS 1024
#define MIGRATION_POOL_SIZE 128
#define COMMIT_PERIOD HZ
#define MIGRATION_COUNT_WINDOW 10

/*
 * The block size of the device holding cache data must be >= 32KB
 */
#define DATA_DEV_BLOCK_SIZE_MIN_SECTORS (32 * 1024 >> SECTOR_SHIFT)

/*
 * FIXME: the cache is read/write for the time being.
 */
enum cache_mode {
	CM_WRITE,		/* metadata may be changed */
	CM_READ_ONLY,		/* metadata may not be changed */
};

struct cache_features {
	enum cache_mode mode;
	bool write_through:1;
};

struct cache {
	struct dm_target *ti;
	struct dm_target_callbacks callbacks;

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

	/*
	 * Cache features such as write-through.
	 */
	struct cache_features features;

	/*
	 * Size of the origin device in _complete_ blocks and native sectors.
	 */
	dm_oblock_t origin_blocks;
	sector_t origin_sectors;

	/*
	 * Size of the cache device in blocks.
	 */
	dm_cblock_t cache_size;

	/*
	 * Fields for converting from sectors to blocks.
	 */
	sector_t sectors_per_block;
	int sectors_per_block_shift;

	struct dm_cache_metadata *cmd;

	spinlock_t lock;
	struct bio_list deferred_bios;
	struct bio_list deferred_flush_bios;
	struct list_head quiesced_migrations;
	struct list_head completed_migrations;
	struct list_head need_commit_migrations;
	sector_t migration_threshold;
	atomic_t nr_migrations;
	wait_queue_head_t migration_wait;

	/*
	 * cache_size entries, dirty if set
	 */
	dm_cblock_t nr_dirty;
	unsigned long *dirty_bitset;

	/*
	 * origin_blocks entries, discarded if set.
	 */
	sector_t discard_block_size; /* a power of 2 times sectors per block */
	dm_dblock_t discard_nr_blocks;
	unsigned long *discard_bitset;

	struct dm_kcopyd_client *copier;
	struct workqueue_struct *wq;
	struct work_struct worker;

	struct delayed_work waker;
	unsigned long last_commit_jiffies;

	struct dm_bio_prison *prison;
	struct dm_deferred_set *all_io_ds;

	mempool_t *migration_pool;
	struct dm_cache_migration *next_migration;

	struct dm_cache_policy *policy;
	unsigned policy_nr_args;

	bool need_tick_bio:1;
	bool sized:1;
	bool quiescing:1;
	bool commit_requested:1;
	bool loaded_mappings:1;
	bool loaded_discards:1;

	atomic_t read_hit;
	atomic_t read_miss;
	atomic_t write_hit;
	atomic_t write_miss;
	atomic_t demotion;
	atomic_t promotion;
	atomic_t copies_avoided;
	atomic_t cache_cell_clash;
	atomic_t commit_count;
};

struct dm_cache_endio_hook {
	bool tick:1;
	unsigned req_nr:2;
	struct dm_deferred_entry *all_io_entry;
};

struct dm_cache_migration {
	struct list_head list;
	struct cache *cache;

	unsigned long start_jiffies;
	dm_oblock_t old_oblock;
	dm_oblock_t new_oblock;
	dm_cblock_t cblock;

	bool err:1;
	bool writeback:1;
	bool demote:1;
	bool promote:1;

	struct dm_bio_prison_cell *old_ocell;
	struct dm_bio_prison_cell *new_ocell;
};

/*
 * Processing a bio in the worker thread may require these memory
 * allocations.  We prealloc to avoid deadlocks (the same worker thread
 * frees them back to the mempool).
 */
struct prealloc {
	struct dm_cache_migration *mg;
	struct dm_bio_prison_cell *cell1;
	struct dm_bio_prison_cell *cell2;
};

static void wake_worker(struct cache *cache)
{
	queue_work(cache->wq, &cache->worker);
}

/*----------------------------------------------------------------*/

static int prealloc_data_structs(struct cache *cache, struct prealloc *p)
{
	// FIXME: given we're doing this, can we get rid of the mempools?

	if (!p->mg) {
		p->mg = mempool_alloc(cache->migration_pool, GFP_ATOMIC);
		if (!p->mg)
			return -ENOMEM;
	}

	if (!p->cell1) {
		p->cell1 = dm_bio_prison_alloc_cell(cache->prison, GFP_ATOMIC);
		if (!p->cell1)
			return -ENOMEM;
	}

	if (!p->cell2) {
		p->cell2 = dm_bio_prison_alloc_cell(cache->prison, GFP_ATOMIC);
		if (!p->cell2)
			return -ENOMEM;
	}

	return 0;
}

static void prealloc_free_structs(struct cache *cache, struct prealloc *p)
{
	if (p->cell2)
		dm_bio_prison_free_cell(cache->prison, p->cell2);

	if (p->cell1)
		dm_bio_prison_free_cell(cache->prison, p->cell1);

	if (p->mg)
		mempool_free(p->mg, cache->migration_pool);
}

static struct dm_cache_migration *prealloc_get_migration(struct prealloc *p)
{
	struct dm_cache_migration *mg = p->mg;

	BUG_ON(!mg);
	p->mg = NULL;

	return mg;
}

static struct dm_bio_prison_cell **prealloc_get_cell(struct prealloc *p)
{
	if (p->cell1)
		return &p->cell1;

	if (p->cell2)
		return &p->cell2;

	BUG();
	return NULL;
}

/*----------------------------------------------------------------*/

static void build_key(dm_oblock_t oblock, struct dm_cell_key *key)
{
	key->virtual = 0;
	key->dev = 0;
	key->block = from_oblock(oblock);
}

// FIXME: refactor these three
static int bio_detain_(struct cache *cache,
		       dm_oblock_t oblock, struct bio *bio,
		       gfp_t gfp,
		       struct dm_bio_prison_cell **result)
{
	int r;
	struct dm_cell_key key;
	struct dm_bio_prison_cell *cell;

	cell = dm_bio_prison_alloc_cell(cache->prison, gfp);

	build_key(oblock, &key);
	r = dm_bio_detain(cache->prison, &key, bio, cell, result);

	if (r)
		dm_bio_prison_free_cell(cache->prison, cell);

	return r;
}

static int bio_detain(struct cache *cache, struct prealloc *structs,
		      dm_oblock_t oblock, struct bio *bio,
		      struct dm_bio_prison_cell **result)
{
	int r;
	struct dm_cell_key key;
	struct dm_bio_prison_cell **cell;

	cell = prealloc_get_cell(structs);

	build_key(oblock, &key);
	r = dm_bio_detain(cache->prison, &key, bio, *cell, result);

	if (!r)
		/* cell was used */
		*cell = NULL;

	return r;
}

static int get_cell(struct cache *cache,
		    struct prealloc *structs,
		    dm_oblock_t oblock,
		    struct dm_bio_prison_cell **result)
{
	int r;
	struct dm_cell_key key;
	struct dm_bio_prison_cell **cell;

	cell = prealloc_get_cell(structs);

	build_key(oblock, &key);
	r = dm_get_cell(cache->prison, &key, *cell, result);

	if (!r)
		*cell = NULL;

	return r;
}

 /*----------------------------------------------------------------*/

static bool is_dirty(struct cache *cache, dm_cblock_t b)
{
	return test_bit(from_cblock(b), cache->dirty_bitset);
}

static void set_dirty(struct cache *cache, dm_oblock_t oblock, dm_cblock_t cblock)
{
	if (!test_and_set_bit(from_cblock(cblock), cache->dirty_bitset)) {
		cache->nr_dirty = to_cblock(from_cblock(cache->nr_dirty) + 1);
		policy_set_dirty(cache->policy, oblock);
	}
}

static void clear_dirty(struct cache *cache, dm_oblock_t oblock, dm_cblock_t cblock)
{
	if (test_and_clear_bit(from_cblock(cblock), cache->dirty_bitset)) {
		policy_clear_dirty(cache->policy, oblock);
		cache->nr_dirty = to_cblock(from_cblock(cache->nr_dirty) - 1);
		if (!from_cblock(cache->nr_dirty))
			dm_table_event(cache->ti->table);
	}
}

/*----------------------------------------------------------------*/

/*
 * The discard bitset is accessed from both the worker thread and the
 * cache_map function, we need to protect it.
 */
static dm_dblock_t oblock_to_dblock(struct cache *cache, dm_oblock_t oblock)
{
	dm_block_t b = (__force dm_block_t) oblock;
	do_div(b, cache->discard_block_size);
	return (__force dm_dblock_t) b;
}

static void set_discard(struct cache *cache, dm_dblock_t b)
{
	unsigned long flags;

	spin_lock_irqsave(&cache->lock, flags);
	set_bit(from_dblock(b), cache->discard_bitset);
	spin_unlock_irqrestore(&cache->lock, flags);
}

static void clear_discard(struct cache *cache, dm_dblock_t b)
{
	unsigned long flags;

	spin_lock_irqsave(&cache->lock, flags);
	clear_bit(from_dblock(b), cache->discard_bitset);
	spin_unlock_irqrestore(&cache->lock, flags);
}

static bool is_discarded(struct cache *cache, dm_dblock_t b)
{
	int r;
	unsigned long flags;

	spin_lock_irqsave(&cache->lock, flags);
	r = test_bit(from_dblock(b), cache->discard_bitset);
	spin_unlock_irqrestore(&cache->lock, flags);

	return r;
}

static bool is_discarded_oblock(struct cache *cache, dm_oblock_t b)
{
	int r;
	unsigned long flags;

	spin_lock_irqsave(&cache->lock, flags);
	r = test_bit(from_dblock(oblock_to_dblock(cache, b)),
		     cache->discard_bitset);
	spin_unlock_irqrestore(&cache->lock, flags);

	return r;
}

/*----------------------------------------------------------------*/

static void load_stats(struct cache *cache)
{
	struct dm_cache_statistics stats;

	dm_cache_get_stats(cache->cmd, &stats);
	atomic_set(&cache->read_hit, stats.read_hits);
	atomic_set(&cache->read_miss, stats.read_misses);
	atomic_set(&cache->write_hit, stats.write_hits);
	atomic_set(&cache->write_miss, stats.write_misses);
}

static void save_stats(struct cache *cache)
{
	struct dm_cache_statistics stats;

	stats.read_hits = atomic_read(&cache->read_hit);
	stats.read_misses = atomic_read(&cache->read_miss);
	stats.write_hits = atomic_read(&cache->write_hit);
	stats.write_misses = atomic_read(&cache->write_miss);

	dm_cache_set_stats(cache->cmd, &stats);
}

/*----------------------------------------------------------------
 * Remapping
 *--------------------------------------------------------------*/
static bool block_size_is_power_of_two(struct cache *cache)
{
	return cache->sectors_per_block_shift >= 0;
}

static void remap_to_origin(struct cache *cache, struct bio *bio)
{
	bio->bi_bdev = cache->origin_dev->bdev;
}

static void remap_to_cache(struct cache *cache, struct bio *bio,
			   dm_cblock_t cblock)
{
	sector_t bi_sector = bio->bi_sector;

	bio->bi_bdev = cache->cache_dev->bdev;
	if (!block_size_is_power_of_two(cache))
		bio->bi_sector = (from_cblock(cblock) * cache->sectors_per_block) +
				sector_div(bi_sector, cache->sectors_per_block);
	else
		bio->bi_sector = (from_cblock(cblock) << cache->sectors_per_block_shift) |
				(bi_sector & (cache->sectors_per_block - 1));
}

static void check_if_tick_bio_needed(struct cache *cache, struct bio *bio)
{
	unsigned long flags;
	struct dm_cache_endio_hook *h =
		dm_bio_get_per_request_data(bio, sizeof(struct dm_cache_endio_hook));

	spin_lock_irqsave(&cache->lock, flags);
	if (cache->need_tick_bio &&
	    !(bio->bi_rw & (REQ_FUA | REQ_FLUSH | REQ_DISCARD))) {
		h->tick = true;
		cache->need_tick_bio = false;
	}
	spin_unlock_irqrestore(&cache->lock, flags);
}

static void remap_to_origin_dirty(struct cache *cache, struct bio *bio,
				  dm_oblock_t oblock)
{
	check_if_tick_bio_needed(cache, bio);
	remap_to_origin(cache, bio);
	if (bio_data_dir(bio) == WRITE)
		clear_discard(cache, oblock_to_dblock(cache, oblock));
}

static void remap_to_cache_dirty(struct cache *cache, struct bio *bio,
				 dm_oblock_t oblock, dm_cblock_t cblock)
{
	remap_to_cache(cache, bio, cblock);
	if (bio_data_dir(bio) == WRITE) {
		set_dirty(cache, oblock, cblock);
		clear_discard(cache, oblock_to_dblock(cache, oblock));
	}
}

static dm_oblock_t get_bio_block(struct cache *cache, struct bio *bio)
{
	sector_t block_nr = bio->bi_sector;

	if (!block_size_is_power_of_two(cache))
		(void) sector_div(block_nr, cache->sectors_per_block);
	else
		block_nr >>= cache->sectors_per_block_shift;

	return to_oblock(block_nr);
}

static int bio_triggers_commit(struct cache *cache, struct bio *bio)
{
	return (bio->bi_rw & (REQ_FLUSH | REQ_FUA)) &&
		dm_cache_changed_this_transaction(cache->cmd);
}

static void issue(struct cache *cache, struct bio *bio)
{
	unsigned long flags;

	if (!bio_triggers_commit(cache, bio)) {
		generic_make_request(bio);
		return;
	}

	/*
	 * Batch together any bios that trigger commits and then issue a
	 * single commit for them in do_worker().
	 */
	spin_lock_irqsave(&cache->lock, flags);
	cache->commit_requested = true;
	bio_list_add(&cache->deferred_flush_bios, bio);
	spin_unlock_irqrestore(&cache->lock, flags);
}

/*----------------------------------------------------------------
 * Migration processing
 *
 * Migration covers moving data from the origin device to the cache, or
 * vice versa.
 *--------------------------------------------------------------*/
static void free_migration(struct dm_cache_migration *mg)
{
	mempool_free(mg, mg->cache->migration_pool);
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
	(holder ? dm_cell_release : dm_cell_release_no_holder)
		(cache->prison, cell, &cache->deferred_bios);
	dm_bio_prison_free_cell(cache->prison, cell);
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

static void cleanup_migration(struct dm_cache_migration *mg)
{
	dec_nr_migrations(mg->cache);
	free_migration(mg);
}

static void migration_failure(struct dm_cache_migration *mg)
{
	struct cache *cache = mg->cache;

	if (mg->writeback) {
		DMWARN_LIMIT("writeback failed; couldn't copy block");
		set_dirty(cache, mg->old_oblock, mg->cblock);
		cell_defer(cache, mg->old_ocell, false);

	} else if (mg->demote) {
		DMWARN_LIMIT("demotion failed; couldn't copy block");
		policy_force_mapping(cache->policy, mg->new_oblock, mg->old_oblock);

		cell_defer(cache, mg->old_ocell, mg->promote ? 0 : 1);
		if (mg->promote)
			cell_defer(cache, mg->new_ocell, 1);
	} else {
		DMWARN_LIMIT("promotion failed; couldn't copy block");
		policy_remove_mapping(cache->policy, mg->new_oblock);
		cell_defer(cache, mg->new_ocell, 1);
	}

	cleanup_migration(mg);
}

static void migration_success_pre_commit(struct dm_cache_migration *mg)
{
	unsigned long flags;
	struct cache *cache = mg->cache;

	if (mg->writeback) {
		cell_defer(cache, mg->old_ocell, false);
		clear_dirty(cache, mg->old_oblock, mg->cblock);
		cleanup_migration(mg);
		return;

	} else if (mg->demote) {
		if (dm_cache_remove_mapping(cache->cmd, mg->cblock)) {
			DMWARN_LIMIT("demotion failed; couldn't update on disk metadata");
			policy_force_mapping(cache->policy, mg->new_oblock,
					     mg->old_oblock);
			if (mg->promote)
				cell_defer(cache, mg->new_ocell, true);
			cleanup_migration(mg);
			return;
		}
	} else {
		if (dm_cache_insert_mapping(cache->cmd, mg->cblock, mg->new_oblock)) {
			DMWARN_LIMIT("promotion failed; couldn't update on disk metadata");
			policy_remove_mapping(cache->policy, mg->new_oblock);
			cleanup_migration(mg);
			return;
		}
	}

	spin_lock_irqsave(&cache->lock, flags);
	list_add_tail(&mg->list, &cache->need_commit_migrations);
	cache->commit_requested = true;
	spin_unlock_irqrestore(&cache->lock, flags);
}

static void migration_success_post_commit(struct dm_cache_migration *mg)
{
	unsigned long flags;
	struct cache *cache = mg->cache;

	if (mg->writeback) {
		DMWARN("shouldn't get here");
		return;

	} else if (mg->demote) {
		cell_defer(cache, mg->old_ocell, mg->promote ? 0 : 1);

		if (mg->promote) {
			mg->demote = false;

			spin_lock_irqsave(&cache->lock, flags);
			list_add_tail(&mg->list, &cache->quiesced_migrations);
			spin_unlock_irqrestore(&cache->lock, flags);

			// FIXME: this mapping no longer in policy so can't call.
			clear_dirty(cache, mg->old_oblock, mg->cblock);

		} else {
			// FIXME: this mapping no longer in policy so can't call.
			clear_dirty(cache, mg->old_oblock, mg->cblock);
			cleanup_migration(mg);
		}

	} else {
		cell_defer(cache, mg->new_ocell, true);
		clear_dirty(cache, mg->new_oblock, mg->cblock);
		cleanup_migration(mg);
	}
}

static void copy_complete(int read_err, unsigned long write_err, void *context)
{
	unsigned long flags;
	struct dm_cache_migration *mg = (struct dm_cache_migration *) context;
	struct cache *cache = mg->cache;

	if (read_err || write_err)
		mg->err = true;

	spin_lock_irqsave(&cache->lock, flags);
	list_add_tail(&mg->list, &cache->completed_migrations);
	spin_unlock_irqrestore(&cache->lock, flags);

	wake_worker(cache);
}

static void issue_copy_real(struct dm_cache_migration *mg)
{
	int r;
	struct dm_io_region o_region, c_region;
	struct cache *cache = mg->cache;

	o_region.bdev = cache->origin_dev->bdev;
	o_region.count = cache->sectors_per_block;

	c_region.bdev = cache->cache_dev->bdev;
	c_region.sector = from_cblock(mg->cblock) * cache->sectors_per_block;
	c_region.count = cache->sectors_per_block;

	if (mg->writeback || mg->demote) {
		/* demote */
		o_region.sector = from_oblock(mg->old_oblock) * cache->sectors_per_block;
		r = dm_kcopyd_copy(cache->copier, &c_region, 1, &o_region, 0, copy_complete, mg);
	} else {
		/* promote */
		o_region.sector = from_oblock(mg->new_oblock) * cache->sectors_per_block;
		r = dm_kcopyd_copy(cache->copier, &o_region, 1, &c_region, 0, copy_complete, mg);
	}

	if (r < 0)
		migration_failure(mg);
}

static void avoid_copy(struct dm_cache_migration *mg)
{
	atomic_inc(&mg->cache->copies_avoided);
	migration_success_pre_commit(mg);
}

static void issue_copy(struct dm_cache_migration *mg)
{
	bool avoid;
	struct cache *cache = mg->cache;

	if (mg->writeback || mg->demote)
		avoid = !is_dirty(cache, mg->cblock) ||
			is_discarded_oblock(cache, mg->old_oblock);
	else
		avoid = is_discarded_oblock(cache, mg->new_oblock);

	avoid ? avoid_copy(mg) : issue_copy_real(mg);
}

static void complete_migration(struct dm_cache_migration *mg)
{
	if (mg->err)
		migration_failure(mg);
	else
		migration_success_pre_commit(mg);
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

static void __queue_quiesced_migration(struct dm_cache_migration *mg)
{
	list_add_tail(&mg->list, &mg->cache->quiesced_migrations);
}

static void queue_quiesced_migration(struct dm_cache_migration *mg)
{
	unsigned long flags;
	struct cache *cache = mg->cache;

	spin_lock_irqsave(&cache->lock, flags);
	__queue_quiesced_migration(mg);
	spin_unlock_irqrestore(&cache->lock, flags);

	wake_worker(cache);
}

static void queue_quiesced_migrations(struct cache *cache, struct list_head *work)
{
	unsigned long flags;
	struct dm_cache_migration *mg, *tmp;

	spin_lock_irqsave(&cache->lock, flags);
	list_for_each_entry_safe(mg, tmp, work, list)
		__queue_quiesced_migration(mg);
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

static void quiesce_migration(struct dm_cache_migration *mg)
{
	if (!dm_deferred_set_add_work(mg->cache->all_io_ds, &mg->list))
		queue_quiesced_migration(mg);
}

static void promote(struct cache *cache, struct prealloc *structs,
		    dm_oblock_t oblock, dm_cblock_t cblock,
		    struct dm_bio_prison_cell *cell)
{
	struct dm_cache_migration *mg = prealloc_get_migration(structs);

	mg->err = false;
	mg->writeback = false;
	mg->demote = false;
	mg->promote = true;
	mg->cache = cache;
	mg->new_oblock = oblock;
	mg->cblock = cblock;
	mg->old_ocell = NULL;
	mg->new_ocell = cell;
	mg->start_jiffies = jiffies;

	inc_nr_migrations(cache);
	quiesce_migration(mg);
}

static void writeback(struct cache *cache, struct prealloc *structs,
		      dm_oblock_t oblock, dm_cblock_t cblock,
		      struct dm_bio_prison_cell *cell)
{
	struct dm_cache_migration *mg = prealloc_get_migration(structs);

	mg->err = false;
	mg->writeback = true;
	mg->demote = false;
	mg->promote = false;
	mg->cache = cache;
	mg->old_oblock = oblock;
	mg->cblock = cblock;
	mg->old_ocell = cell;
	mg->new_ocell = NULL;
	mg->start_jiffies = jiffies;

	inc_nr_migrations(cache);
	quiesce_migration(mg);
}

static void demote_then_promote(struct cache *cache, struct prealloc *structs,
				dm_oblock_t old_oblock, dm_oblock_t new_oblock,
				dm_cblock_t cblock,
				struct dm_bio_prison_cell *old_ocell,
				struct dm_bio_prison_cell *new_ocell)
{
	struct dm_cache_migration *mg = prealloc_get_migration(structs);

	mg->err = false;
	mg->writeback = false;
	mg->demote = true;
	mg->promote = true;
	mg->cache = cache;
	mg->old_oblock = old_oblock;
	mg->new_oblock = new_oblock;
	mg->cblock = cblock;
	mg->old_ocell = old_ocell;
	mg->new_ocell = new_ocell;
	mg->start_jiffies = jiffies;

	inc_nr_migrations(cache);
	quiesce_migration(mg);
}

/*----------------------------------------------------------------
 * bio processing
 *--------------------------------------------------------------*/
static void defer_bio(struct cache *cache, struct bio *bio)
{
	unsigned long flags;

	spin_lock_irqsave(&cache->lock, flags);
	bio_list_add(&cache->deferred_bios, bio);
	spin_unlock_irqrestore(&cache->lock, flags);

	wake_worker(cache);
}

static void process_flush_bio(struct cache *cache, struct bio *bio)
{
	struct dm_cache_endio_hook *h =
		dm_bio_get_per_request_data(bio, sizeof(struct dm_cache_endio_hook));

	BUG_ON(bio->bi_size);
	if (!h->req_nr)
		remap_to_origin(cache, bio);
	else
		remap_to_cache(cache, bio, 0);

	issue(cache, bio);
}

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
static void process_discard_bio(struct cache *cache, struct bio *bio)
{
	dm_block_t start_block = bio->bi_sector;
	dm_block_t end_block = bio->bi_sector + bio_sectors(bio);
	dm_block_t b;

	do_div(start_block, cache->discard_block_size);
	do_div(end_block, cache->discard_block_size);

	for (b = start_block; b < end_block; b++)
		set_discard(cache, to_dblock(b));

	bio_endio(bio, 0);
}

static bool may_migrate(struct cache *cache)
{
	sector_t current_volume = (atomic_read(&cache->nr_migrations) + 1) *
		cache->sectors_per_block;
	return current_volume < cache->migration_threshold;
}

static bool is_writethrough_io(struct cache *cache, struct bio *bio,
			       dm_cblock_t cblock)
{
	return bio_data_dir(bio) == WRITE &&
		cache->features.write_through && !is_dirty(cache, cblock);
}

static void process_bio(struct cache *cache, struct prealloc *structs,
			struct bio *bio)
{
	int r;
	bool release_cell = true;
	dm_oblock_t block = get_bio_block(cache, bio);
	struct dm_bio_prison_cell *old_ocell, *new_ocell;
	struct policy_result lookup_result;
	struct dm_cache_endio_hook *h =
		dm_bio_get_per_request_data(bio, sizeof(struct dm_cache_endio_hook));
	bool can_migrate = may_migrate(cache);
	bool discarded_block = is_discarded_oblock(cache, block);

	/*
	 * Check to see if that block is currently migrating.
	 */
	r = bio_detain(cache, structs, block, bio, &new_ocell);
	if (r > 0)
		return;

	policy_map(cache->policy, block, can_migrate, discarded_block,
		   bio, &lookup_result);
	switch (lookup_result.op) {
	case POLICY_HIT:
		atomic_inc(bio_data_dir(bio) == READ ?
			   &cache->read_hit : &cache->write_hit);
		h->all_io_entry = dm_deferred_entry_inc(cache->all_io_ds);

		if (is_writethrough_io(cache, bio, lookup_result.cblock)) {
			/*
			 * No need to mark anything dirty in write through mode.
			 */
			h->req_nr == 0 ?
				remap_to_cache(cache, bio, lookup_result.cblock) :
				remap_to_origin(cache, bio);
		} else
			remap_to_cache_dirty(cache, bio, block, lookup_result.cblock);

		issue(cache, bio);
		break;

	case POLICY_MISS:
		atomic_inc(bio_data_dir(bio) == READ ?
			   &cache->read_miss : &cache->write_miss);
		h->all_io_entry = dm_deferred_entry_inc(cache->all_io_ds);

		if (h->req_nr != 0) {
			/*
			 * This is a duplicate writethrough io that is no
			 * longer needed because the block has been demoted.
			 */
			bio_endio(bio, 0);
		} else {
			remap_to_origin_dirty(cache, bio, block);
			issue(cache, bio);
		}
		break;

	case POLICY_NEW:
		atomic_inc(&cache->promotion);
		promote(cache, structs, block, lookup_result.cblock, new_ocell);
		release_cell = false;
		break;

	case POLICY_REPLACE:
		r = bio_detain(cache, structs, lookup_result.old_oblock,
			       bio, &old_ocell);
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

		demote_then_promote(cache, structs, lookup_result.old_oblock,
				    block, lookup_result.cblock,
				    old_ocell, new_ocell);
		release_cell = false;
		break;

	default:
		DMERR("unknown policy op returned, erroring bio\n");
		bio_io_error(bio);
	}

	if (release_cell)
		cell_defer(cache, new_ocell, false);
}

static int need_commit_due_to_time(struct cache *cache)
{
	return jiffies < cache->last_commit_jiffies ||
	       jiffies > cache->last_commit_jiffies + COMMIT_PERIOD;
}

static int commit_if_needed(struct cache *cache)
{
	if (cache->commit_requested || need_commit_due_to_time(cache)) {
		atomic_inc(&cache->commit_count);
		cache->last_commit_jiffies = jiffies;
		cache->commit_requested = false;
		return dm_cache_commit(cache->cmd, false);
	}

	return 0;
}

static void process_deferred_bios(struct cache *cache)
{
	unsigned long flags;
	struct bio_list bios;
	struct bio *bio;
	struct prealloc structs;

	memset(&structs, 0, sizeof(structs));
	bio_list_init(&bios);

	spin_lock_irqsave(&cache->lock, flags);
	bio_list_merge(&bios, &cache->deferred_bios);
	bio_list_init(&cache->deferred_bios);
	spin_unlock_irqrestore(&cache->lock, flags);

	while (!bio_list_empty(&bios)) {
		/*
		 * If we've got no free migration structs, and processing
		 * this bio might require one, we pause until there are some
		 * prepared mappings to process.
		 */
		if (prealloc_data_structs(cache, &structs)) {
			spin_lock_irqsave(&cache->lock, flags);
			bio_list_merge(&cache->deferred_bios, &bios);
			spin_unlock_irqrestore(&cache->lock, flags);
			break;
		}

		bio = bio_list_pop(&bios);

		if (bio->bi_rw & REQ_FLUSH)
			process_flush_bio(cache, bio);
		else if (bio->bi_rw & REQ_DISCARD)
			process_discard_bio(cache, bio);
		else
			process_bio(cache, &structs, bio);
	}

	prealloc_free_structs(cache, &structs);
}

static void process_deferred_flush_bios(struct cache *cache, bool submit_bios)
{
	unsigned long flags;
	struct bio_list bios;
	struct bio *bio;

	bio_list_init(&bios);

	spin_lock_irqsave(&cache->lock, flags);
	bio_list_merge(&bios, &cache->deferred_flush_bios);
	bio_list_init(&cache->deferred_flush_bios);
	spin_unlock_irqrestore(&cache->lock, flags);

	while ((bio = bio_list_pop(&bios)))
		submit_bios ? generic_make_request(bio) : bio_io_error(bio);
}

static void writeback_some_dirty_blocks(struct cache *cache)
{
	int r = 0;
	dm_oblock_t oblock;
	dm_cblock_t cblock;
	struct prealloc structs;
	struct dm_bio_prison_cell *old_ocell;

	memset(&structs, 0, sizeof(structs));

	while (may_migrate(cache)) {
		if (prealloc_data_structs(cache, &structs))
			break;

		r = policy_writeback_work(cache->policy, &oblock, &cblock);
		if (r)
			break;

		r = get_cell(cache, &structs, oblock, &old_ocell);
		if (r) {
			policy_set_dirty(cache->policy, oblock);
			break;
		}

		writeback(cache, &structs, oblock, cblock, old_ocell);
	}

	prealloc_free_structs(cache, &structs);
}

/*----------------------------------------------------------------
 * Main worker loop
 *--------------------------------------------------------------*/
static void start_quiescing(struct cache *cache)
{
	unsigned long flags;

	spin_lock_irqsave(&cache->lock, flags);
	cache->quiescing = 1;
	spin_unlock_irqrestore(&cache->lock, flags);
}

static void stop_quiescing(struct cache *cache)
{
	unsigned long flags;

	spin_lock_irqsave(&cache->lock, flags);
	cache->quiescing = 0;
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
	wait_event(cache->migration_wait, !atomic_read(&cache->nr_migrations));
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
			!list_empty(&cache->completed_migrations) ||
			!list_empty(&cache->need_commit_migrations);
	else
		return !bio_list_empty(&cache->deferred_bios) ||
			!bio_list_empty(&cache->deferred_flush_bios) ||
			!list_empty(&cache->quiesced_migrations) ||
			!list_empty(&cache->completed_migrations) ||
			!list_empty(&cache->need_commit_migrations);
}

static void do_worker(struct work_struct *ws)
{
	struct cache *cache = container_of(ws, struct cache, worker);

	do {
		if (!is_quiescing(cache))
			process_deferred_bios(cache);

		process_migrations(cache, &cache->quiesced_migrations, issue_copy);
		process_migrations(cache, &cache->completed_migrations, complete_migration);

		writeback_some_dirty_blocks(cache);

		if (commit_if_needed(cache)) {
			process_deferred_flush_bios(cache, false);

			/*
			 * FIXME: rollback metadata or just go into a
			 * failure mode and error everything
			 */
		} else {
			process_deferred_flush_bios(cache, true);
			process_migrations(cache, &cache->need_commit_migrations,
					   migration_success_post_commit);
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

/*----------------------------------------------------------------*/

static int is_congested(struct dm_dev *dev, int bdi_bits)
{
	struct request_queue *q = bdev_get_queue(dev->bdev);
	return bdi_congested(&q->backing_dev_info, bdi_bits);
}

static int cache_is_congested(struct dm_target_callbacks *cb, int bdi_bits)
{
	struct cache *cache = container_of(cb, struct cache, callbacks);

	return is_congested(cache->origin_dev, bdi_bits) ||
		is_congested(cache->cache_dev, bdi_bits);
}

/*----------------------------------------------------------------
 * Target methods
 *--------------------------------------------------------------*/

/*
 * This function gets called on the error paths of the constructor, so we
 * have to cope with a partially initialised struct.
 */
static void destroy(struct cache *cache)
{
	if (cache->next_migration)
		mempool_free(cache->next_migration, cache->migration_pool);

	if (cache->migration_pool)
		mempool_destroy(cache->migration_pool);

	if (cache->all_io_ds)
		dm_deferred_set_destroy(cache->all_io_ds);

	if (cache->prison)
		dm_bio_prison_destroy(cache->prison);

	if (cache->wq)
		destroy_workqueue(cache->wq);

	if (cache->dirty_bitset)
		free_bitset(cache->dirty_bitset);

	if (cache->discard_bitset)
		free_bitset(cache->discard_bitset);

	if (cache->copier)
		dm_kcopyd_client_destroy(cache->copier);

	if (cache->cmd)
		dm_cache_metadata_close(cache->cmd);

	if (cache->metadata_dev)
		dm_put_device(cache->ti, cache->metadata_dev);

	if (cache->origin_dev)
		dm_put_device(cache->ti, cache->origin_dev);

	if (cache->cache_dev)
		dm_put_device(cache->ti, cache->cache_dev);

	if (cache->policy)
		dm_cache_policy_destroy(cache->policy);

	kfree(cache);
}

static void cache_dtr(struct dm_target *ti)
{
	struct cache *cache = ti->private;

	pr_alert("dm-cache statistics:\n");
	pr_alert("read hits:\t%u\n", (unsigned) atomic_read(&cache->read_hit));
	pr_alert("read misses:\t%u\n", (unsigned) atomic_read(&cache->read_miss));
	pr_alert("write hits:\t%u\n", (unsigned) atomic_read(&cache->write_hit));
	pr_alert("write misses:\t%u\n", (unsigned) atomic_read(&cache->write_miss));
	pr_alert("demotions:\t%u\n", (unsigned) atomic_read(&cache->demotion));
	pr_alert("promotions:\t%u\n", (unsigned) atomic_read(&cache->promotion));
	pr_alert("copies avoided:\t%u\n", (unsigned) atomic_read(&cache->copies_avoided));
	pr_alert("cache cell clashs:\t%u\n", (unsigned) atomic_read(&cache->cache_cell_clash));
	pr_alert("commits:\t\t%u\n", (unsigned) atomic_read(&cache->commit_count));

	destroy(cache);
}

static sector_t get_dev_size(struct dm_dev *dev)
{
	return i_size_read(dev->bdev->bd_inode) >> SECTOR_SHIFT;
}

/*----------------------------------------------------------------*/

/*
 * Construct a cache device mapping.
 *
 * cache <metadata dev> <cache dev> <origin dev> <block size>
 *       <#feature_args> [<arg>]* <policy> <#policy_args> [<arg>]*
 *
 * metadata dev    : fast device holding the persistent metadata
 * cache dev	   : fast device holding cached data blocks
 * origin dev	   : slow device holding original data blocks
 * block size	   : cache unit size in sectors
 * #feature args [<arg>]* : number of feature arguments followed by
 *                          optional arguments * cache dev
 * policy          : the replacement policy to use

 * #policy_args  [<arg>]* : number of policy arguments followed by optional
 *                          arguments; see policy plugin for instances
 *			    (key value pairs count as 2; delimiter is space)
 *
 * Optional feature arguments are:
 *	writeback: write back cache allowing cache block contents to
 *                 differ from origin blocks for performance reasons
 *	writethrough: write through caching prohibiting cache block
 *                    content from being distinct from origin block content
 */
struct cache_args {
	struct dm_target *ti;

	struct dm_dev *metadata_dev;

	struct dm_dev *cache_dev;
	sector_t cache_sectors;

	struct dm_dev *origin_dev;
	sector_t origin_sectors;

	sector_t block_size;

	const char *policy_name;
	int policy_argc;
	char **policy_argv;

	struct cache_features features;
};

static void destroy_cache_args(struct cache_args *ca)
{
	if (ca->metadata_dev)
		dm_put_device(ca->ti, ca->metadata_dev);

	if (ca->cache_dev)
		dm_put_device(ca->ti, ca->cache_dev);

	if (ca->origin_dev)
		dm_put_device(ca->ti, ca->origin_dev);

	kfree(ca);
}

static int ensure_args__(struct dm_arg_set *as,
		       unsigned count, char **error)
{
	if (as->argc < count) {
		*error = "Insufficient args";
		return -EINVAL;
	}

	return 0;
}

#define ensure_args(n) \
	r = ensure_args__(as, n, error); \
	if (r) \
		return r;

static int parse_metadata_dev(struct cache_args *ca, struct dm_arg_set *as,
			      char **error)
{
	int r;
	sector_t metadata_dev_size;
	char b[BDEVNAME_SIZE];

	ensure_args(1);

	r = dm_get_device(ca->ti, dm_shift_arg(as), FMODE_READ | FMODE_WRITE,
			  &ca->metadata_dev);
	if (r) {
		*error = "Error opening metadata device";
		return r;
	}

	metadata_dev_size = get_dev_size(ca->metadata_dev);
	if (metadata_dev_size > CACHE_METADATA_MAX_SECTORS_WARNING)
		DMWARN("Metadata device %s is larger than %u sectors: excess space will not be used.",
		       bdevname(ca->metadata_dev->bdev, b), THIN_METADATA_MAX_SECTORS);

	return 0;
}

static int parse_cache_dev(struct cache_args *ca, struct dm_arg_set *as,
			   char **error)
{
	int r;

	ensure_args(1);
	r = dm_get_device(ca->ti, dm_shift_arg(as), FMODE_READ | FMODE_WRITE,
			  &ca->cache_dev);
	if (r) {
		*error = "Error opening cache device";
		return r;
	}
	ca->cache_sectors = get_dev_size(ca->cache_dev);

	return 0;
}

static int parse_origin_dev(struct cache_args *ca, struct dm_arg_set *as,
			    char **error)
{
	int r;

	ensure_args(1);
	r = dm_get_device(ca->ti, dm_shift_arg(as), FMODE_READ | FMODE_WRITE,
			  &ca->origin_dev);
	if (r) {
		*error = "Error opening origin device";
		return r;
	}

	ca->origin_sectors = get_dev_size(ca->origin_dev);
	if (ca->ti->len > ca->origin_sectors) {
		*error = "Device size larger than cached device";
		return -EINVAL;
	}

	return 0;
}

static int parse_block_size(struct cache_args *ca, struct dm_arg_set *as,
			    char **error)
{
	int r;
	unsigned long tmp;

	ensure_args(1);
	if (kstrtoul(dm_shift_arg(as), 10, &tmp) || !tmp ||
	    tmp < DATA_DEV_BLOCK_SIZE_MIN_SECTORS ||
	    tmp & (DATA_DEV_BLOCK_SIZE_MIN_SECTORS - 1)) {
		*error = "Invalid data block size";
		return -EINVAL;
	}
	ca->block_size = tmp;

	return 0;
}

static void init_features(struct cache_features *cf)
{
	cf->mode = CM_WRITE;
	cf->write_through = false;
}

static int parse_features(struct cache_args *ca, struct dm_arg_set *as,
			  char **error)
{
	static struct dm_arg _args[] = {
		{0, 1, "Invalid number of cache feature arguments"},
	};

	int r;
	unsigned argc;
	const char *arg;
	struct cache_features *cf = &ca->features;

	init_features(cf);

	r = dm_read_arg_group(_args, as, &argc, error);
	if (r)
		return -EINVAL;

	while (argc--) {
		arg = dm_shift_arg(as);

		if (!strcasecmp(arg, "writeback"))
			cf->write_through = false;

		else if (!strcasecmp(arg, "writethrough"))
			cf->write_through = true;

		else {
			*error = "Unrecognised cache feature requested";
			return -EINVAL;
		}
	}

	return 0;
}

static int parse_policy(struct cache_args *ca, struct dm_arg_set *as,
			char **error)
{
	static struct dm_arg _args[] = {
		{0, 1024, "Invalid number of policy arguments"},
	};

	int r;
	ensure_args(1);
	ca->policy_name = dm_shift_arg(as);

	r = dm_read_arg_group(_args, as, &ca->policy_argc, error);
	if (r)
		return -EINVAL;

	ca->policy_argv = as->argv;
	dm_consume_args(as, ca->policy_argc);

	return 0;
}

static int parse_cache_args(struct cache_args *ca, int argc, char **argv,
			    char **error)
{
	int r;
	struct dm_arg_set as;

	as.argc = argc;
	as.argv = argv;

#define parse(name) \
	r = parse_ ## name(ca, &as, error); \
	if (r) \
		return r;

	parse(metadata_dev);
	parse(cache_dev);
	parse(origin_dev);
	parse(block_size);
	parse(features);
	parse(policy);
#undef parse

	return 0;
}

/*----------------------------------------------------------------*/

// FIXME: get rid of this
static struct kmem_cache *_migration_cache;

static int create_cache_policy(struct cache *cache, struct cache_args *ca,
			       char **error)
{
	cache->policy =	dm_cache_policy_create(ca->policy_name,
					       cache->cache_size,
					       cache->origin_sectors,
					       cache->sectors_per_block,
					       ca->policy_argc, ca->policy_argv);
	if (!cache->policy) {
		*error = "Error creating cache's policy";
		return -ENOMEM;
	}

	return 0;
}

/*
 * We want the discard block size to be a power of two, at least the size
 * of the cache block size, and have no more than 2^14 discard blocks
 * across the origin.
 */
static sector_t calculate_discard_block_size(sector_t cache_block_size,
					     sector_t origin_size)
{
	sector_t r = roundup_pow_of_two(cache_block_size);
	sector_t max_blocks = 1 << 14;

	while ((r * max_blocks) < origin_size)
		r *= 2;

	return r;
}

static int cache_create(struct cache_args *ca, struct cache **result)
{
	int r = 0;
	char **error = &ca->ti->error;
	struct cache *cache;
	struct dm_target *ti = ca->ti;
	dm_block_t origin_blocks;
	struct dm_cache_metadata *cmd;
	bool may_format = ca->features.mode == CM_WRITE;

	cache = kzalloc(sizeof(*cache), GFP_KERNEL);
	if (!cache)
		return -ENOMEM;

	cache->ti = ca->ti;
	ti->private = cache;
	ti->num_flush_requests = 2;
	ti->flush_supported = true;
	ti->per_request_data = sizeof(struct dm_cache_endio_hook);

	ti->num_discard_requests = 1;
	ti->discards_supported = true;
	ti->discard_zeroes_data_unsupported = true;

	cache->callbacks.congested_fn = cache_is_congested;
	dm_table_add_target_callbacks(ti->table, &cache->callbacks);

#define consume(n) n; n = NULL;

	cache->metadata_dev = consume(ca->metadata_dev);
	cache->origin_dev = consume(ca->origin_dev);
	cache->cache_dev = consume(ca->cache_dev);
	memcpy(&cache->features, &ca->features, sizeof(cache->features));

	// FIXME: factor out this whole section
	origin_blocks = cache->origin_sectors = ca->origin_sectors;
	do_div(origin_blocks, ca->block_size);
	cache->origin_blocks = to_oblock(origin_blocks);

	cache->sectors_per_block = ca->block_size;
	if (dm_set_target_max_io_len(ti, cache->sectors_per_block)) {
		r = -EINVAL;
		goto bad;
	}

	if (ca->block_size & (ca->block_size - 1)) {
		dm_block_t cache_size = ca->cache_sectors;

		cache->sectors_per_block_shift = -1;
		(void) sector_div(cache_size, ca->block_size);
		cache->cache_size = to_cblock(cache_size);
	} else {
		cache->sectors_per_block_shift = __ffs(ca->block_size);
		cache->cache_size = to_cblock(ca->cache_sectors >> cache->sectors_per_block_shift);
	}

	cmd = dm_cache_metadata_open(cache->metadata_dev->bdev,
				     ca->block_size, may_format);
	if (IS_ERR(cmd)) {
		*error = "Error creating metadata object";
		r = PTR_ERR(cmd);
		goto bad;
	}
	cache->cmd = cmd;

	spin_lock_init(&cache->lock);
	bio_list_init(&cache->deferred_bios);
	bio_list_init(&cache->deferred_flush_bios);
	INIT_LIST_HEAD(&cache->quiesced_migrations);
	INIT_LIST_HEAD(&cache->completed_migrations);
	INIT_LIST_HEAD(&cache->need_commit_migrations);
	cache->migration_threshold = 2048 * 100; /* FIXME: needs a #define */
	atomic_set(&cache->nr_migrations, 0);
	init_waitqueue_head(&cache->migration_wait);

	cache->nr_dirty = 0;
	cache->dirty_bitset = alloc_bitset(from_cblock(cache->cache_size));
	if (!cache->dirty_bitset) {
		*error = "Couldn't allocate dirty bitset";
		goto bad;
	}
	clear_bitset(cache->dirty_bitset, from_cblock(cache->cache_size));

	cache->discard_block_size =
		calculate_discard_block_size(cache->sectors_per_block,
					     cache->origin_sectors);
	cache->discard_nr_blocks = oblock_to_dblock(cache, cache->origin_blocks);
	cache->discard_bitset = alloc_bitset(from_dblock(cache->discard_nr_blocks));
	if (!cache->discard_bitset) {
		*error = "Couldn't allocate discard bitset";
		goto bad;
	}
	clear_bitset(cache->discard_bitset, from_dblock(cache->discard_nr_blocks));

	cache->copier = dm_kcopyd_client_create();
	if (IS_ERR(cache->copier)) {
		*error = "Couldn't create kcopyd client";
		r = PTR_ERR(cache->copier);
		goto bad;
	}

	cache->wq = alloc_ordered_workqueue(DAEMON, WQ_MEM_RECLAIM);
	if (!cache->wq) {
		*error = "Couldn't create workqueue for metadata object";
		goto bad;
	}
	INIT_WORK(&cache->worker, do_worker);
	INIT_DELAYED_WORK(&cache->waker, do_waker);
	cache->last_commit_jiffies = jiffies;

	cache->prison = dm_bio_prison_create(PRISON_CELLS);
	if (!cache->prison) {
		*error = "Couldn't create bio prison";
		goto bad;
	}

	cache->all_io_ds = dm_deferred_set_create();
	if (!cache->all_io_ds) {
		*error = "Couldn't create all_io deferred set";
		goto bad;
	}

	cache->migration_pool = mempool_create_slab_pool(MIGRATION_POOL_SIZE,
							 _migration_cache);
	if (!cache->migration_pool) {
		*error = "Error creating cache's endio_hook mempool";
		goto bad;
	}

	cache->next_migration = NULL;

	r = create_cache_policy(cache, ca, error);
	if (r)
		goto bad;

	cache->policy_nr_args = ca->policy_argc;

	cache->need_tick_bio = true;
	cache->sized = false;
	cache->quiescing = false;
	cache->commit_requested = false;
	cache->loaded_mappings = false;
	cache->loaded_discards = false;

	load_stats(cache);

	atomic_set(&cache->demotion, 0);
	atomic_set(&cache->promotion, 0);
	atomic_set(&cache->copies_avoided, 0);
	atomic_set(&cache->cache_cell_clash, 0);
	atomic_set(&cache->commit_count, 0);

	*result = cache;
	return 0;

bad:
	destroy(cache);
	return r;
}

static int cache_ctr(struct dm_target *ti, unsigned argc, char **argv)
{
	int r = -EINVAL;
	struct cache_args *ca;
	struct cache *cache = NULL;

	ca = kzalloc(sizeof(*ca), GFP_KERNEL);
	if (!ca) {
		ti->error = "Error allocating memory for cache";
		return -ENOMEM;
	}
	ca->ti = ti;

	r = parse_cache_args(ca, argc, argv, &ti->error);
	if (r)
		goto out;

	r = cache_create(ca, &cache);
	ti->private = cache;

out:
	destroy_cache_args(ca);
	return r;
}

static struct dm_cache_endio_hook *hook_endio(struct cache *cache,
					      struct bio *bio, unsigned req_nr)
{
	struct dm_cache_endio_hook *h =
		dm_bio_get_per_request_data(bio, sizeof(struct dm_cache_endio_hook));

	h->tick = false;
	h->req_nr = req_nr;
	h->all_io_entry = NULL;

	return h;
}

static unsigned cache_get_num_duplicates(struct dm_target *ti, struct bio *bio)
{
	struct cache *cache = ti->private;
	struct policy_result lookup_result;
	dm_oblock_t block = get_bio_block(cache, bio);

	policy_map(cache->policy, block, false, false, bio, &lookup_result);
	return (lookup_result.op == POLICY_HIT &&
		is_writethrough_io(cache, bio, lookup_result.cblock)) ? 2 : 1;
}

static int cache_map(struct dm_target *ti, struct bio *bio)
{
	struct cache *cache = ti->private;

	int r;
	dm_oblock_t block = get_bio_block(cache, bio);
	bool can_migrate = false;
	bool discarded_block;
	struct dm_bio_prison_cell *cell;
	struct policy_result lookup_result;
	struct dm_cache_endio_hook *h;

	if (from_oblock(block) > from_oblock(cache->origin_blocks)) {
		/*
		 * This can only occur if the io goes to a partial block at
		 * the end of the origin device.  We don't cache these.
		 * Just remap to the origin and carry on.
		 */
		remap_to_origin(cache, bio);
		return DM_MAPIO_REMAPPED;
	}

	h = hook_endio(cache, bio, dm_bio_get_target_request_nr(bio));

	if (bio->bi_rw & (REQ_FLUSH | REQ_FUA | REQ_DISCARD)) {
		defer_bio(cache, bio);
		return DM_MAPIO_SUBMITTED;
	}

	/*
	 * Check to see if that block is currently migrating.
	 */
	r = bio_detain_(cache, block, bio, GFP_ATOMIC, &cell);
	if (r) {
		if (r < 0)
			defer_bio(cache, bio);

		return DM_MAPIO_SUBMITTED;
	}

	discarded_block = is_discarded_oblock(cache, block);

	r = policy_map(cache->policy, block, can_migrate, discarded_block,
		       bio, &lookup_result);
	if (r == -EWOULDBLOCK) {
		cell_defer(cache, cell, true);
		return DM_MAPIO_SUBMITTED;
	}

	BUG_ON(r);

	switch (lookup_result.op) {
	case POLICY_HIT:
		atomic_inc(bio_data_dir(bio) == READ ?
			   &cache->read_hit : &cache->write_hit);
		h->all_io_entry = dm_deferred_entry_inc(cache->all_io_ds);

		if (is_writethrough_io(cache, bio, lookup_result.cblock)) {
			/*
			 * No need to mark anything dirty in write through mode.
			 */
			h->req_nr == 0 ?
				remap_to_cache(cache, bio, lookup_result.cblock) :
				remap_to_origin(cache, bio);
			cell_defer(cache, cell, false);
		} else {
			// FIXME: policy_set_dirty can block!
			remap_to_cache_dirty(cache, bio, block, lookup_result.cblock);
			cell_defer(cache, cell, false);
		}
		break;

	case POLICY_MISS:
		atomic_inc(bio_data_dir(bio) == READ ?
			   &cache->read_miss : &cache->write_miss);
		h->all_io_entry = dm_deferred_entry_inc(cache->all_io_ds);

		// FIXME: policy_set_dirty can block!
		if (h->req_nr != 0) {
			/*
			 * This is a duplicate writethrough io that is no
			 * longer needed because the block has been demoted.
			 */
			bio_endio(bio, 0);
			cell_defer(cache, cell, false);
			return DM_MAPIO_SUBMITTED;
		} else {
			remap_to_origin_dirty(cache, bio, block);
			cell_defer(cache, cell, false);
		}
		break;

	default:
		DMERR("policy op: %u\n", (unsigned) lookup_result.op);
		BUG();
	}

	return DM_MAPIO_REMAPPED;
}

static int cache_end_io(struct dm_target *ti, struct bio *bio, int error)
{
	struct cache *cache = ti->private;
	unsigned long flags;
	struct dm_cache_endio_hook *h =
		dm_bio_get_per_request_data(bio, sizeof(struct dm_cache_endio_hook));

	if (h->tick) {
		policy_tick(cache->policy);

		spin_lock_irqsave(&cache->lock, flags);
		cache->need_tick_bio = true;
		spin_unlock_irqrestore(&cache->lock, flags);
	}

	check_for_quiesced_migrations(cache, h);
	return 0;
}

static int write_dirty_bitset(struct cache *cache)
{
	unsigned i, r;

	for (i = 0; i < from_cblock(cache->cache_size); i++) {
		r = dm_cache_set_dirty(cache->cmd, to_cblock(i),
				       is_dirty(cache, to_cblock(i)));
		if (r)
			return r;
	}

	return 0;
}

static int write_discard_bitset(struct cache *cache)
{
	unsigned i, r;

	r = dm_cache_discard_bitset_resize(cache->cmd, cache->discard_block_size,
					   cache->discard_nr_blocks);
	if (r) {
		DMERR("Couldn't resize on-disk discard bitset");
		return r;
	}

	for (i = 0; i < from_dblock(cache->discard_nr_blocks); i++) {
		r = dm_cache_set_discard(cache->cmd, to_dblock(i),
					 is_discarded(cache, to_dblock(i)));
		if (r)
			return r;
	}

	return 0;
}

static int save_hint(void *context, dm_cblock_t cblock, dm_oblock_t oblock,
		     uint32_t hint)
{
	struct cache *cache = context;
	return dm_cache_save_hint(cache->cmd, cblock, hint);
}

static int write_hints(struct cache *cache)
{
	int r;

	r = dm_cache_begin_hints(cache->cmd,
				 dm_cache_policy_get_name(cache->policy));
	if (r) {
		DMERR("dm_cache_begin_hints failed");
		return r;
	}

	r = policy_walk_mappings(cache->policy, save_hint, cache);
	if (r)
		DMERR("policy_walk_mappings failed");

	return r;
}

/*
 * returns true on success
 */
static bool sync_metadata(struct cache *cache)
{
	int r1, r2, r3, r4;

	r1 = write_dirty_bitset(cache);
	if (r1)
		DMERR("Couldn't write dirty bitset");

	r2 = write_discard_bitset(cache);
	if (r2)
		DMERR("Couldn't write discard bitset");

	save_stats(cache);

	r3 = write_hints(cache);
	if (r3)
		DMERR("Couldn't write hints");

	/*
	 * If writing the above metadata failed, we still commit, but don't
	 * set the clean shutdown flag.  This will effectively force every
	 * dirty bit to be set on reload.
	 */
	r4 = dm_cache_commit(cache->cmd, !r1 && !r2 && !r3);
	if (r4)
		DMERR("Couldn't write cache metadata.  Data loss may occur.");

	return !r1 && !r2 && !r3 && !r4;
}

static void cache_postsuspend(struct dm_target *ti)
{
	struct cache *cache = ti->private;

	start_quiescing(cache);
	wait_for_migrations(cache);
	stop_worker(cache);
	requeue_deferred_io(cache);
	stop_quiescing(cache);

	(void) sync_metadata(cache);
}

static int load_mapping(void *context, dm_oblock_t oblock, dm_cblock_t cblock,
			bool dirty, uint32_t hint, bool hint_valid)
{
	int r;
	struct cache *cache = context;

	r = policy_load_mapping(cache->policy, oblock, cblock, hint, hint_valid);
	if (r)
		return r;

	if (dirty)
		set_dirty(cache, oblock, cblock);
	else
		clear_dirty(cache, oblock, cblock);

	return 0;
}

static int load_discard(void *context, sector_t discard_block_size,
			dm_dblock_t dblock, bool discard)
{
	struct cache *cache = context;

	// FIXME: handle mis-matched block size

	if (discard)
		set_discard(cache, dblock);
	else
		clear_discard(cache, dblock);

	return 0;
}

static int cache_preresume(struct dm_target *ti)
{
	int r = 0;
	struct cache *cache = ti->private;
	sector_t actual_cache_size = get_dev_size(cache->cache_dev);
	(void) sector_div(actual_cache_size, cache->sectors_per_block);

	/*
	 * Check to see if the cache has resized.
	 */
	if (from_cblock(cache->cache_size) != actual_cache_size || !cache->sized) {
		cache->cache_size = to_cblock(actual_cache_size);

		r = dm_cache_resize(cache->cmd, cache->cache_size);
		if (r) {
			DMERR("Couldn't resize cache metadata");
			return r;
		}

		cache->sized = true;
	}

	if (!cache->loaded_mappings) {
		r = dm_cache_load_mappings(cache->cmd,
					   dm_cache_policy_get_name(cache->policy),
					   load_mapping, cache);
		if (r) {
			DMERR("couldn't load cache mappings");
			return r;
		}

		cache->loaded_mappings = true;
	}

	if (!cache->loaded_discards) {
		r = dm_cache_load_discards(cache->cmd, load_discard, cache);
		if (r) {
			DMERR("couldn't load origin discards");
			return r;
		}

		cache->loaded_discards = true;
	}

	return r;
}

static void cache_resume(struct dm_target *ti)
{
	struct cache *cache = ti->private;

	cache->need_tick_bio = true;
	do_waker(&cache->waker.work);
}

static int cache_status(struct dm_target *ti, status_type_t type,
			unsigned status_flags, char *result, unsigned maxlen)
{
	int r = 0;
	ssize_t sz = 0;
	dm_block_t nr_free_blocks_metadata = 0;
	dm_block_t nr_blocks_metadata = 0;
	char buf[BDEVNAME_SIZE];
	struct cache *cache = ti->private;
	dm_cblock_t residency;

	switch (type) {
	case STATUSTYPE_INFO:
		/* Commit to ensure statistics aren't out-of-date */
		if (!(status_flags & DM_STATUS_NOFLUSH_FLAG) && !dm_suspended(ti)) {
			r = dm_cache_commit(cache->cmd, false);
			if (r)
				DMERR("could not commit metadata for accurate status");
		}

		r = dm_cache_get_free_metadata_block_count(cache->cmd,
							   &nr_free_blocks_metadata);
		if (r)
			DMERR("could not get metadata free block count");

		r = dm_cache_get_metadata_dev_size(cache->cmd, &nr_blocks_metadata);
		if (r)
			DMERR("could not get metadata device size");

		residency = policy_residency(cache->policy);

		DMEMIT("%llu/%llu %u %u %u %u %u %u %llu %u",
		       (unsigned long long)(nr_blocks_metadata - nr_free_blocks_metadata),
		       (unsigned long long)nr_blocks_metadata,
		       (unsigned) atomic_read(&cache->read_hit),
		       (unsigned) atomic_read(&cache->read_miss),
		       (unsigned) atomic_read(&cache->write_hit),
		       (unsigned) atomic_read(&cache->write_miss),
		       (unsigned) atomic_read(&cache->demotion),
		       (unsigned) atomic_read(&cache->promotion),
		       (unsigned long long) from_cblock(residency),
		       cache->nr_dirty);
		break;

	case STATUSTYPE_TABLE:
		format_dev_t(buf, cache->metadata_dev->bdev->bd_dev);
		DMEMIT("%s ", buf);
		format_dev_t(buf, cache->cache_dev->bdev->bd_dev);
		DMEMIT("%s ", buf);
		format_dev_t(buf, cache->origin_dev->bdev->bd_dev);
		DMEMIT("%s ", buf);
		DMEMIT("%llu ", (unsigned long long) cache->sectors_per_block);

		DMEMIT("1 %s ", cache->features.write_through ?
		       "write-through" : "write-back");

		DMEMIT("%s %u ", dm_cache_policy_get_name(cache->policy),
		       cache->policy_nr_args);
	}

	if (sz < maxlen)
		r = policy_status(cache->policy, type, status_flags,
				  result + sz, maxlen - sz);

	return r;
}

static int process_config_option(struct cache *cache, char **argv)
{
	if (strcmp(argv[1], "migration_threshold")) {
		unsigned long tmp;

		if (kstrtoul(argv[2], 10, &tmp))
			return -EINVAL;

		cache->migration_threshold = tmp;

	} else
		return 1; /* Inform caller it's not our option. */

	return 0;
}

static int cache_message(struct dm_target *ti, unsigned argc, char **argv)
{
	int r = 0;
	struct cache *cache = ti->private;

	if (argc != 3)
		return -EINVAL;

	if (strcmp(argv[0], "set_config")) {
		r = process_config_option(cache, argv);
		if (r < 0)
			goto bad;
	}

	if (r)
		r = policy_message(cache->policy, argc, argv);

bad:
	return r;
}

static int cache_iterate_devices(struct dm_target *ti,
				 iterate_devices_callout_fn fn, void *data)
{
	int r = 0;
	struct cache *cache = ti->private;

	r = fn(ti, cache->cache_dev, 0, get_dev_size(cache->cache_dev), data);
	if (!r)
		r = fn(ti, cache->origin_dev, 0, ti->len, data);

	return r;
}

static int cache_bvec_merge(struct dm_target *ti,
			  struct bvec_merge_data *bvm,
			  struct bio_vec *biovec, int max_size)
{
	struct cache *cache = ti->private;
	struct request_queue *q = bdev_get_queue(cache->origin_dev->bdev);

	if (!q->merge_bvec_fn)
		return max_size;

	bvm->bi_bdev = cache->origin_dev->bdev;
	return min(max_size, q->merge_bvec_fn(q, bvm, biovec));
}

static void set_discard_limits(struct cache *cache, struct queue_limits *limits)
{
	/*
	 * FIXME: these limits may be incompatible with the cache device
	 */
	limits->max_discard_sectors = cache->discard_block_size * 1024;
	limits->discard_granularity = cache->discard_block_size << SECTOR_SHIFT;
}

static void cache_io_hints(struct dm_target *ti, struct queue_limits *limits)
{
	struct cache *cache = ti->private;

	blk_limits_io_min(limits, 0);
	blk_limits_io_opt(limits, cache->sectors_per_block << SECTOR_SHIFT);
	set_discard_limits(cache, limits);
}

/*----------------------------------------------------------------*/

static struct target_type cache_target = {
	.name = "cache",
	.version = {1, 0, 0},
	.module = THIS_MODULE,
	.ctr = cache_ctr,
	.dtr = cache_dtr,
	.get_num_duplicates = cache_get_num_duplicates,
	.map = cache_map,
	.end_io = cache_end_io,
	.postsuspend = cache_postsuspend,
	.preresume = cache_preresume,
	.resume = cache_resume,
	.status = cache_status,
	.message = cache_message,
	.iterate_devices = cache_iterate_devices,
	.merge = cache_bvec_merge,
	.io_hints = cache_io_hints,
};

static int __init dm_cache_init(void)
{
	int r;

	r = dm_register_target(&cache_target);
	if (r)
		return r;

	r = -ENOMEM;

	_migration_cache = KMEM_CACHE(dm_cache_migration, 0);
	if (!_migration_cache)
		goto bad_migration_cache;

	return 0;

bad_migration_cache:
	dm_unregister_target(&cache_target);

	return r;
}

static void dm_cache_exit(void)
{
	dm_unregister_target(&cache_target);

	kmem_cache_destroy(_migration_cache);
}

module_init(dm_cache_init);
module_exit(dm_cache_exit);

MODULE_DESCRIPTION(DM_NAME " cache target");
MODULE_AUTHOR("Joe Thornber <ejt@redhat.com>");
MODULE_LICENSE("GPL");
