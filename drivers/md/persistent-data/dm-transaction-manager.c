#include "dm-transaction-manager.h"
#include "dm-space-map-disk.h"
#include "dm-space-map-metadata.h"

#include <linux/slab.h>
#include <linux/device-mapper.h> /* For DMERR */

#define DM_MSG_PREFIX "transaction manager"

/*----------------------------------------------------------------*/

struct shadow_info {
	struct hlist_node hlist;
	dm_block_t where;
};

/* it would be nice if we scaled with the size of transaction */
#define HASH_SIZE 256
#define HASH_MASK (HASH_SIZE - 1)
struct dm_transaction_manager {
	int is_clone;
	struct dm_transaction_manager *real;

	struct dm_block_manager *bm;
	struct dm_space_map *sm;

	struct hlist_head buckets[HASH_SIZE];

	/* stats */
	unsigned shadow_count;
};

/*----------------------------------------------------------------*/

/* FIXME: similar code in block-manager */
static unsigned hash_block(dm_block_t b)
{
	const unsigned BIG_PRIME = 4294967291UL;
	return (((unsigned) b) * BIG_PRIME) & HASH_MASK;
}

static int is_shadow(struct dm_transaction_manager *tm, dm_block_t b)
{
	unsigned bucket = hash_block(b);
	struct shadow_info *si;
	struct hlist_node *n;

	hlist_for_each_entry(si, n, tm->buckets + bucket, hlist)
		if (si->where == b)
			return 1;

	return 0;
}

/*
 * This can silently fail if there's no memory.  We're ok with this since
 * creating redundant shadows causes no harm.
 */
static void insert_shadow(struct dm_transaction_manager *tm, dm_block_t b)
{
	unsigned bucket;
	struct shadow_info *si;

	si = kmalloc(sizeof(*si), GFP_NOIO);
	if (si) {
		si->where = b;
		bucket = hash_block(b);
		hlist_add_head(&si->hlist, tm->buckets + bucket);
	}
}

static void wipe_shadow_table(struct dm_transaction_manager *tm)
{
	int i;
	for (i = 0; i < HASH_SIZE; i++) {
		struct shadow_info *si;
		struct hlist_node *n, *tmp;
		struct hlist_head *bucket = tm->buckets + i;
		hlist_for_each_entry_safe(si, n, tmp, bucket, hlist)
			kfree(si);

		INIT_HLIST_HEAD(bucket);
	}

	tm->shadow_count = 0;
}

/*----------------------------------------------------------------*/

struct dm_transaction_manager *dm_tm_create(struct dm_block_manager *bm,
					    struct dm_space_map *sm)
{
	int i;
	struct dm_transaction_manager *tm;

	tm = kmalloc(sizeof(*tm), GFP_KERNEL);
	if (!tm)
		return ERR_PTR(-ENOMEM);

	tm->is_clone = 0;
	tm->real = NULL;
	tm->bm = bm;
	tm->sm = sm;

	for (i = 0; i < HASH_SIZE; i++)
		INIT_HLIST_HEAD(tm->buckets + i);

	tm->shadow_count = 0;

	return tm;
}
EXPORT_SYMBOL_GPL(dm_tm_create);

struct dm_transaction_manager *
dm_tm_create_non_blocking_clone(struct dm_transaction_manager *real)
{
	struct dm_transaction_manager *tm;

	tm = kmalloc(sizeof(*tm), GFP_KERNEL);
	if (tm) {
		tm->is_clone = 1;
		tm->real = real;
	}

	return tm;
}
EXPORT_SYMBOL_GPL(dm_tm_create_non_blocking_clone);

void dm_tm_destroy(struct dm_transaction_manager *tm)
{
	kfree(tm);
}
EXPORT_SYMBOL_GPL(dm_tm_destroy);

int dm_tm_reserve_block(struct dm_transaction_manager *tm, dm_block_t b)
{
	if (tm->is_clone)
		return -EWOULDBLOCK;

	return dm_sm_inc_block(tm->sm, b);
}
EXPORT_SYMBOL_GPL(dm_tm_reserve_block);

int dm_tm_begin(struct dm_transaction_manager *tm)
{
	return 0;
}
EXPORT_SYMBOL_GPL(dm_tm_begin);

int dm_tm_pre_commit(struct dm_transaction_manager *tm)
{
	int r;

	if (tm->is_clone)
		return -EWOULDBLOCK;

	r = dm_sm_commit(tm->sm);
	if (r < 0)
		return r;

	return 0;
}
EXPORT_SYMBOL_GPL(dm_tm_pre_commit);

int dm_tm_commit(struct dm_transaction_manager *tm, struct dm_block *root)
{
	if (tm->is_clone)
		return -EWOULDBLOCK;

	wipe_shadow_table(tm);
	return dm_bm_flush_and_unlock(tm->bm, root);
}
EXPORT_SYMBOL_GPL(dm_tm_commit);

int dm_tm_alloc_block(struct dm_transaction_manager *tm, dm_block_t *new_block)
{
	if (tm->is_clone)
		return -EWOULDBLOCK;

	return dm_sm_new_block(tm->sm, new_block);
}
EXPORT_SYMBOL_GPL(dm_tm_alloc_block);

int dm_tm_new_block(struct dm_transaction_manager *tm,
		    struct dm_block_validator *v,
		    struct dm_block **result)
{
	int r;
	dm_block_t new_block;

	if (tm->is_clone)
		return -EWOULDBLOCK;

	r = dm_sm_new_block(tm->sm, &new_block);
	if (r < 0)
		return r;

	r = dm_bm_write_lock_zero(tm->bm, new_block, v, result);
	if (r < 0) {
		dm_sm_dec_block(tm->sm, new_block);
		return r;
	}

	/*
	 * New blocks count as shadows, in that they don't need to be
	 * shadowed again.
	 */
	insert_shadow(tm, new_block);

	return 0;
}
EXPORT_SYMBOL_GPL(dm_tm_new_block);

static int __shadow_block(struct dm_transaction_manager *tm, dm_block_t orig,
			  struct dm_block_validator *v,
			  struct dm_block **result, int *inc_children)
{
	int r;
	dm_block_t new;
	uint32_t count;
	struct dm_block *orig_block;

	r = dm_sm_new_block(tm->sm, &new);
	if (r < 0)
		return r;

	r = dm_bm_write_lock_zero(tm->bm, new, v, result);
	if (r < 0) {
		dm_sm_dec_block(tm->sm, new);
		return r;
	}

	r = dm_bm_read_lock(tm->bm, orig, v, &orig_block);
	if (r < 0) {
		dm_sm_dec_block(tm->sm, new);
		return r;
	}
	memcpy(dm_block_data(*result), dm_block_data(orig_block),
	       dm_bm_block_size(tm->bm));
	r = dm_bm_unlock(orig_block);
	if (r < 0) {
		dm_sm_dec_block(tm->sm, new);
		return r;
	}

	r = dm_sm_get_count(tm->sm, orig, &count);
	if (r < 0) {
		dm_sm_dec_block(tm->sm, new);
		dm_bm_unlock(*result);
		return r;
	}

	r = dm_sm_dec_block(tm->sm, orig);
	if (r < 0) {
		dm_sm_dec_block(tm->sm, new);
		dm_bm_unlock(*result);
		return r;
	}

	*inc_children = count > 1;
	return 0;
}

int dm_tm_shadow_block(struct dm_transaction_manager *tm, dm_block_t orig,
		       struct dm_block_validator *v, struct dm_block **result,
		       int *inc_children)
{
	int r, more_than_one;

	if (tm->is_clone)
		return -EWOULDBLOCK;

	if (is_shadow(tm, orig)) {
		r = dm_sm_count_is_more_than_one(tm->sm, orig, &more_than_one);
		if (r < 0)
			return r;

		if (!more_than_one) {
			*inc_children = 0;
			return dm_bm_write_lock(tm->bm, orig, v, result);
		}
		/* fall through */
	}

	r = __shadow_block(tm, orig, v, result, inc_children);
	if (r < 0)
		return r;
	tm->shadow_count++;
	insert_shadow(tm, dm_block_location(*result));

	return r;
}
EXPORT_SYMBOL_GPL(dm_tm_shadow_block);

int dm_tm_read_lock(struct dm_transaction_manager *tm, dm_block_t b,
		    struct dm_block_validator *v,
		    struct dm_block **blk)
{
	if (tm->is_clone)
		return dm_bm_read_try_lock(tm->real->bm, b, v, blk);

	return dm_bm_read_lock(tm->bm, b, v, blk);
}
EXPORT_SYMBOL_GPL(dm_tm_read_lock);

int dm_tm_unlock(struct dm_transaction_manager *tm, struct dm_block *b)
{
	return dm_bm_unlock(b);
}
EXPORT_SYMBOL_GPL(dm_tm_unlock);

void dm_tm_inc(struct dm_transaction_manager *tm, dm_block_t b)
{
	BUG_ON(tm->is_clone);
	dm_sm_inc_block(tm->sm, b);
}
EXPORT_SYMBOL_GPL(dm_tm_inc);

void dm_tm_dec(struct dm_transaction_manager *tm, dm_block_t b)
{
	BUG_ON(tm->is_clone);
	dm_sm_dec_block(tm->sm, b);
}
EXPORT_SYMBOL_GPL(dm_tm_dec);

int dm_tm_ref(struct dm_transaction_manager *tm, dm_block_t b,
	      uint32_t *result)
{
	if (tm->is_clone)
		return -EWOULDBLOCK;

	return dm_sm_get_count(tm->sm, b, result);
}
EXPORT_SYMBOL_GPL(dm_tm_ref);

struct dm_block_manager *dm_tm_get_bm(struct dm_transaction_manager *tm)
{
	BUG_ON(tm->is_clone);
	return tm->bm;
}
EXPORT_SYMBOL_GPL(dm_tm_get_bm);

/*----------------------------------------------------------------*/

static int dm_tm_create_internal(struct dm_block_manager *bm,
				 dm_block_t sb_location,
				 struct dm_block_validator *sb_validator,
				 size_t root_offset, size_t root_max_len,
				 struct dm_transaction_manager **tm,
				 struct dm_space_map **sm,
				 struct dm_block **sblock,
				 int create)
{
	int r;

	*sm = dm_sm_metadata_init();
	if (IS_ERR(*sm))
		return PTR_ERR(*sm);

	*tm = dm_tm_create(bm, *sm);
	if (IS_ERR(*tm)) {
		dm_sm_destroy(*sm);
		return -1;
	}

	r = dm_tm_begin(*tm);
	if (r < 0)
		goto bad1;

	if (create) {
		r = dm_bm_write_lock_zero(dm_tm_get_bm(*tm), sb_location,
					  sb_validator, sblock);
		if (r < 0) {
			DMERR("couldn't lock superblock");
			goto bad1;
		}

		r = dm_sm_metadata_create(*sm, *tm, dm_bm_nr_blocks(bm),
					  sb_location);
		if (r) {
			DMERR("couldn't create metadata space map");
			goto bad2;
		}

	} else {
		r = dm_bm_write_lock(dm_tm_get_bm(*tm), sb_location,
				     sb_validator, sblock);
		if (r < 0) {
			DMERR("couldn't lock superblock");
			goto bad1;
		}

		r = dm_sm_metadata_open(*sm, *tm,
					dm_block_data(*sblock) + root_offset,
					root_max_len);
		if (IS_ERR(*sm)) {
			DMERR("couldn't open metadata space map");
			goto bad2;
		}
	}

	r = dm_tm_begin(*tm);
	if (r < 0)
		goto bad2;

	return 0;

bad2:
	dm_tm_unlock(*tm, *sblock);
bad1:
	dm_tm_destroy(*tm);
	dm_sm_destroy(*sm);
	return r;
}

int dm_tm_create_with_sm(struct dm_block_manager *bm, dm_block_t sb_location,
			 struct dm_block_validator *sb_validator,
			 struct dm_transaction_manager **tm,
			 struct dm_space_map **sm, struct dm_block **sblock)
{
	return dm_tm_create_internal(bm, sb_location, sb_validator,
				     0, 0, tm, sm, sblock, 1);
}
EXPORT_SYMBOL_GPL(dm_tm_create_with_sm);

int dm_tm_open_with_sm(struct dm_block_manager *bm, dm_block_t sb_location,
		       struct dm_block_validator *sb_validator,
		       size_t root_offset, size_t root_max_len,
		       struct dm_transaction_manager **tm,
		       struct dm_space_map **sm, struct dm_block **sblock)
{
	return dm_tm_create_internal(bm, sb_location, sb_validator, root_offset,
				     root_max_len, tm, sm, sblock, 0);
}
EXPORT_SYMBOL_GPL(dm_tm_open_with_sm);

unsigned dm_tm_shadow_count(struct dm_transaction_manager *tm)
{
	return tm->shadow_count;
}
EXPORT_SYMBOL_GPL(dm_tm_shadow_count);
/*----------------------------------------------------------------*/