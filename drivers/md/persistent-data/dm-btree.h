#ifndef DM_BTREE_H
#define DM_BTREE_H

#include "dm-transaction-manager.h"

/*----------------------------------------------------------------*/

/*
 * Manipulates hierarchical B+ trees with 64bit keys and arbitrary sized
 * values.
 */

/*
 * The btree needs some knowledge about the values stored within it.  This
 * is provided by a |btree_value_type| structure.
 */
struct dm_btree_value_type {
	void *context;

	/*
	 * The size in bytes of each value.
	 */
	uint32_t size;

	/*
	 * Any of these methods can be safely set to NULL if you do not
	 * need this feature.
	 */

	/*
	 * The btree is making a duplicate of the value, for instance
	 * because previously shared btree nodes have now diverged.  The
	 * |value| argument is the new copy, the copy function may modify
	 * it.  Probably it just wants to increment a reference count
	 * somewhere.  This method is _not_ called for insertion of a new
	 * value, it's assumed the ref count is already 1.
	 */
	void (*inc)(void *context, void *value);

	/*
	 * This value is being deleted.  The btree takes care of freeing
	 * the memory pointed to by |value|.  Often the |del| function just
	 * needs to decrement a reference count somewhere.
	 */
	void (*dec)(void *context, void *value);

	/*
	 * An test for equality between two values.  When a value is
	 * overwritten with a new one the old one has the |dec| method
	 * called, _unless_ the new and old value are deemed equal.
	 */
	int (*equal)(void *context, void *value1, void *value2);
};

/*
 * The |btree_info| structure describes the shape and contents of a btree.
 */
struct dm_btree_info {
	struct dm_transaction_manager *tm;

	/* number of nested btrees (not the depth of a single tree). */
	unsigned levels;
	struct dm_btree_value_type value_type;
};

/* Set up an empty tree.  O(1). */
int dm_btree_empty(struct dm_btree_info *info, dm_block_t *root);

/*
 * Delete a tree.  O(n) - this is the slow one!  It can also block, so
 * please don't call it on an io path.
 */
int dm_btree_del(struct dm_btree_info *info, dm_block_t root);

/*
 * Delete part of a tree.  This is really specific to truncation of
 * multisnap devs.  It only removes keys from the bottom level btree that
 * are greater than key[info->levels - 1].
 */
int dm_btree_del_gt(struct dm_btree_info *info, dm_block_t root, uint64_t *key,
		    dm_block_t *new_root);

/*
 * All the lookup functions return -ENODATA if the key cannot be found.
 */

/* Tries to find a key that matches exactly.  O(ln(n)) */
int dm_btree_lookup(struct dm_btree_info *info, dm_block_t root,
		    uint64_t *keys, void *value);

/*
 * Find the greatest key that is less than or equal to that requested.  A
 * ENODATA result indicates the key would appear in front of all (possibly
 * zero) entries.  O(ln(n))
 */
int dm_btree_lookup_le(struct dm_btree_info *info, dm_block_t root,
		       uint64_t *keys, uint64_t *rkey, void *value);

/*
 * Find the least key that is greater than or equal to that requested.
 * ENODATA indicates all the keys are below.  O(ln(n))
 */
int dm_btree_lookup_ge(struct dm_btree_info *info, dm_block_t root,
		       uint64_t *keys, uint64_t *rkey, void *value);

/*
 * Insertion (or overwrite an existing value).
 * O(ln(n))
 */
int dm_btree_insert(struct dm_btree_info *info, dm_block_t root,
		    uint64_t *keys, void *value, dm_block_t *new_root);

/*
 * A variant of insert that indicates whether it actually inserted or just
 * overwrote.  Useful if you're keeping track of the number of entries in a
 * tree.
 */
int dm_btree_insert_notify(struct dm_btree_info *info, dm_block_t root,
			   uint64_t *keys, void *value, dm_block_t *new_root,
			   int *inserted);

/*
 * Remove a key if present.  This doesn't remove empty sub trees.  Normally
 * subtrees represent a separate entity, like a snapshot map, so this is
 * correct behaviour.
 * O(ln(n)).
 * Returns ENODATA if the key isn't present.
 */
int dm_btree_remove(struct dm_btree_info *info, dm_block_t root,
		    uint64_t *keys, dm_block_t *new_root);

/* Clone a tree. O(1) */
int dm_btree_clone(struct dm_btree_info *info, dm_block_t root,
		   dm_block_t *clone);

/*
 * Returns < 0 on failure.  Otherwise the number of key entries that have
 * been filled out.  Remember trees can have zero entries, and as such have
 * no highest key.
 */
int dm_btree_find_highest_key(struct dm_btree_info *info, dm_block_t root,
			      uint64_t *result_keys);

/*----------------------------------------------------------------*/

#endif