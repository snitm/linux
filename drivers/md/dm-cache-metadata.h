/*
 * Copyright (C) 2012 Red Hat, Inc.
 *
 * This file is released under the GPL.
 */

#ifndef DM_CACHE_METADATA_H
#define DM_CACHE_METADATA_H

#include "persistent-data/dm-block-manager.h"

/*----------------------------------------------------------------*/

#define CACHE_METADATA_BLOCK_SIZE 4096

/* FIXME: remove this restriction */
/*
 * The metadata device is currently limited in size.
 *
 * We have one block of index, which can hold 255 index entries.  Each
 * index entry contains allocation info about 16k metadata blocks.
 */
#define CACHE_METADATA_MAX_SECTORS (255 * (1 << 14) * (CACHE_METADATA_BLOCK_SIZE / (1 << SECTOR_SHIFT)))

enum superblock_flag_bits {
	__CACHE_DIRTY,	    /* cache policy data is not to be trusted on resume */
};

#define CACHE_DIRTY		(1 << __CACHE_DIRTY)

/*
 * Compat feature flags.  Any incompat flags beyond the ones
 * specified below will prevent use of the thin metadata.
 */
#define CACHE_FEATURE_COMPAT_SUPP	  0UL
#define CACHE_FEATURE_COMPAT_RO_SUPP	  0UL
#define CACHE_FEATURE_INCOMPAT_SUPP	  0UL

/*
 * Reopens or creates a new, empty metadata volume.
 * Returns an ERR_PTR on failure.
 */
struct dm_cache_metadata *dm_cache_metadata_open(struct block_device *bdev,
						 sector_t data_block_size,
						 bool may_format_device);

int dm_cache_metadata_close(struct dm_cache_metadata *cmd);

/*
 * The metadata needs to know how many cache blocks there are.  We're dont
 * care about the origin, assuming the core target is giving us valid
 * origin blocks to map to.
 */
int dm_cache_resize(struct dm_cache_metadata *cmd, dm_block_t new_cache_size);
dm_block_t dm_cache_size(struct dm_cache_metadata *cmd);

int dm_cache_remove_mapping(struct dm_cache_metadata *cmd, dm_block_t cblock);
int dm_cache_insert_mapping(struct dm_cache_metadata *cmd, dm_block_t cblock, dm_block_t oblock);
int dm_cache_changed_this_transaction(struct dm_cache_metadata *cmd);

typedef int (*load_mapping_fn)(void *context, dm_block_t oblock, dm_block_t cblock);
int dm_cache_load_mappings(struct dm_cache_metadata *cmd,
			   load_mapping_fn fn,
			   void *context);

int dm_cache_read_superblock_flags(struct dm_cache_metadata *cmd, unsigned *flags);

int dm_cache_commit_with_flags(struct dm_cache_metadata *cmd, unsigned *flags);

int dm_cache_commit(struct dm_cache_metadata *cmd);

int dm_cache_abort_metadata(struct dm_cache_metadata *cmd);

bool dm_cache_aborted_changes(struct dm_cache_metadata *cmd);

int dm_cache_get_free_metadata_block_count(struct dm_cache_metadata *cmd,
					   dm_block_t *result);

int dm_cache_get_metadata_dev_size(struct dm_cache_metadata *cmd,
				   dm_block_t *result);

/*
 * Flicks the underlying block manager into read only mode, so you know
 * that nothing is changing.
 */
void dm_cache_metadata_read_only(struct dm_cache_metadata *cmd);

void dm_cache_dump(struct dm_cache_metadata *cmd);

/*----------------------------------------------------------------*/

#endif
