/*
 *
 * Copyright (C) 2012 Red Hat. All rights reserved.
 *
 * basic/fifo/filo/lru/mru/lfu/mfu/lfu_ws/mfu_ws/random/multiqueue/multiqueue_ws/q2/twoqueue cache replacement policies.
 *
 * This file is released under the GPL.
 */

#include "dm-cache-policy.h"
#include "dm.h"

#include <linux/btree.h>
#include <linux/hash.h>
#include <linux/list.h>
#include <linux/module.h>
#include <linux/random.h>
#include <linux/slab.h>

/* "multiqueue" policy defines. */
#define	MQ_QUEUE_TMO	(10UL * HZ)	/* MQ_QUEUE_TMO seconds queue maximum lifetime per entry. */

/* Cache input queue defines. */
#define	READ_PROMOTE_THRESHOLD	1U			/* Minimum read cache in queue promote per element threshold. */
#define	WRITE_PROMOTE_THRESHOLD	4U			/* Minimum write cache in queue promote per element threshold. */
#define DISCARDED_PROMOTE_THRESHOLD 1U	/* The target has discarded the block -> lowest promotion prioritiy. */

/*----------------------------------------------------------------------------*/
/*
 * Large, sequential ios are probably better left on the origin device since
 * spindles tend to have good bandwidth.
 *
 * The io_tracker tries to spot when the io is in
 * one of these sequential modes.
 */
#define RANDOM_THRESHOLD 1
#define SEQUENTIAL_THRESHOLD 2

enum io_pattern {
	PATTERN_SEQUENTIAL,
	PATTERN_RANDOM
};

struct io_tracker {
	sector_t next_start_osector, nr_seq_sectors;

	unsigned nr_rand_samples;
	enum io_pattern pattern;
};

static void iot_init(struct io_tracker *t)
{
	t->pattern = PATTERN_RANDOM;
	t->nr_seq_sectors = t->nr_rand_samples = t->next_start_osector = 0;
}

static bool iot_sequential_pattern(struct io_tracker *t)
{
	return t->pattern == PATTERN_SEQUENTIAL;
}

static void iot_update_stats(struct io_tracker *t, struct bio *bio)
{
	sector_t sectors = bio_sectors(bio);

	if (bio->bi_sector == t->next_start_osector) {
		t->nr_seq_sectors += sectors;

	} else {
		/*
		 * Just one non-sequential IO is
		 * enough to reset the counters.
		 */
		if (t->nr_seq_sectors)
			t->nr_seq_sectors = t->nr_rand_samples = 0;

		t->nr_rand_samples++;
	}

	t->next_start_osector = bio->bi_sector + sectors;
}

static void iot_check_for_pattern_switch(struct io_tracker *t, sector_t block_size)
{
	bool reset = iot_sequential_pattern(t) ? (t->nr_rand_samples >= RANDOM_THRESHOLD) :
		                                 (t->nr_seq_sectors >= SEQUENTIAL_THRESHOLD * block_size);
	if (reset)
		t->nr_seq_sectors = t->nr_rand_samples = 0;
}

/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------*/

/* The common cache entry part for all policies. */
struct common_entry {
	struct hlist_node hlist;
	struct list_head list;
	dm_block_t oblock;
	unsigned count[2][2], tick;
};

/* Cache entry struct. */
struct basic_cache_entry {
	struct common_entry ce;

	dm_block_t cblock;
	unsigned long access, expire;
	unsigned saved;
};

/* Pre and post cache queue entry. */
struct track_queue_entry {
	struct common_entry ce;
};

enum policy_type {
	P_fifo,
	P_filo,
	P_lru,
	P_mru,
	P_lfu,
	P_mfu,
	P_lfu_ws,
	P_mfu_ws,
	P_random,
	P_multiqueue,
	P_multiqueue_ws,
	P_q2,
	P_twoqueue,
	P_basic	/* The default selecting one of the above. */
};

struct policy;
typedef void (*queue_add_fn)(struct policy *, struct list_head *);
typedef void (*queue_del_fn)(struct policy *, struct list_head *);
typedef struct list_head * (*queue_evict_fn)(struct policy *);

struct queue_fns {
	queue_add_fn add;
	queue_del_fn del;
	queue_evict_fn evict;
};

#define	IS_LFU(p)			(p->queues.fns->add == &queue_add_lfu)
#define	IS_MULTIQUEUE(p)		(p->queues.fns->evict == &queue_evict_multiqueue)
#define	IS_Q2(p)			(p->queues.fns->add == &queue_add_q2)
#define	IS_TWOQUEUE(p)			(p->queues.fns->add == &queue_add_twoqueue)

#define	IS_FIFO_FILO(p)			(p->queues.fns->del == &queue_del_fifo_filo)
#define	IS_Q2_TWOQUEUE(p)		(p->queues.fns->evict == &queue_evict_q2_twoqueue)
#define	IS_MULTIQUEUE_Q2_TWOQUEUE(p)	(p->queues.fns->del == &queue_del_multiqueue)
#define	IS_LFU_MFU_WS(p)		(p->queues.fns->del == &queue_del_lfu_mfu)

static unsigned next_power(unsigned n, unsigned min)
{
	return roundup_pow_of_two(max(n, min));
}

struct hash {
	struct hlist_head *table;
	dm_block_t hash_bits;
	unsigned nr_buckets;
};

enum count_type {
	T_HITS,
	T_SECTORS
};
struct track_queue {
	struct hash hash;
	struct track_queue_entry *elts;
	struct list_head used, free;
	unsigned count[2][2], size, nr_elts;
};

struct policy {
	struct dm_cache_policy policy;
	struct mutex lock;

	struct io_tracker tracker;

	sector_t origin_size, block_size;
	unsigned calc_threshold_hits, promote_threshold[2], hits;

	struct {
		struct queue_fns *fns;

		union {
			struct list_head *mq;
			struct list_head used;
		} u;

		/* Pre- and post-cache queues. */
		struct track_queue pre, post;
		enum count_type ctype;

		/*
		 * FIXME:
		 * mempool based kernel lib btree used for lfu,mfu,lfu_ws and mfu_ws
		 *
		 * Now preallocating all objects on creation in order to avoid OOM deadlock.
		 *
		 * Replace with priority heap.
		 */
		struct btree_head32 fu_head;
		mempool_t *fu_pool;

		unsigned mqueues, q0_size, twoqueue_q0_max_elts;
		struct list_head free; /* Free cache entry list; used for all policies. */
	} queues;

	/* MINORME: allocate only for multiqueue? */
	unsigned long jiffies;

	struct {
		atomic_t t_ext;
		unsigned t_int;
	} tick;

	/*
	 * We know exactly how many cblocks will be needed, so we can
	 * allocate them up front.
	 */
	/* FIXME: unify with track_queue? */
	dm_block_t cache_size;
	unsigned cache_nr_words;
	struct basic_cache_entry *cblocks;
	struct hash chash;
	unsigned cache_count[2][2];

	/* Cache entry allocation bitset. */
	unsigned long *allocation_bitset;
	dm_block_t nr_cblocks_allocated;
};

/*----------------------------------------------------------------------------*/
/* Low-level queue functions. */
static struct policy *to_policy(struct dm_cache_policy *p)
{
	return container_of(p, struct policy, policy);
}

static void queue_init(struct list_head *q)
{
	INIT_LIST_HEAD(q);
}

static bool queue_empty(struct list_head *q)
{
	return list_empty(q);
}

static void queue_add(struct list_head *q, struct list_head *elt)
{
	list_add(elt, q);
}

static void queue_add_tail(struct list_head *q, struct list_head *elt)
{
	list_add_tail(elt, q);
}

static void queue_del(struct list_head *elt)
{
	list_del(elt);
}

static struct list_head *queue_pop(struct list_head *q)
{
	struct list_head *r = q->next;

	BUG_ON(!r);
	list_del(r);

	return r;
}

static void queue_move_tail(struct list_head *q, struct list_head *elt)
{
	list_move_tail(elt, q);
}

static bool updated_this_tick(struct policy *p, struct common_entry *ce)
{
	return ce->tick == p->tick.t_int;
}

/*----------------------------------------------------------------------------*/

/* Allocate/free various resources. */
static int alloc_hash(struct hash *hash, unsigned elts)
{
	hash->nr_buckets = next_power(elts >> 4, 16);
	hash->hash_bits = ffs(hash->nr_buckets) - 1;
	hash->table = vzalloc(sizeof(*hash->table) * hash->nr_buckets);

	return hash->table ? 0 : -ENOMEM;
}

static void free_hash(struct hash *hash)
{
	vfree(hash->table);
}

static int alloc_cache_blocks_with_hash(struct policy *p, dm_block_t cache_size)
{
	int r = -ENOMEM;

	p->cblocks = vzalloc(sizeof(*p->cblocks) * cache_size);
	if (p->cblocks) {
		queue_init(&p->queues.free);

		while (cache_size--)
			queue_add(&p->queues.free, &p->cblocks[cache_size].ce.list);

		p->nr_cblocks_allocated = 0;

		/* Cache entries hash. */
		r = alloc_hash(&p->chash, cache_size);
		if (r)
			vfree(p->cblocks);
	}

	return r;
}

static void free_cache_blocks_and_hash(struct policy *p)
{
	free_hash(&p->chash);
	vfree(p->cblocks);
}

static int alloc_track_queue_with_hash(struct track_queue *q, unsigned elts)
{
	int r;
	unsigned u;

	queue_init(&q->free);
	queue_init(&q->used);

	q->elts = vzalloc(sizeof(*q->elts) * elts);
	if (!q->elts)
		return -ENOMEM;

	u = q->nr_elts = elts;
	while (u--)
		queue_add(&q->free, &q->elts[u].ce.list);

	r = alloc_hash(&q->hash, elts);
	if (r) {
		vfree(q->elts);
		return -ENOMEM;
	}

	return 0;
}

static void free_track_queue(struct track_queue *q)
{
	free_hash(&q->hash);
	vfree(q->elts);
}

static int alloc_multiqueues(struct policy *p, unsigned mqueues)
{
	/* Multiqueue heads. */
	p->queues.mqueues = mqueues;
	p->queues.u.mq = vzalloc(sizeof(*p->queues.u.mq) * mqueues);
	if (!p->queues.u.mq)
		return -ENOMEM;

	while (mqueues--)
		queue_init(&p->queues.u.mq[mqueues]);

	return 0;
}

static void free_multiqueues(struct policy *p)
{
	vfree(p->queues.u.mq);
}

static struct basic_cache_entry *alloc_cache_entry(struct policy *p)
{
	struct basic_cache_entry *e;

	BUG_ON(p->nr_cblocks_allocated >= p->cache_size);

	e = list_entry(queue_pop(&p->queues.free), struct basic_cache_entry, ce.list);
	memset(&e->ce.count, 0, sizeof(e->ce.count));
	p->nr_cblocks_allocated++;

	return e;
}

static void alloc_cblock(struct policy *p, dm_block_t cblock)
{
	BUG_ON(cblock >= p->cache_size);
	BUG_ON(test_bit(cblock, p->allocation_bitset));
	set_bit(cblock, p->allocation_bitset);
}

static void free_cblock(struct policy *p, dm_block_t cblock)
{
	BUG_ON(cblock >= p->cache_size);
	BUG_ON(!test_bit(cblock, p->allocation_bitset));
	clear_bit(cblock, p->allocation_bitset);
}

static void queue_add_twoqueue(struct policy *p, struct list_head *elt);
static bool any_free_cblocks(struct policy *p)
{
	if (IS_TWOQUEUE(p)) {
		/*
		 * Only allow a certain amount of the total cache size in queue 0
		 * (cblocks with hit count 1).
		 */
		if (p->queues.q0_size == p->queues.twoqueue_q0_max_elts)
			return false;
	}

	return !queue_empty(&p->queues.free);
}

/*----------------------------------------------------------------*/

static unsigned long *alloc_bitset(unsigned nr_cblocks)
{
	return vzalloc(sizeof(unsigned long) * dm_sector_div_up(nr_cblocks, BITS_PER_LONG));
}

static void free_bitset(unsigned long *bits)
{
	vfree(bits);
}
/*----------------------------------------------------------------------------*/

/* Hash functions (lookup, insert, remove). */
static struct common_entry *__lookup_common_entry(struct hash *hash, dm_block_t oblock)
{
	unsigned h = hash_64(oblock, hash->hash_bits);
	struct common_entry *cur;
	struct hlist_node *tmp;
	struct hlist_head *bucket = &hash->table[h];

	hlist_for_each_entry(cur, tmp, bucket, hlist) {
		if (cur->oblock == oblock) {
			/* Move upfront bucket for faster access. */
			hlist_del(&cur->hlist);
			hlist_add_head(&cur->hlist, bucket);
			return cur;
		}
	}

	return NULL;
}

static struct basic_cache_entry *lookup_cache_entry(struct policy *p, dm_block_t oblock)
{
	struct common_entry *ce = __lookup_common_entry(&p->chash, oblock);

	return ce ? container_of(ce, struct basic_cache_entry, ce) : NULL;
}

static void insert_cache_hash_entry(struct policy *p, struct basic_cache_entry *e)
{
	unsigned h = hash_64(e->ce.oblock, p->chash.hash_bits);

	hlist_add_head(&e->ce.hlist, &p->chash.table[h]);
}

static void remove_cache_hash_entry(struct policy *p, struct basic_cache_entry *e)
{
	hlist_del(&e->ce.hlist);
}

/* Cache track queue. */
static struct track_queue_entry *lookup_track_queue_entry(struct track_queue *q, dm_block_t oblock)
{
	struct common_entry *ce = __lookup_common_entry(&q->hash, oblock);

	return ce ? container_of(ce, struct track_queue_entry, ce) : NULL;
}

static void insert_track_queue_hash_entry(struct track_queue *q, struct track_queue_entry *tqe)
{
	unsigned h = hash_64(tqe->ce.oblock, q->hash.hash_bits);

	hlist_add_head(&tqe->ce.hlist, &q->hash.table[h]);
}

static void remove_track_queue_hash_entry(struct track_queue_entry *tqe)
{
	hlist_del(&tqe->ce.hlist);
}
/*----------------------------------------------------------------------------*/


/* Out of cache queue support functions. */
static struct track_queue_entry *pop_track_queue(struct track_queue *q)
{
	struct track_queue_entry *r;

	if (queue_empty(&q->free)) {
		unsigned t, u, end = ARRAY_SIZE(r->ce.count[T_HITS]);

		BUG_ON(queue_empty(&q->used));
		r = list_entry(queue_pop(&q->used), struct track_queue_entry, ce.list);
		remove_track_queue_hash_entry(r);
		q->size--;

		for (t = 0; t < end; t++) {
			for (u = 0; u < end; u++) {
				q->count[t][u] -= r->ce.count[t][u];
				r->ce.count[t][u] = 0;
			}
		}

	} else
		r = list_entry(queue_pop(&q->free), struct track_queue_entry, ce.list);

	return r;
}

/* Retrieve track entry from free list _or_ evict one from track queue. */
static struct track_queue_entry *pop_add_and_insert_track_queue_entry(struct track_queue *q, dm_block_t oblock)
{
	struct track_queue_entry *r = pop_track_queue(q);

	r->ce.oblock = oblock;
	queue_add_tail(&q->used, &r->ce.list);
	insert_track_queue_hash_entry(q, r);
	q->size++;

	return r;
}

static unsigned ctype_threshold(struct policy *p, unsigned th)
{
	return th * (p->queues.ctype == T_HITS ? 1 : p->block_size);
}

#define	MAX_EVALUATE_ENTRIES	15
static void calc_rw_threshold(struct policy *p)
{
        if (++p->hits >= p->calc_threshold_hits && !any_free_cblocks(p)) {
		unsigned nr = 0;
		struct track_queue *pre_q = &p->queues.pre, *post_q = &p->queues.post;
		struct track_queue_entry *tqe;

		p->hits = p->promote_threshold[0] = p->promote_threshold[1] = 0;

		list_for_each_entry(tqe, &pre_q->used, ce.list) {
                	p->promote_threshold[0] += pre_q->count[p->queues.ctype][0];
                	p->promote_threshold[1] += pre_q->count[p->queues.ctype][1];

			if (++nr >= MAX_EVALUATE_ENTRIES)
				break;
		}

		if (nr) {
			p->promote_threshold[0] /= nr;
			p->promote_threshold[1] /= nr;
		}

		/* Average pre cache and cache; add default thresholds. FIXME: explicit cast. */
		p->promote_threshold[0] = ((p->promote_threshold[0] + p->cache_count[p->queues.ctype][0] / (unsigned) p->cache_size) >> 1) +
					  ctype_threshold(p, READ_PROMOTE_THRESHOLD);
		p->promote_threshold[1] = ((p->promote_threshold[1] + p->cache_count[p->queues.ctype][1] / (unsigned) p->cache_size) >> 1) +
					  ctype_threshold(p, WRITE_PROMOTE_THRESHOLD);

		pr_alert("promote thresholds = %u/%u queue stats = %u/%u\n",
			 p->promote_threshold[0], p->promote_threshold[1], pre_q->size, post_q->size);
        }
}

/* Add or update track queue entry. */
static struct track_queue_entry *update_track_queue(struct policy *p, struct track_queue *q, dm_block_t oblock, int rw, unsigned hits, sector_t sectors)
{
	struct track_queue_entry *r = lookup_track_queue_entry(q, oblock);

	if (r) {
		if (!updated_this_tick(p, &r->ce)) {
			queue_move_tail(&q->used, &r->ce.list);
			r->ce.tick = p->tick.t_int;
		}

	} else {
		r = pop_add_and_insert_track_queue_entry(q, oblock);
		BUG_ON(!r);
		r->ce.tick = p->tick.t_int;
	}

	r->ce.count[T_HITS][rw] += hits;
	r->ce.count[T_SECTORS][rw] += sectors;

	q->count[T_HITS][rw] += hits;
	q->count[T_SECTORS][rw] += sectors;

	return r;
}

/* Get hit/sector counts from track queue entry if exists and delete the entry. */
static void get_any_counts_from_track_queue(struct track_queue *q, struct basic_cache_entry *e, dm_block_t oblock)
{
	struct track_queue_entry *tqe = lookup_track_queue_entry(q, oblock);

	if (tqe) {
		/* On track queue -> retrieve memorized hit count and sectors in order to sort into appropriate queue on add_cache_entry(). */
		unsigned t, u, end = ARRAY_SIZE(e->ce.count[T_HITS]);

		remove_track_queue_hash_entry(tqe);
		queue_move_tail(&q->free, &tqe->ce.list);
		q->size--;

		for (t = 0; t < end; t++) {
			for (u = 0; u < end; u++) {
				e->ce.count[t][u] += tqe->ce.count[t][u];
				q->count[t][u] -= tqe->ce.count[t][u];
				tqe->ce.count[t][u] = 0;
			}
		}
	}
}

static unsigned sum_count(struct common_entry *ce, enum count_type t)
{
	return ce->count[t][0] + ce->count[t][1];
}

/*----------------------------------------------------------------------------*/

/* queue_add_.*() functions. */
static void __queue_add_default(struct policy *p, struct list_head *elt, bool to_head)
{
	struct list_head *q = &p->queues.u.used;

	to_head ? queue_add(q, elt) : queue_add_tail(q, elt);
}

static void queue_add_default(struct policy *p, struct list_head *elt)
{
	__queue_add_default(p, elt, true);
}

static void queue_add_default_tail(struct policy *p, struct list_head *elt)
{
	__queue_add_default(p, elt, false);
}

static u32 __make_key(u32 k, bool is_lfu)
{
	/* Invert key in case of lfu to allow btree_last() to retrieve the minimum used list. */
	return is_lfu ? ~k : k;
}

static void __queue_add_lfu_mfu(struct policy *p, struct list_head *elt, bool is_lfu, enum count_type ctype)
{
	struct list_head *head;
	struct basic_cache_entry *e = list_entry(elt, struct basic_cache_entry, ce.list);
	u32 key = __make_key(sum_count(&e->ce, ctype), is_lfu);

	e->saved = key; /* Memorize key for deletion (e->ce.count[T_HITS]/e->ce.count[T_SECTORS] will have changed before) */

	/*
	 * Key is e->ce.count[T_HITS]/e->ce.count[T_SECTORS] for mfu or ~e->ce.count[T_HITS]/~e->ce.count[T_SECTORS] for lfu in order to
	 * allow for btree_last() to be able to retrieve the appropriate node.
	 *
	 * A list of cblocks sharing the same hit/sector count is hanging off that node.
	 *
	 * FIXME: replace with priority heap.
	 */
	head = btree_lookup32(&p->queues.fu_head, key);
	if (head) {
		/* Always add to the end where we'll pop cblocks off */
		list_add_tail(elt, head);

		if (is_lfu) {
			/* For lfu, point to added new head, so that the older entry will get popped first. */
			int r = btree_update32(&p->queues.fu_head, key, (void *) elt);

			BUG_ON(r);
		}

	} else {
		/* New key, insert into tree. */
		int r = btree_insert32(&p->queues.fu_head, key, (void *) elt, GFP_KERNEL);

		BUG_ON(r);
		INIT_LIST_HEAD(elt);
	}
}

static void queue_add_lfu(struct policy *p, struct list_head *elt)
{
	__queue_add_lfu_mfu(p, elt, true, T_HITS);
}

static void queue_add_mfu(struct policy *p, struct list_head *elt)
{
	__queue_add_lfu_mfu(p, elt, false, T_HITS);
}

static void queue_add_lfu_ws(struct policy *p, struct list_head *elt)
{
	__queue_add_lfu_mfu(p, elt, true, T_SECTORS);
}

static void queue_add_mfu_ws(struct policy *p, struct list_head *elt)
{
	__queue_add_lfu_mfu(p, elt, false, T_SECTORS);
}

static unsigned __select_multiqueue(struct policy *p, struct basic_cache_entry *e, enum count_type ctype)
{
	unsigned val = sum_count(&e->ce, ctype);

	return min((unsigned) ilog2(val * val * val), p->queues.mqueues - 1U);
}

static unsigned __get_twoqueue(struct policy *p, struct basic_cache_entry *e)
{
	return sum_count(&e->ce, T_HITS) > 1 ? 1 : 0;
}

static unsigned long __queue_tmo_multiqueue(struct policy *p)
{
	return p->jiffies + MQ_QUEUE_TMO;
}

static void add_cache_count(struct policy *p, struct common_entry *ce)
{
	unsigned t, u, end = ARRAY_SIZE(p->cache_count[T_HITS]);

	for (t = 0; t < end; t++)
		for (u = 0; u < end; u++)
			p->cache_count[t][u] += ce->count[t][u];
}

static void sub_cache_count(struct policy *p, struct common_entry *ce)
{
	unsigned t, u, end = ARRAY_SIZE(p->cache_count[T_HITS]);

	for (t = 0; t < end; t++)
		for (u = 0; u < end; u++)
			p->cache_count[t][u] -= ce->count[t][u];
}

static void adjust_entry_counters(struct policy *p, struct common_entry *ce, unsigned queue)
{
	unsigned base = (2 << queue);

	sub_cache_count(p, ce);
	ce->count[T_HITS][0] = base + READ_PROMOTE_THRESHOLD;
	ce->count[T_HITS][1] = base + WRITE_PROMOTE_THRESHOLD;
	ce->count[T_SECTORS][0] = ce->count[T_HITS][0] * p->block_size;
	ce->count[T_SECTORS][1] = ce->count[T_HITS][1] * p->block_size;
	add_cache_count(p, ce);
}

static void demote_multiqueues(struct policy *p)
{
	struct list_head *cur = p->queues.u.mq, *end = cur + p->queues.mqueues;

	if (!queue_empty(&p->queues.free) || !queue_empty(cur))
		return;

	/* Start with 2nd queue, because we conditionally move from queue to queue - 1 */
	while (++cur < end) {
		while (!queue_empty(cur)) {
			/* Reference head element. */
			struct basic_cache_entry *e = list_first_entry(cur, struct basic_cache_entry, ce.list);

			/* If expired, move entry from head of higher prio queue to tail of lower prio one. */
			if (time_after_eq(p->jiffies, e->expire)) {
				queue_move_tail(cur - 1, &e->ce.list);
				e->expire = __queue_tmo_multiqueue(p);
				adjust_entry_counters(p, &e->ce, cur - 1 - p->queues.u.mq);

			} else
				break;
		}
	}
}

static void __queue_add_multiqueue(struct policy *p, struct list_head *elt, enum count_type ctype)
{
	struct basic_cache_entry *e = list_entry(elt, struct basic_cache_entry, ce.list);
	unsigned queue = __select_multiqueue(p, e, ctype);

	e->expire = __queue_tmo_multiqueue(p);
	queue_add_tail(&p->queues.u.mq[queue], &e->ce.list);
}

static void queue_add_multiqueue(struct policy *p, struct list_head *elt)
{
	__queue_add_multiqueue(p, elt, T_HITS);
}

static void queue_add_multiqueue_ws(struct policy *p, struct list_head *elt)
{
	__queue_add_multiqueue(p, elt, T_SECTORS);
}

static void queue_add_q2(struct policy *p, struct list_head *elt)
{
	struct basic_cache_entry *e = list_entry(elt, struct basic_cache_entry, ce.list);

	queue_add_tail(&p->queues.u.mq[0], &e->ce.list);
}

static void queue_add_twoqueue(struct policy *p, struct list_head *elt)
{
	unsigned queue;
	struct basic_cache_entry *e = list_entry(elt, struct basic_cache_entry, ce.list);

	queue = e->saved = __get_twoqueue(p, e);
	if (!queue)
		p->queues.q0_size++;

	queue_add_tail(&p->queues.u.mq[queue], &e->ce.list);
}
/*----------------------------------------------------------------------------*/

/* queue_del_.*() functions. */
static void queue_del_default(struct policy *p, struct list_head *elt)
{
	queue_del(elt);
}

static void queue_del_fifo_filo(struct policy *p, struct list_head *elt)
{
	queue_del(elt);
}

static void queue_del_lfu_mfu(struct policy *p, struct list_head *elt)
{
	struct list_head *head;
	struct basic_cache_entry *e = list_entry(elt, struct basic_cache_entry, ce.list);
	u32 key = e->saved; /* Retrieve saved key which has been saved by queue_add_lfu_mfu(). */

	head = btree_lookup32(&p->queues.fu_head, key);
	BUG_ON(!head);
	if (head == elt) {
		/* Need to remove head, because it's the only element. */
		if (list_empty(head)) {
			struct list_head *h = btree_remove32(&p->queues.fu_head, key);

			BUG_ON(!h);

		} else {
			int r;

			/* Update node to point to next entry as new head. */
			head = head->next;
			list_del(elt);
			r = btree_update32(&p->queues.fu_head, key, (void *) head);
			BUG_ON(r);
		}

	} else
		list_del(elt); /* If not head, we can simply remove the element from the list. */
}

static void queue_del_multiqueue(struct policy *p, struct list_head *elt)
{
	if (IS_TWOQUEUE(p)) {
		struct basic_cache_entry *e = list_entry(elt, struct basic_cache_entry, ce.list);
		unsigned queue = e->saved;

		if (!queue)
			p->queues.q0_size--;
	}

	queue_del(elt);
}
/*----------------------------------------------------------------------------*/

/* queue_evict_.*() functions. */
static struct list_head *queue_evict_default(struct policy *p)
{
	return queue_pop(&p->queues.u.used);
}

static struct list_head *queue_evict_lfu_mfu(struct policy *p)
{
	u32 k;
	struct list_head *r;
	struct basic_cache_entry *e;

	/* This'll retrieve lfu/mfu entry because of __make_key(). */
	r = btree_last32(&p->queues.fu_head, &k);
	BUG_ON(!r);

	if (list_empty(r))
		r = btree_remove32(&p->queues.fu_head, k);

	else {
		/* Retrieve last element in order to minimize btree updates. */
		r = r->prev;
		BUG_ON(!r);
		list_del(r);
	}

	e = list_entry(r, struct basic_cache_entry, ce.list);
	e->saved = 0;
	memset(&e->ce.count, 0, sizeof(e->ce.count));

	return r;
}

static struct list_head *queue_evict_random(struct policy *p)
{
	struct basic_cache_entry *e;
	struct list_head *r;
	dm_block_t off = random32();

	/* Be prepared for large caches ;-) */
	if (p->cache_size >= UINT_MAX)
		off |= ((dm_block_t) random32() << 32);

	e = p->cblocks + do_div(off, p->cache_size);
	r = &e->ce.list;
	queue_del(r);

	return r;
}

static struct list_head *queue_evict_multiqueue(struct policy *p)
{
	struct list_head *cur = p->queues.u.mq - 1;	/* -1 because of ++cur below. */

	while (++cur < p->queues.u.mq + p->queues.mqueues) {
		if (!queue_empty(cur)) {
			if (IS_TWOQUEUE(p) && cur == p->queues.u.mq)
				p->queues.q0_size--;

			return queue_pop(cur);
		}

		if (IS_MULTIQUEUE(p))
			break;
	}

	return NULL;
}


static struct list_head *queue_evict_q2_twoqueue(struct policy *p)
{
	return queue_evict_multiqueue(p);
}

/*----------------------------------------------------------------------------*/

/*
 * This doesn't allocate the block.
 */
static int find_free_cblock(struct policy *p, dm_block_t *result)
{
	unsigned w;

	for (w = 0; w < p->cache_nr_words; w++) {
		/*
		 * ffz is undefined if no zero exists
		 */
		if (p->allocation_bitset[w] != ~0UL) {
			*result = (w * BITS_PER_LONG) + ffz(p->allocation_bitset[w]);

			return (*result < p->cache_size) ? 0 : -ENOSPC;
		}
	}

	return -ENOSPC;
}

static void add_cache_entry(struct policy *p, struct basic_cache_entry *e)
{
	p->queues.fns->add(p, &e->ce.list);
	alloc_cblock(p, e->cblock);
	insert_cache_hash_entry(p, e);
}

static struct basic_cache_entry *evict_cache_entry(struct policy *p)
{
	struct basic_cache_entry *r;
	struct list_head *elt = p->queues.fns->evict(p);

	if (elt) {
		r = list_entry(elt, struct basic_cache_entry, ce.list);
		remove_cache_hash_entry(p, r);
		free_cblock(p, r->cblock);
		sub_cache_count(p, &r->ce);
	} else
		r = NULL;

	return r;
}

static void update_cache_entry(struct policy *p, struct basic_cache_entry *e, struct bio *bio, struct policy_result *result)
{
	int rw = (bio_data_dir(bio) == WRITE ? 1 : 0);
	unsigned sectors = bio_sectors(bio);

	result->op = POLICY_HIT;
	result->cblock = e->cblock;

	e->ce.count[T_HITS][rw]++;
	e->ce.count[T_SECTORS][rw] += sectors;

	p->cache_count[T_HITS][rw]++;
	p->cache_count[T_SECTORS][rw] += sectors;

	if (updated_this_tick(p, &e->ce))
		return;

	/* No queue deletion and reinsertion needed with fifo/filo; ie. avoid queue reordering for those. */
	if (!IS_FIFO_FILO(p)) {
		p->queues.fns->del(p, &e->ce.list);
		p->queues.fns->add(p, &e->ce.list);
	}

	e->ce.tick = p->tick.t_int;
}

static void get_cache_block(struct policy *p, dm_block_t oblock, struct bio *bio, struct policy_result *result)
{
	int rw = (bio_data_dir(bio) == WRITE ? 1 : 0);
	struct basic_cache_entry *e;

	if (queue_empty(&p->queues.free)) {
		if (IS_MULTIQUEUE(p))
			demote_multiqueues(p);

		e = evict_cache_entry(p);
		if (!e)
			return;

		result->op = POLICY_REPLACE;

		/* Memorize hits and sectors of just evicted entry on out queue. */
		update_track_queue(p, &p->queues.post, e->ce.oblock, rw, e->ce.count[T_HITS][rw], e->ce.count[T_SECTORS][rw]);
		result->old_oblock = e->ce.oblock;

	} else {
		int r;

		result->op = POLICY_NEW;

		e = alloc_cache_entry(p);
		r = find_free_cblock(p, &e->cblock);
		BUG_ON(r);
	}

	/*
	 * If an entry for oblock exists on track queues ->
	 * retrieve hit counts and sectors from track queues and delete the respective tracking entries.
	 */
	memset(&e->ce.count, 0, sizeof(e->ce.count));
	get_any_counts_from_track_queue(&p->queues.pre, e, oblock);
	get_any_counts_from_track_queue(&p->queues.post, e, oblock);

	result->cblock = e->cblock;
	e->ce.oblock = oblock;
	e->ce.tick = p->tick.t_int;
	add_cache_entry(p, e);
}

static bool is_promotion_candidate(struct policy *p, struct track_queue_entry *tqe, bool discarded_oblock, int rw)
{
	if (discarded_oblock && any_free_cblocks(p))
		/*
		 * We don't need to do any copying at all, so give this a
		 * very low threshold.  In practice this only triggers
		 * during initial population after a format.
		 */
		return true;

	return tqe->ce.count[p->queues.ctype][rw] >= p->promote_threshold[rw];
}

static bool should_promote(struct policy *p, dm_block_t oblock, bool discarded_oblock, struct bio *bio, struct policy_result *result)
{
	int rw = (bio_data_dir(bio) == WRITE ? 1 : 0);
	struct track_queue_entry *tqe = update_track_queue(p, &p->queues.pre, oblock, rw, 1, bio_sectors(bio));

	calc_rw_threshold(p);

	return is_promotion_candidate(p, tqe, discarded_oblock, rw);
}

static void map_prerequisites(struct policy *p, struct bio *bio)
{
	/* Update io tracker. */
	iot_update_stats(&p->tracker, bio);
	iot_check_for_pattern_switch(&p->tracker, p->block_size);

	/* Get start jiffies needed for time based queue demotion. */
	if (IS_MULTIQUEUE(p))
		p->jiffies = get_jiffies_64();

	p->tick.t_int = atomic_read(&p->tick.t_ext);
}

static int map(struct policy *p, dm_block_t oblock,
	       bool can_migrate, bool discarded_oblock,
	       struct bio *bio, struct policy_result *result)
{
	struct basic_cache_entry *e = lookup_cache_entry(p, oblock);

	result->op = POLICY_MISS;

	if (e)
		/* Cache hit: update entry on queues, increment its hit count... */
		update_cache_entry(p, e, bio, result);

	else if (iot_sequential_pattern(&p->tracker))
		;

	else if (!can_migrate)
		return -EWOULDBLOCK;

	else if (should_promote(p, oblock, discarded_oblock, bio, result))
		get_cache_block(p, oblock, bio, result);

	return 0;
}

/* Public interface (see dm-cache-policy.h */
static int basic_map(struct dm_cache_policy *pe, dm_block_t oblock,
		     bool can_migrate, bool discarded_oblock,
		     struct bio *bio,
		     struct policy_result *result)
{
	int r;
	struct policy *p = to_policy(pe);

	if (can_migrate)
		mutex_lock(&p->lock);

	else if (!mutex_trylock(&p->lock))
		return -EWOULDBLOCK;

	map_prerequisites(p, bio);
	r = map(p, oblock, can_migrate, discarded_oblock, bio, result);

	mutex_unlock(&p->lock);

	return r;
}

static void basic_destroy(struct dm_cache_policy *pe)
{
	struct policy *p = to_policy(pe);

	if (IS_LFU_MFU_WS(p))
		btree_destroy32(&p->queues.fu_head);

	else if (IS_MULTIQUEUE_Q2_TWOQUEUE(p))
		free_multiqueues(p);

	free_track_queue(&p->queues.post);
	free_track_queue(&p->queues.pre);
	free_bitset(p->allocation_bitset);
	free_cache_blocks_and_hash(p);
	kfree(p);
}

static int basic_load_mapping(struct dm_cache_policy *pe, dm_block_t oblock, dm_block_t cblock)
{
	int r = 0;
	struct policy *p = to_policy(pe);
	struct basic_cache_entry *e;

	mutex_lock(&p->lock);
	e = alloc_cache_entry(p);
	if (!e) {
		r = -ENOMEM;
		goto bad;
	}

	e->cblock = cblock;
	e->ce.oblock = oblock;
	e->ce.count[T_HITS][0]++; /* Count as a read. */
	add_cache_entry(p, e);

bad:
	mutex_unlock(&p->lock);

	return r;
}

static struct basic_cache_entry *__basic_force_remove_mapping(struct policy *p, dm_block_t block)
{
	struct basic_cache_entry *r = lookup_cache_entry(p, block);

	BUG_ON(!r);

	free_cblock(p, r->cblock);
	p->queues.fns->del(p, &r->ce.list);
	remove_cache_hash_entry(p, r);

	return r;
}

static void basic_remove_mapping(struct dm_cache_policy *pe, dm_block_t oblock)
{
	struct policy *p = to_policy(pe);
	struct basic_cache_entry *e, tmp;

	mutex_lock(&p->lock);
	e = __basic_force_remove_mapping(p, oblock);
	queue_add_tail(&p->queues.free, &e->ce.list);
	get_any_counts_from_track_queue(&p->queues.pre, &tmp, oblock);
	get_any_counts_from_track_queue(&p->queues.post, &tmp, oblock);
	BUG_ON(!p->nr_cblocks_allocated);
	p->nr_cblocks_allocated--;
	mutex_unlock(&p->lock);
}

static void basic_force_mapping(struct dm_cache_policy *pe,
				dm_block_t current_oblock, dm_block_t oblock)
{
	struct policy *p = to_policy(pe);
	struct basic_cache_entry *e;

	mutex_lock(&p->lock);
	e = __basic_force_remove_mapping(p, current_oblock);
	e->ce.oblock = oblock;
	add_cache_entry(p, e);
	mutex_unlock(&p->lock);
}

static dm_block_t basic_residency(struct dm_cache_policy *pe)
{
	struct policy *p = to_policy(pe);
	dm_block_t r;

	mutex_lock(&p->lock);
	r = p->nr_cblocks_allocated;
	mutex_unlock(&p->lock);

	return r;
}

static void basic_tick(struct dm_cache_policy *pe)
{
	atomic_inc(&to_policy(pe)->tick.t_ext);
}

/* Init the policy plugin interface function pointers. */
static void init_policy_functions(struct policy *p)
{
	p->policy.destroy = basic_destroy;
	p->policy.map = basic_map;
	p->policy.load_mapping = basic_load_mapping;
	p->policy.remove_mapping = basic_remove_mapping;
	p->policy.force_mapping = basic_force_mapping;
	p->policy.residency = basic_residency;
	p->policy.tick = basic_tick;
}

static struct dm_cache_policy *basic_policy_create(dm_block_t cache_size,
						   sector_t origin_size,
						   sector_t block_size,
						   enum policy_type type)
{
	int r;
	unsigned mqueues = 0;
	static struct queue_fns queue_fns[] = {
		/* These have to be in 'enum policy_type' order! */
		{ &queue_add_default_tail,  &queue_del_fifo_filo,	&queue_evict_default },		/* P_fifo */
		{ &queue_add_default,       &queue_del_fifo_filo,	&queue_evict_default },		/* P_filo */
		{ &queue_add_default_tail,  &queue_del_default,		&queue_evict_default },		/* P_lru */
		{ &queue_add_default,       &queue_del_default,		&queue_evict_default },		/* P_mru */
		{ &queue_add_lfu,           &queue_del_lfu_mfu,		&queue_evict_lfu_mfu },		/* P_lfu */
		{ &queue_add_mfu,           &queue_del_lfu_mfu,		&queue_evict_lfu_mfu },		/* P_mfu */
		{ &queue_add_lfu_ws,        &queue_del_lfu_mfu,		&queue_evict_lfu_mfu },		/* P_lfu_ws */
		{ &queue_add_mfu_ws,        &queue_del_lfu_mfu,		&queue_evict_lfu_mfu },		/* P_mfu_ws */
		{ &queue_add_default_tail,  &queue_del_default,		&queue_evict_random },		/* P_random */
		{ &queue_add_multiqueue,    &queue_del_multiqueue,	&queue_evict_multiqueue },	/* P_multiqueue */
		{ &queue_add_multiqueue_ws, &queue_del_multiqueue,	&queue_evict_multiqueue },	/* P_multiqueue_ws */
		{ &queue_add_q2,            &queue_del_multiqueue,	&queue_evict_q2_twoqueue },	/* P_q2 */
		{ &queue_add_twoqueue,      &queue_del_multiqueue,	&queue_evict_q2_twoqueue }	/* P_twoqueue */
	};
	struct policy *p = kzalloc(sizeof(*p), GFP_KERNEL);

	if (!p)
		return NULL;

	/* Set default (aka basic) policy (doesn't need a queue_fns entry above). */
	if (type == P_basic)
		type = P_multiqueue_ws;

	/* Distinguish policies */
	p->queues.fns = queue_fns + type;

	init_policy_functions(p);
	iot_init(&p->tracker);

	p->cache_size = cache_size;
	p->cache_nr_words = dm_sector_div_up(cache_size, BITS_PER_LONG);
	p->block_size = block_size;
	p->origin_size = origin_size;
	p->calc_threshold_hits = max(cache_size >> 2, (dm_block_t) 128);
	p->queues.ctype = T_HITS; /* FIXME: call argument to select T_HITS/T_SECTORS? */
	p->promote_threshold[0] = ctype_threshold(p, READ_PROMOTE_THRESHOLD);
	p->promote_threshold[1] = ctype_threshold(p, WRITE_PROMOTE_THRESHOLD);
	atomic_set(&p->tick.t_ext, 0);
	mutex_init(&p->lock);

	/* Allocate cache entry structs and add them to free list. */
	r = alloc_cache_blocks_with_hash(p, cache_size);
	if (r)
		goto bad_free_policy;

	/* Cache allocation bitset. */
	p->allocation_bitset = alloc_bitset(cache_size);
	if (!p->allocation_bitset)
		goto bad_free_cache_blocks_and_hash;

	/* Create in queue to track entries waiting for the cache in order to stear their promotion. */
	r = alloc_track_queue_with_hash(&p->queues.pre, max(cache_size, (dm_block_t) 128));
	if (r)
		goto bad_free_allocation_bitset;

	/* Create cache_size queue to track evicted cache entries. */
	r = alloc_track_queue_with_hash(&p->queues.post, max(cache_size >> 1, (dm_block_t) 128));
	if (r)
		goto bad_free_track_queue_pre;

	if (IS_LFU_MFU_WS(p)) {
		/* FIXME: replace with priority heap. */
		p->queues.fu_pool = mempool_create(cache_size, btree_alloc, btree_free, NULL);
		if (!p->queues.fu_pool)
			goto bad_free_track_queue_post;

		btree_init_mempool32(&p->queues.fu_head, p->queues.fu_pool);

	} else if (IS_Q2(p))
		mqueues = 1; /* Not really multiple queues but code can be shared... */

	else if (IS_TWOQUEUE(p)) {
		/*
 		 * Just 2 prio queues.
 		 *
		 * Only allow 25% of the total cache size maximum in queue 0 (hit count 1).
		 * Ie. 75% minimum is reserved for cblocks with multiple hits.
		 */
		mqueues = 2;
		p->queues.twoqueue_q0_max_elts = min(max(cache_size >> 2, (dm_block_t) 16), cache_size);

	} else if (IS_MULTIQUEUE(p)) {
		mqueues = min(max((dm_block_t) ilog2(block_size << 13), (dm_block_t) 8), cache_size); /* Multiple queues. */
		p->jiffies = get_jiffies_64();
	}


	if (mqueues) {
		r = alloc_multiqueues(p, mqueues);
		if (r)
			goto bad_free_track_queue_post;

	} else
		queue_init(&p->queues.u.used);

	return &p->policy;

bad_free_track_queue_post:
	free_track_queue(&p->queues.post);
bad_free_track_queue_pre:
	free_track_queue(&p->queues.pre);
bad_free_allocation_bitset:
	free_bitset(p->allocation_bitset);
bad_free_cache_blocks_and_hash:
	free_cache_blocks_and_hash(p);
bad_free_policy:
	kfree(p);

	return NULL;
}
/*----------------------------------------------------------------------------*/

/* Policy type creation magic. */
#define __CREATE_POLICY(policy) \
static struct dm_cache_policy * policy ## _create(dm_block_t cache_size, sector_t origin_size, sector_t block_size) \
{ \
	return basic_policy_create(cache_size, origin_size, block_size, P_ ## policy); \
}

#define	__POLICY_TYPE(policy) \
static struct dm_cache_policy_type policy ## _policy_type = { \
	.name = #policy, \
	.owner = THIS_MODULE, \
        .create = policy ## _create \
};

#define	__CREATE_POLICY_TYPE(policy) \
	__CREATE_POLICY(policy); \
	__POLICY_TYPE(policy);

/*
 * Create all fifo_create,filo_create,lru_create,... functions and
 * declare and initialize all fifo_policy_type,filo_policy_type,... structures.
 */
__CREATE_POLICY_TYPE(fifo);
__CREATE_POLICY_TYPE(filo);
__CREATE_POLICY_TYPE(lru);
__CREATE_POLICY_TYPE(mru);
__CREATE_POLICY_TYPE(lfu);
__CREATE_POLICY_TYPE(mfu);
__CREATE_POLICY_TYPE(lfu_ws);
__CREATE_POLICY_TYPE(mfu_ws);
__CREATE_POLICY_TYPE(random);
__CREATE_POLICY_TYPE(multiqueue);
__CREATE_POLICY_TYPE(multiqueue_ws);
__CREATE_POLICY_TYPE(q2);
__CREATE_POLICY_TYPE(twoqueue);
__CREATE_POLICY_TYPE(basic);

static struct dm_cache_policy_type *policy_types[] = {
	&fifo_policy_type,
	&filo_policy_type,
	&lru_policy_type,
	&mru_policy_type,
	&lfu_policy_type,
	&mfu_policy_type,
	&lfu_ws_policy_type,
	&mfu_ws_policy_type,
	&random_policy_type,
	&multiqueue_policy_type,
	&multiqueue_ws_policy_type,
	&q2_policy_type,
	&twoqueue_policy_type,
	&basic_policy_type
};

static int __init basic_init(void)
{
	int i = ARRAY_SIZE(policy_types), r;

	while (i--) {
		r = dm_cache_policy_register(policy_types[i]);
		if (r)
			break;
	}

	return r;
}

static void __exit basic_exit(void)
{
	int i = ARRAY_SIZE(policy_types);

	while (i--)
		dm_cache_policy_unregister(policy_types[i]);
}

module_init(basic_init);
module_exit(basic_exit);

MODULE_AUTHOR("Joe Thornber/Heinz Mauelshagen");
MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("basic/fifo/filo/lru/mru/lfu/mfu/lfu_ws/mfu_ws/random/multiqueue/multiqueue_ws/q2/twoqueue cache policies");

MODULE_ALIAS("dm-cache-basic"); /* Default mapped to one underneath in basic_policy_create() */
MODULE_ALIAS("dm-cache-fifo");
MODULE_ALIAS("dm-cache-filo");
MODULE_ALIAS("dm-cache-lru");
MODULE_ALIAS("dm-cache-mru");
MODULE_ALIAS("dm-cache-lfu");
MODULE_ALIAS("dm-cache-mfu");
MODULE_ALIAS("dm-cache-lfu_ws");
MODULE_ALIAS("dm-cache-mfu_ws");
MODULE_ALIAS("dm-cache-random");
MODULE_ALIAS("dm-cache-multiqueue");
MODULE_ALIAS("dm-cache-multiqueue_ws");
MODULE_ALIAS("dm-cache-q2");
MODULE_ALIAS("dm-cache-twoqueue");

/*----------------------------------------------------------------------------*/
