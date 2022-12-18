/* TODO: replace all FILE*s */
#include <stdlib.h>
#include <stdint.h>
#include <limits.h>
#include <stdatomic.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#include "phash.h"

const _Bool debug = 0;

void* insert_pmap_th(void* vpmap);

/* a great way to keep size of file down with varying keylengths would be to have 
 * a bucket that very large keys go into
 */
int hash(char* key, int n_buckets){
	int idx = 0;
	for(char* i = key; *i; ++i){
		idx += (*i*(i-key+1));
	}
	return idx % n_buckets;
}

/*
 * 11.88M insertions in 57.202s,2m28.329s,59.408s in 20 threads with queue capacity of 1000
 * 11.88M insertions in 38.558s,0m48.033s,1m5.96s in 20 threads with queue capacity of 20000, clear improvement
 * 11.88M insertions in 1m0.24s,1m9.0000s,1m27.00 in 40 threads with queue capacity of 20000, slowest
 * 11.88M insertions in 1m0.24s,1m9.0000s,1m27.00 in 30 threads with queue capacity of 40000, a little faster, 10 secs
 * 11.88M insertions in 1m0.24s,1m9.0000s,1m27.00 in 20 threads with queue capacity of 30000, a little faster, 10 secs
 *
 * sweet spot seems to be 20/20000 1:1000 threads:capacity
 *
 * can get a little bit of performance out of increasing capacity - try 1:1500 if i have the memory for it
 */
void init_pmi_q(struct pmi_q* pq, int capacity){
    pq->ins_idx = pq->pop_idx = 0;
    pq->const_capacity = capacity;
    pq->entries = calloc(sizeof(struct pmi_entry*), pq->const_capacity);
    pq->n_popped = 0;
    /* this is updated during building of pmap_hdr */
    pq->pop_target = 0;
}

/* TODO: try with a linked list, less useless iteration */
/*
 * and we can guarantee size with locks
 * weird iteration is a good test though to see which is faster even
 * when lock free should have the advantage
*/
void init_lpi_q(struct locking_pmi_q* lpq, int cap){
    lpq->cap = cap;
    lpq->sz = 0;
    lpq->pop_idx = 0;
    lpq->ins_idx = 0;
    lpq->n_popped = 0;
    /* this is updated during building of pmap_hdr */
    lpq->pop_target = 0;
    pthread_mutex_init(&lpq->lock, NULL);
    pthread_cond_init(&lpq->pop_ready, NULL);
    pthread_cond_init(&lpq->ins_ready, NULL);
    lpq->entries = calloc(sizeof(struct pmi_entry*), lpq->cap);
}

void insert_lpi_q(struct locking_pmi_q* lpq, char* key, int val){
    struct pmi_entry* e = malloc(sizeof(struct pmi_entry));
    e->key = strdup(key);
    e->val = val;
    pthread_mutex_lock(&lpq->lock);
    while(1){
        if(lpq->ins_idx == lpq->cap){
            lpq->ins_idx = 0;
        }
        /* we shouldn't wait in this case, pop() should */
        if(lpq->sz == lpq->cap){
            pthread_cond_wait(&lpq->ins_ready, &lpq->lock);
            /*
             * could have been spuriously woken up, continue
             * so that we can check - lock is acquired after return
            */
            continue;
        }
        /* at this point i != p, guaranteed */
        lpq->entries[lpq->ins_idx++] = e;
        ++lpq->sz;
        pthread_cond_signal(&lpq->pop_ready);

        break;
    }
    pthread_mutex_unlock(&lpq->lock);
}

/* blocks until an entry appears unless we've reached expected */
struct pmi_entry* pop_lpi_q(struct locking_pmi_q* lpq){
    struct pmi_entry* ret = NULL;
    _Bool finished;
    pthread_mutex_lock(&lpq->lock);
    while(lpq->n_popped != lpq->pop_target){
        if(lpq->pop_idx == lpq->cap){
            lpq->pop_idx = 0;
        }
        if(!lpq->sz){
            /*not returning NULL*/
            ret = NULL;
            pthread_cond_wait(&lpq->pop_ready, &lpq->lock);
            continue;
        }
        ret = lpq->entries[lpq->pop_idx];
        if(!ret){
            continue;
        }
        lpq->entries[lpq->pop_idx++] = NULL;
        pthread_cond_signal(&lpq->ins_ready);
        ++lpq->n_popped;
        --lpq->sz;
        break;
    }
    finished = lpq->n_popped == lpq->pop_target;
    pthread_mutex_unlock(&lpq->lock);
    if(finished){
        pthread_cond_broadcast(&lpq->pop_ready);
        /*ret = NULL;*/
    }
    return ret;
}
/*
 * threads should be less than capacity
 * TODO: this should take in capacity of pmi_q and set n_threads to equal a ratio of it, capped out at a certain point
 * define max_threads and use sweet spot above, 1:1500
 * TODO: if n_threads <= 0, auto calc using 1:1500
*/
void init_pmap_hdr(struct pmap* p, int n_buckets, int n_threads, int pq_cap, _Bool duplicates_expected){
    int adjusted_n_threads;

    if(duplicates_expected)adjusted_n_threads = 1;
    else adjusted_n_threads = n_threads > pq_cap ? pq_cap : n_threads;

	p->hdr.n_buckets = n_buckets;
	p->hdr.col_map = calloc(sizeof(int), n_buckets);
	p->hdr.max_keylen_map = calloc(sizeof(int), n_buckets);
	p->hdr.bucket_offset = calloc(sizeof(int), n_buckets);

    p->hdr.pmi.bucket_ins_idx = calloc(sizeof(int), n_buckets);
    /* this will be updated in finalize_pmap_hdr() */
    p->hdr.pmi.rwbuf_sz = 0;
    init_pmi_q(&p->hdr.pmi.pq, pq_cap);
    init_lpi_q(&p->hdr.pmi.lpq, pq_cap);
    p->hdr.pmi.duplicates_expected = duplicates_expected;
    p->hdr.pmi.n_threads = adjusted_n_threads;
}

/* TODO: elements_in_mem should be provided in terms of bytes */
void init_pmap(struct pmap* p, char* fn, int n_buckets, int n_threads, int elements_in_mem, _Bool duplicates_expected){
	init_pmap_hdr(p, n_buckets, n_threads, elements_in_mem, duplicates_expected);
	strncpy(p->fn, fn, sizeof(p->fn)-1);
	p->fp = fopen(p->fn, "wb");
	p->insert_ready = 0;
}

void build_pmap_hdr(struct pmap* p, char* key){
	int idx = hash(key, p->hdr.n_buckets);
	int keylen = strlen(key);
	++p->hdr.col_map[idx];
    ++p->hdr.pmi.pq.pop_target;
    ++p->hdr.pmi.lpq.pop_target;
	if(keylen > p->hdr.max_keylen_map[idx])
		p->hdr.max_keylen_map[idx] = keylen;
}

/* spawns threads to pop queue and insert into phash */
void spawn_pmi_q_pop_threads(struct pmap* p){
    int n_threads = p->hdr.pmi.n_threads;
    p->hdr.pmi.pmi_q_pop_threads = malloc(sizeof(pthread_t)*n_threads);
    for(int i = 0; i < n_threads; ++i){
        pthread_create(p->hdr.pmi.pmi_q_pop_threads+i, NULL, insert_pmap_th, p);
    }
}

/* build the scaffolding that all k/v will fit into */
/*
 * TODO: this can use a lot of improvement - it should optionally not
 * be necessary, and loops should be rolled together and fixed with
 * seek/write after
*/
void finalize_pmap_hdr(struct pmap* p){
	int cur_offset = sizeof(int)+(3*sizeof(int)*p->hdr.n_buckets);
    uint8_t* zerobuf;
    /*
	 * can i alloc in this loop? i'll need to alloc hdr first
	 * then go back in the end with fseek() to overwrite bucket_offset
    */
	for(int i = 0; i < p->hdr.n_buckets; ++i){
		p->hdr.bucket_offset[i] = cur_offset;
		/* number of items per idx * (reserved space per key + value int) */
		cur_offset += (p->hdr.col_map[i]*(p->hdr.max_keylen_map[i]+sizeof(int)));
        if(p->hdr.max_keylen_map[i] > p->hdr.pmi.rwbuf_sz){
            p->hdr.pmi.rwbuf_sz = p->hdr.max_keylen_map[i];
        }
	}

    /* add space needed for value - this will grow when we're storing more data
     * rwbuf_sz = max_keylen+sizeof(values)
     */
    p->hdr.pmi.rwbuf_sz += sizeof(int);
    zerobuf = calloc(p->hdr.pmi.rwbuf_sz, 1);

    /*
     * offset can be calculated during insertion pass, everything can be aside from max_keylen
     * which can be preset by the user
    */

	/* write header */
	fwrite(&p->hdr.n_buckets, sizeof(int), 1, p->fp);
	fwrite(p->hdr.col_map, sizeof(int), p->hdr.n_buckets, p->fp);
	fwrite(p->hdr.max_keylen_map, sizeof(int), p->hdr.n_buckets, p->fp);
	fwrite(p->hdr.bucket_offset, sizeof(int), p->hdr.n_buckets, p->fp);

	/* writing bucket array */
	for(int i = 0; i < p->hdr.n_buckets; ++i){
        fwrite(zerobuf, p->hdr.col_map[i]*(p->hdr.max_keylen_map[i]+sizeof(int)), 1, p->fp);
        if(debug)printf("wrote %li zeroes for idx %i\n", p->hdr.col_map[i]*(p->hdr.max_keylen_map[i]+sizeof(int)), i);
	}
    free(zerobuf);
    fclose(p->fp);
    p->fp = fopen(p->fn, "rb+");
    spawn_pmi_q_pop_threads(p);
	p->insert_ready = 1;
}

/* frees memory and joins threads used for pmap insertion */
struct timespec cleanup_pmi(struct pmap* p){
    struct timespec join_time;
    for(int i = 0; i < p->hdr.pmi.n_threads; ++i){
        pthread_join(p->hdr.pmi.pmi_q_pop_threads[i], NULL);
    }
    clock_gettime(CLOCK_MONOTONIC, &join_time);

    free(p->hdr.bucket_offset);
    free(p->hdr.max_keylen_map);
    free(p->hdr.col_map);

    free(p->hdr.pmi.bucket_ins_idx);
    
    free(p->hdr.pmi.pq.entries);
    free(p->hdr.pmi.lpq.entries);

    free(p->hdr.pmi.pmi_q_pop_threads);

    return join_time;
}

struct timespec seal_pmap(struct pmap* p){
    return cleanup_pmi(p);
}

_Bool mempty(uint8_t* buf, int len){
	for(int i = 0; i < len; ++i){
		if(buf[i])return 0;
	}
	return 1;
}

/* returns attempts needed for an insertion */
int insert_pmi_q(struct pmi_q* pq, char* key, int val){
    int idx, capacity, attempts = 0;
    _Atomic struct pmi_entry* ne, * e = malloc(sizeof(struct pmi_entry));
    _Atomic struct pmi_entry tmp_e = {.key = strdup(key), .val = val};
    atomic_store(e, tmp_e);
    while(1){
        ++attempts;
        /* reset to 0 if neccessary */
        capacity = pq->const_capacity;
        atomic_compare_exchange_strong(&pq->ins_idx, &capacity, 0);
        idx = atomic_fetch_add(&pq->ins_idx, 1);
        /*
         * this could potentially occur if we fetch add simultaneously
         * easy fix is just to continue
         * TODO: is there a more elegant solution?
        */
        if(idx >= pq->const_capacity){
            atomic_store(&pq->ins_idx, 0);
            continue;
        }
        /*
         * if our ins_idx is NULL, we can update the entry
         * otherwise, keep iterating
        */
        ne = NULL;
        /*
         * TODO: could this be replaced with just atomic_store(), idx is already reserved
         * nvm, could have been NULLified by a popper or could have been taken already
         * need to cas() to ensure that it's not occupied since we're not 
        */
        if(atomic_compare_exchange_strong(pq->entries+idx, &ne, e))
            break;
        /*
         * atomic_store();
         * i can use atomic_store for both insert and pop()
         * because once i reserve an index it won't be written to from another pop/insert()
         * is this okay with NULL entries?
         *
         * think about the implications of this with pops/insertions and eachother
         *
         * i think there's no need to check if entries+idx == NULL, we can assume it is
         * we can just check nonatomically
         * can we just assume it's null if we've been given access?
         * no because we need to coordinate with pop thread
         *
        */
    }
    return attempts;
}

/* returns NULL if all data have been popped */
_Atomic struct pmi_entry* pop_pmi_q(struct pmi_q* pq){
    int idx, capacity;
    /* -O3 demands ret be set to NULL */
    _Atomic struct pmi_entry* ret = NULL;
    /* was this always the condition? */
    while(pq){
        if(atomic_load(&pq->n_popped) == pq->pop_target)
            return NULL;
        capacity = pq->const_capacity;

        atomic_compare_exchange_strong(&pq->pop_idx, &capacity, 0);
        idx = atomic_fetch_add(&pq->pop_idx, 1);
        if(idx >= pq->const_capacity){
            /*
             * we store atomic_store(0) in idx, not a huge deal if we ruin our current popping progress
             * in another thread since the actual removal and insertion is threadsafe
            */
            atomic_store(&pq->pop_idx, 0);
            continue;
        }
        ret = atomic_load(pq->entries+idx);
        if(!ret)continue;
        if(atomic_compare_exchange_strong(pq->entries+idx, &ret, NULL)){
            atomic_fetch_add(&pq->n_popped, 1);
            break;
        }
    }
    return ret;
}

void insert_pmap(struct pmap* p, char* key, int val){
    insert_pmi_q(&p->hdr.pmi.pq, key, val);
}

/*
 * p->fp is no longer used in insert_pmap but can't be removed because it's still used in reading
 * operations - load/lookup() and in building the map hdr
*/
void insert_pmap_internal(struct pmap* p, char* key, int val, uint8_t* rdbuf, uint8_t* wrbuf, int fd){
	int idx = hash(key, p->hdr.n_buckets), ins_idx;
	int kv_sz = p->hdr.max_keylen_map[idx]+sizeof(int);
	int cur_offset;
    int slen;
    long int br;
    int dupes = 0;
    slen = strnlen(key, p->hdr.max_keylen_map[idx]);
    /* zero the section of wrbuf we'll be using */
    memset(wrbuf+slen, 0, p->hdr.max_keylen_map[idx]-slen);
    memcpy(wrbuf, key, slen);
    free(key);
	memcpy(wrbuf+p->hdr.max_keylen_map[idx], &val, sizeof(int));
	cur_offset = p->hdr.bucket_offset[idx];
    /*
     * can i organize the data in such a way that it's easier to compare strings?
     * buckets are getting large and O(N) is not so easy anymore
     * could also write a better hashing function
     * 
     * sort insertions to make finding duplicates easier
     * some kind of binary search?
    */
    
	if(debug)printf("idx is %i with max keylen: %i\n", idx, p->hdr.max_keylen_map[idx]);
    /* TODO: use mutex locks if(duplicates) to enable > 1 thread */
    /* if duplicates are expected, n_threads must be set to 1 - the following code isn't threadsafe */
    if(p->hdr.pmi.duplicates_expected){
        lseek(fd, cur_offset, SEEK_SET);
        for(int i = 0; i < p->hdr.col_map[idx]; ++i){
            br = read(fd, rdbuf, kv_sz);
            if(debug)printf("    %i/%i: read %li/%i bytes\n", i, p->hdr.col_map[idx], br, kv_sz);
            /*
             * if empty - rewind, insert
             * if key is identical, update
            */
            if(!memcmp(rdbuf, wrbuf, p->hdr.max_keylen_map[idx]))printf("dupes: %i\n", ++dupes);
            if(!memcmp(rdbuf, wrbuf, p->hdr.max_keylen_map[idx]) || mempty(rdbuf, kv_sz)){
                if(debug)printf("    found a spot to write\n");
                lseek(fd, -kv_sz, SEEK_CUR);
                write(fd, wrbuf, kv_sz);
                break;
            }
        }
	}
    /* threadsafe insertion into atomically reserved index */
    else{
        /*
         * because we have pre-allocated space for every element we don't have to
         * check the idx before inserting
         * bucket_ins_idx will naturally grow by 1 to col_map[idx]
        */
        ins_idx = atomic_fetch_add(p->hdr.pmi.bucket_ins_idx+idx, 1);
        /* wrbuf is ready to write, just need to write into cur_offset+(kv_sz*ins_idx) */
        lseek(fd, cur_offset+(kv_sz*ins_idx), SEEK_SET);
        write(fd, wrbuf, kv_sz);
    }
}

/*
 * each thread could be assigned a range of buckets so they can compete less
 * or each thread can be given a thread id and use modulus to find operation window
*/
void* insert_pmap_th(void* vpmap){
    int insertions = 0;
    struct pmap* p = vpmap;
    /* only wrbuf must be zeroed and this is done in insert_pmap_internal() */
    uint8_t* rdbuf = malloc(p->hdr.pmi.rwbuf_sz), * wrbuf = malloc(p->hdr.pmi.rwbuf_sz);
    int fd = open(p->fn, p->hdr.pmi.duplicates_expected ? O_RDWR : O_WRONLY);
    _Atomic struct pmi_entry* ae;
    struct pmi_entry e;
    while(1){
        /*
         * pop_pmi_q busy waits - we can just check to see if we should exit!
         * it'll return NULL if ready to exit and it can do its own math
         * each thread pops/inserts until there's no more data to pop()
        */
        if(!locking){
            ae = pop_pmi_q(&p->hdr.pmi.pq);
            if(!ae)break;
            e = atomic_load(ae);
            free(ae);
        }
        else{
            struct pmi_entry* ep = pop_lpi_q(&p->hdr.pmi.lpq);
            if(!ep)break;
            e.key = ep->key;
            e.val = ep->val;
            free(ep);
        }
        insert_pmap_internal(p, e.key, e.val, rdbuf, wrbuf, fd);
        ++insertions;
    }
    close(fd);
    free(rdbuf);
    free(wrbuf);
    return NULL;
}

/* TODO: this should optionally just allocate space for the header
 * and wait for partial loads to populate it as needed
 */
void load_pmap(struct pmap* p, char* fn){
    strcpy(p->fn, fn);
    p->fp = fopen(fn, "rb");
    fread(&p->hdr.n_buckets, sizeof(int), 1, p->fp);
    p->hdr.col_map = malloc(sizeof(int)*p->hdr.n_buckets);
    p->hdr.max_keylen_map = malloc(sizeof(int)*p->hdr.n_buckets);
    p->hdr.bucket_offset = malloc(sizeof(int)*p->hdr.n_buckets);

    fread(p->hdr.col_map, sizeof(int), p->hdr.n_buckets, p->fp);
    fread(p->hdr.max_keylen_map, sizeof(int), p->hdr.n_buckets, p->fp);
    fread(p->hdr.bucket_offset, sizeof(int), p->hdr.n_buckets, p->fp);
}

int lookup_pmap(const struct pmap* p, char* key){
    int idx = hash(key, p->hdr.n_buckets);
    int kv_sz = sizeof(int)+p->hdr.max_keylen_map[idx];;
    char* rdbuf = malloc(kv_sz);
    fseek(p->fp, p->hdr.bucket_offset[idx], SEEK_SET);

	for(int i = 0; i < p->hdr.col_map[idx]; ++i){
        fread(rdbuf, 1, kv_sz, p->fp);
        if(!strncmp(rdbuf, key, p->hdr.max_keylen_map[idx])){
            return *((int*)(rdbuf+p->hdr.max_keylen_map[idx]));
        }
    }
    return -1;
}

/* this can be run without load_pmap() */
/* TODO: partial loads should incrementally build headers */
/* TODO: this should not use FILE*s */ 
int partial_load_lookup_pmap(FILE* fp, char* key){
    int n_buckets, idx, bucket_width, max_keylen, offset, tmpval;
    int kv_sz;
    char* rdbuf;
    fread(&n_buckets, sizeof(int), 1, fp);
    idx = hash(key, n_buckets);
    /* seek to col_map[idx] */
    fseek(fp, (1+idx)*sizeof(int), SEEK_SET);
    fread(&bucket_width, sizeof(int), 1, fp);
    /* seek to max_keylen_map[idx] */
    fseek(fp, sizeof(int)+sizeof(int)*n_buckets+(sizeof(int)*idx), SEEK_SET);
    fread(&max_keylen, sizeof(int), 1, fp);
    /* seek to bucket_offset[idx] */
    fseek(fp, sizeof(int)+(sizeof(int)*2*n_buckets)+(sizeof(int)*idx), SEEK_SET);
    fread(&offset, sizeof(int), 1, fp);

    kv_sz = max_keylen+sizeof(int);
    rdbuf = malloc(kv_sz);

    fseek(fp, offset, SEEK_SET);
    for(int i = 0; i < bucket_width; ++i){
        fread(rdbuf, 1, max_keylen, fp);
        fread(&tmpval, 1, sizeof(int), fp);
        if(!strncmp(rdbuf, key, max_keylen)){
            return tmpval;
        }
    }
    return -1;
}
