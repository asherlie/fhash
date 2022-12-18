#include <stdlib.h>
#include <stdint.h>
#include <limits.h>
#include <stdatomic.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#include "phash.h"

void* insert_pmap_th(void* vpmap);

const _Bool debug = 0;
const _Bool locking = 0;

// a great way to keep size of file down with varying keylengths would be to have a bucket that very large
// keys go into
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
    // this is updated during building of pmap_hdr
    pq->pop_target = 0;
    /*pq->finished = 0;*/
}

// TODO: try with a linked list, less useless iteration
// and we can guarantee size with locks
// weird iteration is a good test though to see which is faster even
// when lock free should have the advantage
void init_lpi_q(struct locking_pmi_q* lpq, int cap){
    lpq->cap = cap;
    lpq->sz = 0;
    lpq->pop_idx = 0;
    lpq->ins_idx = 0;
    lpq->n_popped = 0;
    // this is updated during building of pmap_hdr
    lpq->pop_target = 0;
    pthread_mutex_init(&lpq->lock, NULL);
    pthread_cond_init(&lpq->pop_ready, NULL);
    pthread_cond_init(&lpq->ins_ready, NULL);
    lpq->entries = calloc(sizeof(struct pmi_entry*), lpq->cap);
}

// blocks until we have space to insert
// good exercise to write this, haven't played around with cond_t in a while
// interested in seeing if this is faster than lfq due to cpu overuse
//
// we insert from beginning
// [1,2,3,4,_,_,_]
// 
// we pop from beginning
// [_,2,3,4,_,_]
//
// if pop == cap, pop = 0
// pop waits if pop_idx is null
//
// ins waits if ins == cap
//
//[1,_,_,4,5,6]
//[1,_,_,4,5,6]
// p         i 
//
// []
// ins 1
// [1 ]
//  pi
//  pop
//  []
//  ins 1, 2, 3
//  [1,2,3,_,_]
//   p   i
//   pop
//  [_,2,3,_,_]
//     p   i
//   pop
//  [_,_,3,4,5]
//   i   p 
//
//   if i == p, i must wait for p to pop
//
void insert_lpi_q(struct locking_pmi_q* lpq, char* key, int val){
    struct pmi_entry* e = malloc(sizeof(struct pmi_entry));
    e->key = strdup(key);
    e->val = val;
    pthread_mutex_lock(&lpq->lock);
    /*if(lpq->ins_idx == lpq->cap)lpq->ins_idx = 0;*/
    while(1){

        /*
         * [1,2,ins_idx, _, _]
         * []
         * pop = 0
        */

        /*if(lpq->ins_idx == lpq->pop_idx || (lpq->ins_idx == lpq->cap && lpq->pop_idx == 0)){*/
        /*if(lpq->sz == lpq->cap){*/
        if(lpq->ins_idx == lpq->cap){
            lpq->ins_idx = 0;
            /*printf("ins: %i->%i\n", lpq->cap, lpq->ins_idx);*/
        }
        // we shouldn't wait in this case, pop() should
        /*if(lpq->ins_idx == lpq->pop_idx){*/
        if(lpq->sz == lpq->cap){
            pthread_cond_wait(&lpq->ins_ready, &lpq->lock);
            // could have been spuriously woken up, continue
            // so that we can check - lock is acquired after return
            continue;
        }
        // at this point, i != p, guaranteed
        lpq->entries[lpq->ins_idx++] = e;
        /*printf("ins: %i->%i\n", lpq->ins_idx-1, lpq->ins_idx);*/
        /*printf("pop: %i, sz: %i\n", lpq->pop_idx, lpq->sz);*/
        ++lpq->sz;
        pthread_cond_signal(&lpq->pop_ready);

        break;
    }
    pthread_mutex_unlock(&lpq->lock);
}

// [1,2,3,4,5,_,_,_]
//  p         i
// [_,_,_,_,_,_,_,_]
//          p i
// blocks until an entry appears unless we've reached expected
// what if this didn't return a malloc()d variable and was instead passed
struct pmi_entry* pop_lpi_q(struct locking_pmi_q* lpq){
    struct pmi_entry* ret = NULL;
    _Bool finished;
    pthread_mutex_lock(&lpq->lock);
    while(lpq->n_popped != lpq->pop_target){
        if(lpq->pop_idx == lpq->cap){
            lpq->pop_idx = 0;
            /*printf("pop: %i->%i\n", lpq->cap, lpq->pop_idx);*/
        }
        if(!lpq->sz){
            /*not returning NULL*/
            ret = NULL;
            pthread_cond_wait(&lpq->pop_ready, &lpq->lock);
            continue;
        }
        ret = lpq->entries[lpq->pop_idx];
        // stuck in cont loop here
        if(!ret){
            /*printf("sz %i, pop idx %i\n", lpq->sz, lpq->pop_idx);*/
            continue;
        }
        lpq->entries[lpq->pop_idx++] = NULL;
        /*printf("pop: %i->%i\n", lpq->pop_idx-1, lpq->pop_idx);*/
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

// threads should be less than capacity
// TODO: this should take in capacity of pmi_q and set n_threads to equal a ratio of it, capped out at a certain point
// define max_threads and use sweet spot above, 1:1500
// TODO: if n_threads <= 0, auto calc using 1:1500
void init_pmap_hdr(struct pmap* p, int n_buckets, int n_threads, int pq_cap, _Bool duplicates_expected){
    int adjusted_n_threads;

    if(duplicates_expected)adjusted_n_threads = 1;
    else adjusted_n_threads = n_threads > pq_cap ? pq_cap : n_threads;

	p->hdr.n_buckets = n_buckets;
	p->hdr.col_map = calloc(sizeof(int), n_buckets);
	p->hdr.max_keylen_map = calloc(sizeof(int), n_buckets);
	p->hdr.bucket_offset = calloc(sizeof(int), n_buckets);

    // TODO: pay attention to duplicates_expected
    p->hdr.pmi.bucket_ins_idx = calloc(sizeof(int), n_buckets);
    // set target_entries to 0, it will be updated while we build the pmap hdr
    // this is then used for each insertion thread to check if it should stop
    // still won't be perfect
    p->hdr.pmi.target_entries = 0;
    // this will be updated in finalize_col_map()
    p->hdr.pmi.rwbuf_sz = 0;
    /*p->hdr.pmi.n_entries = 0;*/
    // too high?
    /*we don't know n_entries until after finalize*/
    /*init_pmi_q(&p->hdr.pmi.pq, n_buckets);*/
    init_pmi_q(&p->hdr.pmi.pq, pq_cap);
    init_lpi_q(&p->hdr.pmi.lpq, pq_cap);
    p->hdr.pmi.duplicates_expected = duplicates_expected;
    p->hdr.pmi.n_threads = adjusted_n_threads;
}

// TODO: elements_in_mem should be provided in terms of bytes
void init_pmap(struct pmap* p, char* fn, int n_buckets, int n_threads, int elements_in_mem, _Bool duplicates_expected){
	init_pmap_hdr(p, n_buckets, n_threads, elements_in_mem, duplicates_expected);
	strncpy(p->fn, fn, sizeof(p->fn)-1);
	p->fp = fopen(p->fn, "wb");
	p->insert_ready = 0;
	/*fwrite(&p->hdr, sizeof(struct p));*/
}

void build_pmap_hdr(struct pmap* p, char* key){
	int idx = hash(key, p->hdr.n_buckets);
	int keylen = strlen(key);
	++p->hdr.col_map[idx];
    // TODO: this can be removed, target_entries is not used
    ++p->hdr.pmi.target_entries;
    ++p->hdr.pmi.pq.pop_target;
    ++p->hdr.pmi.lpq.pop_target;
	if(keylen > p->hdr.max_keylen_map[idx])
		p->hdr.max_keylen_map[idx] = keylen;
}

// spawns threads to pop queue and insert into phash
void spawn_pmi_q_pop_threads(struct pmap* p){
    int n_threads = p->hdr.pmi.n_threads;
    p->hdr.pmi.pmi_q_pop_threads = malloc(sizeof(pthread_t)*n_threads);
    for(int i = 0; i < n_threads; ++i){
        pthread_create(p->hdr.pmi.pmi_q_pop_threads+i, NULL, insert_pmap_th, p);
    }
}

void write_zeroes(FILE* fp, int nbytes){
	int* z = calloc(nbytes, 1);
	fwrite(z, nbytes, 1, fp);
	free(z);
}

// build the scaffolding that all k/v will fit into
// TODO: this should be renamed to finalize_pmap_hdr()
void finalize_col_map(struct pmap* p){
	int cur_offset = sizeof(int)+(3*sizeof(int)*p->hdr.n_buckets);
    // TODO: alloc here?
	// can i alloc in this loop? i'll need to alloc hdr first
	// then go back in the end with fseek(wtvr) to overwrite bucket_offset
	for(int i = 0; i < p->hdr.n_buckets; ++i){
		p->hdr.bucket_offset[i] = cur_offset;
        /*printf("offset_idx");*/
		// number of items per idx * (reserved space per key + value int)
		cur_offset += (p->hdr.col_map[i]*(p->hdr.max_keylen_map[i]+sizeof(int)));
	}
    /*
     * offset can be calculated during insertion pass, everything can be aside from max_keylen
     * which can be preset by the user
    */
	/* write header */
	fwrite(&p->hdr.n_buckets, sizeof(int), 1, p->fp);
	fwrite(p->hdr.col_map, sizeof(int), p->hdr.n_buckets, p->fp);
	fwrite(p->hdr.max_keylen_map, sizeof(int), p->hdr.n_buckets, p->fp);
	fwrite(p->hdr.bucket_offset, sizeof(int), p->hdr.n_buckets, p->fp);

	/*fwrite();*/
	// writing bucket array
	// need to alloc diff amounts of space based on col and keylen maps
	for(int i = 0; i < p->hdr.n_buckets; ++i){
		// write zeroes
		// alloc curr_offset[i]-curr_offset[i-1]
		write_zeroes(p->fp, p->hdr.col_map[i]*(p->hdr.max_keylen_map[i]+sizeof(int)));
        if(debug)printf("wrote %li zeroes for idx %i\n", p->hdr.col_map[i]*(p->hdr.max_keylen_map[i]+sizeof(int)), i);
        if(p->hdr.max_keylen_map[i] > p->hdr.pmi.rwbuf_sz){
            p->hdr.pmi.rwbuf_sz = p->hdr.max_keylen_map[i];
        }
	}
    // add space needed for value - this will grow when we're storing more data
    p->hdr.pmi.rwbuf_sz += sizeof(int);
	/*
	 * todo - write an integer of cur_bucket_idx - 0 at start
	 * we know total number but not number in progress
	 * we will increment this int each time we insert_pmap()
	 *
	 * nvm i'll just iterate through, i'll need to check for duplicates
	 * anyway
	*/
    /*fflush(p->fp);*/
    fclose(p->fp);
    p->fp = fopen(p->fn, "rb+");
    spawn_pmi_q_pop_threads(p);
	p->insert_ready = 1;
}

// frees memory and joins threads used for pmap insertion
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

    return join_time;
}

_Bool mempty(uint8_t* buf, int len){
	for(int i = 0; i < len; ++i){
		if(buf[i])return 0;
	}
	return 1;
}

// insert needs to fseek() using offset finder of hash()
// fseek(seek_set, 4+2*n_buckets+)
/*void insert_pmap(struct pmap* p, char* key, int val){*/
// need to pass rbuf and wrbuf - they should be alloc'd in the caller()
// max_kv_sz = max(max_keylen_map)
// i can potentially make this threadsafe and insert in different offsets from different threads
// not sure if this will corrupt anything - having multiple FILE*s to the same file
// this will be renamed _internal_insert_pmap. insert_pmap() will insert into a queue that's shared
// with a thread that is continuously popping from the queue and calling _internal_insert_pmap() 
// this queue will only be able to contain a limited number of entries at a time
// it will pthread_cond_wait() until  
// wait actually i think it'll use atomic vars to see how many threads are currently 
// nvm it'll use an atomic var to see how many entries are in it
// it will only be inserted into if this number < a specified number
//
// q will live in pmi - the struct containing all data needed for insertion
// that will not be passed along to the file being built
//
// a = atomic_load(p->hdr.pmi.q->sz)
// should never be gt cutoff
// if(a >= cutoff_q_sz){
//  pthread_cond_wait(p->hdr.pmi.q->cond);
// }

// TODO: should these use mutex locks instead of atomic operations?
// does using cpu time negatively impact actual insertion into phash?
// keep track of attempts per thread and insertions per thread
// returns attempts
int insert_pmi_q(struct pmi_q* pq, char* key, int val){
    int idx, capacity, attempts = 0;
    _Atomic struct pmi_entry* ne, * e = malloc(sizeof(struct pmi_entry));
    // TODO: key must be free()d after insertion
    // key isn't freed!
    _Atomic struct pmi_entry tmp_e = {.key = strdup(key), .val = val};
    // TODO: key must be alloc'd/put into a buffer for this thread
    // TODO: ensure this is freed after insertion
    atomic_store(e, tmp_e);
    /*
     * atomic_store(&e->key, strdup(key));
     * atomic_store(&e->val, val);
    */
    /*e->key = strdup(key);*/
    /*e->val = val;*/
    while(1){
        ++attempts;
        // reset to 0 if neccessary
        /*pq->capacity = pq->const_capacity;*/
        /*atomic_store(&pq->capacity, pq->const_capacity);*/
        /*printf("cap: %i\n", pq->capacity);*/
        // there's still a chance that pq->capacity could be set by another thread
        capacity = pq->const_capacity;
        atomic_compare_exchange_strong(&pq->ins_idx, &capacity, 0);
        idx = atomic_fetch_add(&pq->ins_idx, 1);
        // this could potentially occur if we fetch add simultaneously
        // easy fix is just to continue
        // TODO: is there a more elegant solution?
        if(idx >= pq->const_capacity){
            /*puts("bad idx");*/
            atomic_store(&pq->ins_idx, 0);
            continue;
        }
        /*printf("ins idx %i\n", idx);*/
        // if our ins_idx is NULL, we can update the entry
        // otherwise, keep iterating
        ne = NULL;
        // TODO: could this be replaced with just atomic_store(), idx is already reserved
        // nvm, could have been NULLified by a popper or could have been taken already
        // need to cas() to ensure that it's not occupied since we're not 
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
    /*printf("\rinserted in idx %i", idx);*/
    // aha! enqueing correctly but only one char is being popped!
    /*printf("enqueued %s: %i\n", key, val);*/
    return attempts;
}

/*
 * this may not return if our list is full
 * try to reproduce issue with sequential writes
*/
// returns NULL if all data have been popped
// print number of tries for pop/insert
_Atomic struct pmi_entry* pop_pmi_q(struct pmi_q* pq){
    int idx, capacity;
    // -O3 demands ret to be assigned to NULL
    _Atomic struct pmi_entry* ret = NULL;
    /*atomic_load(pq->pop_idx);*/
    // was this always the condition?
    while(pq){
        if(atomic_load(&pq->n_popped) == pq->pop_target)
            return NULL;
        /*if(pq->n_entries)*/
        /*atomic_store(&pq->capacity, pq->const_capacity);*/
        capacity = pq->const_capacity;

        #if !1
        if there's only one popping thread then there won't be a way to reset pop_idx
        nope, same thread can just do this
        #endif


        atomic_compare_exchange_strong(&pq->pop_idx, &capacity, 0);
        idx = atomic_fetch_add(&pq->pop_idx, 1);
        if(idx >= pq->const_capacity){
            /*found hte pborblem! always bad idx when hanging*/
            /*printf("bad pdx %i\n", idx);*/
            atomic_store(&pq->pop_idx, 0);
            /*
             * we should atomic_store(0) in idx, not a huge deal if we ruin our current popping progress
             * in another thread since the actual removal and insertion is threadsafe
             * this should also be done in insert_pmi_q() but for some reason it doesn't have the same issue
            */
            continue;
        }
        /*printf("pop idx %i\n", idx);*/
        ret = atomic_load(pq->entries+idx);
        if(!ret)continue;
        if(atomic_compare_exchange_strong(pq->entries+idx, &ret, NULL)){
            atomic_fetch_add(&pq->n_popped, 1);
            break;
        }
    }
    return ret;
}

// rdbuf/wrbuf are now contained in p->hdr.pmi
// each thread could be assigned a range of buckets so they can compete less
// or each thread can be given a thread id and 
void* insert_pmap_th(void* vpmap){
    // n_threads of these will be spawned - make sure to pass a uniqe FP to each thread
    int insertions = 0;
    struct pmap* p = vpmap;
    // these are not zeroed, only wrbuf must be zeroed and this is done in insert_pmap()
    uint8_t* rdbuf = malloc(p->hdr.pmi.rwbuf_sz), * wrbuf = malloc(p->hdr.pmi.rwbuf_sz);
    /*FILE* fp = fopen(p->fn, "rb+");*/
    // need to use RDWR if duplicates
    int fd = open(p->fn, O_WRONLY);
    _Atomic struct pmi_entry* ae;
    struct pmi_entry e;
    while(1){
        /*
         * if(atomic_load(&p->hdr.pmi.n_entries) == p->hdr.pmi.target_entries)
         *     break;
        */
        // pop_pmi_q busy waits! we can just check to see if we should exit!
        // it'll return NULL if ready to exit and it can do its own math
        //
        // we will run each thread until there's no more data to pop(), makes sense
        if(!locking){
            ae = pop_pmi_q(&p->hdr.pmi.pq);
            if(!ae)break;
            // weird that this works but not assigning the pointer
            e = atomic_load(ae);
            free(ae);
        }
        else{
            struct pmi_entry* ep = pop_lpi_q(&p->hdr.pmi.lpq);
            if(!ep)break;
            e.key = ep->key;
            /*printf("popped %s\n", e.key);*/
            e.val = ep->val;
            free(ep);
        }
        /* which is correct?
         * or 
         * e = *atomic_load(&ae);
        */
        /*printf("dequeued %s: %i\n", e.key, e.val);*/
        // buffers must be alloc'd up top DO NOT USE the one in hdr
        // can't be shared like this, there must be one allocated per thread
        // we can pass along size though, that's what should live in the struct
        /*printf("inserting %s:%i\n", e.key, e.val);*/
        insert_pmap(p, e.key, e.val, rdbuf, wrbuf, fd);
        ++insertions;
    /*
     * we can check the value of fetch_add() - exit thread if == n_entries
     * and also atomic_load() before iterating
    */
    }
    close(fd);
    /*fclose(fp);*/
    free(rdbuf);
    free(wrbuf);
    /*printf("%i insertions from thread x\n", insertions);*/
    return NULL;
}

// the new insert_pmap() function will just insert a request into the queue, waiting until 
// a new idx opens up if necessary
//

// p->fp is no longer used in insert_pmap but can't be removed because it's still used in reading
// operations - load/lookup() and in building the map hdr
void insert_pmap(struct pmap* p, char* key, int val, uint8_t* rdbuf, uint8_t* wrbuf, int fd){
	int idx = hash(key, p->hdr.n_buckets), ins_idx;
	int kv_sz = p->hdr.max_keylen_map[idx]+sizeof(int);
	int cur_offset;
    int slen;
    long int br;
    int dupes = 0;
    /*
	 * uint8_t* rdbuf = malloc(kv_sz);
	 * uint8_t* wrbuf = calloc(kv_sz, 1);
    */
    /* zero the section of wrbuf we'll be using */
    /*memset(wrbuf, 0, kv_sz);*/
    // zero keylen
    // we can also just pre-calculate strlen and zero only what isn't used
    slen = strnlen(key, p->hdr.max_keylen_map[idx]);
    /*slen = 4;*/
    /*memset(wrbuf, 0, p->hdr.max_keylen_map[idx]);*/
    /*printf("memsetting %i\n", p->hdr.max_keylen_map[idx]-slen);*/
    memset(wrbuf+slen, 0, p->hdr.max_keylen_map[idx]-slen);
    /*free key mem*/
    // shouldn't be necessary but helps debugging
    /*memset(rdbuf, 0, kv_sz);*/
    memcpy(wrbuf, key, slen);
    free(key);
	memcpy(wrbuf+p->hdr.max_keylen_map[idx], &val, sizeof(int));
	cur_offset = p->hdr.bucket_offset[idx];
    // future work:
    //
    // store n_entries in hdr
    //
    // can i organize the data in such a way that it's easier to compare strings?
    // buckets are getting large and O(N) is not so easy anymore
    // could also write a better hashing function
    //
    // ok the options are:
    //  multithreading and split up the insertion
    //      this is a good option - i insert_x() can add a request to a queue
    //      this queue will have a limited amount of space and will block insertions
    //      until it's been sufficienty popped and its elements moved to the hash
    //      this way we can abstract the splitting up of insertions into hashmap from the user
    //      and can limit memory being used
    //      we can keep mem to n_threads
    //      it'll be very simple to make this threadsafe - just add mutex locks on a per bucket basis // //      but i can do better maybe, can i use a lock free datastructure
    //      and use CAS/atomic incrementation to reserve spots in a bucket?
    //      we can guarantee that there will be exactly one spot for each entry
    //      this means that we can avoid any annoying edge cases
    //
    //  sort insertions to make finding duplicates easier
    //  some kind of binary search?
    //
    //  can also enable the assumption that no duplicates will
    //  be inserted
    //      this is another good option - write this as a proof of concept
    //
    //  this will be the case in spotify
    //
    //  if we can enable duplicate detection only for updates!
    //
    //  write a better hash()
	/*if(debug)printf("seeking to offset %i for key \"%s\"\n", cur_offset, key);*/
	if(debug)printf("idx is %i with max keylen: %i\n", idx, p->hdr.max_keylen_map[idx]);
	// iterate over all entries in a bucket looking for duplicates
	// if none are found, insert at idx 0
    // i can store a struct just to help with insertions assuming all insertions will be done at once
    // this struct can contain info that doesn't need to be contained in 
    // the following loop is used only if duplicates are expected, it is NOT threadsafe
    // if duplicates are expected, n_threads must be set to 1
    if(p->hdr.pmi.duplicates_expected){
        lseek(fd, cur_offset, SEEK_SET);
        for(int i = 0; i < p->hdr.col_map[idx]; ++i){
            br = read(fd, rdbuf, kv_sz);
            if(debug)printf("    %i/%i: read %li/%i bytes\n", i, p->hdr.col_map[idx], br, kv_sz);
            /*perror("");*/
            /*if empty - rewind, insert*/
            /*if key is identical, update*/
            // okay, cool. no dupes. should be good to use lock free threadsafety
            // each thread will likely need its own FILE*. these can be passed into the threads
            // in the place they're spawned
            // there'll be code that sets up the queue and spawns insert_pmap() threads
            // spawn_insert_pmap(){
            //  FILE* fp = ...()
            //  spawn_thread(queue, pmap)
            // }
            //
            // if(no_duplicates){
            //  col_idx = atomic_increment(p->hdr.pmap_insertion.bucket_ins_idx[idx])
            //  fseek(cur_offset+(kv_sz*col_idx))
            //  fwrite()
    #if !1
    this!   //  // no need to rdbuf in this case, no reading whatsoever! just reserving idx and inserting !!!
    #endif
            // }
            // else {do what we do now, later write lock impl}
            if(!memcmp(rdbuf, wrbuf, p->hdr.max_keylen_map[idx]))printf("dupes: %i\n", ++dupes);
            if(!memcmp(rdbuf, wrbuf, p->hdr.max_keylen_map[idx]) || mempty(rdbuf, kv_sz)){
                if(debug)printf("    found a spot to write\n");
                lseek(fd, -kv_sz, SEEK_CUR);
                write(fd, wrbuf, kv_sz);
                break;
            }
        }
	}
    // threadsafe insertion into atomically reserved index
    else{
        // because we have pre-allocated space for every element we don't have to
        // check the idx before inserting
        // bucket_ins_idx will naturally grow by 1 to col_map[idx]
        ins_idx = atomic_fetch_add(p->hdr.pmi.bucket_ins_idx+idx, 1);
         /*wrbuf is ready to fwrite, just need to write into cur_offset+(kv_sz*ins_idx)*/
        // TODO: 
        lseek(fd, cur_offset+(kv_sz*ins_idx), SEEK_SET);
        /*fwrite_unlocked(wrbuf, kv_sz, 1, fp);*/
        /*printf("write(%i)\n", fd);*/
        write(fd, wrbuf, kv_sz);
        // why are there duplicates? keys being inserted multiple times
        // okay, only one of each is being inserted, see print statement at end of insert_pmi_q()
        // okay, we're inserting only one "zzzz" into the queue
        // but popping many
        //
        // see if i can reproduce this in a single thread
        // okay, still happening with only one thread which is good news because it's not a concurrency problem
        // it's probably a data struct problem
        /*printf("inserted %s into bucket[%i]:%i\n", key, idx, ins_idx);*/
    }
    /*fflush(fp);*/
    /*
     * fclose(p->fp);
     * p->fp = fopen(p->fn, "rb+");
    */
}

// this is from the client perspective
// possibly don't need to load all the header info in if we're doing a single read
// pop key: "asher"
//  idx = hash(key)
//  fseek(fp, sizeof(int)*idx)
//  fread(fp, bucket_len);
//  bucket_sz = d
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

// a lot of this code can be reused from server
int lookup_pmap(const struct pmap* p, char* key){
    int idx = hash(key, p->hdr.n_buckets);
    int kv_sz = sizeof(int)+p->hdr.max_keylen_map[idx];;
    char* rdbuf = malloc(kv_sz);
    /*printf("seeking to %i\n", p->hdr.bucket_offset[idx]);*/
    fseek(p->fp, p->hdr.bucket_offset[idx], SEEK_SET);

	for(int i = 0; i < p->hdr.col_map[idx]; ++i){
        fread(rdbuf, 1, kv_sz, p->fp);
        if(!strncmp(rdbuf, key, p->hdr.max_keylen_map[idx])){
            //printf("%s: %i %i/%i, %i, %i\n", rdbuf, p->hdr.bucket_offset[idx], i, p->hdr.col_map[idx], p->hdr.max_keylen_map[idx], kv_sz);
            return *((int*)(rdbuf+p->hdr.max_keylen_map[idx]));
        }
    }
    return -1;
}

// this can be run without load_pmap()
// TODO: partial loads should incrementally build headers
int partial_load_lookup_pmap(FILE* fp, char* key){
    int n_buckets, idx, bucket_width, max_keylen, offset, tmpval;
    int kv_sz;
    char* rdbuf;
    fread(&n_buckets, sizeof(int), 1, fp);
    idx = hash(key, n_buckets);
    // seek to col_map[idx]
    fseek(fp, (1+idx)*sizeof(int), SEEK_SET);
    fread(&bucket_width, sizeof(int), 1, fp);
    // seek to max_keylen_map[idx]
    fseek(fp, sizeof(int)+sizeof(int)*n_buckets+(sizeof(int)*idx), SEEK_SET);
    fread(&max_keylen, sizeof(int), 1, fp);
    // seek to bucket_offset[idx]
    fseek(fp, sizeof(int)+(sizeof(int)*2*n_buckets)+(sizeof(int)*idx), SEEK_SET);
    fread(&offset, sizeof(int), 1, fp);

    kv_sz = max_keylen+sizeof(int);
    rdbuf = malloc(kv_sz);

    /*printf("seeking to %i\n", offset);*/
    fseek(fp, offset, SEEK_SET);
    for(int i = 0; i < bucket_width; ++i){
        /*fread(rdbuf, 1, kv_sz, fp);*/
        fread(rdbuf, 1, max_keylen, fp);
        fread(&tmpval, 1, sizeof(int), fp);
        if(!strncmp(rdbuf, key, max_keylen)){
            //printf("%s: %i %i/%i, %i, %i\n", rdbuf, offset, i, bucket_width, max_keylen, kv_sz);
            return tmpval;
        }
    }
    return -1;
}

void lookup_test(char* fn, char* key, _Bool partial){
    struct pmap p;
    int val;

    if(partial){
        FILE* fp;
        printf("val: %i\n", partial_load_lookup_pmap(fp = fopen(fn, "rb"), key));
        fclose(fp);
        return;
    }
    load_pmap(&p, fn);
    val = lookup_pmap(&p, key);
    printf("VAL: %i\n", val);
    fclose(p.fp);
}

/*should contain total number of k/v pairs*/

_Atomic int insertions = 0, pops = 0;
void* insert_pmi_thread(void* vpq){
    struct pmi_q* pq = vpq;
    for(int i = 0; i < 20; ++i){
        insert_pmi_q(pq, "key", 99);
        atomic_fetch_add(&insertions, 1);
        printf("inserted %i\n", i);
    }
    return NULL;
}

void* pop_pmi_thread(void* vpq){
    struct pmi_q* pq = vpq;
    for(int i = 0; i < 20; ++i){
        pop_pmi_q(pq);
        atomic_fetch_add(&pops, 1);
        printf("popped %i\n", i);
    }
    return NULL;
}

void sequential_pmi_q_debug(){
    struct pmi_q pq;
    init_pmi_q(&pq, 300);
    for(int i = 0; i < 11; ++i){
        insert_pmi_q(&pq, "key", i);
    }
    for(int i = 0; i < 200; ++i){
        pop_pmi_q(&pq);
        insert_pmi_q(&pq, "key", i);
    }
}

void pmi_q_test(){
    /*
     * sequential_pmi_q_debug();
     * return;
    */
    // testing out concurrent reads/writes
    // should pop exactly what is inserted
    // and should never contain more than capacity
    // once this is written i can write the insertion thread and just have the user decide how many
    // to spawn with init_pmap()
    // remember to open multiple file pointers and to free up memory for keys
    //
    int n_threads = 2;
    pthread_t* ins = malloc(sizeof(pthread_t)*n_threads);
    pthread_t* pop = malloc(sizeof(pthread_t)*n_threads);
    struct pmi_q pq;
    init_pmi_q(&pq, 100);

    for(int i = 0; i < n_threads; ++i){
        pthread_create(ins+i, NULL, insert_pmi_thread, &pq);
        pthread_create(pop+i, NULL, pop_pmi_thread, &pq);
    }

    /*i think we hang because all popper threads are joined and we do an insertion*/
    for(int i = 0; i < n_threads; ++i){
        pthread_join(ins[i], NULL);
        printf("join ins[%i]\n", i);
        pthread_join(pop[i], NULL);
        printf("join pop[%i]\n", i);
    }

    printf("pops: %i, insertions: %i\n", pops, insertions);

    return;

    for(int i = 0; i < 481; ++i){
        insert_pmi_q(&pq, "key", 99);
        pop_pmi_q(&pq);
        printf("inserted %i!\n", i);
    }
}

void bad_pop_test(){
    struct pmi_q pq;
    _Atomic struct pmi_entry* ae;
    struct pmi_entry e;
    init_pmi_q(&pq, 2);
    pq.pop_target = 2;

    insert_pmi_q(&pq, "a", 0);
    insert_pmi_q(&pq, "b", 1);

    for(int i = 0; i < pq.pop_target; ++i){
        ae = pop_pmi_q(&pq);
        //if(!ae)return;
        e = *atomic_load(&ae);
        printf("%s %i\n", e.key, e.val);
    }
}

void test_lpi_q(){
    struct locking_pmi_q lpq;
    init_lpi_q(&lpq, 30000);
    lpq.pop_target = 30+30;

    for(int i = 0; i < 30; ++i){
        insert_lpi_q(&lpq, "ashini", 99);
        printf("inserted %i\n", i);
    }
    for(int i = 0; i < 20; ++i){
        printf("popped: %s\n", pop_lpi_q(&lpq)->key);
    }
    for(int i = 0; i < 30; ++i){
        insert_lpi_q(&lpq, "ashini", 99);
        pop_lpi_q(&lpq);
        printf("inserted %i\n", i);
    }
}

int main(int argc, char** argv){
    /*
     * test_lpi_q();
     * return 1;
    */
    /*pmi_q_test();*/
    /*bad_pop_test();*/
    /*return 1;*/
    if(argc > 1){
        lookup_test("PM", argv[1], 1);
        return 0;
    }
	struct pmap p;
    char str[6] = {0};
    int n_str = 0;
    int attempts = 0;
    double elapsed;
    struct timespec st, fin;
    /*
     * locking took about the same time as lock free! with 45000 cap and 30 threads:
     *     locking: 3m49s
     *
     * 145000 cap:
     *      locking: 4m10s -- this should be thrown out, was inserting into both and popping from locked
     *
     * 145000 cap and 90 threads: without [5]
     *      locking: 28.4s
     *
     * 45000 cap and 90 threads: without [5]
     *      locking: 29.8s
     *      lock free: 
     *
     * 45000 cap and 10 threads: without [5]
     *      locking: 29.8s
     *      lock free: 
    */

    // no diff in run time based on n_threads, weird - even 1 thread is within seconds
    // 1 thread:
    //  1m3 
    // 2
    //  54s
    // 3
    //  42s
    // 4
    //  40s
    // 5
    //  42
    // 6
    // can't let thread count get too high while keeping capcity low or they compete over slots to pop from
    // TODO: these should be dynamically chosen using expected insertions and max_threads and memory
    // high threadcount with low memory usage is unproductive
    init_pmap(&p, "PM", 1024, 32, 524288, 0);
    /*init_pmap(&p, "PM", 10000, 20, 450000, 0);*/
	/*init_pmap(&p, "PM", 10000, 1, 1000, 0);*/
    // inserting (26^5)7 strings - ~83.1M takes 4m36s
    for(int i = 0; i < 2; ++i){
        for(char a = 'a'; a <= 'z'; ++a){
            for(char b = 'a'; b <= 'z'; ++b){
                for(char c = 'a'; c <= 'z'; ++c){
                    for(char d = 'a'; d <= 'z'; ++d){
                        for(char e = 'a'; e <= 'z'; ++e){
                            /*for(char f = 'a'; f <= 'g'; ++f){*/
                                /*++n_str;*/
                                str[0] = a;
                                str[1] = b;
                                str[2] = c;
                                str[3] = d;
                                str[4] = e;
                                /*str[5] = f;*/

                                if(i == 0){
                                    build_pmap_hdr(&p, str);
                                }
                                else{
                                    ++n_str;
                                    // multithreaded insertions that leverage p.hdr.pmi.pq
                                    /*inesrt_pmap_par();*/
                                    #if 0
                                    insert_pmap(&p, str, a-'a', rdbuf, wrbuf);
                                    threads will be spawned by finalize()
                                    the user will queue insertions - name it somethign good 
                                    queue_insert_pmap() HERE

                                    threads can then be joined by a cleanup function

                                    actual insert will need to grab some FILE*s and fclose()
                                    #endif
                                    if(!locking){
                                        attempts += insert_pmi_q(&p.hdr.pmi.pq, str, a-'a'+e-'a');
                                        /*printf("\rinserted idx %i", n_str-1);*/
                                    }
                                    else insert_lpi_q(&p.hdr.pmi.lpq, str, a-'a');
                                    /*printf("\rinserted %.4i", ++n_str);*/
                                }
                            /*}*/
                        }
                    }
                }
            }
        }
        if(i == 0){
            finalize_col_map(&p);
            puts("beginning expensive insertions");
            clock_gettime(CLOCK_MONOTONIC, &st);
        }
    }
    fin = cleanup_pmi(&p);
    elapsed = fin.tv_sec-st.tv_sec;
    elapsed += (fin.tv_nsec-st.tv_nsec)/1000000000.0;
    printf("generated %i strings\n", n_str);
    printf("%i unfruitful lock free queue insertion attempts\n", attempts-n_str);
    printf("relevant insertion took %lf seconds\n", elapsed);
    /*this isn't accurrate actually - need to stop clock after joins*/
	/*build_pmap_hdr(&p, "ashini");*/
    /*
	 * build_pmap_hdr(&p, "baby");
	 * build_pmap_hdr(&p, "abby");
	 * build_pmap_hdr(&p, "a slightly longer string");
    */

    /*
     * insert_pmap(&p, "baby", 99);
     * insert_pmap(&p, "abby", 19);
	 * insert_pmap(&p, "a slightly longer string", 5999);
    */
	/*insert_pmap(&p, "ashini", 49);*/
    fclose(p.fp);
}
