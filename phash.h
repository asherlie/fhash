#include <stdio.h>
#include <stdint.h>
#include <pthread.h>

/* this is the same as pmi_entry... remove one */
/* TODO: separate out pmi_q code/defs */
struct pmi_entry{
    char* key;
    int val;
};

struct pmi_q{
    _Atomic int ins_idx, pop_idx;
    // pop target is set to the total number of entries that will be inserted
    // this will be known because insertion is only done on a second pass of the data
    // after calculating key size
    // although if we know there won't be collisions we can maybe just also assume a keylen
    // and do a single pass
    //
    // if this is changed to use single pass then we can use a timeout mechanism
    //
    int const_capacity, pop_target;
    _Atomic int n_popped;

    _Atomic struct pmi_entry** entries;
};

struct locking_pmi_q{
    struct pmi_entry** entries;
    int cap, sz, pop_idx, ins_idx;
    int n_popped, pop_target;
    pthread_mutex_t lock;
    pthread_cond_t pop_ready;
    pthread_cond_t ins_ready;
};

struct pmap_insertion{
    // if duplicates are expected, opt for the more conservative mutex lock
    // in case of collision
    // if !duplicates_expected, we can just reserve idx atomically because we know ther
    // will not be any collisions
    _Bool duplicates_expected;
    int n_threads;
    // used to reserve insertion indices per bucket
    _Atomic int* bucket_ins_idx;
    _Atomic int n_entries;
    int rwbuf_sz;
    struct pmi_q pq;
    struct locking_pmi_q lpq;
    pthread_t* pmi_q_pop_threads;
};
// hdr will be loaded into memory and used to know
// how many buckets exist
// the col_map will also be included here
// or will it be?
// we can just NULL terminate collission lists in each bucket
// we only strictly need col_map to know the cardinality
// of col arrays when allocating without all loaded to mem
struct pmap_hdr{
	int entries, n_buckets;
	// max_keylen_map is used to keep track of the longest
	// key length in a given bucket
	// this is so that we don't need to take up memory
	// to store long keys for buckets without any
	// bucket_offset is used to determine fp offset of a bucket
	int* col_map, * max_keylen_map, * bucket_offset;
    // stored in a diff struct because this is never written to phash file
    struct pmap_insertion pmi;
};

/*
 * IMPORTANT: i should use #defines to make this modular
 * user must define a (packed?) struct
 * or maybe they can just pass in a struct and its size
 *
 * TODO: i must be able to pass in keys/values and this should work with abstract data/arbitrary structs
*/
struct pmap_entry{
	//int key_len;
	char* key;
	int val;
};

//get rid of FILE*, use pread() if i must
struct pmap{
	_Bool insert_ready;
	char fn[50];
	FILE* fp;
	struct pmap_hdr hdr;
};

void init_pmap(struct pmap* p, char* fn, int n_buckets, int n_threads, int elements_in_mem, _Bool duplicates_expected);
// col_map must be built with identical data
// to what will be inserted
//
void init_pmap_hdr(struct pmap* p, int n_buckets, int n_threads, int pq_cap, _Bool duplicates_expected);
void build_pmap_hdr(struct pmap* p, char* key);
void finalize_pmap_hdr(struct pmap* p);
//void cleanup_pmi(struct pmap* p);
struct timespec cleanup_pmi(struct pmap* p);

// inserts k/v pair into pmap 
//void insert_pmap(struct pmap* p, char* key, int val);
//void insert_pmap(struct pmap* p, char* key, int val, uint8_t* rdbuf, uint8_t* wrbuf, FILE* fp);
void insert_pmap(struct pmap* p, char* key, int val, uint8_t* rdbuf, uint8_t* wrbuf, int fd);


/* client */
void load_pmap(struct pmap* p, char* fn);
int lookup_pmap(const struct pmap* p, char* key);
