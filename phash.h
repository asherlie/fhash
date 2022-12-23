#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>

static const _Bool locking = 0;

/* this is the same as pmi_entry... remove one */
/* TODO: separate out pmi_q code/defs */
struct pmi_entry{
    void* key;
    void* val;
};

struct pmi_q{
    _Atomic int ins_idx, pop_idx;
    /*
     * pop target is set to the total number of entries that will be inserted
     * this is known because insertion is only done on a second pass of the data
     * after generating/writing header data
    */
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
    _Bool duplicates_expected;
    int n_threads;
    /* used to reserve insertion indices per bucket */
    _Atomic int* bucket_ins_idx;
    _Atomic int n_entries;
    int rwbuf_sz, max_bucket_len;
    struct pmi_q pq;

    int (*hash_func)(void*, int);
    /* TODO: remove locking related fields */
    struct locking_pmi_q lpq;
    pthread_t* pmi_q_pop_threads;
};

/*
 * TODO: allocate/write max_vallen_map
 * max vallen and keylen should both be set to [keylen/vallen]*n_buckets python style
*/
struct pmap_hdr{
	int entries, n_buckets;
    /*
	 * max_keylen_map is used to keep track of the longest key length in a given bucket
	 * this is so that we don't need to take up memory to store long keys for buckets without any
     * TODO: get rid of max_*len_map - if a user wants variable length strings they can just
     * use max string length
     * i can keep it around for now but it may get overly complex having the option of variable
     * length
    */
	int* col_map, * max_keylen_map, * max_vallen_map, * bucket_offset;
    /* stored in a diff struct because this is never written to phash file */
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
	char* key;
	int val;
};

struct pmap{
	_Bool insert_ready;
	char fn[50];
	struct pmap_hdr hdr;
    // TODO: fields below should be moved to pmi because they're not loaded
    size_t keylen, vallen;
    /* redundant but convenient - the above will be set to 0 if variable */
    _Bool variable_keylen, variable_vallen;
};

// do i need to use stringification?##
// okay, should this define an inline function?
// i need to generate code that initializes a pmap of a given type
// info should somehow be stored about the type
// insertions should know how many bytes are contained in k/v
//
// maybe pmap_entry should be redefined based on this
// maybe it'll create a new struct of struct .... that leads to a pmap
//
// need an init function that will create a phash struct that contains
// an underlying pmap as well as the sizeof the type
//
// it also must define a set of functions that lookup/insert
// lookup will return the type
//
// how will init_pmap() take in this new type?
// should this overwrite pmi_entry/pmap_entry?
//
// these should be passed a name to set along with key and value
//
// insert/init/lookup/build - all function will be updated to take in sizeof(key/value)
// and replaced with *_internal()
// macros will define actual implementations using sizeof(KEYTYPE/VALTYPE)
//
// user will use init_pmap_intlong
//
// first step is to make init,build_hdr,insert,lookup functions take in key/value bytelen
// alternatively, init() can just take in size of each and store it for use in other functions
//
// no need to write to a file, as reader/writers will both have these macros being set
// the programmer will create the high level functions needed with these calls
// that will implicitly set up the sizeof(fields)
//
// hmm, can i just guarantee that a macro will be called before anything else and require
// a new pmap_entry_##NAME to be defined and passed into everything
// eh i think not actually, i'll just use void*s and bytelen variables
// doesn't matter so much, especially if we get rid of risk by not exposing the programmer to this
// by abstracting it away with the preprocessor

// these will mostly be removed from this header in favor of just the macro and the inline functions they define
/* TODO: potentially roll together init/build, insert/finalize - they can check if(_) and run operations */
/* NOTE: pmaps are only meant to be used with variable key/val len when using strings */
void init_pmap(struct pmap* p, char* fn, int (*hash_func)(void*, int), size_t keylen, size_t vallen, 
               int n_buckets, int n_threads, int elements_in_mem, _Bool duplicates_expected);
void build_pmap_hdr(struct pmap* p, void* key, void* val);
void finalize_pmap_hdr(struct pmap* p);
void insert_pmap(struct pmap* p, void* key, void* val);
struct timespec seal_pmap(struct pmap* p);

void load_pmap(struct pmap* p, char* fn);
void* lookup_pmap(const struct pmap* p, void* key, int (*hash_func)(void*, int));
int partial_load_lookup_pmap(int fd, char* key);

// defines a shallow wrapper for struct pmap* to be returned by init()
// we really need to be able to use variable length strings - or even static length
#define define_pmap(NAME, KEYTYPE, VALTYPE, hash_func)                                       \
typedef struct NAME{                                                                         \
    struct pmap* p;                                                                          \
}NAME;                                                                                       \
                                                                                             \
static inline NAME* init_##NAME(char* fn){                                                   \
    NAME* r = malloc(sizeof(NAME));                                                          \
    r->p = malloc(sizeof(struct pmap));                                                      \
    /* TODO: get variable length char* working */                                            \
    /* to do this i'll need separate macros for string maps */                               \
    init_pmap(r->p, fn, hash_func, sizeof(KEYTYPE), sizeof(VALTYPE), 1024, 5, 524288, 0);    \
    return r;                                                                                \
}                                                                                            \
                                                                                             \
static inline void build_##NAME##_hdr(NAME* map, KEYTYPE key, VALTYPE val){                  \
    build_pmap_hdr(map->p, (void*)&key, (void*)&val);                                        \
}                                                                                            \
                                                                                             \
static inline void finalize_##NAME##_hdr(NAME* map){                                           \
    finalize_pmap_hdr(map->p);                                                               \
}                                                                                            \
                                                                                             \
static inline void insert_##NAME(NAME* map, KEYTYPE* key, VALTYPE* val){                     \
    insert_pmap(map->p, (void*)key, (void*)val);                                             \
}                                                                                            \
                                                                                             \
static inline void seal_##NAME(NAME* map){                                                   \
    seal_pmap(map->p);                                                                       \
}                                                                                            \
                                                                                             \
static inline NAME* load_##NAME(char* fn){                                                   \
    NAME* r = malloc(sizeof(NAME));                                                          \
    r->p = malloc(sizeof(struct pmap));                                                      \
    load_pmap(r->p, fn);                                                                     \
    return r;                                                                                \
}                                                                                            \
                                                                                             \
static inline VALTYPE* lookup_##NAME(NAME* map, KEYTYPE* key){                                \
    return (VALTYPE*)lookup_pmap(map->p, (void*)key, hash_func);                            \
}

static inline int hash_int(void* key, int buckets){
    return *(int*)key % buckets;
}


struct spotify_song{
    uint32_t uid;
    int danciness;
    int key;
    float tempo;
    int volume;
};

struct spotify_uri{
    char uri[32];
};

define_pmap(test_map, int, int, hash_int)
