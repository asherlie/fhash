#include <stdio.h>

// hdr will be loaded into memory and used to know
// how many buckets exist
// the col_map will also be included here
// or will it be?
// we can just NULL terminate collission lists in each bucket
// we only strictly need col_map to know the cardinality
// of col arrays when allocating without all loaded to mem
struct pmap_hdr{
	int n_buckets;
	// max_keylen_map is used to keep track of the longest
	// key length in a given bucket
	// this is so that we don't need to take up memory
	// to store long keys for buckets without any
	// bucket_offset is used to determine fp offset of a bucket
	int* col_map, * max_keylen_map, * bucket_offset;
};

struct pmap_entry{
	int key_len;
	char* key;
	int val;
};

struct pmap{
	_Bool insert_ready;
	char fn[50];
	FILE* fp;
	struct pmap_hdr hdr;
};

void init_pmap(struct pmap* p, char* fn, int n_buckets);
// col_map must be built with identical data
// to what will be inserted
//
void init_pmap_hdr(struct pmap* p, int n_buckets);
void build_pmap_hdr(struct pmap* p, char* key);
void finalize_col_map(struct pmap* p);

// inserts k/v pair into pmap 
void insert_pmap(struct pmap* p, char* key, int val);


/* client */
void load_pmap(struct pmap* p, char* fn);
int lookup_pmap(const struct pmap* p, char* key);
