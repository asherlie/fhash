#include "phash.h"
/*
 * i need a separate library that makes use of this and initializes structs in the background
 * the user shouldn't need to use any of these data structures - they should specify a file and
 * a global struct pmap should be set
 * in the future there can be a map of these each associated with a file living in global scope
 * 
 * init_pmap("fn", buckets, threads, 1000, 0)
 *     init_pmap(&global_pmap, buckets, ...)
*/
struct pmap P;


void init(char* fn, int n_buckets, int n_threads, int elements_in_mem, _Bool duplicates_expected){
    init_pmap(&P, fn, n_threads, n_threads, elements_in_mem, duplicates_expected);
}

void build_hdr(char* key){
    build_pmap_hdr(&P, key);
}

void finalize_hdr(){
    finalize_pmap_hdr(&P);
}

void insert(char* key, int val){
    insert_pmap(&P, key, val);
}
void seal(){
    seal_pmap(&P);
}

void load(char* fn){
    load_pmap(&P, fn);
}

int lookup(char* key){
    return lookup_pmap(&P, key);
}

int lookup_quick(char* fn, char* key){
    FILE* fp = fopen(fn, "rb");
    return partial_load_lookup_pmap(fp, key);
}
