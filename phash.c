#include <stdlib.h>
#include <stdint.h>
#include <limits.h>
#include <string.h>

#include "phash.h"

const _Bool debug = 0;

int hash(char* key, int n_buckets){
	int idx = 0;
	for(char* i = key; *i; ++i){
		idx += (*i*(i-key+1));
	}
	return idx % n_buckets;
}

void init_pmap_hdr(struct pmap* p, int n_buckets){
	p->hdr.n_buckets = n_buckets;
	p->hdr.col_map = calloc(sizeof(int), n_buckets);
	p->hdr.max_keylen_map = calloc(sizeof(int), n_buckets);
	p->hdr.bucket_offset = calloc(sizeof(int), n_buckets);
}

void init_pmap(struct pmap* p, char* fn, int n_buckets){
	init_pmap_hdr(p, n_buckets);
	strncpy(p->fn, fn, sizeof(p->fn)-1);
	p->fp = fopen(p->fn, "wb");
	p->insert_ready = 0;
	/*fwrite(&p->hdr, sizeof(struct p));*/
}

void build_pmap_hdr(struct pmap* p, char* key){
	int idx = hash(key, p->hdr.n_buckets);
	int keylen = strlen(key);
	++p->hdr.col_map[idx];
	if(keylen > p->hdr.max_keylen_map[idx])
		p->hdr.max_keylen_map[idx] = keylen;
}

void write_zeroes(FILE* fp, int nbytes){
	int* z = calloc(nbytes, 1);
	fwrite(z, nbytes, 1, fp);
	free(z);
}
// build the scaffolding that all k/v will fit into
void finalize_col_map(struct pmap* p){
	int cur_offset = sizeof(int)+(3*sizeof(int)*p->hdr.n_buckets);
	// can i alloc in this loop? i'll need to alloc hdr first
	// then go back in the end with fseek(wtvr) to overwrite bucket_offset
	for(int i = 0; i < p->hdr.n_buckets; ++i){
		p->hdr.bucket_offset[i] = cur_offset;
        /*printf("offset_idx");*/
		// number of items per idx * (reserved space per key + value int)
		cur_offset += (p->hdr.col_map[i]*(p->hdr.max_keylen_map[i]+sizeof(int)));
	}
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
        /*
         * not enough zeroes being written, should be 32 - 16 initially, 8 for each k/v pair
         * are they being written?
        */
	}
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
	p->insert_ready = 1;
}

_Bool mempty(uint8_t* buf, int len){
	for(int i = 0; i < len; ++i){
		if(buf[i])return 0;
	}
	return 1;
}

/*
 * need to store n_entries in hdr
 * need insertions to be much faster
*/
// insert needs to fseek() using offset finder of hash()
// fseek(seek_set, 4+2*n_buckets+)
/*void insert_pmap(struct pmap* p, char* key, int val){*/
// need to pass rbuf and wrbuf - they should be alloc'd in the caller()
// max_kv_sz = max(max_keylen_map)
// i can potentially make this threadsafe and insert in different offsets from different threads
// not sure if this will corrupt anything - having multiple FILE*s to the same file
void insert_pmap(struct pmap* p, char* key, int val, uint8_t* rdbuf, uint8_t* wrbuf){
	int idx = hash(key, p->hdr.n_buckets);
	int kv_sz = p->hdr.max_keylen_map[idx]+sizeof(int);
	int cur_offset;
    long int br;
    /*
	 * uint8_t* rdbuf = malloc(kv_sz);
	 * uint8_t* wrbuf = calloc(kv_sz, 1);
    */
    /* zero the section of wrbuf we'll be using */
    memset(wrbuf, 0, kv_sz);
    // shouldn't be necessary but helps debugging
    /*memset(rdbuf, 0, kv_sz);*/
	memcpy(wrbuf, key, strlen(key));
	memcpy(wrbuf+p->hdr.max_keylen_map[idx], &val, sizeof(int));
	/*p->hdr.bucket_offset[idx];*/
	cur_offset = p->hdr.bucket_offset[idx];
	fseek(p->fp, cur_offset, SEEK_SET);
    /*perror("FSEEK");*/
    // can i organize the data in such a way that it's easier to compare strings?
    // buckets are getting large and O(N) is not so easy anymore
    // could also write a better hashing function
    //
    // ok the options are:
    //  multithreading and split up the insertion
    //      this is a good option - i insert_x() can add a request to a queue
    //      this queue will have a limited amount of space and will block insertions
    //      until it's been sufficienty popped and its elements moved to the hash
    //
    //  sort insertions to make finding duplicates easier
    //  some kind of binary search?
    //
    //  can also enable the assumption that no duplicates will
    //  be inserted
    //      this is anothr good option - write this as a proof of concept
    //
    //  this will be the case in spotify
    //
    //  if we can enable duplicate detection only for updates!
    //
    //  write a better hash()
	if(debug)printf("seeking to offset %i for key \"%s\"\n", cur_offset, key);
	if(debug)printf("idx is %i with max keylen: %i\n", idx, p->hdr.max_keylen_map[idx]);
	// iterate over all entries in a bucket looking for duplicates
	// if none are found, insert at idx 0
	for(int i = 0; i < p->hdr.col_map[idx]; ++i){
        br = fread(rdbuf, 1, kv_sz, p->fp);
		if(debug)printf("    %i/%i: read %li/%i bytes\n", i, p->hdr.col_map[idx], br, kv_sz);
        /*perror("");*/
		/*if empty - rewind, insert*/
		/*if key is identical, update*/
		if(!memcmp(rdbuf, wrbuf, p->hdr.max_keylen_map[idx]) || mempty(rdbuf, kv_sz)){
            if(debug)printf("    found a spot to write\n");
			fseek(p->fp, -kv_sz, SEEK_CUR);
			fwrite(wrbuf, kv_sz, 1, p->fp);
			break;
		}
	}
    fflush(p->fp);
    /*
     * fclose(p->fp);
     * p->fp = fopen(p->fn, "rb+");
    */
}

// this is from the client perspective
void load_pmap(struct pmap* p, char* fn){
    strcpy(p->fn, fn);
    p->fp = fopen(fn, "rb+");
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
    fseek(p->fp, p->hdr.bucket_offset[idx], SEEK_SET);

	for(int i = 0; i < p->hdr.col_map[idx]; ++i){
        fread(rdbuf, 1, kv_sz, p->fp);
        if(!strncmp(rdbuf, key, p->hdr.max_keylen_map[idx]))
            return *((int*)(rdbuf+p->hdr.max_keylen_map[idx]));
    }
    return -1;
}

void lookup_test(char* fn){
    struct pmap p;
    int val;
    load_pmap(&p, fn);
    val = lookup_pmap(&p, "ashy");
    printf("VAL: %i\n", val);
    fclose(p.fp);
}

/*should contain total number of k/v pairs*/

int main(){
    /*lookup_test("PM");*/
    /*return 0;*/
	struct pmap p;
    char str[5];
    int n_str = 0, tmp_keylen;
    uint8_t* rdbuf, * wrbuf;
	init_pmap(&p, "PM", 100000);
    // inserting 26^4 strings - ~500k
    for(int i = 0; i < 2; ++i){
        for(char a = 'a'; a <= 'z'; ++a){
            for(char b = 'a'; b <= 'z'; ++b){
                for(char c = 'a'; c <= 'z'; ++c){
                    for(char d = 'a'; d <= 'z'; ++d){
                        /*for(char e = 'a'; e <= 'z'; ++e){*/
                            /*++n_str;*/
                            str[0] = a;
                            str[1] = b;
                            str[2] = c;
                            str[3] = d;
                            /*str[4] = e;*/

                            if(i == 0){
                                build_pmap_hdr(&p, str);
                            }
                            else{
                                insert_pmap(&p, str, a-'a', rdbuf, wrbuf);
                                /*printf("\rinserted %.4i", ++n_str);*/
                            /*}*/
                        }
                    }
                }
            }
        }
        if(i == 0){
            finalize_col_map(&p);
            tmp_keylen = 0;
            for(int j = 0; j < p.hdr.n_buckets; ++j){
                if(p.hdr.max_keylen_map[j] > tmp_keylen)
                    tmp_keylen = p.hdr.max_keylen_map[j];
            }
            tmp_keylen += sizeof(int);
            rdbuf = malloc(tmp_keylen);
            wrbuf = malloc(tmp_keylen);
        }
    }
    printf("generated %i strings\n", n_str);
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

    lookup_test("PM");
}
