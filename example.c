#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#include "phash.h"

// TODO: test functions msut go in a separate test file
#if 0
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
     * testing out concurrent reads/writes
     * should pop exactly what is inserted
     * and should never contain more than capacity
     * once this is written i can write the insertion thread and just have the user decide how many
     * to spawn with init_pmap()
    */
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
#endif

struct test_struct{
    int val;
    char stree[5];
};

int hash_str(void* key, int n_buckets){
    int idx = 0;
    for(char* i = key; *i; ++i){
        idx += (*i*(i-(char*)key+1));
    }
    return idx % n_buckets;
}

void lookup_test(char* fn, char* key, _Bool partial){
    struct pmap p;
    void* ret;
    struct test_struct* val;

    if(partial){
        int fd = open(fn, O_RDONLY);
        printf("val: %i\n", partial_load_lookup_pmap(fd, key));
        close(fd);
        return;
    }
    load_pmap(&p, fn);
    ret = lookup_pmap(&p, key, hash_str);
    if(!ret){
        puts("no match found");
        return;
    }
    val = ret;
    printf("stree: \"%s\", val: %i\n", val->stree, val->val);
}

int* get_heap_int(int x){
    int* ret = malloc(sizeof(int));
    *ret = x;
    return ret;
}

int kv_test(){
    struct pmap p;
    int x;
    char fn[] = "kt";
    /*init_pmap(&p, "kt", sizeof(int), sizeof(struct test_struct), 1024, 5, 524288, 0);*/
    init_pmap(&p, fn, hash_str, sizeof(int), 0, 1024, 5, 524288, 0);
    for(int i = 0; i < 10; ++i){
        // val must be specified if variable length
        build_pmap_hdr(&p, &i, "STRING");
    }
    finalize_pmap_hdr(&p);
    for(int i = 0; i < 10; ++i){
        insert_pmap(&p, get_heap_int(i), strdup("STRING"));
    }
    seal_pmap(&p);

    load_pmap(&p, fn);
    x = 3;
    /*key must be same exact length*/
    puts((char*)lookup_pmap(&p, &x, hash_str));

    return 0;
}

/*
 * int hash_int(void* key, int buckets){
 *     return *(int*)key % buckets;
 * }
 * 
*/
void macro_test(){
    int* k = malloc(4), * v = malloc(sizeof(4));
    test_map* map = init_test_map("test_map"), * loaded;
    build_test_map_hdr(map, 0, 59);
    finalize_test_map_hdr(map);
    *k = 0;
    *v = 59;
    insert_test_map(map, k, v);
    seal_test_map(map);

    loaded = load_test_map("test_map");
    k = lookup_test_map(loaded, k);
    printf("val: %i\n", *k);
}

struct spotify_song* gen_song(int danciness, float tempo, int volume, int key, int uid){
    struct spotify_song* s = malloc(sizeof(struct spotify_song));
    s->key = key;
    s->danciness = danciness;
    s->tempo = tempo;
    s->volume = volume;
    s->uid = uid;
    return s;
}

struct spotify_uri* gen_uri(char* str){
    struct spotify_uri* u = malloc(sizeof(struct spotify_uri));
    strncpy(u->uri, str, 32);
    return u;
}

/*
 * could have hashing function generated by a macro
 * and take in granularity for each option - if 0, not counted, the larger the granularity the larger the buckets
*/
#define def_hash_func(NAME, DANCE_GRAN, KEY_GRAN, TEMPO_GRAN, VOLUME_GRAN)
static inline int hash_song_dance_tempo(void* song, int buckets){
    struct spotify_song* s = song;
    // the higher the granularity value, the more similar songs in buckets should be
    int granularity = 1;
    int danciness_window = s->danciness/(10*granularity);
    int tempo_window = s->tempo*10;
    return (danciness_window*3+tempo_window*7) % buckets;
    return (s->danciness+s->key) % buckets;

/*
    low granularity - windows are large - 94, 91, 82, 11
    94/low_num
    94, 91, 82 will be the same bucket

    high granularity - windows large 94, 91, 82, 11
    94,91 same bucket j


hmm, higher denominator the larger the windows
THERE WILL BE `DENOMINATOR` ITEMS PER WINDOW - easy!

    low - 9, 9, 8, 1
*/
}

define_pmap(spotify_map, struct spotify_song, struct spotify_uri, hash_song_dance_tempo)

void spotify_test(){
    struct spotify_song* songs[2] = {gen_song(94, .4, 9, 10, 0), gen_song(1, .4, 2, 10, 1)};
    struct spotify_uri* uris[] = {gen_uri("dancy medium speed loud key 10"), gen_uri("not-dancy medium speed quiet key 10")};
    struct spotify_song key;
    /*memcpy(ls, s, sizeof(struct spotify_song));*/
    spotify_map* m = init_spotify_map("spot");
    for(int hdrins = 0; hdrins < 2; ++hdrins){
        for(uint32_t i = 0; i < sizeof(songs)/sizeof(struct spotify_song*); ++i){
            if(hdrins == 0)
                build_spotify_map_hdr(m, *songs[i], *uris[i]);
            else insert_spotify_map(m, songs[i], uris[i]);
        }
        if(hdrins == 0)finalize_spotify_map_hdr(m);
    }
    seal_spotify_map(m);

    spotify_map* lm = load_spotify_map("spot");
    key.danciness = 94;
    key.tempo = .4;
    /*key.key = ;*/
    struct spotify_uri** lu  = lookup_spotify_map_bucket(lm, &key, 0, 1000);
    int i;
    for(i = 0; i < 1000; ++i){
        if(!lu[i])break;
        printf("uri: \"%s\"\n", lu[i]->uri);
    }
    printf("found %i matches\n", i);
}

int main(int argc, char** argv){
    spotify_test();
    /*macro_test();*/
    return 0;
    return kv_test();
	struct pmap p;
    char str[6] = {0};
    struct test_struct* val;
    int n_str = 0;
    int attempts = 0;
    double elapsed;
    struct timespec st, fin;

    if(argc > 1){
        lookup_test("fyle", argv[1], 0);
        return 0;
    }

    /* can't let thread count get too high while keeping capcity low or they compete over slots to pop from */
    /* TODO: these should be dynamically chosen using expected insertions and max_threads and memory */
    /* 3/4 threads seems good for this - at 9.7s for 11M */
    init_pmap(&p, "fyle", hash_str, 0, sizeof(struct test_struct), 1024, 5, 524288, 0);
    /* inserting (26^5)7 strings - ~83.1M takes 4m36s */
    for(int i = 0; i < 2; ++i){
        for(char a = 'a'; a <= 'z'; ++a){
            for(char b = 'a'; b <= 'z'; ++b){
                for(char c = 'a'; c <= 'z'; ++c){
                    for(char d = 'a'; d <= 'z'; ++d){
                        for(char e = 'a'; e <= 'z'; ++e){
                            /*for(char f = '0'; f <= '7'; ++f){*/
                                str[0] = a;
                                str[1] = b;
                                str[2] = c;
                                str[3] = d;
                                str[4] = e;
                                /*str[5] = f;*/

                                if(i == 0){
                                    /* key is variable length, value is not */
                                    build_pmap_hdr(&p, str, NULL);
                                }
                                else{
                                    ++n_str;
                                    if(!locking){
                                        val = malloc(sizeof(struct test_struct));
                                        memcpy(val->stree, str, 5);
                                        val->val = a;
                                        /**val = a-'a'+d-'a';*/
                                        /**val = a;*/
                                        /**val = n_str;*/
                                        insert_pmap(&p, strdup(str), val);
                                    }
                                    /*else insert_lpi_q(&p.hdr.pmi.lpq, str, a-'a');*/
                                }
                            }
                        /*}*/
                    }
                }
            }
        }
        if(i == 0){
            finalize_pmap_hdr(&p);
            puts("beginning expensive insertions");
            clock_gettime(CLOCK_MONOTONIC, &st);
        }
    }
    fin = seal_pmap(&p);
    elapsed = fin.tv_sec-st.tv_sec;
    elapsed += (fin.tv_nsec-st.tv_nsec)/1000000000.0;
    printf("generated %i strings\n", n_str);
    printf("%i unfruitful lock free queue insertion attempts\n", attempts-n_str);
    printf("relevant insertion took %lf seconds\n", elapsed);
}
