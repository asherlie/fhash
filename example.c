#include <fcntl.h>
#include <unistd.h>

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

void lookup_test(char* fn, char* key, _Bool partial){
    struct pmap p;
    int val;

    if(partial){
        int fd = open(fn, O_RDONLY);
        printf("val: %i\n", partial_load_lookup_pmap(fd, key));
        close(fd);
        return;
    }
    load_pmap(&p, fn);
    val = lookup_pmap(&p, key);
    printf("VAL: %i\n", val);
}

int main(int argc, char** argv){
	struct pmap p;
    char str[6] = {0};
    int n_str = 0;
    int attempts = 0;
    double elapsed;
    struct timespec st, fin;

    if(argc > 1){
        lookup_test("PM", argv[1], 1);
        return 0;
    }

    /* can't let thread count get too high while keeping capcity low or they compete over slots to pop from */
    /* TODO: these should be dynamically chosen using expected insertions and max_threads and memory */
    /* 3/4 threads seems good for this - at 9.7s for 11M */
    init_pmap(&p, "PM", 10240, 5, 524288, 0);
    /* inserting (26^5)7 strings - ~83.1M takes 4m36s */
    for(int i = 0; i < 2; ++i){
        for(char a = 'a'; a <= 'z'; ++a){
            for(char b = 'a'; b <= 'z'; ++b){
                for(char c = 'a'; c <= 'z'; ++c){
                    for(char d = 'a'; d <= 'z'; ++d){
                        /*for(char e = 'a'; e <= 'z'; ++e){*/
                            /*for(char f = '0'; f <= '7'; ++f){*/
                                str[0] = a;
                                str[1] = b;
                                str[2] = c;
                                str[3] = d;
                                /*str[4] = e;*/
                                /*str[5] = f;*/

                                if(i == 0){
                                    build_pmap_hdr(&p, str);
                                }
                                else{
                                    ++n_str;
                                    if(!locking){
                                        insert_pmap(&p, str, a-'a'+d-'a');
                                    }
                                    /*else insert_lpi_q(&p.hdr.pmi.lpq, str, a-'a');*/
                                }
                            /*}*/
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
