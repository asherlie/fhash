#include <fcntl.h>
#include <string.h>
#include <unistd.h>

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
struct song_name{
    char name[1000];
};

struct song_attributes{
    float danceability;
    float energy;
    int key;
    float loudness;
    float speechiness;
    float acousticness;
};

static inline int hash_song_dance_key(void* song, int buckets){
    struct song_attributes* a = song;
    return ((int)(a->danceability*11+a->key*7)) % buckets;
    // the higher the granularity value, the more similar songs in buckets should be
    int granularity = 10;
    int danciness_window = a->danceability/(10*granularity);
    int key_window = a->key*10*granularity;
    return (danciness_window*3+key_window*7) % buckets;
}

define_pmap(spot_map, struct song_attributes, struct song_name, hash_song_dance_key)

spot_map* P;


void init(char* fn){
    P = init_spot_map(fn);
}

void build_hdr(float danceability, float energy, int key, float loudness, float speechiness, float acousticness, char* name){
    struct song_name n;
    struct song_attributes a = 
        {.danceability=danceability, .energy=energy, .key=key, .loudness=loudness,
        .speechiness=speechiness, .acousticness=acousticness};
    strcpy(n.name, name);

    build_spot_map_hdr(P, a, n);
}

void finalize_hdr(){
    finalize_spot_map_hdr(P);
}

void insert(float danceability, float energy, int key, float loudness, float speechiness, float acousticness, char* name){
    struct song_name* n = calloc(sizeof(struct song_name), 1);
    struct song_attributes* a = malloc(sizeof(struct song_attributes));

    strcpy(n->name, name);
    a->danceability = danceability;
    a->energy = energy;
    a->key = key;
    a->loudness = loudness;
    a->speechiness = speechiness;
    a->acousticness = acousticness;

    insert_spot_map(P, a, n);
}

void seal(){
    seal_spot_map(P);
}

void load(char* fn){
    P = load_spot_map(fn);
}

char* lookup(float danceability, float energy, int key, float loudness, float speechiness, float acousticness){
    struct song_attributes a = 
        {.danceability=danceability, .energy=energy, .key=key, .loudness=loudness, 
        .speechiness=speechiness, .acousticness=acousticness};
    return lookup_spot_map(P, &a)->name;
}

char** lookup_bucket(float danceability, float energy, int key, float loudness, float speechiness, float acousticness){
    struct song_attributes a = 
        {.danceability=danceability, .energy=energy, .key=key, .loudness=loudness, 
        .speechiness=speechiness, .acousticness=acousticness};
    return (char**)lookup_spot_map_bucket(P, &a, 0, 1000);
}

/*
 * int lookup_quick(char* fn, char* key){
 *     int fd = open(fn, O_RDONLY), ret;
 *     ret = partial_load_lookup_pmap(fd, key);
 *     close(fd);
 *     return ret;
 * }
*/
/*
 * 
 * void init(char* fn);
 * void build_hdr(float danceability, float energy, int key, float loudness, float speechiness, float acousticness, char* name);
 * void finalize_hdr();
 * void insert(float danceability, float energy, int key, float loudness, float speechiness, float acousticness, char* name);
 * void seal();
 * void load(char* fn);
 * char* lookup(float danceability, float energy, int key, float loudness, float speechiness, float acousticness);
 * char** lookup_bucket(float danceability, float energy, int key, float loudness, float speechiness, float acousticness);
*/
