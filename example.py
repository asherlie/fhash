# i will use the search endpoint along with the audio features endpoint!

# i can maybe search for an empty string and read results 50 at a time

# results in each category will be put into ranges, the granularity of which can be adjusted
# danceability range of 10% maybe - 0.0-0.1, 0.1-0.2, etc.

# hash() will choose buckets based on danceability_range*3+key*7+speechiness*11
# could even just add them all together - the sum of these categories will be guaranteed to contain similar songs
# but also could contain dissimilar ones - we will still need to check individual fields

# user can optionally exclude certain fields from hashing function if they want similar songs that could possibly be
# differnt in danciness! so cool

# i will have a bitfield of important_categories or filtering_categories
# they will be a subset of danceability, energy, key, loudness, speechiness, acousticness, liveness, valence, tempo, time_sig
# and monthly listeners

# key can just be an integer with all these values combined - probably each one put into a range and possibly multiplied by
# a different prime to avoid collissions

# value will be track id - a string of length 22

# it will not be good to have to do multiple passes on the data, though - i should just include estimates for this
# and overallocate
# this will also enable inserting into old maps


# new priorities are now:
    # arbitrary key/value
    # overallocation and enabling of updating old maps

# actually it'll be acceptable to do multiple passes - we'll read spotify data to a file
# to minimize api calls

import spotmap 
import spot
import json
import sys

from cffi import FFI

ffi = FFI()

def test_songs(fn):
    sm = spotmap.SpotMap('spot_test')
    songs = spot.load_releases(fn)
    for tries in range(2):
        for i in songs:
            # print(f"{i[2]['danceability']}, {i[2]['energy']}, {i[2]['key']}, {i[2]['loudness']}, {i[2]['speechiness']}, {i[2]['acousticness']}, {i[0] + ' ' + i[1]}")
            if tries == 0:
                sm.build_header(i[2]['danceability'], i[2]['energy'], i[2]['key'], i[2]['loudness'], i[2]['speechiness'], i[2]['acousticness'], i[0] + ' ' + i[1])
            else: sm.insert(i[2]['danceability'], i[2]['energy'], i[2]['key'], i[2]['loudness'], i[2]['speechiness'], i[2]['acousticness'], i[0] + ' ' + i[1])
    sm.seal()

def test_similar_songs(fn, danceability, energy, key, loudness, speechiness, acousticness):
    sm = spotmap.SpotMap('spot_test')
    sim = sm.lookup_similar(danceability, energy, key, loudness, speechiness, acousticness)
    # print(sim)
    return sim

def prewritten_sim():
    return test_similar_songs('', .755, .635, 8, -6.666, .28, .00253)

def lookup_song(fn, danceability, energy, key, loudness, speechiness, acousticness):
    sm = spotmap.SpotMap('spot_test')
    return str(ffi.string(sm.lookup(danceability, energy, key, loudness, speechiness, acousticness)))[2:-1]


def old_test():
    n = 0
    sm = spotmap.SpotMap('fyle')
    if len(sys.argv) > 1:
        # print(ph.lookup(sys.argv[1]))
        print(sm.lookup_quick(sys.argv[1]))
    else:
        alph="abcdefghijklmnopqrstuvwxyz"
        for x in range(2):
            for i in alph:
                for j in alph:
                    for k in alph:
                        for l in alph:
                            for m in alph:
                                genstr = i+j+k+l+m
                                if x == 0:
                                    ph.build_header(genstr)
                                if x == 1:
                                    n += 1
                                    sm.insert(genstr, n)

        sm.seal()
        print(f'inserted {n} entries');

jf = 'songs.json'
test_songs(jf)
# only danceability and key are used for now
# test_similar_songs(jf, .755, 0, 8, 0, 0, 0)
# sim = test_similar_songs(jf, .755, 0, 8, 0, 0, 0)
sim = test_similar_songs(jf, .193, 0, 7, 0, 0, 0)
idx = 0
while sim[idx] != ffi.NULL:
    print(f'{idx}: {str(ffi.string(sim[idx]))[2:-1]}')
    idx += 1
# print(ffi.string(sim[1]))
# print(lookup_song("spot_test", .755, .635, 8, -6.666, .28, .00253))
