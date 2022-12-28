from cffi import FFI

ffibuilder=FFI()

ffibuilder.set_source(
"_phash",
"""
void init(char* fn);
void build_hdr(float danceability, float energy, int key, float loudness, float speechiness, float acousticness, char* name);
void finalize_hdr();
void insert(float danceability, float energy, int key, float loudness, float speechiness, float acousticness, char* name);
void seal();
void load(char* fn);
char* lookup(float danceability, float energy, int key, float loudness, float speechiness, float acousticness);
char** lookup_bucket(float danceability, float energy, int key, float loudness, float speechiness, float acousticness);
""",
sources = ['phash.c', 'pyffi.c'],
library_dirs = [],
libraries = ['atomic'],
)

ffibuilder.cdef("""
void init(char* fn);
void build_hdr(float danceability, float energy, int key, float loudness, float speechiness, float acousticness, char* name);
void finalize_hdr();
void insert(float danceability, float energy, int key, float loudness, float speechiness, float acousticness, char* name);
void seal();
void load(char* fn);
char* lookup(float danceability, float energy, int key, float loudness, float speechiness, float acousticness);
char** lookup_bucket(float danceability, float energy, int key, float loudness, float speechiness, float acousticness);
""")

ffibuilder.compile(verbose=True);
