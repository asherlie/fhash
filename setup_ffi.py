from cffi import FFI

ffibuilder=FFI()

ffibuilder.set_source(
"_phash",
"""
void init(char*, int, int, int, _Bool);
void build_hdr(char*);
void finalize_hdr();
void insert(char*, int);
void seal();
int lookup_quick(char*, char*);
void load(char*);
int lookup(char*);
""",
sources = ['phash.c', 'pyffi.c'],
library_dirs = [],
libraries = ['atomic'],
)

ffibuilder.cdef("""
void init(char*, int, int, int, _Bool);
void build_hdr(char*);
void finalize_hdr();
void insert(char*, int);
void seal();
int lookup_quick(char*, char*);
void load(char*);
int lookup(char*);
""")

# ffibuilder.cdef("""
# typedef struct pmap pmap_t;
# void init_pmap(pmap_t*, char* fn, int n_buckets, int n_threads, int elements_in_mem, _Bool duplicates_expected);
# """)

ffibuilder.compile(verbose=True);
#void init_pmap(pmap_t*, char* fn, int n_buckets, int n_threads, int elements_in_mem, _Bool duplicates_expected);
