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
int lookup(char*, char*);
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
int lookup(char*, char*);
""")

# ffibuilder.cdef("""
# typedef struct pmap pmap_t;
# void init_pmap(pmap_t*, char* fn, int n_buckets, int n_threads, int elements_in_mem, _Bool duplicates_expected);
# """)

ffibuilder.compile(verbose=True);
#void init_pmap(pmap_t*, char* fn, int n_buckets, int n_threads, int elements_in_mem, _Bool duplicates_expected);

from _phash import lib as phash
print(phash.init(b"FOOFOO", 1000, 32, 50000, False))
phash.build_hdr(b"asher");
phash.build_hdr(b"eteri");
phash.finalize_hdr();
phash.insert(b"asher", 900)
phash.insert(b"eteri", 141)
phash.seal()
print(phash.lookup(b"FOOFOO", b"asher"))
