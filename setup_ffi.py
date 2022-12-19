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

ffibuilder.compile(verbose=True);
