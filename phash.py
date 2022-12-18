from _phash import lib as phash

class phash:
    def __init__(self, fn, buckets=1024, threads=32, entries=50000, duplicates=False):
        self.fn = fn
        self.initialized = False
        self.finalized = False
        self.buckets=buckets
        self.threads=threads
        self.entries=entries
        self.duplicates=duplicates

    def build_header(self, key):
        if not self.initialized:
            phash.init(self.fn, self.buckets, self.threads, self.entries, self.duplicates)
            self.initialized = True
        phash.build_hdr(bytes(key, 'utf-8'))

print(phash.init(b"FOOFOO", 1000, 32, 50000, False))
phash.build_hdr(b"asher");
phash.build_hdr(b"eteri");
phash.finalize_hdr();
phash.insert(b"asher", 900)
phash.insert(b"eteri", 141)
phash.seal()
print(phash.lookup_quick(b"FOOFOO", b"asher"))
