from _phash import lib as lphash

class phash:
    def __init__(self, fn, buckets=1024, threads=32, entries=50000, duplicates=False):
        self.fn = bytes(fn, 'utf-8')
        self.initialized = False
        self.finalized = False
        self.loaded = False
        self.buckets=buckets
        self.threads=threads
        self.entries=entries
        self.duplicates=duplicates

    def build_header(self, key):
        if not self.initialized:
            lphash.init(self.fn, self.buckets, self.threads, self.entries, self.duplicates)
            self.initialized = True
        lphash.build_hdr(bytes(key, 'utf-8'))

    def insert(self, key, value):
        if not self.finalized:
            lphash.finalize_hdr()
            self.finalized = True
        lphash.insert(bytes(key, 'utf-8'), value)

    def seal(self):
        lphash.seal()

    def lookup(self, key):
        if not self.loaded:
            lphash.load(self.fn)
            self.loaded = True
        return lphash.lookup(bytes(key, 'utf-8'))

    def lookup_quick(self, key):
        return lphash.lookup_quick(self.fn, bytes(key, 'utf-8'))

ph = phash('storage')
print(ph.lookup('zhi'))
alph=['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
for x in range(2):
    for i in alph:
        for j in alph:
            for k in alph:
                # for l in alph:
                    # for m in alph:
                genstr = i+j+k
                if x == 0:
                    ph.build_header(genstr)
                if x == 1:
                    ph.insert(genstr, ord(i)-ord('a'))

ph.seal()
