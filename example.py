import fhash
import sys

n = 0
ph = fhash.Fhash('fyle', threads=32, entries=32*1500)
if len(sys.argv) > 1:
    # print(ph.lookup(sys.argv[1]))
    print(ph.lookup_quick(sys.argv[1]))
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
                                ph.insert(genstr, n)

    ph.seal()
    print(f'inserted {n} entries');
