import sys
import operator

fname = sys.argv[1]

with open(fname, 'r') as fin:
    file_cnt = 0
    tree_map = {}
    for line in fin:
        file_cnt += 1
        file_hash, tree_hash = line.split()
        try:
            tree_map[tree_hash].append(file_hash)
        except KeyError:
            tree_map[tree_hash] = []
            tree_map[tree_hash].append(file_hash)

    print "Unique Files:", file_cnt
    print "Unique Tree Hashes:", len(tree_map.keys())
    print "-"*80
    sort_hashes = sorted(tree_map, key=lambda key: len(tree_map[key]))
    for sorted_key in sort_hashes:
        print sorted_key,len(tree_map.get(sorted_key))
