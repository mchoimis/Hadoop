#!/usr/bin/env python 

import sys

(last_key, sum, count) = (None, 0.0, 0)

for line in sys.stdin:
    (key, val) = line.split("\t")

    if last_key and last_key != key:
        print last_key + "\t" + str(sum / count) 
        (sum, count) = (0.0, 0)

    last_key = key
    sum   += float(val)
    count += 1

print last_key + "\t" + str(sum / count)
