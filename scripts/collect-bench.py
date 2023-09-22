#!/usr/bin/env python3

import os
import sys
import glob
import json

os.chdir(sys.path[0])

crit_dir = '../target/criterion'

def is_bad_path(path):
    return (not os.path.isdir(path)) or os.path.basename(path) == 'report'

with open('benchmark.csv', 'w') as out:
    out.write('bench,name,itr,val\n')
    for bench in glob.iglob(crit_dir + '/*'):
        if is_bad_path(bench):
            continue

        for name in glob.iglob(bench + '/*'):
            if is_bad_path(name):
                continue
            
            with open(name + '/base/sample.json') as data_file:
                data = json.load(data_file)
                for (itr, val) in zip(data['iters'], data['times']):
                    out.write(f'{os.path.basename(bench)},{os.path.basename(name)},{itr},{val}\n')