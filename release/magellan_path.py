#!/bin/env python

import sys
import h5py
import re
import numpy as np

if len(sys.argv) < 5:
    print ("Usage %s <datset_name> <src_prefix> <dst_prefix> <hdf5file>"%sys.argv[0])
    sys.exit(1)

dataset = sys.argv[1]
src_pattern = "^%s"%sys.argv[2]
dst_pattern = sys.argv[3]
file = sys.argv[4]

f = h5py.File(file, "r")
grp = f['/xpcs']
local_path = grp['input_file_local']
remote_path = grp['input_file_remote']

local_path = local_path[()].decode('ascii')
new_local_path = re.sub(src_pattern, dst_pattern, local_path)

remote_path = remote_path[()].decode('ascii')

print (new_local_path, remote_path)

