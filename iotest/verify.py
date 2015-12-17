#!/usr/bin/python

import sys
import random
import os
import time

random.seed()

ref, other = sys.argv[1:3]

ref_size = os.path.getsize(ref)
other_size = os.path.getsize(other)

assert ref_size == other_size

block_size = 1000

import fcntl
F_NOCACHE = 48 #             /* turn data caching off/on for this fd */

def open_no_cache(fn):
	fd = open(fn)
#   on mac os, this will cause reads to bypass cache.   (Make sure to run "purge" first to avoid file being pre-cached)
#	fcntl.fcntl(fd, F_NOCACHE, 1)
	return fd

offsets = [x * block_size for x in range((ref_size+block_size-1)/block_size)]
random.shuffle(offsets)
ref_fd = open_no_cache(ref)

other_fd = open_no_cache(other)

read_count = 0
read_byte_count = 0

start_time = time.time ()
for offset in offsets:
	ref_fd.seek(offset)
	ref_val = ref_fd.read(block_size)
	other_fd.seek(offset)
	other_val = other_fd.read(block_size)
	assert ref_val == other_val, "After reading {0} bytes, block at offset {1}, length {2} did not match".format(read_byte_count, offset, block_size)
	read_count += 1
	read_byte_count += len(other_val)
stop_time = time.time()
elapsed = stop_time - start_time

def fmtunit(x):
	if x > 1024*1024*1024:
		return "%.1fGB" % (x/(1024.0*1024*1024))
	if x > 1024*1024:
		return "%.1fMB" % (x/(1024.0*1024))
	if x > 1024:
		return "%.1fKB" % (x/1024.0)
	return "%d bytes" % x

print "Verified %s, %s/sec (%d reads, %d reads/sec)" % (fmtunit(read_byte_count), fmtunit(read_byte_count/elapsed), read_count, read_count/elapsed)

