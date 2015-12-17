#!/usr/bin/python

import sys
import random

CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

random.seed()

def main():
	filename = sys.argv[1]
	size = int(sys.argv[2])

	with open(filename, "w") as fd:	
		BLOCK_SIZE = 80
		while size > 0:
			this_block_size = min(BLOCK_SIZE, size)
			block = "".join([random.choice(CHARACTERS) for x in range(this_block_size)])+"\n"
			size -= this_block_size+1
			fd.write(block)
	fd.close()

main()