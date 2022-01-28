#!/usr/bin/env python3

import sys
import pickle
import time
import os
import psutil

from lib.Block import Block
from lib.Window import Window
from lib.globals import utxo
from lib.globals import log
from lib.globals import window_sizes
from lib.statistics import process

MIN_PYTHON = (3, 7) # ordered dictionaries
if sys.version_info < MIN_PYTHON:
        sys.exit('Python {}.{} or later is required.\n'.format(MIN_PYTHON[0], MIN_PYTHON[1]))

rows, columns = [int(x) for x in os.popen('stty size', 'r').read().split()]

datadir = '/scratch/bitcoin-0.19.0.1-datadir/'
indexdb = 'blockindex.pdb'
MAGIC = bytes.fromhex('d9b4bef9')

# read array containing sorted block hashes of active chain
with open(indexdb, 'rb') as fp:
    blockindex = pickle.load(fp)

# blockindex is an ordered array with position corresponding block number;
# its elements are dictionaries, each of which has the following entries:
#
# blockhash: the block's hash
# version: the block's version
# height: the block's height
# status: the block's status
# ntx: the number of transactions recorded in the block
# fileno (if applicable): file number where the block's contents are stored
# datapos (if applicable): position in the file where the block's contents are stored
# undopos (if applicable): position in the file where the block's undo info is stored
# blockver: the block's version number
# hash_prev_block: the hash of the block's parent
# merkle_root: the block's merkle root
# timestamp: the block's timestamp
# diffuculty: the block's difficulty bits
# nonce: the block's nonce

# Iterate over all blocks
# Open file, seek to position, validate block magic (there's a magic number, 0xD9B4BEF9, at the beginning of each block in the block chain format used by bitcoin core)

chain_done = 0      # amount of block-chain data processed [GB]
chain_size = 300    # size of block chain [GB]

start = time.time()
tip = len(blockindex)
fps = {} # dict for file pointers

window = Window(window_sizes)


for height, block in enumerate(blockindex):

    fileno = block['fileno']
    filename = datadir + '/blocks/blk{:05}.dat'.format(fileno)

    # Put a limit on the number of open files (eight)
    # Check if file already open using dict. if not, open it, put handle in dict
    if fileno in fps:
        f = fps[fileno]
    else:
        # Make sure there are a maximum of eight open files
        # To this end, discard smallest first item in dict, which corresponds
        # to the oldest one (Python >=3.7 has ordered dicts)
        if len(fps) == 8:
            remove = list(fps.keys())[0]
            remove_fp = fps.pop(remove, None)
            remove_fp.close()
        # Use a ten-megabyte buffer
        f = open(filename, 'rb', 10*1024*1024)
        fps[fileno] = f

    # move to correct position in file
    # datapos points to block header, we rewind by two times four bytes to
    # make it point to magic (four bytes), block size (four bytes), block
    # header
    datapos = block['datapos']
    f.seek(datapos-8)

    # read four bytes magic
    magic = f.read(4)[::-1]
    if magic != MAGIC:
        raise Exception('Error: Block magic not found! (read {}, should be {})'.format(magic, MAGIC))

    # read four-byte integer corresponding to the block size
    blocksize_raw = f.read(4)
    blocksize = int.from_bytes(blocksize_raw, 'little')
    pos_start = f.tell()

    # deserialize Block
    b = Block.deserialize(f)

    # there must not be any data left in the block's Stream after it has been
    # deserialized
    if f.tell() - pos_start != blocksize:
        raise Exception('{} bytes left in serialized stream after deserialization !'.format(stream.bytes_left()))

    # Do processing
    process(b, height, window)

    # add amount of processed bytes for calculation of remaining runtime
    chain_done += b.size / (1024**3)

    # display statistics every 10,000 blocks
    if (height > 0 and height % 10000 == 0):
        # determine remaining runtime: [work left in GB] * [runtime per GB]
        runtime = time.time() - start
        time_left = (chain_size-chain_done) * (runtime / chain_done)
        rss_GB = psutil.Process().memory_full_info().rss / (1024 ** 3)
        rss_perc = psutil.Process().memory_percent(memtype='rss')
        print('block {}/{}, elapsed time {:.1f}h, processed {:.1f}/{:.1f}GB ({:.1f}%), remaining time: {:.1f}h, number of open files: {}, block time: {}, mem. usage: {:.1f}GB ({:.1f}% of total)'.format(height, tip, runtime/3600, chain_done, chain_size, chain_done/chain_size*100.0, time_left/3600, len(fps), b.get_timestamp(), rss_GB, rss_perc))

stop = time.time()
print('processed {} blocks in {:.1f}s'.format(tip+1, stop-start))
log.write_histograms()
log.compress()

# # dump UTXO set
# with open('utxo-full.pkl', 'wb') as fp:
#     p = pickle.Pickler(fp)
#     p.fast = True
#     p.dump(utxo.dict)
#     fp.flush()
# print(f'stored UTXO set')
