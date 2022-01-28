#!/usr/bin/env python3

import sys
import pickle
import plyvel
import time
import os

rows, columns = [int(x) for x in os.popen('stty size', 'r').read().split()]

datadir = '/scratch/bitcoin-0.19.0.1-datadir/'
blockhashdb = 'blockhashes.pdb'

DEBUG = False


# Block status bits
# Taken from Bitcoin Core's BlockStatus in src/chain.h

# Incremental validation levels
BLOCK_VALID_UNKNOWN      =    0,
BLOCK_VALID_HEADER       =    1 # 001
BLOCK_VALID_TREE         =    2 # 010
BLOCK_VALID_TRANSACTIONS =    3 # 011
BLOCK_VALID_CHAIN        =    4 # 100
BLOCK_VALID_SCRIPTS      =    5 # 101

# Set all valid bits
BLOCK_VALID_MASK         =   BLOCK_VALID_HEADER | BLOCK_VALID_TREE | BLOCK_VALID_TRANSACTIONS | BLOCK_VALID_CHAIN | BLOCK_VALID_SCRIPTS # 111

BLOCK_HAVE_DATA          =   1<<3 # full block available in blk*.dat
BLOCK_HAVE_UNDO          =   1<<4 # undo data available in rev*.dat
BLOCK_FAILED_VALID       =   1<<5 # don't know what exatly this is, should check that bit is not set
BLOCK_FAILED_CHILD       =   1<<6 # don't know what exatly this is, should check that bit is not set
BLOCK_FAILED_MASK        =   BLOCK_FAILED_VALID | BLOCK_FAILED_CHILD,
BLOCK_OPT_WITNESS        =   1<<7 # block data in blk*.data was received with a witness-enforcing client

def bits_to_diff(bits):

    baseline_shift = '1d'
    baseline_value = '00ffff'
    baseline_hex = int(baseline_value, 16) * 2**(8*(int(baseline_shift, 16) - 3))

    shift = bits[:2]
    value = bits[2:]
    expanded_hex = int(value, 16) * 2**(8*(int(shift, 16) - 3))

    return baseline_hex / expanded_hex

class Stream:
    def __init__(self, data):

        # Sanity checking: determine if string is hex
        try:
            int(data, 16)
        except ValueError:
            raise Exception('data {} is not hexadecimal'.format(data))

        # Sanity checking: length must be a multiple of two
        if len(data) % 2 != 0:
            raise Exception('data length is not a multiple of two')

        self.data = data
        self.byte_pos = 0
        self.byte_len = int(len(data)/2)

    def debug(self):
        print('=' * columns)
        print('DEBUG')
        print('=' * columns)
        print('full data: {}'.format(self.data))
        print('byte_pos: {}'.format(self.byte_pos))
        print('byte_len: {}'.format(self.byte_len))
        print('data before pos: {}'.format(self.data[:self.byte_pos*2]))
        print('data after pos: {}'.format(self.data[self.byte_pos*2:]))
        print('=' * columns)
        print('DEBUG')
        print('=' * columns)

    def bytes_left(self):
        return self.byte_len - self.byte_pos

    def rewind(self, nbytes):
        # Sanity checking: do we rewind past the beginning of the stream?
        if self.byte_pos-nbytes < 0:
            raise Exception('Attempting to read past the beginning of stream! byte_pos: {}, bytes to rewind: {}'.format(self.byte_pos, nbytes))
        else:
            self.byte_pos = self.byte_pos - nbytes


    def read_bytes(self, nbytes, endianness='undefined'):

        # Sanity checking: is the current position valid?
        if self.byte_pos < 0 or self.byte_pos > self.byte_len:
            raise Exception('Undefined state! byte_pos: {}'.format(self.byte_pos))

        # Sanity checking: do we read past the end of the stream?
        if self.byte_pos+nbytes > self.byte_len:
            raise Exception('Attempting to read past end of stream! byte_pos: {}, requested bytes: {}, byte_len: {}'.format(self.byte_pos, nbytes, self.byte_len))

        # Check if endianness is required
        if nbytes > 1 and endianness == 'undefined':
            raise Exception('Reading more than one byte requires specifying an endianness.')

        # read nbytes*2 hexadecimal characters and adjust byte_pos
        raw = self.data[self.byte_pos*2:self.byte_pos*2+nbytes*2]
        self.byte_pos += nbytes

        # If required, adjust endianness
        if nbytes > 1:
            raw = endian(raw, endianness)

        return raw

    # alternative varint implementation
    # see ReadVarInt in Bitcoin Core's src/serialize.h for details
    def read_alt_varint(self):

        res = 0
        raw = ''

        # Read until encountering a byte which does not have the most significat bit set
        while True:

            # read one byte
            new_raw = self.read_bytes(1)

            # append new raw data to raw data string used for debugging
            raw += new_raw

            # if the most significant bit is not set, we stop
            stop = int(new_raw, 16) & 0x80 == 0

            # data is in lower seven bits
            data = int(new_raw, 16) & 0x7f

            # shift previous data seven bits left, append new data
            res = (res << 7) | data

            if stop:
                return res, raw
            else:
                # I currently do not know why we have to add one to the result
                res = res + 1

    def read_varint(self):

        # read one byte
        raw = self.read_bytes(1)
     
        # Convert to integer and check length of varint
        value = int(raw, 16)
        if (value == 0xFD):
            additional_data_bytes = 2
        elif (value == 0xFE):
            additional_data_bytes = 4
        elif (value == 0xFF):
            additional_data_bytes = 8
        else:
            additional_data_bytes = 0

        if additional_data_bytes != 0:
            additional_raw = self.read_bytes(additional_data_bytes, 'little')
            raw += additional_raw
            value = int(additional_raw, 16)

        return value, raw, 1+additional_data_bytes

# Adjust endianness if required
def endian(string, expected):
    if expected != 'big' and expected != 'little':
        raise Exception('Endianness must be either \'big\' or \'little\' but is set to \'{}\''.format(expected))

    if (sys.byteorder != expected):
        splited = [str(string)[i:i + 2] for i in range(0, len(str(string)), 2)]
        splited.reverse()
        return "".join(splited)
    else:
        return string

# read array containing sorted block hashes of active chain
with open(blockhashdb, 'rb') as fp:
        blockhashes = pickle.load(fp)

# open block index
db = plyvel.DB(datadir + '/blocks/index', compression=None)

# locate raw blocks using the active chain's hashes in blockhashes
i = 0
index = []
start = time.time()
tip = len(blockhashes)
for pos, blockhash in enumerate(blockhashes):

    # estimate remaining runtime
    if (pos > 0 and pos % 10000 == 0):

        # determine performance (in hashes per second) based on empirical data
        runtime = time.time() - start
        hps = (pos+1)/runtime

        # estimate remaining runtime by dividing remaining work through
        # estimated performance
        time_left = ((tip+1) - (pos+1))/hps

        print('{}/{} (elapsed time {:.1f}s, remaining time: {:.1f}s)'.format(pos, tip, runtime, time_left))

    blockhash_serialized = endian(blockhash, 'big')
    data = db.get('b'.encode('ascii') + bytes.fromhex(blockhash_serialized))


    if DEBUG: print('-' * columns)
    if DEBUG: print('{} {}'.format(pos, data.hex()))

    # create data stream
    stream = Stream(data.hex())

    # See CDiskBlockIndex:SerializationOp in Bitcoin Core's src/chain.h
    # for data layout used in the LevelDB value

    # store relevant data in block index
    data = {}
    data['blockhash'] = blockhash

    # version of Satoshi client (alternative varint)
    expected_version = 190001
    version, raw = stream.read_alt_varint()
    if version != expected_version:
        raise Exception('version of block {} is {} (raw: {}) but should be {}'.format(pos, version, raw, expected_version))
    data['version'] = version

    # height (alternative varint)
    height, raw = stream.read_alt_varint()
    if height != pos:
        raise Exception('height is {} (raw: {}) but should be {}'.format(height, raw, pos))
    data['height'] = height

    # status (alternative varint)
    status, raw = stream.read_alt_varint()
    if DEBUG: print('block {}: status: {} (raw: {}, bin: {})'.format(pos, status, raw, bin(int(raw,16))))
    if status & (BLOCK_FAILED_VALID or BLOCK_FAILED_CHILD):
        raise Exception('block marked invalid: status is {} (raw: {})'.format(status, raw))
    data['status'] = status

    # number of transactions (alternative varint)
    ntx, raw = stream.read_alt_varint()
    if DEBUG: print('ntx: {} (raw: {})'.format(ntx, raw))
    data['ntx'] = ntx

    # file number, block pos, undo pos (all alternative varints)
    if status & (BLOCK_HAVE_DATA or BLOCK_HAVE_UNDO):
        fileno, raw = stream.read_alt_varint()
        if DEBUG: print('file number: {} (raw: {})'.format(fileno, raw))
        data['fileno'] = fileno

    # data position
    if status & BLOCK_HAVE_DATA:
        datapos, raw = stream.read_alt_varint()
        if DEBUG: print('data position: {} (raw: {})'.format(datapos, raw))
        data['datapos'] = datapos

    # undo position
    if status & BLOCK_HAVE_UNDO:
        undopos, raw = stream.read_alt_varint()
        if DEBUG: print('undo position: {} (raw: {})'.format(undopos, raw))
        data['undopos'] = undopos

    # block version, four-byte big-endian int
    blockver_raw = stream.read_bytes(4, 'big')
    blockver = int(blockver_raw, 16)
    if DEBUG: print('block version: {} (raw: {})'.format(blockver, blockver_raw))
    data['blockver'] = blockver

    # hash of previous block
    hash_prev_block = stream.read_bytes(32, 'big')
    if DEBUG: print('hash of previous block: {}'.format(hash_prev_block))
    data['hash_prev_block'] = hash_prev_block

    # merkle root
    merkle_root = stream.read_bytes(32, 'big')
    if DEBUG: print('merkle root: {}'.format(merkle_root))
    data['merkle_root'] = merkle_root

    # timestamp
    timestamp = stream.read_bytes(4, 'big')
    readable = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(timestamp, 16)))
    if DEBUG: print('timestamp: {} (raw: {})'.format(readable, timestamp))
    data['timestamp'] = timestamp

    # difficulty bits
    bits_raw = stream.read_bytes(4, 'big')
    diff = bits_to_diff(bits_raw)
    if DEBUG: print('difficulty: {} (raw: {})'.format(diff, bits_raw))
    data['difficulty'] = diff

    # nonce
    nonce = stream.read_bytes(4, 'big')
    if DEBUG: print('nonce: {}'.format(nonce))
    data['nonce'] = nonce

    # make sure we read all data from stream
    if stream.bytes_left():
        stream.debug()
        raise Exception('data left in stream')

    # append this block's index data to array
    index.append(data)

# close database
db.close()

# Integrity check of index
hash_prev_block_expected = '0'*64 # previous blockhash of genesis block
for pos, item in enumerate(index):

    # Assert that index and blockchain positions match
    if (item['height'] != pos):
        raise Exception('block height is {} but should be {}'.format(item['height'], pos))

    # Make sure prevbockhash in block matches index
    if (item['hash_prev_block'] != hash_prev_block_expected):
        raise Exception('previous block hash is {} but should be {}'.format(hash_prev_block, hash_prev_block_expected))
    else:
        hash_prev_block_expected = item['blockhash']

print('size of index: {}'.format(len(index)))

# Write index to file
db = 'blockindex.pdb'
with open(db, 'wb') as fp:
        pickle.dump(index, fp, protocol=pickle.HIGHEST_PROTOCOL)

stop = time.time()
print('wrote {} block hashes to {} in {:.1f}s'.format(tip+1, db, stop-start))
