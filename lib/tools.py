#!/usr/bin/env python3

import sys
from hashlib import sha256

def hash256(data):
    return sha256(sha256(data).digest()).digest()

def hash160(data):
    return hashlib.new('ripemd160', hashlib.sha256(data).digest()).digest()

def max_block_subsidy(height):
    halvings = int(height / 210000)

    # cutoff for reference client's data type
    if halvings >= 64:
        return 0

    initial_subsidy = 50 * 100 * 1000 * 1000
    subsidy = initial_subsidy >>  halvings
    return subsidy

# Expand bits used in difficulty, used by difficulty function
def expand(bits):
    shift = bits[:2]
    value = bits[2:]
    expanded = int(value, 16) * 2**(8*(int(shift, 16) - 3))
    return expanded

def bits_to_diff(bits):
    return expand('1d00ffff') / expand(bits)

def read_varint(stream):
    value = int.from_bytes(stream.read(1), 'little')
    
    if (value < 0xFD):
        return value
    elif (value == 0xFD):
        return int.from_bytes(stream.read(2), 'little')
    elif (value == 0xFE):
        return int.from_bytes(stream.read(4), 'little')
    elif (value == 0xFF):
        return int.from_bytes(stream.read(8), 'little')
