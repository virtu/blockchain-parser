#!/usr/bin/env python3

from .Script import Script, decode_script
from .tools import read_varint

class Input:

    __slots__ = ['txid', 'pos', 'script_sig', 'seq_no', 'size', 'spent_UTXO_type', 'spent_UTXO_script', 'witness']

    def __init__(self, txid, pos, script_sig, seq_no, size):
        self.txid = txid
        self.pos = pos
        self.script_sig = script_sig
        self.seq_no = seq_no
        self.size = size
        self.witness = None

    @staticmethod
    def deserialize(stream):
        pos_start = stream.tell()
        # process raw data
        txid = stream.read(32)[::-1]
        pos =  int.from_bytes(stream.read(4), 'little')
        len_script_sig = read_varint(stream)
        script_sig = Script(stream.read(len_script_sig))
        seq_no =  int.from_bytes(stream.read(4), 'little')
        # determine input size
        size = stream.tell() - pos_start
        # create and return an Input object
        return Input(txid, pos, script_sig, seq_no, size)
