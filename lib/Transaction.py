#!/usr/bin/env python3

from .Input import Input
from .Output import Output, txout_type_solver
from .Witness import Witness
from .globals import utxo
from .tools import hash256, read_varint
from .Constants import TXOUT_TYPE
import os

class Transaction:

    __slots__ = ['version', 'is_segwit', 'inputs', 'outputs', 'locktime', 'fee', 'size', 'stripped_size', 'txid', 'weight']

    def __init__(self, version, segwit, inputs, outputs, locktime, size, stripped_size, txid):
        self.version = version
        self.is_segwit = segwit
        self.inputs = inputs
        self.outputs = outputs
        self.locktime = locktime
        self.size = size
        self.stripped_size = stripped_size
        self.txid = txid
        self.weight = stripped_size * 4 + (size - stripped_size)

    def fee_and_type(self):
        # calculate transaction fee and annotate spent UTXO types in inputs

        # check for coinbase
        if self.inputs[0].txid == b'\x00' * 32 and self.inputs[0].pos == int('ff'*4, 16):
            self.fee = 0
            self.inputs[0].spent_UTXO_type = TXOUT_TYPE.COINBASE
            return

        # non-coinbase tx
        inputs_amount = 0
        for inp in self.inputs:
            # get referenced UTXO
            txid = inp.txid
            pos = inp.pos
            # get UTXO referenced by input
            referenced_UTXO = utxo.consume(txid, pos)
            # get amount available in referenced UTXO
            inputs_amount += referenced_UTXO.amount
            # determine spent UTXO type (pass input as well to identify nested segwig TX (P2SH-P2WPKH and P2SH-P2WSH)
            try:
                inp.spent_UTXO_type = txout_type_solver(referenced_UTXO.script_pubkey, script_sig=inp.script_sig, witness=inp.witness)
            except:
                raise Exception(f'error in {self.txid}')
            inp.spent_UTXO_script = referenced_UTXO.script_pubkey
        # determine amount of all outputs
        outputs_amount = sum([output.amount for output in self.outputs])
        # determine fee
        self.fee = inputs_amount - outputs_amount


    @staticmethod
    def deserialize(stream):
        pos_start = stream.tell()

        # version number (four bytes)
        version = int.from_bytes(stream.read(4), 'little')

        # To distinguish between segwit and non-segwit tx, a special '0x00'
        # marker is used in segwit tx. In non-segwit tx, the marker is
        # missing; instead, a varint, which cannot be zero (0x00), is used to
        # indicate the number of inputs.

        # check for marker
        marker = stream.read(1)
        if marker == bytes.fromhex('00'):
            # Marker found: segwit transaction
            segwit = True
            # Check segwit flag (currently, 0x01 must be used)
            flag = stream.read(1)
            if flag != bytes.fromhex('01'):
                raise Exception('invalid segwit flag \'{}\''.format(flag))
        else:
            # Marker not found: non-segwit transaction
            segwit = False
            # rewind stream
            stream.seek(-1, 1)

        # Deserialize inputs
        num_inputs = read_varint(stream)
        inputs = []
        for i in range(num_inputs):
            inputs.append(Input.deserialize(stream))
    
        # Deserialize outputs
        num_outputs = read_varint(stream)
        outputs = []
        for i in range(num_outputs):
            outputs.append(Output.deserialize(stream))
    
        # Handle SegWit
        if segwit == True:
            # Beginning of segwit data in tx
            witness_start = stream.tell() - pos_start
            for i in range(num_inputs):
                inputs[i].witness = Witness.deserialize(stream)
            # End of segwit data in tx
            witness_stop = stream.tell() - pos_start
    
        # lock time (four bytes)
        locktime = int.from_bytes(stream.read(4), 'little')
    
        # determine tx size
        size = stream.tell() - pos_start

        # determine stripped tx size
        if segwit == True:
            stripped_size = size - (witness_stop - witness_start) - 2 # two extra bytes are for segwit flags
        else:
            stripped_size = size 


        # calculate txid, which is needed to record this transactions outputs
        # in the UTXO set
        stream.seek(-size, 1)
        tx_data = stream.read(size)
        if segwit == False:
            txid = hash256(tx_data)[::-1]
        else:
            # legacy txid is calculated without:
            # 1. segwit marker and flag, which start after the four-byte version;
            # 2. witness data, which starts and ends at positions
            #    witness_start and witness_stop, respectively
            txid = hash256(tx_data[0:4] + tx_data[6:witness_start] + tx_data[witness_stop:])[::-1]

        # store all of the tx's outputs in the UTXO set if not OP_RETURN
        utxo.add(txid, outputs)
    
        # create a Transaction object
        return Transaction(version, segwit, inputs, outputs, locktime, size, stripped_size, txid)
