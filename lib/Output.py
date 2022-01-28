#!/usr/bin/env python3

from .Script import Script, decode_script
from .Constants import TXOUT_TYPE
from .tools import read_varint

def txout_type_solver(script_pubkey, script_sig=None, witness=None):
    # Start with most common types to exit early

    if script_pubkey.is_P2PKH():
        return TXOUT_TYPE.P2PKH

    # P2SH: check for P2SH-P2WSH/P2SH-P2WPKH
    if script_pubkey.is_P2SH():
        if script_sig:
            redeem_script = script_sig.get_redeem_script()
            if redeem_script.size() == 22 and redeem_script.is_P2WPKH():
                return TXOUT_TYPE.P2SH_P2WPKH
            if redeem_script.size() == 34 and redeem_script.is_P2WSH():
                if witness:
                    witness_script = witness.get_witness_script()
                    if witness_script.is_MULTISIG():
                        return TXOUT_TYPE.P2SH_P2WSH_MULTISIG
                # else wrapped P2WSH other than multisig
                return TXOUT_TYPE.P2SH_P2WSH
            if redeem_script.is_MULTISIG():
                return TXOUT_TYPE.P2SH_MULTISIG
        # no corresponding input provided or no nested segwit found, return regular P2SH
        return TXOUT_TYPE.P2SH

    if script_pubkey.is_OP_RETURN():
        return TXOUT_TYPE.OP_RETURN

    if script_pubkey.is_P2WPKH():
        return TXOUT_TYPE.P2WPKH

    if script_pubkey.is_P2WSH():
        if witness:
            witness_script = witness.get_witness_script()
            if witness_script.is_MULTISIG():
                return TXOUT_TYPE.P2WSH_MULTISIG
        # no corresponding witness provided or no multisig in witness script
        return TXOUT_TYPE.P2WSH

    if script_pubkey.is_P2W_UNKNOWN():
        return TXOUT_TYPE.P2W_UNKNOWN

    # Hardly used any more
    if script_pubkey.is_P2UPK():
        return TXOUT_TYPE.P2UPK
    if script_pubkey.is_P2CPK():
        return TXOUT_TYPE.P2CPK

    # Most expensive to check, last
    if script_pubkey.is_MULTISIG():
        return TXOUT_TYPE.MULTISIG

    # Fallback
    return TXOUT_TYPE.NONSTANDARD

class Output:

    __slots__ = ['amount', 'script_pubkey', 'created_UTXO_type', 'size']

    def __init__(self, amount, script_pubkey, created_UTXO_type, size):
        self.amount = amount
        self.script_pubkey = script_pubkey
        self.created_UTXO_type = created_UTXO_type
        self.size = size

    @staticmethod
    def deserialize(stream):
        pos_start = stream.tell()
        # process raw data
        amount = int.from_bytes(stream.read(8), 'little')
        len_scipt_pubkey = read_varint(stream)
        script_pubkey = Script(stream.read(len_scipt_pubkey))
        created_UTXO_type = txout_type_solver(script_pubkey)
        # determine input size
        size = stream.tell() - pos_start
        # create and return an Output object
        return Output(amount, script_pubkey, created_UTXO_type, size)
