from .Transaction import Transaction
from lib.tools import read_varint, bits_to_diff
import time

class Block:
    def __init__(self, version, hash_prev_block, merkle_root, timestamp, diffbits, nonce, ntx, txs, size):
        self.version = version
        self.prev_hash = hash_prev_block
        self.merkle_root = merkle_root
        self.timestamp = timestamp
        self.diffbits = diffbits
        self.nonce = nonce
        self.ntx = ntx
        self.transactions = txs
        self.size = size

    def get_diff(self):
        return bits_to_diff(self.diffbits)

    def get_timestamp(self):
        return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(self.timestamp))

    @staticmethod
    def deserialize(stream):
        # deserialize one block

        # number of bytes left in stream; required later to calculate block size 
        pos_start = stream.tell()

        # version number (four bytes)
        version =  int.from_bytes(stream.read(4), 'little')
        # hash of previous block (32 bytes)
        hash_prev_block = stream.read(32).hex()[::-1]
        # merkle root (32 bytes)
        merkle_root = stream.read(32).hex()[::-1]
        # block timestamp
        timestamp = int.from_bytes(stream.read(4), 'little')
        # difficulty bits
        diffbits =  stream.read(4)[::-1].hex()
        # nonce
        nonce = stream.read(4).hex()[::-1]
        # number of transactions (varint)
        ntx = read_varint(stream)
        # deserialize transactions
        txs = []
        for i in range(ntx):
            tx = Transaction.deserialize(stream)
            # determine fee and spent UTXO type for tx's inputs
            tx.fee_and_type()
            txs.append(tx)

        # determine block size
        size = stream.tell() - pos_start

        # return a Block object
        return Block(version, hash_prev_block, merkle_root, timestamp, diffbits, nonce, ntx, txs, size)
