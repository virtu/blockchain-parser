from .Constants import TXOUT_TYPE

class UTXO:

    def __init__(self):
        self.dict = {}

    def add(self, txid, outputs):
        for pos, output in enumerate(outputs):
            if output.created_UTXO_type == TXOUT_TYPE.OP_RETURN:
                continue
            key = txid + int.to_bytes(pos, 4, 'big')
            self.dict[key] = output

    def consume(self, txid, pos):
        key = txid + int.to_bytes(pos, 4, 'big')
        res = self.dict[key]
        del self.dict[key]
        return res

    def clear(self):
        self.dict.clear()
