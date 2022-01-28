#!/usr/bin/env python3

from .tools import read_varint
from .Script import Script

class Witness:

    __slots__ = ['items', 'size']

    def __init__(self, items, size):
        self.items = items
        self.size = size

    #def get_program(self):
    #    # handle witness's items
    #    stream = stream(self.data)
    #    nitems =  stream.read_varint()
    #    witness = '[items: {} '.format(nitems)
    #    for i in range(nitems):
    #        # determine item's length
    #        itemlen = stream.read_varint()
    #        item_raw = stream.read_hex(itemlen, 'big')
    #        witness += '(item: {} len: {} data: {})'.format(i, itemlen, item_raw)
    #    witness += ']'
    #    return witness

    def get_witness_script(self):
        if not self.items:
            raise ValueError(f'no witness data: {self.items}')
        return Script(self.items[-1])

    @staticmethod
    def deserialize(stream):
        # number of bytes left in stream; required later to calculate input size 
        pos_start = stream.tell()
        num_items = read_varint(stream)
        if num_items == 0:
            return None
        items = []
        for i in range(num_items):
            len_item = read_varint(stream)
            item = stream.read(len_item)
            items.append(item)
        # determine witness size
        size = stream.tell() - pos_start
        # create and return Witness object
        return Witness(items, size)
