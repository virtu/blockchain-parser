#!/usr/bin/env python3

from .Constants import UNCOMPRESSED_KEY_LENGTH, COMPRESSED_KEY_LENGTH
from .Constants import OP_PUSHDATA1, OP_PUSHDATA2, OP_PUSHDATA4, OP_1NEGATE, OP_0, OP_1, OP_16, OP_VERIFY, OP_DUP, OP_EQUAL, OP_EQUALVERIFY, OP_HASH160, OP_CHECKSIG, OP_RETURN, OP_CHECKMULTISIG

class Script:

    __slots__ = ['data']

    def __init__(self, data):
        self.data = data

    def __getitem__(self, pos):
        # automatically converted to int
        return self.data[pos]

    def size(self):
        return len(self.data)

    def get_redeem_script(self):
        pos = 0
        redeem_script = False
        while pos < self.size():
            # read one byte
            item = self[pos]
            pos += 1
            # if item is not a constant, it is an operation, which we skip
            if item > 0x4e:
                continue
            # get number of bytes
            if item == OP_PUSHDATA1:
                redeem_script_size = self[pos]
                pos += 1
                redeem_script = self[pos:pos+redeem_script_size]
                pos += redeem_script_size
            elif item == OP_PUSHDATA2:
                redeem_script_size = int.from_bytes(self[pos:pos+2], 'little')
                pos += 2
                redeem_script = self[pos:pos+redeem_script_size]
                pos += redeem_script_size
            elif item == OP_PUSHDATA4:
                redeem_script_size = int.from_bytes(self[pos:pos+4], 'little')
                pos += 4
                redeem_script = self[pos:pos+redeem_script_size]
                pos += redeem_script_size
            elif item == OP_0:
                redeem_script = b''
            elif item == OP_1NEGATE:
                redeem_script = (-1).to_bytes(1, 'little', signed=True)
            elif OP_1 <= item <= OP_16:
                redeem_script = bytes([item - OP_1 + 1])
            # Constant indicating length
            else:
                redeem_script_size = item
                redeem_script = self[pos:pos+redeem_script_size]
                pos += redeem_script_size

        if redeem_script == False or pos != self.size():
            raise Exception(f'error locating redeem script in {self.data}')

        return Script(redeem_script)

    def is_P2UPK(self):
        if self.size() != UNCOMPRESSED_KEY_LENGTH + 2:
            return False
        if self[0] != UNCOMPRESSED_KEY_LENGTH:
            return False
        if self[1 + UNCOMPRESSED_KEY_LENGTH] != OP_CHECKSIG:
            return False
        return True

    def is_P2CPK(self):
        if self.size() != COMPRESSED_KEY_LENGTH + 2:
            return False
        if self[0] != COMPRESSED_KEY_LENGTH:
            return False
        if self[1 + COMPRESSED_KEY_LENGTH] != OP_CHECKSIG:
            return False
        return True

    def is_P2PKH(self):
        if self.size() != 25:
            return False
        if self[0] != OP_DUP:
            return False
        if self[1] != OP_HASH160:
            return False
        if self[2] != 20:
            return False
        if self[2+20+1] != OP_EQUALVERIFY:
            return False
        if self[2+20+2] != OP_CHECKSIG:
            return False
        return True

    def is_MULTISIG(self, params=False):
        if not self.size() > 1:
            return False

        # Read m and make sure OP_1 <= m <= OP_16
        m = self[0]
        if not (OP_1 <= m <= OP_16):
            return False
        # decode OP_x -> number x
        m = m - OP_1 + 1

        # Read m keys
        pos = 1
        while (pos < self.size() - 2):
            # read <data length>
            key_len = self[pos]
            pos += 1
            if not key_len > 0:
                return False

            # read <data>
            key = self[pos:(pos + key_len)]
            pos += key_len

            # first byte must be valid SEC prefix, data size must match specs
            prefix = key[0]
            if prefix in (2, 3):
                if key_len != COMPRESSED_KEY_LENGTH:
                    return False
            elif prefix in (4, 6, 7):
                if key_len != UNCOMPRESSED_KEY_LENGTH:
                    return False
            else:
                return False

        # There have to be two items left: n and OP_CHECKMULTISIG
        if pos != self.size() - 2 :
            return False

        # Read n and make sure OP_1 <= n <= OP_16
        n = self[pos]
        pos += 1
        if not (OP_1 <= n <= OP_16):
            return False
        # decode OP_x -> number x
        n = n - OP_1 + 1

        # make sure m < n
        if n < m:
            return False

        # make sure we're on last item, and item is OP_CHECKMULTISIG
        if pos != self.size() - 1:
            return False
        if self[pos] != OP_CHECKMULTISIG:
            return False

        if params:
            return m, n
        else:
            return True

    def is_P2SH(self):
        if self.size() != 23:
            return False
        if self[0] != OP_HASH160:
            return False
        if self[1] != 20:
            return False
        if self[22] != OP_EQUAL:
            return False
        return True

    def is_P2WPKH(self):
        if self.size() != 22:
            return False
        if self[0] != OP_0:
            return False
        if self[1] != 20:
            return False
        return True

    def is_P2WSH(self):
        if self.size() != 34:
            return False
        if self[0] != OP_0:
            return False
        if self[1] != 32:
            return False
        return True

    def is_P2W_UNKNOWN(self):
        if not (4 <= self.size() <= 42):
            return False

        # known witness programs use OP_0, so unknown uses one of (OP_1, ..., OP_16)
        if not (OP_1 <= self[0] <= OP_16):
            return False

        # length must be <length of witness programm> + 2 (OP_* <len witness program> <witness program>)
        if self[1] + 2 != self.size():
            return False

        return True

    def is_OP_RETURN(self):
        if self.size() < 1:
            return False
        if self[0] != OP_RETURN:
            return False
        # Only push data (i.e., length-indicator for data plus data) is allowed after OP_RETURN
        pos = 1
        while (pos < self.size()):
            item = self[pos]
            # Values indicating length are constants with values from 0-96. OP_16 (0x60) corresponds to 96.
            if item > OP_16:
                return False
            # OP_1NEGATE, OP_0, ..., OP_16 push values, not following data
            elif item == OP_1NEGATE or item == OP_0 or (OP_1 <= item <= OP_16):
                pos += 1
            # OP_PUSHDATA
            elif item == OP_PUSHDATA1:
                pos += 1
                num_bytes = self[pos]
                pos += 1 + num_bytes
            elif item == OP_PUSHDATA2:
                pos += 1
                num_bytes = int.from_bytes(self[pos:pos+2], 'little')
                pos += 2 + num_bytes
            elif item == OP_PUSHDATA4:
                pos += 1
                num_bytes = int.from_bytes(self[pos:pos+4], 'little')
                pos += 4 + num_bytes
            # Constant indicating length
            else:
                num_bytes = item
                pos += 1 + num_bytes
        # must be at end
        if pos != self.size():
            return False
        return True


# Convert raw bitcoin script to human-readable format
def decode_script(script_raw):

    if len(script_raw) == 0:
        return '<empty script>'

    stream = script_raw
    result = ''

    # read script
    while stream.bytes_left() > 0:

        opcode = '0x' + stream.read_hex(1)

        # OP_FALSE
        if opcode == '0x00':
            result += 'OP_FALSE '

        # Push next 'opcode' bytes onto stack
        elif opcode >= '0x01' and opcode <= '0x4b':
            nbytes = int(opcode, 16)
            try:
                data = stream.read_hex(nbytes, 'big')
            except IndexError:
                return result + 'PUSHDATA:{} [IndexError]'.format(nbytes)
            result += 'PUSHDATA:{} [{}] '.format(nbytes, data)

        # OP_PUSHDATA1
        elif opcode == '0x4c':
            # The next byte contains how many bytes to read from the script
            try:
                nbytes = stream.read_hex(1)
            except IndexError:
                return result + 'PUSHDATA:[IndexError]'
            nbytes = int(nbytes, 16)
            try:
                data = stream.read_hex(nbytes, 'big')
            except IndexError:
                return result + 'PUSHDATA:{} [IndexError]'.format(nbytes)
            result += 'OP_PUSHDATA1 ({} bytes) [{}] '.format(nbytes, data)

        # OP_PUSHDATA2
        elif opcode == '0x4d':
            # The next two bytes contains how many bytes to read from the script
            try:
                nbytes = stream.read_hex(2, 'little')
            except IndexError:
                return result + 'PUSHDATA:[IndexError]'
            nbytes = int(nbytes, 16)
            try:
                data = stream.read_hex(nbytes, 'big')
            except IndexError:
                return result + 'PUSHDATA:{} [IndexError]'.format(nbytes)
            result += 'OP_PUSHDATA2 ({} bytes) [{}] '.format(nbytes, data)

        # OP_PUSHDATA4
        elif opcode == '0x4e':
            # The next four bytes contains how many bytes to read from the script
            try:
                nbytes = stream.read_hex(4, 'little')
            except IndexError:
                return result + 'PUSHDATA:[IndexError]'
            nbytes = int(nbytes, 16)
            try:
                data = stream.read_hex(nbytes, 'big')
            except IndexError:
                return result + 'PUSHDATA:{} [IndexError]'.format(nbytes)
            result += 'OP_PUSHDATA2 ({} bytes) [{}] '.format(nbytes, data)

        # OP_1NEGATE
        elif opcode == '0x4f':
            result += 'OP_1NEGATE '

        # OP_RESERVED
        elif opcode == '0x50':
            result += 'OP_RESERVED '

        # OP_1 - OP_16
        elif opcode >= '0x51' and opcode <= '0x60':
            result += 'OP_{} '.format(int(opcode, 16) - 80)

        # OP_NOP
        elif opcode == '0x61':
            result += 'OP_NOP '

        # OP_VER
        elif opcode == '0x62':
            result += 'OP_VER '

        # OP_IF
        elif opcode == '0x63':
            result += 'OP_IF '

        # OP_NOTIF
        elif opcode == '0x64':
            result += 'OP_NOTIF '

        # OP_VERIF
        elif opcode == '0x65':
            result += 'OP_VERIF '

        # OP_VERNOTIF
        elif opcode == '0x66':
            result += 'OP_VERNOTIF '

        # OP_ELSE
        elif opcode == '0x67':
            result += 'OP_ELSE '

        # OP_ENDIF
        elif opcode == '0x68':
            result += 'OP_ENDIF '

        # OP_VERIFY
        elif opcode == '0x69':
            result += 'OP_VERIFY '

        # OP_RETURN
        elif opcode == '0x6a':
            result += 'OP_RETURN '

        # OP_TOALTSTACK
        elif opcode == '0x6b':
            result += 'OP_TOALTSTACK '

        # OP_FROMALTSTACK
        elif opcode == '0x6c':
            result += 'OP_FROMALTSTACK '

        # OP_2DUP
        elif opcode == '0x6e':
            result += 'OP_2DUP '

        # OP_2OVER
        elif opcode == '0x70':
            result += 'OP_2OVER '

        # OP_2SWAP
        elif opcode == '0x72':
            result += 'OP_2SWAP '

        # OP_IFDUP
        elif opcode == '0x73':
            result += 'OP_IFDUP '

        # OP_DEPTH
        elif opcode == '0x74':
            result += 'OP_DEPTH '

        # OP_DROP
        elif opcode == '0x75':
            result += 'OP_DROP '

        # OP_DUP
        elif opcode == '0x76':
            result += 'OP_DUP '

        # OP_NIP
        elif opcode == '0x77':
            result += 'OP_NIP '

        # OP_OVER
        elif opcode == '0x78':
            result += 'OP_OVER '

        # OP_PICK
        elif opcode == '0x79':
            result += 'OP_PICK '

        # OP_ROLL
        elif opcode == '0x7a':
            result += 'OP_ROLL '

        # OP_ROT
        elif opcode == '0x7b':
            result += 'OP_ROT '

        # OP_SWAP
        elif opcode == '0x7c':
            result += 'OP_SWAP '

        # OP_TUCK
        elif opcode == '0x7d':
            result += 'OP_TUCK '

        # OP_LEFT (disabled)
        elif opcode == '0x80':
            result += 'OP_LEFT[disabled] '

        # OP_RIGHT (disabled)
        elif opcode == '0x81':
            result += 'OP_RIGHT[disabled] '

        # OP_SIZE
        elif opcode == '0x82':
            result += 'OP_SIZE '

        # OP_INVERT
        elif opcode == '0x83':
            result += 'OP_INVERT '

        # OP_AND
        elif opcode == '0x84':
            result += 'OP_AND '

        # OP_OR
        elif opcode == '0x85':
            result += 'OP_OR '

        # OP_XOR
        elif opcode == '0x86':
            result += 'OP_XOR '

        # OP_EQUAL
        elif opcode == '0x87':
            result += 'OP_EQUAL '

        # OP_EQUALVERIFY
        elif opcode == '0x88':
            result += 'OP_EQUALVERIFY '

        # OP_RESERVED1
        elif opcode == '0x89':
            result += 'OP_RESERVED1 '

        # OP_RESERVED2
        elif opcode == '0x8a':
            result += 'OP_RESERVED2 '

        # OP_1ADD
        elif opcode == '0x8b':
            result += 'OP_1ADD '

        # OP_1SUB
        elif opcode == '0x8c':
            result += 'OP_1SUB '

        # OP_2DIV (disabled)
        elif opcode == '0x8e':
            result += 'OP_2DIV[disabled] '

        # OP_NEGATE
        elif opcode == '0x8f':
            result += 'OP_NEGATE '

        # OP_ABS
        elif opcode == '0x90':
            result += 'OP_ABS '

        # OP_NOT
        elif opcode == '0x91':
            result += 'OP_NOT '

        # OP_0NOTEQUAL
        elif opcode == '0x92':
            result += 'OP_0NOTEQUAL '

        # OP_ADD
        elif opcode == '0x93':
            result += 'OP_ADD '

        # OP_SUB
        elif opcode == '0x94':
            result += 'OP_SUB '

        # OP_MUL (disabled)
        elif opcode == '0x95':
            result += 'OP_MUL[disabled] '

        # OP_DIV (disabled)
        elif opcode == '0x96':
            result += 'OP_DIV[disabled] '

        # OP_MOD (disabled)
        elif opcode == '0x97':
            result += 'OP_MOD[disabled] '

        # OP_LSHIFT (disabled)
        elif opcode == '0x98':
            result += 'OP_LSHIFT[disabled] '

        # OP_RSHIFT (disabled)
        elif opcode == '0x99':
            result += 'OP_RSHIFT[disabled] '

        # OP_BOOLAND
        elif opcode == '0x9a':
            result += 'OP_BOOLAND '

        # OP_BOOLOR
        elif opcode == '0x9b':
            result += 'OP_BOOLOR '

        # OP_NUMEQUAL
        elif opcode == '0x9c':
            result += 'OP_NUMEQUAL '

        # OP_NUMEQUALVERIFY
        elif opcode == '0x9d':
            result += 'OP_NUMEQUALVERIFY '

        # OP_LESSTHAN
        elif opcode == '0x9f':
            result += 'OP_LESSTHAN '

        # OP_GREATERTHAN
        elif opcode == '0xa0':
            result += 'OP_GREATERTHAN '

        # OP_LESSTHANOREQUAL
        elif opcode == '0xa1':
            result += 'OP_LESSTHANOREQUAL '

        # OP_GREATERTHANOREQUAL
        elif opcode == '0xa2':
            result += 'OP_GREATERTHANOREQUAL '

        # OP_MIN
        elif opcode == '0xa3':
            result += 'OP_MIN '

        # OP_MAX
        elif opcode == '0xa4':
            result += 'OP_MAX '

        # OP_WITHIN
        elif opcode == '0xa5':
            result += 'OP_WITHIN '

        # OP_RIPEMD160
        elif opcode == '0xa6':
            result += 'OP_RIPEMD160 '

        # OP_SHA1
        elif opcode == '0xa7':
            result += 'OP_SHA1 '

        # OP_SHA256
        elif opcode == '0xa8':
            result += 'OP_SHA256 '

        # OP_HASH160
        elif opcode == '0xa9':
            result += 'OP_HASH160 '

        # OP_HASH256
        elif opcode == '0xaa':
            result += 'OP_HASH256 '

        # OP_CODESEPARATOR
        elif opcode == '0xab':
            result += 'OP_CODESEPARATOR '

        # OP_CHECKSIG
        elif opcode == '0xac':
            result += 'OP_CHECKSIG '

        # OP_CHECKSIGVERIFY
        elif opcode == '0xad':
            result += 'OP_CHECKSIGVERIFY '

        # OP_CHECKMULTISIG
        elif opcode == '0xae':
            result += 'OP_CHECKMULTISIG '

        # OP_CHECKMULTISIGVERIFY
        elif opcode == '0xaf':
            result += 'OP_CHECKMULTISIGVERIFY '

        # OP_NOP1
        elif opcode == '0xb0':
            result += 'OP_NOP1 '

        # OP_CHECKLOCKTIMEVERIFY
        elif opcode == '0xb1':
            result += 'OP_CHECKLOCKTIMEVERIFY '

        # OP_CHECKSEQUENCEVERIFY
        elif opcode == '0xb2':
            result += 'OP_CHECKSEQUENCEVERIFY '

        # OP_NOP4-OP_NOP10
        elif opcode >= '0xb3' and opcode <= '0xb9':
            result += 'OP_NOP{} '.format(int(opcode, 16) - 175)
        # OP_INVALIDOPCODE
        elif opcode == '0xff':
            result += 'OP_INVALIDOPCODE '

        # OP_UNKNOWN (0xe3)
        elif opcode == '0xe3':
            result += 'OP_UNKNOWN[e3] '

        # OP_UNKNOWN (0xe4)
        elif opcode == '0xe4':
            result += 'OP_UNKNOWN[e4] '

        # OP_UNKNOWN (0xe5)
        elif opcode == '0xe5':
            result += 'OP_UNKNOWN[e5] '

        # OP_UNKNOWN (0xe6)
        elif opcode == '0xe6':
            result += 'OP_UNKNOWN[e6] '

        # OP_UNKNOWN (0xe7)
        elif opcode == '0xe7':
            result += 'OP_UNKNOWN[e7] '

        # OP_UNKNOWN (0xe8)
        elif opcode == '0xe8':
            result += 'OP_UNKNOWN[e8] '

        # OP_UNKNOWN (0xe9)
        elif opcode == '0xe9':
            result += 'OP_UNKNOWN[e9] '

        # OP_UNKNOWN (0xef)
        elif opcode == '0xef':
            result += 'OP_UNKNOWN[ef] '

        # OP_UNKNOWN (0xba)
        elif opcode == '0xba':
            result += 'OP_UNKNOWN[ba] '

        # OP_UNKNOWN (0xbb)
        elif opcode == '0xbb':
            result += 'OP_UNKNOWN[bb] '

        # OP_UNKNOWN (0xbc)
        elif opcode == '0xbc':
            result += 'OP_UNKNOWN[bc] '

        # OP_UNKNOWN (0xbd)
        elif opcode == '0xbd':
            result += 'OP_UNKNOWN[bd] '

        # OP_UNKNOWN (0xbe)
        elif opcode == '0xbe':
            result += 'OP_UNKNOWN[be] '

        # OP_UNKNOWN (0xbf)
        elif opcode == '0xbf':
            result += 'OP_UNKNOWN[bf] '

        # OP_UNKNOWN (0xfa)
        elif opcode == '0xfa':
            result += 'OP_UNKNOWN[fa] '

        # Not implemented
        else:
            raise Exception('Warning: Opcode {} in script {} not yet implemented!'.format(opcode, script_raw))

    return result
