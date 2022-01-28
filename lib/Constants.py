UNCOMPRESSED_KEY_LENGTH     = 65
COMPRESSED_KEY_LENGTH       = 33

OP_0             = 0x00
OP_PUSHDATA1     = 0x4c
OP_PUSHDATA2     = 0x4d
OP_PUSHDATA4     = 0x4e
OP_1NEGATE       = 0x4f
OP_1             = 0x51
OP_16            = 0x60

OP_VERIFY        = 0x69
OP_RETURN        = 0x6a
OP_DUP           = 0x76
OP_EQUAL         = 0x87
OP_EQUALVERIFY   = 0x88
OP_HASH160       = 0xa9
OP_CHECKSIG      = 0xac
OP_CHECKMULTISIG = 0xae

from enum import Enum, unique
@unique
class TXOUT_TYPE(Enum):
    COINBASE	        = 0
    P2UPK	        = 1
    P2CPK	        = 2
    P2PKH	        = 3
    P2SH                = 4
    P2SH_MULTISIG       = 5
    MULTISIG	        = 6
    OP_RETURN	        = 7
    P2WPKH	        = 8
    P2SH_P2WPKH         = 9
    P2WSH	        = 10
    P2WSH_MULTISIG      = 11
    P2SH_P2WSH          = 12
    P2SH_P2WSH_MULTISIG = 13
    P2W_UNKNOWN	        = 14
    NONSTANDARD	        = 15
