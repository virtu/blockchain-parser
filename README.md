# Disclaimer
This is a just-for-fun blockchain parser I developed back in 2020 to
teach myself about Bitcoin Core's various blockchain databases, and
the serialization of blocks, transactions, input, outputs, and witnesses.

Please don't use it for anything serious, as it super slow and may contain
bugs.

# How to use

## Get block hashes

To get all block hashes (from 0 to tip) of the valid chain, run
`create_blockhash_db.py`. This script collects the relevant data from Bitcoin
Core using the `getblockhash` RPC call, so `bitcoind` must be running for this
step. Results are written to `blockhashes.pdb`.

## Get index data

To get the index data for each block hash in `blockhashes.pdb`, run the
`create_index_db.py` script. This script locates the relevant information by
reading Bitcoin Core's index database (`blocks/index/`). The results are stored
in `blockindex.pdb`. Note that LevelDB, used by Bitcoin Core for the index,
does not support concurrent DB access, so `bitcoind` must not be running for
this step.

## Parse blockchain

Finally, Bitcoin Core's raw blockchain data can be parsed by running the
`parse_blockchain.py` script.

## tl;dr:

1. Make sure bitcoin core is running
2. Run `create_blockhash_db.py` to query all block hashes from bitcoin core and
   save them im `blockhashes.pdb`
3. Stop bitcoin core to make it release its lock on its block-index database
4. Run `create_index_db.py` to get index data for each block from bitcoin
   core's block-index database and save it to `blockindex.pdb`
5. Restart bitcoin core
6. You can now use the data is `blockindex.pdb` to locate blocks in Bitcoin
   core's blockchain
