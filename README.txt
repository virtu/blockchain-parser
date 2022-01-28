The workflow to create a python-readable data structure that contains the
Bitcoin blockchain is as follows:

First, the block hashes of all blocks in the main chain are queried from
bitcoin core using the getblockhash RPC call.  This is done using the
create_blockhash_db.py script, which creates a python array that contains the
ordered (from 0 to tip) hashes of all blocks. This array is saved in
'blockhashes.pdb'. Note that bitcoind must be running for this step.

Next, the index data for each block hash in 'blockhashes.pdb' is copied from
bitcoin core's index database (blocks/index/*) and stored in a python-friendly
format. This is done using the 'create_index_db.py' script.  Because
LevelDB used by bitcoin core supports only one client, bitcoin core must not
be running for this step. All index data is stored in the 'blockindex.pdb'
file.

Finally, the block chain can be parsed using the index data in
'blockindex.pdb'.

tl;dr:

1. Make sure bitcoin core is running
2. Run create_blockhash_db.py to query all block hashes from bitcoin core and
   save them im 'blockhashes.pdb'
3. Stop bitcoin core to make it release its lock on its block-index database
4. Run create_index_db.py to get index data for each block from bitcoin
   core's block-index database and save it to 'blockindex.pdb'
5. Restart bitcoin core
6. You can now use the data is 'blockindex.pdb' to locate blocks in Bitcoin
   core's blockchain
