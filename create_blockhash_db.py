#!/usr/bin/env python3

from bitcoin.rpc import RawProxy
from time import time
import pickle

#
# locate raw blocks using the active chain's hashes provided by bitcoin core
#

datadir = '/scratch/bitcoin-0.19.0.1-datadir/'
db = 'blockhashes.pdb'

# open connection to bitcoin core
p = RawProxy(btc_conf_file=datadir + '/bitcoin.conf')

# get highest block in active chain
tip = p.getblockcount()

blockhashes = []
start = time()
# get block hashes via 'getblockhash' RPC
for i in range(tip+1):
    blockhashes.append(p.getblockhash(i))
    if (i > 0 and i % 10000 == 0):

        # determine performance (in hashes per second) based on empirical data
        runtime = time() - start
        hps = (i+1)/runtime

        # estimate remaining runtime by dividing remaining work through
        # estimated performance
        time_left = ((tip+1) - (i+1))/hps

        print('{}/{} (elapsed time {:.1f}s, remaining time: {:.1f}s)'.format(i, tip, runtime, time_left))


# save array to file
with open(db, 'wb') as fp:
    pickle.dump(blockhashes, fp, protocol=pickle.HIGHEST_PROTOCOL)

stop = time()
print('wrote {} block hashes to {} in {:.1f}s'.format(tip+1, db, stop-start))
