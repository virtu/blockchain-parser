#!/usr/bin/env python3

DEBUG = 3

import os
import re
import sys
import random
import statistics
import pandas as pd
import numpy as np
from bitcoin.rpc import RawProxy

from Cache import Cache
from params import window_sizes, mandatory_metrics

cand_dir = ''
core_dir = '/scratch/bitcoin-0.19.0.1-datadir/'         # path to bitcoin core
p = RawProxy(btc_conf_file=core_dir + '/bitcoin.conf')  # proxy to bitcoin core

# find all candidate metrics
def get_metrics(path):
    metrics = os.listdir(path)
    # skip lost_subsidy for now because it does not use height,min,max,...-header
    metrics = [re.sub('-\d+[.]csv.bz2$', '', f) for f in metrics if not f.startswith('lost_subsidy')]
    # drop duplicates
    return list(set(metrics))

def coin(val):
    COIN = 100 * 1000 * 1000
    return int(val * COIN)

def varint_size_by_value(val):
    # https://en.bitcoin.it/wiki/Protocol_documentation#Variable_length_integer
    if val < 0xFD:
        return 1
    elif val < 0xFFFF:
        return 3
    elif val < 0xFFFFFFFF:
        return 5
    else:
        return 9

def debug(string, level, newline=False):
    if DEBUG >= level:
        if newline:
            print('')
        print('[DEBUG {}]: '.format(level) + string)

# return a dict with reference data for 'height'
def get_reference(metric, window_start, window_size):
    debug('entered get_reference(): metric: {} window_start: {} window_size {}'.format(metric, window_start, window_size), 3)

    blocks = range(window_start, window_start+window_size)
    ref = {}
    ref['height'] = []
    ref[metric] = []
    
    # height and time
    for block in blocks:
        b = p.getblock(p.getblockhash(block))
        ref['height'].append(b['height'])

    if metric == 'created_UTXO_type_P2PKH':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                for output in t['vout']:
                    if output['scriptPubKey']['type'] == 'pubkeyhash':
                        num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric == 'created_UTXO_type_P2SH':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                for output in t['vout']:
                    if output['scriptPubKey']['type'] == 'scripthash':
                        num_txout_type += 1
            ref[metric].append(num_txout_type)


    elif metric == 'created_UTXO_type_OP_RETURN':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                for output in t['vout']:
                    if output['scriptPubKey']['type'] == 'nulldata':
                        num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric == 'created_UTXO_type_P2UPK':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                for output in t['vout']:
                    if output['scriptPubKey']['type'] == 'pubkey':
                        if len(output['scriptPubKey']['hex'])/2 == 67:
                            num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric == 'created_UTXO_type_P2CPK':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                for output in t['vout']:
                    if output['scriptPubKey']['type'] == 'pubkey':
                        if len(output['scriptPubKey']['hex'])/2 == 35:
                            num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric == 'created_UTXO_type_P2WPKH':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                for output in t['vout']:
                    if output['scriptPubKey']['type'] == 'witness_v0_keyhash':
                            num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric == 'created_UTXO_type_P2WSH':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                for output in t['vout']:
                    if output['scriptPubKey']['type'] == 'witness_v0_scripthash':
                            num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric == 'created_UTXO_type_P2W_UNKNOWN':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                for output in t['vout']:
                    if output['scriptPubKey']['type'] == 'witness_unknown':
                            num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric == 'created_UTXO_type_NONSTANDARD':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                for output in t['vout']:
                    if output['scriptPubKey']['type'] == 'nonstandard':
                            num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric == 'spent_UTXO_type_COINBASE':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                if 'coinbase' in t['vin'][0].keys():
                    num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric == 'spent_UTXO_type_P2CPK':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip coinbase
                if 'coinbase' in t['vin'][0].keys():
                    continue
                for inp in t['vin']:
                    ref_txid = inp['txid']
                    ref_pos = inp['vout']
                    ref_output = p.decoderawtransaction(p.getrawtransaction(ref_txid), True)['vout'][ref_pos]
                    if ref_output['scriptPubKey']['type'] == 'pubkey':
                        if len(ref_output['scriptPubKey']['hex'])/2 == 35:
                            num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric == 'spent_UTXO_type_P2UPK':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip coinbase
                if 'coinbase' in t['vin'][0].keys():
                    continue
                for inp in t['vin']:
                    ref_txid = inp['txid']
                    ref_pos = inp['vout']
                    ref_output = p.decoderawtransaction(p.getrawtransaction(ref_txid), True)['vout'][ref_pos]
                    if ref_output['scriptPubKey']['type'] == 'pubkey':
                        if len(ref_output['scriptPubKey']['hex'])/2 == 67:
                            num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric == 'spent_UTXO_type_P2PKH':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip coinbase
                if 'coinbase' in t['vin'][0].keys():
                    continue
                for inp in t['vin']:
                    ref_txid = inp['txid']
                    ref_pos = inp['vout']
                    ref_output = p.decoderawtransaction(p.getrawtransaction(ref_txid), True)['vout'][ref_pos]
                    if ref_output['scriptPubKey']['type'] == 'pubkeyhash':
                        num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric == 'spent_UTXO_type_NONSTANDARD':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip coinbase
                if 'coinbase' in t['vin'][0].keys():
                    continue
                for inp in t['vin']:
                    ref_txid = inp['txid']
                    ref_pos = inp['vout']
                    ref_output = p.decoderawtransaction(p.getrawtransaction(ref_txid), True)['vout'][ref_pos]
                    if ref_output['scriptPubKey']['type'] == 'nonstandard':
                        num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric == 'spent_UTXO_type_P2WPKH':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip coinbase
                if 'coinbase' in t['vin'][0].keys():
                    continue
                for inp in t['vin']:
                    ref_txid = inp['txid']
                    ref_pos = inp['vout']
                    ref_output = p.decoderawtransaction(p.getrawtransaction(ref_txid), True)['vout'][ref_pos]
                    if ref_output['scriptPubKey']['type'] == 'witness_v0_keyhash':
                        num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric == 'spent_UTXO_type_P2WSH':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip coinbase
                if 'coinbase' in t['vin'][0].keys():
                    continue
                for inp in t['vin']:
                    ref_txid = inp['txid']
                    ref_pos = inp['vout']
                    ref_output = p.decoderawtransaction(p.getrawtransaction(ref_txid), True)['vout'][ref_pos]
                    if ref_output['scriptPubKey']['type'] == 'witness_v0_scripthash':
                        # don't include P2WSH-MULTISIG
                        witness_script = inp['txinwitness'][-1]
                        if witness_script[-2::] == 'ae' and p.decodescript(witness_script)['type'] == 'multisig':
                            continue
                        num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric == 'spent_UTXO_type_P2SH':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip coinbase
                if 'coinbase' in t['vin'][0].keys():
                    continue
                for inp in t['vin']:
                    ref_txid = inp['txid']
                    ref_pos = inp['vout']
                    ref_output = p.decoderawtransaction(p.getrawtransaction(ref_txid), True)['vout'][ref_pos]
                    if ref_output['scriptPubKey']['type'] == 'scripthash':
                        # don't include P2SH-MULTISIG
                        redeem_script = inp['scriptSig']['asm'].split(' ')[-1]
                        if redeem_script[-2::] == 'ae' and p.decodescript(redeem_script)['type'] == 'multisig':
                            continue
                        # don't include P2SH-P2WPKH
                        script_sig = bytes.fromhex(inp['scriptSig']['hex'])
                        if script_sig[0] == 22 and script_sig[1] == 0x00 and script_sig[2] == 20:
                            continue
                        # don't include P2SH-P2WSH
                        if script_sig[0] == 34 and script_sig[1] == 0x00 and script_sig[2] == 32:
                            continue
                        num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric.startswith('spent_UTXO_type_MULTISIG'):
        m = int(metric.split('-')[1])
        n = int(metric.split('-')[3])
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip coinbase
                if 'coinbase' in t['vin'][0].keys():
                    continue
                for inp in t['vin']:
                    ref_txid = inp['txid']
                    ref_pos = inp['vout']
                    ref_output = p.decoderawtransaction(p.getrawtransaction(ref_txid), True)['vout'][ref_pos]
                    if ref_output['scriptPubKey']['type'] == 'multisig':
                        script_m = int(ref_output['scriptPubKey']['hex'][0:2], 16) - (0x51 - 1)
                        script_n = int(ref_output['scriptPubKey']['hex'][-4:-2], 16) - (0x51 - 1)
                        # print(f'm: {m} n: {n} script_m: {script_m} n_script: {script_n}')
                        if script_m == m and script_n == n:
                            num_txout_type += 1
                            print(f'hit! num_txout_type now: {num_txout_type}')
            ref[metric].append(num_txout_type)

    elif metric.startswith('created_UTXO_type_MULTISIG'):
        m = int(metric.split('-')[1])
        n = int(metric.split('-')[3])
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                for output in t['vout']:
                    if output['scriptPubKey']['type'] == 'multisig':
                        script_m = int(output['scriptPubKey']['hex'][0:2], 16) - (0x51 - 1)
                        script_n = int(output['scriptPubKey']['hex'][-4:-2], 16) - (0x51 - 1)
                        # print(f'm: {m} n: {n} script_m: {script_m} n_script: {script_n}')
                        if script_m == m and script_n == n:
                            num_txout_type += 1
                            print(f'hit! num_txout_type now: {num_txout_type}')
            ref[metric].append(num_txout_type)

    elif metric.startswith('spent_UTXO_type_P2WSH_MULTISIG'):
        m = int(metric.split('-')[1])
        n = int(metric.split('-')[3])
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip coinbase
                if 'coinbase' in t['vin'][0].keys():
                    continue
                for inp in t['vin']:
                    ref_txid = inp['txid']
                    ref_pos = inp['vout']
                    ref_output = p.decoderawtransaction(p.getrawtransaction(ref_txid), True)['vout'][ref_pos]
                    if ref_output['scriptPubKey']['type'] == 'witness_v0_scripthash':
                        witness_script = inp['txinwitness'][-1]
                        if witness_script[-2::] == 'ae' and p.decodescript(witness_script)['type'] == 'multisig':
                            script_m = int(witness_script[0:2], 16) - (0x51 - 1)
                            script_n = int(witness_script[-4:-2], 16) - (0x51 - 1)
                            # print(f'm: {m} n: {n} script_m: {script_m} n_script: {script_n}')
                            if script_m == m and script_n == n:
                                num_txout_type += 1
                                print(f'hit! num_txout_type now: {num_txout_type}')
            ref[metric].append(num_txout_type)

    elif metric.startswith('spent_UTXO_type_P2WSH_MULTISIG'):
        m = int(metric.split('-')[1])
        n = int(metric.split('-')[3])
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip coinbase
                if 'coinbase' in t['vin'][0].keys():
                    continue
                for inp in t['vin']:
                    ref_txid = inp['txid']
                    ref_pos = inp['vout']
                    ref_output = p.decoderawtransaction(p.getrawtransaction(ref_txid), True)['vout'][ref_pos]
                    if ref_output['scriptPubKey']['type'] == 'witness_v0_scripthash':
                        witness_script = inp['txinwitness'][-1]
                        if witness_script[-2::] == 'ae' and p.decodescript(witness_script)['type'] == 'multisig':
                            script_m = int(witness_script[0:2], 16) - (0x51 - 1)
                            script_n = int(witness_script[-4:-2], 16) - (0x51 - 1)
                            # print(f'm: {m} n: {n} script_m: {script_m} n_script: {script_n}')
                            if script_m == m and script_n == n:
                                num_txout_type += 1
                                print(f'hit! num_txout_type now: {num_txout_type}')
            ref[metric].append(num_txout_type)

    elif metric.startswith('spent_UTXO_type_P2SH_P2WSH_MULTISIG'):
        m = int(metric.split('-')[1])
        n = int(metric.split('-')[3])
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip coinbase
                if 'coinbase' in t['vin'][0].keys():
                    continue
                for inp in t['vin']:
                    ref_txid = inp['txid']
                    ref_pos = inp['vout']
                    ref_output = p.decoderawtransaction(p.getrawtransaction(ref_txid), True)['vout'][ref_pos]
                    if ref_output['scriptPubKey']['type'] == 'scripthash':
                        script_sig = bytes.fromhex(inp['scriptSig']['hex'])
                        if script_sig[0] == 34 and script_sig[1] == 0x00 and script_sig[2] == 32:
                            if 'txinwitness' in inp:
                                witness_script = inp['txinwitness'][-1]
                                if witness_script[-2::] == 'ae' and p.decodescript(witness_script)['type'] == 'multisig':
                                    script_m = int(witness_script[0:2], 16) - (0x51 - 1)
                                    script_n = int(witness_script[-4:-2], 16) - (0x51 - 1)
                                    # print(f'm: {m} n: {n} script_m: {script_m} n_script: {script_n}')
                                    if script_m == m and script_n == n:
                                        num_txout_type += 1
                                        print(f'hit! num_txout_type now: {num_txout_type}')
            ref[metric].append(num_txout_type)

    elif metric.startswith('spent_UTXO_type_P2SH_MULTISIG'):
        m = int(metric.split('-')[1])
        n = int(metric.split('-')[3])
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip coinbase
                if 'coinbase' in t['vin'][0].keys():
                    continue
                for inp in t['vin']:
                    ref_txid = inp['txid']
                    ref_pos = inp['vout']
                    ref_output = p.decoderawtransaction(p.getrawtransaction(ref_txid), True)['vout'][ref_pos]
                    if ref_output['scriptPubKey']['type'] == 'scripthash':
                        redeem_script = inp['scriptSig']['asm'].split(' ')[-1]
                        if redeem_script[-2::] == 'ae' and p.decodescript(redeem_script)['type'] == 'multisig':
                            script_m = int(redeem_script[0:2], 16) - (0x51 - 1)
                            script_n = int(redeem_script[-4:-2], 16) - (0x51 - 1)
                            # print(f'm: {m} n: {n} script_m: {script_m} n_script: {script_n}')
                            if script_m == m and script_n == n:
                                num_txout_type += 1
                                print(f'hit! num_txout_type now: {num_txout_type}')
            ref[metric].append(num_txout_type)


    elif metric == 'spent_UTXO_type_P2SH_P2WPKH':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip coinbase
                if 'coinbase' in t['vin'][0].keys():
                    continue
                for inp in t['vin']:
                    ref_txid = inp['txid']
                    ref_pos = inp['vout']
                    ref_output = p.decoderawtransaction(p.getrawtransaction(ref_txid), True)['vout'][ref_pos]
                    if ref_output['scriptPubKey']['type'] == 'scripthash':
                        script_sig = bytes.fromhex(inp['scriptSig']['hex'])
                        if script_sig[0] == 22 and script_sig[1] == 0x00 and script_sig[2] == 20:
                            num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric == 'spent_UTXO_type_P2SH_P2WSH':
        ref[metric] = []
        for block in blocks:
            num_txout_type = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip coinbase
                if 'coinbase' in t['vin'][0].keys():
                    continue
                for inp in t['vin']:
                    ref_txid = inp['txid']
                    ref_pos = inp['vout']
                    ref_output = p.decoderawtransaction(p.getrawtransaction(ref_txid), True)['vout'][ref_pos]
                    if ref_output['scriptPubKey']['type'] == 'scripthash':
                        script_sig = bytes.fromhex(inp['scriptSig']['hex'])
                        if script_sig[0] == 34 and script_sig[1] == 0x00 and script_sig[2] == 32:
                            # don't include P2SH-P2WSH-MULTISIG
                            if 'txinwitness' in inp:
                                witness_script = inp['txinwitness'][-1]
                                if witness_script[-2::] == 'ae' and p.decodescript(witness_script)['type'] == 'multisig':
                                    continue
                            num_txout_type += 1
            ref[metric].append(num_txout_type)

    elif metric == 'tx_size':
        ref['tx_size'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                ref['tx_size'].append(t['size'])
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'tx_weight':
        ref['tx_weight'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                ref['tx_weight'].append(t['weight'])
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'block_transactions_weight':
        ref['block_transactions_weight'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            block_transactions_weight = 0
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                block_transactions_weight += t['weight']
            ref['block_transactions_weight'].append(block_transactions_weight)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'block_timestamp':
        ref['block_timestamp'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            ref['block_timestamp'].append(b['time'])
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'block_diff':
        ref['block_diff'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            ref['block_diff'].append(float(b['difficulty']))
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'block_version':
        ref['block_version'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            ref['block_version'].append(float(b['version']))
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'block_size':
        ref['block_size'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            ref['block_size'].append(b['size'])
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'stripped_block_size':
        ref['stripped_block_size'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            ref['stripped_block_size'].append(b['strippedsize'])
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'block_weight':
        ref['block_weight'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            ref['block_weight'].append(b['weight'])
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'block_reward':
        ref['block_reward'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            # reward: sum of coinbase outputs
            t = p.decoderawtransaction(p.getrawtransaction(b['tx'][0]), True)
            reward = sum(coin(outp['value']) for outp in t['vout'])
            ref['block_reward'].append(reward)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'block_subsidy':
        ref['block_subsidy'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            # reward = subsidy + fees --> subsidy = reward - fees
            # calc reward (sum of coinbase outputs)
            t = p.decoderawtransaction(p.getrawtransaction(b['tx'][0]), True)
            reward = sum(coin(outp['value']) for outp in t['vout'])
            # calc fees
            fee = 0
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip no-fee coinbase
                if 'coinbase' in t['vin'][0]:
                    continue
                # sum of inputs
                sum_inputs = 0
                for inp in t['vin']:
                    # get object for referenced tx
                    ref_t = p.decoderawtransaction(p.getrawtransaction(inp['txid']), True)
                    # which output is referenced in the referenced tx
                    pos = inp['vout']
                    # get amount of referenced output in referenced tx
                    sum_inputs += coin(ref_t['vout'][pos]['value'])
                # sum of outputs
                sum_outputs = sum(coin(outp['value']) for outp in t['vout'])
                # relative fee
                fee += sum_inputs-sum_outputs
            ref['block_subsidy'].append(reward-fee)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'total_block_fees_incl_coinbase':
        ref['total_block_fees_incl_coinbase'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            # calc fees
            fee = 0
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # zero fee for coinbase
                if 'coinbase' in t['vin'][0]:
                    fee += 0
                    continue
                # sum of inputs
                sum_inputs = 0
                for inp in t['vin']:
                    # get object for referenced tx
                    ref_t = p.decoderawtransaction(p.getrawtransaction(inp['txid']), True)
                    # which output is referenced in the referenced tx
                    pos = inp['vout']
                    # get amount of referenced output in referenced tx
                    sum_inputs += coin(ref_t['vout'][pos]['value'])
                # sum of outputs
                sum_outputs = sum(coin(outp['value']) for outp in t['vout'])
                # relative fee
                fee += sum_inputs-sum_outputs
            ref['total_block_fees_incl_coinbase'].append(fee)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'total_block_fees_excl_coinbase':
        ref['total_block_fees_excl_coinbase'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            # calc fees
            fee = 0
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip no-fee coinbase
                if 'coinbase' in t['vin'][0]:
                    continue
                # sum of inputs
                sum_inputs = 0
                for inp in t['vin']:
                    # get object for referenced tx
                    ref_t = p.decoderawtransaction(p.getrawtransaction(inp['txid']), True)
                    # which output is referenced in the referenced tx
                    pos = inp['vout']
                    # get amount of referenced output in referenced tx
                    sum_inputs += coin(ref_t['vout'][pos]['value'])
                # sum of outputs
                sum_outputs = sum(coin(outp['value']) for outp in t['vout'])
                # relative fee
                fee += sum_inputs-sum_outputs
            ref['total_block_fees_excl_coinbase'].append(fee)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)


    elif metric == 'number_of_tx_per_block':
        ref['number_of_tx_per_block'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            ref['number_of_tx_per_block'].append(len(b['tx']))
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'number_of_witness_tx_per_block':
        ref['number_of_witness_tx_per_block'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            num_segwit = 0
            for tx in b['tx']:
                t_raw = p.getrawtransaction(tx)
                if t_raw[4*2:4*2+2*2] == '0001':
                    num_segwit += 1
            ref['number_of_witness_tx_per_block'].append(num_segwit)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'amount_transferred_per_block':
        ref['amount_transferred_per_block'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            block_outputs = 0
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                tx_outputs = sum(coin(outp['value']) for outp in t['vout'])
                block_outputs += tx_outputs
            ref['amount_transferred_per_block'].append(block_outputs)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'inputs_per_tx':
        ref['inputs_per_tx'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                ref['inputs_per_tx'].append(len(t['vin']))
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'total_inputs_per_block':
        ref['total_inputs_per_block'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            block_inputs = 0
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                tx_inputs = len(t['vin'])
                block_inputs += tx_inputs
            ref['total_inputs_per_block'].append(block_inputs)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'total_outputs_per_block':
        ref['total_outputs_per_block'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            block_outputs = 0
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                tx_outputs = len(t['vout'])
                block_outputs += tx_outputs
            ref['total_outputs_per_block'].append(block_outputs)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'outputs_per_tx':
        ref['outputs_per_tx'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                ref['outputs_per_tx'].append(len(t['vout']))
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'amount_transferred_per_tx':
        ref['amount_transferred_per_tx'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # iterate over all outputs to 'fetch' coins
                sum_outputs = 0
                for output in t['vout']:
                    sum_outputs += coin(output['value'])
                ref['amount_transferred_per_tx'].append(sum_outputs)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'input_size_per_tx':
        ref['input_size_per_tx'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # determine sizes of tx's inputs
                for inp in t['vin']:
                    # use coinbase as script if coinbase tx, else scriptSig
                    script = inp['coinbase'] if 'coinbase' in inp.keys() else inp['scriptSig']['hex']
                    # determine script and varint sizes
                    script_size = len(script)/2
                    varint_size = varint_size_by_value(script_size)
                    # 32-byte txid, four-byte pos, varint (coinbase/scriptSig
                    # length), scriptSig, four-byte seq no.
                    size = 32 + 4 + varint_size + script_size  + 4
                    ref['input_size_per_tx'].append(size)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'output_size_per_tx':
        ref['output_size_per_tx'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # determine sizes of tx's inputs
                for outp in t['vout']:
                    # determine script and varint sizes
                    script_size = len(outp['scriptPubKey']['hex'])/2
                    varint_size = varint_size_by_value(script_size)
                    # 8-byte amount, varint (scriptPubKey length), scriptPubKey
                    size = 8 + varint_size + script_size
                    ref['output_size_per_tx'].append(size)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'absolute_fee_per_tx_incl_coinbase':
        ref['absolute_fee_per_tx_incl_coinbase'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # coinbase has no fee, so add relative fee of zero
                debug('get_metric() t[\'vin\'][0]: {}'.format(t['vin'][0]), 4)
                if 'coinbase' in t['vin'][0]:
                    ref['absolute_fee_per_tx_incl_coinbase'].append(0)
                    continue
                # sum of inputs
                sum_inputs = 0
                for inp in t['vin']:
                    # get object for referenced tx
                    ref_t = p.decoderawtransaction(p.getrawtransaction(inp['txid']), True)
                    # which output is referenced in the referenced tx
                    pos = inp['vout']
                    # get amount of referenced output in referenced tx
                    sum_inputs += coin(ref_t['vout'][pos]['value'])
                # sum of outputs
                sum_outputs = sum(coin(outp['value']) for outp in t['vout'])
                # relative fee
                ref['absolute_fee_per_tx_incl_coinbase'].append(sum_inputs-sum_outputs)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'absolute_fee_per_tx_excl_coinbase':
        ref['absolute_fee_per_tx_excl_coinbase'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # coinbase has no fee, so add relative fee of zero
                debug('get_metric() t[\'vin\'][0]: {}'.format(t['vin'][0]), 4)
                if 'coinbase' in t['vin'][0]:
                    continue
                # sum of inputs
                sum_inputs = 0
                for inp in t['vin']:
                    # get object for referenced tx
                    ref_t = p.decoderawtransaction(p.getrawtransaction(inp['txid']), True)
                    # which output is referenced in the referenced tx
                    pos = inp['vout']
                    # get amount of referenced output in referenced tx
                    sum_inputs += coin(ref_t['vout'][pos]['value'])
                # sum of outputs
                sum_outputs = sum(coin(outp['value']) for outp in t['vout'])
                # relative fee
                ref['absolute_fee_per_tx_excl_coinbase'].append(sum_inputs-sum_outputs)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'relative_fee_per_tx_incl_coinbase':
        ref['relative_fee_per_tx_incl_coinbase'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # coinbase has no fee, so add relative fee of zero
                debug('get_metric() t[\'vin\'][0]: {}'.format(t['vin'][0]), 4)
                if 'coinbase' in t['vin'][0]:
                    ref['relative_fee_per_tx_incl_coinbase'].append(0)
                    continue
                # sum of inputs
                sum_inputs = 0
                for inp in t['vin']:
                    # get object for referenced tx
                    ref_t = p.decoderawtransaction(p.getrawtransaction(inp['txid']), True)
                    # which output is referenced in the referenced tx
                    pos = inp['vout']
                    # get amount of referenced output in referenced tx
                    sum_inputs += coin(ref_t['vout'][pos]['value'])
                # sum of outputs
                sum_outputs = sum(coin(outp['value']) for outp in t['vout'])
                # relative fee
                ref['relative_fee_per_tx_incl_coinbase'].append((sum_inputs-sum_outputs)/t['size'])
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'relative_fee_per_tx_excl_coinbase':
        ref['relative_fee_per_tx_excl_coinbase'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # coinbase has no fee, so add relative fee of zero
                debug('get_metric() t[\'vin\'][0]: {}'.format(t['vin'][0]), 4)
                if 'coinbase' in t['vin'][0]:
                    continue
                # sum of inputs
                sum_inputs = 0
                for inp in t['vin']:
                    # get object for referenced tx
                    ref_t = p.decoderawtransaction(p.getrawtransaction(inp['txid']), True)
                    # which output is referenced in the referenced tx
                    pos = inp['vout']
                    # get amount of referenced output in referenced tx
                    sum_inputs += coin(ref_t['vout'][pos]['value'])
                # sum of outputs
                sum_outputs = sum(coin(outp['value']) for outp in t['vout'])
                # relative fee
                ref['relative_fee_per_tx_excl_coinbase'].append((sum_inputs-sum_outputs)/t['size'])
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'relative_fee_per_WU_per_tx_incl_coinbase':
        ref['relative_fee_per_WU_per_tx_incl_coinbase'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # coinbase has no fee, so add relative fee of zero
                debug('get_metric() t[\'vin\'][0]: {}'.format(t['vin'][0]), 4)
                if 'coinbase' in t['vin'][0]:
                    ref['relative_fee_per_WU_per_tx_incl_coinbase'].append(0)
                    continue
                # sum of inputs
                sum_inputs = 0
                for inp in t['vin']:
                    # get object for referenced tx
                    ref_t = p.decoderawtransaction(p.getrawtransaction(inp['txid']), True)
                    # which output is referenced in the referenced tx
                    pos = inp['vout']
                    # get amount of referenced output in referenced tx
                    sum_inputs += coin(ref_t['vout'][pos]['value'])
                # sum of outputs
                sum_outputs = sum(coin(outp['value']) for outp in t['vout'])
                # relative fee
                ref['relative_fee_per_WU_per_tx_incl_coinbase'].append((sum_inputs-sum_outputs)/t['weight'])
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'relative_fee_per_WU_per_tx_excl_coinbase':
        ref['relative_fee_per_WU_per_tx_excl_coinbase'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # coinbase has no fee, so add relative fee of zero
                debug('get_metric() t[\'vin\'][0]: {}'.format(t['vin'][0]), 4)
                if 'coinbase' in t['vin'][0]:
                    continue
                # sum of inputs
                sum_inputs = 0
                for inp in t['vin']:
                    # get object for referenced tx
                    ref_t = p.decoderawtransaction(p.getrawtransaction(inp['txid']), True)
                    # which output is referenced in the referenced tx
                    pos = inp['vout']
                    # get amount of referenced output in referenced tx
                    sum_inputs += coin(ref_t['vout'][pos]['value'])
                # sum of outputs
                sum_outputs = sum(coin(outp['value']) for outp in t['vout'])
                # relative fee
                ref['relative_fee_per_WU_per_tx_excl_coinbase'].append((sum_inputs-sum_outputs)/t['weight'])
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'relative_fee_per_tx_mean_incl_coinbase':
        ref['relative_fee_per_tx_mean_incl_coinbase'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            sum_fee = 0
            sum_size = 0
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # coinbase has no fee, so add relative fee of zero
                debug('get_metric() t[\'vin\'][0]: {}'.format(t['vin'][0]), 4)
                if 'coinbase' in t['vin'][0]:
                    sum_fee += 0
                    sum_size += t['size']
                    continue
                # sum of inputs
                sum_inputs = 0
                for inp in t['vin']:
                    # get object for referenced tx
                    ref_t = p.decoderawtransaction(p.getrawtransaction(inp['txid']), True)
                    # which output is referenced in the referenced tx
                    pos = inp['vout']
                    # get amount of referenced output in referenced tx
                    sum_inputs += coin(ref_t['vout'][pos]['value'])
                # sum of outputs
                sum_outputs = sum(coin(outp['value']) for outp in t['vout'])
                # relative fee
                sum_fee += sum_inputs - sum_outputs
                sum_size += t['size']
            ref['relative_fee_per_tx_mean_incl_coinbase'].append(sum_fee/sum_size)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'relative_fee_per_tx_mean_excl_coinbase':
        ref['relative_fee_per_tx_mean_excl_coinbase'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            sum_fee = 0
            sum_size = 0
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip coinbase
                debug('get_metric() t[\'vin\'][0]: {}'.format(t['vin'][0]), 4)
                if 'coinbase' in t['vin'][0]:
                    continue
                # sum of inputs
                sum_inputs = 0
                for inp in t['vin']:
                    # get object for referenced tx
                    ref_t = p.decoderawtransaction(p.getrawtransaction(inp['txid']), True)
                    # which output is referenced in the referenced tx
                    pos = inp['vout']
                    # get amount of referenced output in referenced tx
                    sum_inputs += coin(ref_t['vout'][pos]['value'])
                # sum of outputs
                sum_outputs = sum(coin(outp['value']) for outp in t['vout'])
                # relative fee
                sum_fee += sum_inputs - sum_outputs
                sum_size += t['size']
            # A valid window is selected from the candidate data, so the
            # number of tx's excluding the coinbase should be >=1. Consequenly
            # division by zero (sum_size) should never occur in the next line
            ref['relative_fee_per_tx_mean_excl_coinbase'].append(sum_fee/sum_size)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'relative_fee_per_WU_per_tx_mean_incl_coinbase':
        ref['relative_fee_per_WU_per_tx_mean_incl_coinbase'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            sum_fee = 0
            sum_weight = 0
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # coinbase has no fee, so add relative fee of zero
                debug('get_metric() t[\'vin\'][0]: {}'.format(t['vin'][0]), 4)
                if 'coinbase' in t['vin'][0]:
                    sum_fee += 0
                    sum_weight += t['weight']
                    continue
                # sum of inputs
                sum_inputs = 0
                for inp in t['vin']:
                    # get object for referenced tx
                    ref_t = p.decoderawtransaction(p.getrawtransaction(inp['txid']), True)
                    # which output is referenced in the referenced tx
                    pos = inp['vout']
                    # get amount of referenced output in referenced tx
                    sum_inputs += coin(ref_t['vout'][pos]['value'])
                # sum of outputs
                sum_outputs = sum(coin(outp['value']) for outp in t['vout'])
                # relative fee
                sum_fee += sum_inputs - sum_outputs
                sum_weight += t['weight']
            ref['relative_fee_per_WU_per_tx_mean_incl_coinbase'].append(sum_fee/sum_weight)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'relative_fee_per_WU_per_tx_mean_excl_coinbase':
        ref['relative_fee_per_WU_per_tx_mean_excl_coinbase'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            sum_fee = 0
            sum_weight = 0
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip coinbase
                debug('get_metric() t[\'vin\'][0]: {}'.format(t['vin'][0]), 4)
                if 'coinbase' in t['vin'][0]:
                    continue
                # sum of inputs
                sum_inputs = 0
                for inp in t['vin']:
                    # get object for referenced tx
                    ref_t = p.decoderawtransaction(p.getrawtransaction(inp['txid']), True)
                    # which output is referenced in the referenced tx
                    pos = inp['vout']
                    # get amount of referenced output in referenced tx
                    sum_inputs += coin(ref_t['vout'][pos]['value'])
                # sum of outputs
                sum_outputs = sum(coin(outp['value']) for outp in t['vout'])
                # relative fee
                sum_fee += sum_inputs - sum_outputs
                sum_weight += t['weight']
            # A valid window is selected from the candidate data, so the
            # number of tx's excluding the coinbase should be >=1. Consequenly
            # division by zero (sum_size) should never occur in the next line
            ref['relative_fee_per_WU_per_tx_mean_excl_coinbase'].append(sum_fee/sum_weight)
        debug('get_metric() final ref[{}]: {}'.format(metric, ref[metric]), 4)

    elif metric == 'number_of_segwit_tx_per_block':
        ref['number_of_segwit_tx_per_block'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            segwit_tx = 0
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # process only segwit tx
                if 4*t['size'] != t['weight']:
                    segwit_tx = segwit_tx + 1
            ref['number_of_segwit_tx_per_block'].append(segwit_tx)

    elif metric == 'fraction_of_segwit_tx_per_block':
        ref['fraction_of_segwit_tx_per_block'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            total_tx = 0
            segwit_tx = 0
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                total_tx = total_tx + 1
                # process only segwit tx
                if 4*t['size'] != t['weight']:
                    segwit_tx = segwit_tx + 1
            ref['fraction_of_segwit_tx_per_block'].append(segwit_tx/total_tx)

    elif metric == 'segwit_tx_witness_size':
        ref['segwit_tx_witness_size'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip non-segwit tx
                if t['weight'] == 4 * t['size']:
                    continue
                stripped_size = (t['weight'] - t['size'])/3
                witness_size = t['size'] - stripped_size
                ref['segwit_tx_witness_size'].append(witness_size)

    elif metric == 'segwit_ratio_in_segwit_tx':
        ref['segwit_ratio_in_segwit_tx'] = []
        for block in blocks:
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip non-segwit tx
                if t['weight'] == 4 * t['size']:
                    continue
                stripped_size = (t['weight'] - t['size'])/3
                witness_size = t['size'] - stripped_size
                witness_ratio = witness_size / t['size']
                ref['segwit_ratio_in_segwit_tx'].append(witness_ratio)

    elif metric == 'segwit_ratio_mean_in_segwit_tx':
        ref['segwit_ratio_mean_in_segwit_tx'] = []
        for block in blocks:
            sum_size = 0
            sum_witness_size = 0
            b = p.getblock(p.getblockhash(block))
            for tx in b['tx']:
                witness_size = 0
                t = p.decoderawtransaction(p.getrawtransaction(tx), True)
                # skip non-segwit tx
                if t['weight'] == 4 * t['size']:
                    continue
                stripped_size = (t['weight'] - t['size'])/3
                witness_size = t['size'] - stripped_size
                sum_witness_size += witness_size
                sum_size += t['size']
            witness_ratio = sum_witness_size / sum_size
            ref['segwit_ratio_mean_in_segwit_tx'].append(witness_ratio)

    else:
        raise Exception('Error: metric {} not implemented'.format(metric))

    result = {}
    result['height'] = statistics.mean(ref['height'])
    result['min']= min(ref[metric])
    result['max'] = max(ref[metric])
    result['mean'] = statistics.mean(ref[metric])
    result['median'] = statistics.median(ref[metric])
    # use pandas for quartiles because the statistics package introduces support for quartiles only in Python 3.8
    result['q1'], result['q5'], result['q10'], result['q25'], result['median'], result['q75'], result['q90'], result['q95'], result['q99'] = pd.DataFrame(np.array(ref[metric])).quantile([0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]).iloc[:,0:].values
    result['CV'] = np.nan if result['mean'] == 0 else statistics.pstdev(ref[metric])/result['mean']

    debug('leaving get_reference(): metric: {} window_start: {} window_size {} reference: {}'.format(metric, window_start, window_size, result), 3)
    return result


# return array with candidate values for metric
def get_candidate(metric, window_size, search=False):
    debug('entered get_candidate(): metric: {} window_size {}'.format(metric, window_size), 3)

    # construct filename and make sure it exists
    filename = f'{cand_dir}/{metric}-{window_size}.csv.bz2'
    debug('get_candidate(): looking for candidate values in file {}'.format(filename), 4)
    if not os.path.exists(filename):
        raise Exception('Warning: input file {} does not exist!'.format(filename))

    df = pd.read_csv(filename, index_col='mean_height', header=0)

    # in case we found reference data in the cache, search for that data in candidate dataframe
    # in case we found no reference data in cache, select random line from candidate dataframe, for which reference data will be fetched
    # (we select data from candidate data, because it contains no zero values)
    if search:
        try:
            cand = dict(df.loc[search])
            cand['height'] = search
        except KeyError:
            cand = {'height': search, 'min': 0.0, 'max': 0.0, 'mean': 0.0, 'median': 0.0, 'q1': 0.0, 'q5': 0.0, 'q10': 0.0, 'q25': 0.0, 'q75': 0.0, 'q90': 0.0, 'q95': 0.0, 'q99': 0.0, 'CV': 'nan'}
    else:
        cand = dict(df.sample(n=1))
        debug('get_candidate(): metric: {} picked block: {}'.format(metric, cand['height']), 2)

    debug('leaving get_candidate(): metric: {} window_size {} result: {}'.format(metric, window_size, cand), 3)
    return cand

# return array with candidate values for metric
def compare(metric, reference, candidate):
    debug('entered compare(): metric: {} reference: {} candidate {}'.format(metric, reference, candidate), 3)

    for key in candidate.keys():
        # special handling for CV, which can be NaN
        if key == 'CV' and np.isnan(reference[key]) and np.isnan(candidate[key]):
            debug('[OK] metric: {} kind: {} candidate: {} reference: {}'.format(metric, key, candidate[key], reference[key]), 4)
            continue

        if np.isclose(reference[key], candidate[key], rtol=1e-14):
            debug('[OK] metric: {} kind: {} candidate: {} reference: {}'.format(metric, key, candidate[key], reference[key]), 4)
        else:
            debug('[FAIL!] metric: {} kind: {} candidate: {} reference: {}'.format(metric, key, candidate[key], reference[key]), 2)
            raise Exception('reference != candidate\nreference: {}\ncandidate: {}'.format(reference, candidate))

    debug('leaving compare(): metric: {} reference: {} candidate {}'.format(metric, reference, candidate), 3)

def validate(metric, window_size, required_passes):
    debug('entered validate(): metric: {} window_size: {} required_passes {}'.format(metric, window_size, required_passes), 3)

    num_success = 0

    # use cached reference data if available
    cache = Cache(metric, window_size)
    cached_data = cache.get()
    for ref in cached_data:
        debug('validating {} for window size {} with cached data...'.format(metric, window_size), 2, newline=True)

        # get corresponding candidate data
        cand = get_candidate(metric, window_size, search=ref['height'])

        # compare candidate and reference data
        compare(metric, ref, cand)
        num_success += 1
        debug('validating {} for window size {} with cached data - successful {}/{}'.format(metric, window_size, num_success, required_passes), 2)

        # exit early in case we have more reference data than desired check
        if num_success == required_passes:
            break
    
    # in case cache was empty or number of entries not enough, continue with non-cached data (i.e., fetch it from Bitcoin Core)
    while num_success < required_passes:
        debug('validating {} for window size {} with non-cached data...'.format(metric, window_size), 2, newline=True)

        # Get random candidate (no search pattern)
        cand = get_candidate(metric, window_size)

        # calculate appropriate window_start and get reference data
        window_start = int(cand['height'] - (window_size-1)/2)
        ref = get_reference(metric, window_start, window_size)

        # compare candidate and reference data, add reference data to cache in case of match
        compare(metric, ref, cand)
        cache.append(ref)
        num_success += 1
        debug('validating {} for window size {} with non-cached data - successful {}/{}'.format(metric, window_size, num_success, required_passes), 2)

    debug('leaving validate(): metric: {} window_size: {} required_passes {}'.format(metric, window_size, required_passes), 3)

def main():

    # handle path to determine which log files to analyze
    if len(sys.argv) < 2:
        raise Exception('usage: {} <path>'.format(sys.argv[0]))
    else:
        global cand_dir
        cand_dir = sys.argv[1]

    #passes = 9                                     # number of passes per window
    passes = 3                                     # number of passes per window

    # get available metrics
    metrics = get_metrics(cand_dir)
    metrics.sort()

    # validate different window sizes
    for window_size in window_sizes:
        # validate all metrics
        for metric in metrics:
           validate(metric, window_size, passes)
           done_with(metric, mandatory_metrics)

    # make sure all mandatory metrics were validated
    if mandatory_metrics:
        raise Exception('the following metrics were not validated: {} <path>'.format(mandatory_metrics))

def done_with(metric, mandatory):

    # handle fine-granular metrics
    special = ('created_UTXO_type_MULTISIG', 'spent_UTXO_type_MULTISIG', 'spent_UTXO_type_P2SH_MULTISIG', 'spent_UTXO_type_P2SH_P2WSH_MULTISIG', 'spent_UTXO_type_P2WSH_MULTISIG')
    for s in special:
        if metric.startswith(f'{s}-'):
            metric = s

    try:
        mandatory.remove(metric)
    # Might process metrics that are not in mandatory set
    except ValueError:
        pass

# call main function
if __name__ == "__main__":
    main()
