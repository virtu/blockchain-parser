import numpy as np
np.seterr(all='raise')
from .globals import log
from .globals import window_sizes
from .tools import max_block_subsidy
from .Constants import TXOUT_TYPE

def process(b, height, window):
    amount_transferred(b, window)
    inputs_and_outputs(b, window)
    spent_UTXO_types(b, window)
    created_UTXO_types(b, window)
    tx_count_size_weight(b, window)
    fees_and_subsidy(b, window, height)
    block_meta(b, window)

    # write results if necessary
    for window_size in window_sizes:
        if (height + 1) % window_size == 0:
            window.process(height, window_size)

# Amount transferred per tx, per block
def amount_transferred(block, window):
    transferred_per_tx = []
    for tx in block.transactions:
        amount_transferred = 0
        for output in tx.outputs:
            amount_transferred += output.amount
        transferred_per_tx.append(amount_transferred)
    # insert data
    window.insert('amount_transferred_per_tx', transferred_per_tx)
    window.insert('amount_transferred_per_block', sum(transferred_per_tx))

# Number and size of inputs and outputs
def inputs_and_outputs(block, window):
    input_count = []
    output_count = []
    input_size = []
    output_size = []
    for tx in block.transactions:
        # get tx's inputs and outputs
        inputs = tx.inputs
        outputs = tx.outputs
        # number of inputs and outputs
        input_count.append(len(inputs))
        output_count.append(len(outputs))
        # size of inputs and outputs
        for inp in inputs:
            input_size.append(inp.size)
        for outp in outputs:
            output_size.append(outp.size)
    # insert data
    window.insert('inputs_per_tx', input_count)
    window.insert('outputs_per_tx', output_count)
    window.insert('input_size_per_tx', input_size)
    window.insert('output_size_per_tx', output_size)
    window.insert('total_inputs_per_block', sum(input_count))
    window.insert('total_outputs_per_block', sum(output_count))

# number of spent UTXO types
def spent_UTXO_types(block, window):
    counter = {}
    # don't skip coinbase
    for tx in block.transactions:
        for inp in tx.inputs:
            spent_UTXO_type = inp.spent_UTXO_type
            target = spent_UTXO_type.name

            # extract m and n for multisig
            if spent_UTXO_type == TXOUT_TYPE.MULTISIG:
                m, n = inp.spent_UTXO_script.is_MULTISIG(params=True)
                target += f'-{m}-of-{n}'
            if spent_UTXO_type == TXOUT_TYPE.P2SH_MULTISIG:
                m, n = inp.script_sig.get_redeem_script().is_MULTISIG(params=True)
                target += f'-{m}-of-{n}'
            if spent_UTXO_type == TXOUT_TYPE.P2WSH_MULTISIG:
                m, n = inp.witness.get_witness_script().is_MULTISIG(params=True)
                target += f'-{m}-of-{n}'
            if spent_UTXO_type == TXOUT_TYPE.P2SH_P2WSH_MULTISIG:
                m, n = inp.witness.get_witness_script().is_MULTISIG(params=True)
                target += f'-{m}-of-{n}'

            # record in histogram
            log.hist(f'input_{target}', {'script_sig': inp.script_sig.size(),
                                         'spent_UTXO_script_pubkey': 0 if spent_UTXO_type == TXOUT_TYPE.COINBASE else inp.spent_UTXO_script.size(),
                                         'witness': 0 if inp.witness == None else inp.witness.size,
                                         'sum_scripts_and_witness': inp.script_sig.size() + (0 if spent_UTXO_type == TXOUT_TYPE.COINBASE else inp.spent_UTXO_script.size()) + (0 if inp.witness == None else inp.witness.size),
                                         'total': inp.size})

            # initialize or increment counter
            if target not in counter.keys():
                counter[target] = 1
            else:
                counter[target] += 1

    # insert data into window
    for target in counter.keys():
        metric = f'spent_UTXO_type_{target}'
        value = counter[target]
        window.insert(metric, value)


# number of created UTXO types
def created_UTXO_types(block, window):
    counter = {}
    # don't skip coinbase
    for tx in block.transactions:
        for output in tx.outputs:
            created_UTXO_type = output.created_UTXO_type
            target = created_UTXO_type.name

            # extract m and n for multisig
            if created_UTXO_type == TXOUT_TYPE.MULTISIG:
                m, n = output.script_pubkey.is_MULTISIG(params=True)
                target += f'-{m}-of-{n}'

            # record in histogram
            log.hist(f'output_{target}', {'script_pubkey': output.script_pubkey.size(), 'total': output.size})

            # count incidence of individual txout types
            if target not in counter.keys():
                counter[target] = 1
            else:
                counter[target] += 1

    # insert data into window
    for target in counter.keys():
        metric = f'created_UTXO_type_{target}'
        value = counter[target]
        window.insert(metric, value)

# Number of transactions per block
def tx_count_size_weight(block, window):
    # regular tx count
    txs = block.transactions
    window.insert('number_of_tx_per_block', len(txs))

    # absolute and relative segwit tx count
    swtxs = [tx for tx in txs if tx.is_segwit]
    window.insert('number_of_segwit_tx_per_block', len(swtxs))
    window.insert('fraction_of_segwit_tx_per_block', len(swtxs)/len(txs))

    # tx sizes
    tx_sizes = [tx.size for tx in txs]
    window.insert('tx_size', tx_sizes)

    if swtxs:
        # witness data sizes
        window.insert('segwit_tx_witness_size', [(tx.size - tx.stripped_size) for tx in swtxs])
        # ratio of sizes of witness data to overall data per tx
        window.insert('segwit_ratio_in_segwit_tx', [(tx.size - tx.stripped_size)/tx.size for tx in swtxs])
        window.insert('segwit_ratio_mean_in_segwit_tx', sum([tx.size - tx.stripped_size for tx in swtxs])/sum([tx.size for tx in swtxs]))

    # tx weight
    tx_weights = [tx.weight for tx in txs]
    window.insert('tx_weight', tx_weights)

# block size, weight, timestamp, difficulty
def block_meta(block, window):
    # block size [B]
    block_size = block.size
    window.insert('block_size', block_size)

    # stripped size [B]
    segwit_sizes = [(tx.size - tx.stripped_size) for tx in block.transactions]
    stripped_block_size = block_size - sum(segwit_sizes)
    window.insert('stripped_block_size', stripped_block_size)

    # block weight [WU]
    block_weight = stripped_block_size * 4 + sum(segwit_sizes)
    window.insert('block_weight', block_weight)

    # weight of all transactions
    tx_weights = [tx.weight for tx in block.transactions]
    window.insert('block_transactions_weight', sum(tx_weights))

    # block difficulty
    block_diff = block.get_diff()
    window.insert('block_diff', block_diff)

    # timestamp
    timestamp = block.timestamp
    window.insert('block_timestamp', timestamp)

    # timestamp
    version = block.version
    window.insert('block_version', version)


    # network difficulty
    #
    # TODO
    #
    # cf. CalculateNextWorkRequired here: https://github.com/bitcoin/bitcoin/blob/master/src/pow.cpp
    #
    #network_diff = ...
    #log.write('network_diff', network_diff)

# Absolute and relative tx fees, total block fee, and block subsidy
def fees_and_subsidy(block, window, height):

    # absolute fees (sat)
    txs = block.transactions
    fees = [tx.fee for tx in txs]
    window.insert('absolute_fee_per_tx_incl_coinbase', fees)
    if len(txs) > 1:
        window.insert('absolute_fee_per_tx_excl_coinbase', fees[1:])

    # relative fees [sat/B]
    sizes = [tx.size for tx in txs]
    fees_per_byte = [tx.fee/tx.size for tx in txs]
    window.insert('relative_fee_per_tx_incl_coinbase', fees_per_byte)
    # mean fee requires special calculation. if we just use the mean of the
    # individual values, the information about how much each individual
    # relative fee weighs (in terms of to how many bytes it applies) is lost.
    # the correct way to calculate the average relative fee is as follows
    window.insert('relative_fee_per_tx_mean_incl_coinbase', sum(fees)/sum(sizes))
    if len(txs) > 1:
        window.insert('relative_fee_per_tx_excl_coinbase', fees_per_byte[1:])
        window.insert('relative_fee_per_tx_mean_excl_coinbase', sum(fees[1:])/sum(sizes[1:]))

    # relative fees [sat/WU]
    weights = [tx.weight for tx in txs]
    fees_per_WU = [tx.fee/tx.weight for tx in txs]
    window.insert('relative_fee_per_WU_per_tx_incl_coinbase', fees_per_WU)
    # mean fee requires special calculation. if we just use the mean of the
    # individual values, the information about how much each individual
    # relative fee weighs (in terms of to how many bytes it applies) is lost.
    # the correct way to calculate the average relative fee is as follows
    window.insert('relative_fee_per_WU_per_tx_mean_incl_coinbase', sum(fees)/sum(weights))
    if len(txs) > 1:
        window.insert('relative_fee_per_WU_per_tx_excl_coinbase', fees_per_WU[1:])
        window.insert('relative_fee_per_WU_per_tx_mean_excl_coinbase', sum(fees[1:])/sum(weights[1:]))

    # total block fees [sat]
    window.insert('total_block_fees_incl_coinbase', sum(fees))
    if len(txs) > 1:
        window.insert('total_block_fees_excl_coinbase', sum(fees[1:]))

    # reward and subsisdy (NB reward = subsidy + fees)
    reward = sum([output.amount for output in txs[0].outputs])
    window.insert('block_reward', reward)
    subsidy = reward - sum(fees)
    window.insert('block_subsidy', subsidy)
    if (subsidy != max_block_subsidy(height)):
        log.write('lost_subsidy', {'mean_height': height, 'subsidy': subsidy, 'max_subsidy': max_block_subsidy(height)})
