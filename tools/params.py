window_sizes = [
        1,          # per block
        6,          # hourly
        24*6,       # daily
        3*24*6      # per three days
    ]

# find ../log/ | cut -d/ -f4 | grep -- -1.dat | sed s/-1.dat// | sed s/.*/\'\&\',/ | sort | uniq > bla ; vimdiff <this> bla
mandatory_metrics = [
    'absolute_fee_per_tx_excl_coinbase',
    'absolute_fee_per_tx_incl_coinbase',
    'amount_transferred_per_block',
    'amount_transferred_per_tx',
    'block_diff',
    'block_reward',
    'block_size',
    'block_subsidy',
    'block_timestamp',
    'block_transactions_weight',
    'block_weight',
    'created_UTXO_type_MULTISIG',
    'created_UTXO_type_NONSTANDARD',
    'created_UTXO_type_OP_RETURN',
    'created_UTXO_type_P2CPK',
    'created_UTXO_type_P2PKH',
    'created_UTXO_type_P2SH',
    'created_UTXO_type_P2UPK',
    'created_UTXO_type_P2WPKH',
    'created_UTXO_type_P2WSH',
    'created_UTXO_type_P2W_UNKNOWN',
    'fraction_of_segwit_tx_per_block',
    'input_size_per_tx',
    'inputs_per_tx',
    'number_of_segwit_tx_per_block',
    'number_of_tx_per_block',
    'output_size_per_tx',
    'outputs_per_tx',
    'relative_fee_per_tx_excl_coinbase',
    'relative_fee_per_tx_incl_coinbase',
    'relative_fee_per_tx_mean_excl_coinbase',
    'relative_fee_per_tx_mean_incl_coinbase',
    'relative_fee_per_WU_per_tx_excl_coinbase',
    'relative_fee_per_WU_per_tx_incl_coinbase',
    'relative_fee_per_WU_per_tx_mean_excl_coinbase',
    'relative_fee_per_WU_per_tx_mean_incl_coinbase',
    'segwit_ratio_in_segwit_tx',
    'segwit_ratio_mean_in_segwit_tx',
    'segwit_tx_witness_size',
    'spent_UTXO_type_COINBASE',
    'spent_UTXO_type_MULTISIG',
    'spent_UTXO_type_NONSTANDARD',
    'spent_UTXO_type_P2CPK',
    'spent_UTXO_type_P2PKH',
    'spent_UTXO_type_P2SH',
    'spent_UTXO_type_P2SH_MULTISIG',
    'spent_UTXO_type_P2SH_P2WPKH',
    'spent_UTXO_type_P2SH_P2WSH',
    'spent_UTXO_type_P2SH_P2WSH_MULTISIG',
    'spent_UTXO_type_P2UPK',
    'spent_UTXO_type_P2WPKH',
    'spent_UTXO_type_P2WSH',
    'spent_UTXO_type_P2WSH_MULTISIG',
    'stripped_block_size',
    'total_block_fees_excl_coinbase',
    'total_block_fees_incl_coinbase',
    'total_inputs_per_block',
    'total_outputs_per_block',
    'tx_size',
    'tx_weight',
    ]

