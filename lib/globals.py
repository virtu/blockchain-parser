from .UTXO import UTXO
from .Logger import Logger

utxo = UTXO()
log = Logger()

window_sizes = [
        1,          # per block
        6,          # hourly
        24*6,       # daily
        3*24*6,     # per three days
        # too big, don't use them anyway
        #7*24*6,     # weekly
        #14*24*6,    # bi-weekly
        #28*24*6
    ]   # per four weeks
