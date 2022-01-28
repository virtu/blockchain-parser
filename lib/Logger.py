from datetime import datetime
import pandas as pd
import os

def mem_usage():
    import psutil
    print('-'*80)
    print('psutil')
    print('-'*80)
    rss_GB = psutil.Process().memory_full_info().rss / (1024 ** 3)
    rss_perc = psutil.Process().memory_percent(memtype='rss')
    print(f'memory usage: {rss_GB:.1f}GB / {rss_perc:.1f}% of total')
    print(psutil.Process().memory_full_info())

    print('-'*80)
    print('guppy')
    print('-'*80)
    from guppy import hpy
    hp = hpy()
    print(hp.heap())


class Logger:
    def __init__(self):
        self.histograms = {}
        self.loggers = {}
        self.timestamp = datetime.now().strftime('%Y-%m-%d-%H.%M')

    def hist(self, base, entries):
        for key, value in entries.items():

            # add metric if not yet in dict
            metric = base + '.' + key
            if metric not in self.histograms:
                self.histograms[metric] = {}

            # add value if not yet in metric
            # otherwise increment incidence counter
            if value not in self.histograms[metric]:
                self.histograms[metric][value] = 1
            else:
                self.histograms[metric][value] += 1

    def write_stats(self, target, mean_height, minimum, maximum, mean, median, q1, q5, q10, q25, q75, q90, q95, q99, CV):
        # if target has no file object, create one
        if target not in self.loggers:
            self.open(target, header=['mean_height', 'min', 'max', 'mean', 'median', 'q1', 'q5', 'q10', 'q25', 'q75', 'q90', 'q95', 'q99', 'CV'])
        # write
        self.loggers[target].write(f'{mean_height},{minimum},{maximum},{mean},{median},{q1},{q5},{q10},{q25},{q75},{q90},{q95},{q99},{CV}\n')

    def write(self, target, data):
        # if target has no file object, create one
        if target not in self.loggers:
            self.open(target, header=list(data.keys()))

        # create line to write
        line = ','.join([str(item) for item in list(data.values())]) + '\n'
        self.loggers[target].write(line)

    def open(self, target, header):
        # sanity check
        if target in self.loggers:
            raise Exception('Warning: target {} already exists: {}'.format(target, self.loggers))

        # create directory if it doesn't exist
        directory = 'log/{}'.format(self.timestamp)
        if not os.path.exists(directory):
                os.makedirs(directory)

        # make sure file does not exist
        filename = '{}/{}.dat'.format(directory, target)
        if os.path.exists(filename):
            raise Exception('Warning: output file name {} already exists!'.format(filename))

        # open file, write header line, store pointer
        f = open(filename, 'w+')
        header_line = ','.join(header)
        f.write(f'{header_line}\n')
        self.loggers[target] = f

    # create a snapshot of histograms
    def write_histograms(self):
        for target in self.histograms:
            filename = f'log/{self.timestamp}/histogram_{target}.dat'
            f = open(filename, 'w+')
            f.write(f'# {target}\n')
            f.write('# value / occurence\n')
            for key, value in self.histograms[target].items():
                f.write(f'{key} {value}\n')
            f.close()

    def compress(self):
        # reread all previously written output into a pandas dataframe and store as bz2-compressed CSV
        for target in self.loggers:
            print(f'\ncompressing {target}...')
            # write file to disk and close
            f = self.loggers[target]
            f.flush()
            os.fsync(f)
            f.close()

            # read data
            filename = f'log/{self.timestamp}/{target}.dat'
            print(f'reading data in {filename} into pandas df...')
            df = pd.read_csv(filename, header=0, index_col=0)

            # write bz2-compressed CSV
            filename = f'log/{self.timestamp}/{target}.csv.bz2'
            print(f'writing data to {filename}...')
            df.to_csv(filename, index=True, header=True, compression='bz2')

            # remove uncompressed file
            filename = f'log/{self.timestamp}/{target}.dat'
            os.remove(filename)

