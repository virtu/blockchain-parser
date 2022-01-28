import numpy as np
from .globals import log
import time

def key_values(data):
    minimum = np.amin(data)
    maximum = np.amax(data)
    mean = np.mean(data)
    q1, q5, q10, q25, median, q75, q90, q95, q99 = np.quantile(data, [0.01, 0.05, 0.10, 0.25, 0.5, 0.75, 0.90, 0.95, 0.99])
    # coefficient of variation: stddev/mean
    CV = np.std(data, dtype=np.float64) / mean if mean != 0 else np.nan
    return minimum, maximum, mean, median, q1, q5, q10, q25, q75, q90, q95, q99, CV

class Window:
    def __init__(self, sizes):
        self.metrics = {}               # dict holing multiple windows of values for each metric 
        self.window_sizes = sizes       # window sizes

    def insert(self, metric, v):
        # add metric if we haven't seen it before
        if metric not in self.metrics:
            print('adding new metric: {}'.format(metric))
            self.metrics[metric] = {}
            for window_size in self.window_sizes:
                self.metrics[metric][window_size] = []

        # make sure we always add the same type to a list
        if self.metrics[metric][self.window_sizes[0]]:
            if type(self.metrics[metric][self.window_sizes[0]][0]) == list and type(v) != list:
                raise ValueError('Trying to add a {} to a list containing {}'.format(type(v), type(self.metrics[metric][0][0])))

        # insert data point(s) for metric in all windows
        for window_size in self.window_sizes:
            self.metrics[metric][window_size].append(v)

    def process(self, height, window_size):
        for metric in self.metrics:

            # if window is empty, clear and skip
            if len(self.metrics[metric][window_size]) == 0:
                self.metrics[metric][window_size].clear()
                continue

            # if entries are missing from window with scalar values, add zeros
            if len(self.metrics[metric][window_size]) != window_size and (type(self.metrics[metric][window_size][0]) is not list):
                num_missing = window_size - len(self.metrics[metric][window_size])
                self.metrics[metric][window_size].extend([0] * num_missing)

            # flatten list if it is made up of sublists
            if type(self.metrics[metric][window_size][0]) is list:
                self.metrics[metric][window_size] = [item for sublist in self.metrics[metric][window_size] for item in sublist]

            # all data points in list are valid: create statistics
            minimum, maximum, mean, median, q1, q5, q10, q25, q75, q90, q95, q99, CV = key_values(self.metrics[metric][window_size])

            # determine mean height and time
            mean_height = height - (window_size-1)/2
            log.write_stats(f'{metric}-{window_size}', mean_height, minimum, maximum, mean, median, q1, q5, q10, q25, q75, q90, q95, q99, CV)

            # clear window
            self.metrics[metric][window_size].clear()
