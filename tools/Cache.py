import pickle

DEBUG = 2

def debug(string, level, newline=False):
    if DEBUG >= level:
        if newline:
            print('')
        print('[DEBUG {}]: '.format(level) + string)

class Cache:

    def __init__(self, metric, window_size, path='reference_cache'):
        self.path = path
        self.metric = metric
        self.window_size = window_size
        self.filename = f'{path}/{metric}-{window_size}.pkl'
        self.data = []

    def get(self):
        debug('trying to read data from cache (metric: {} window_size {})'.format(self.metric, self.window_size), 2)
        try:
            with open(self.filename, 'rb', 0) as fp:
                self.data = pickle.load(fp)
            debug('success: {} elms in cache (metric: {} window_size {})'.format(len(self.data), self.metric, self.window_size), 2)
        except FileNotFoundError:
            debug('no cached data (metric: {} window_size {})'.format(self.metric, self.window_size), 2)
        return self.data

    def append(self, item):
        # append data
        self.data.append(item)
        # overwrite outdated copy
        with open(self.filename, 'wb', 0) as fp:
            pickle.dump(self.data, fp, protocol=pickle.HIGHEST_PROTOCOL)
        debug('stored new elm in cache, total elms: {} (metric: {} window_size {})'.format(len(self.data), self.metric, self.window_size), 2)

