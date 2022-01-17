import abc
from collections import defaultdict


class OutputHandler(abc.ABC):
    @abc.abstractmethod
    def write(self, kind, value):
        pass

    def clear(self):
        pass


class DataCollector(OutputHandler):
    def __init__(self):
        self.values = defaultdict(list)

    def write(self, kind, value):
        self.values[kind].append(value)

    def __iter__(self):
        # pylint: disable=consider-using-dict-items
        for kind in self.values.keys():
            for value in self.values[kind]:
                yield kind, value

    def pop(self):
        for kind in list(self.values.keys()):
            try:
                yield kind, self.values[kind].pop()
            except IndexError:
                del self.values[kind]

    def clear(self):
        for kind in list(self.values.keys()):
            del self.values[kind]
        del self.values
        self.values = defaultdict(list)
