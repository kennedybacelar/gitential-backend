import abc
from collections import defaultdict


class OutputHandler(abc.ABC):
    @abc.abstractmethod
    def write(self, kind, value):
        pass


class DataCollector(OutputHandler):
    def __init__(self):
        self.values = defaultdict(list)

    def write(self, kind, value):
        self.values[kind].append(value)
