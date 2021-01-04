import inspect
import functools
from typing import Iterable, Callable
from multiprocessing import Pool
import abc
from tqdm import tqdm
from gitential2.settings import GitentialSettings, Executor as ExecutorSettings
from gitential2.extraction.output import DataCollector


class Executor(abc.ABC):
    def __init__(self, **kwargs):
        self._show_progress = kwargs.pop("show_progress", True)
        self._description = kwargs.pop("description", None)
        self._context = kwargs

    def map(self, fn: Callable, items: Iterable):
        fn_partial = construct_partial(fn, self._context)
        progress_bar = self._get_progress_bar()
        ret = self._process(fn_partial, items, progress_bar)
        progress_bar.close()
        return ret

    @abc.abstractmethod
    def _process(self, fn_partial: Callable, items: Iterable, progress_bar):
        pass

    def _get_progress_bar(self):
        kwargs = {"ascii": True}
        if self._description is not None:
            kwargs["desc"] = self._description
        if not self._show_progress:
            kwargs["disable"] = True
        return tqdm(**kwargs)


def construct_partial(fn: Callable, context: dict):
    fn_args = list(inspect.signature(fn).parameters.keys())
    params = {k: v for k, v in context.items() if k in fn_args}
    return functools.partial(fn, **params)


class SingleThreadExecutor(Executor):
    def _process(self, fn_partial: Callable, items: Iterable, progress_bar):
        for item in items:
            progress_bar.update(1)
            fn_partial(item)


class ProcessPoolExecutor(Executor):
    def __init__(self, **kwargs):
        self.pool_size = kwargs.pop("pool_size", 8)
        self.original_output = kwargs.pop("output", DataCollector())
        kwargs["output"] = DataCollector()
        super().__init__(**kwargs)

    def _process(self, fn_partial: Callable, items: Iterable, progress_bar):
        pool = Pool(self.pool_size)
        for output in pool.imap_unordered(fn_partial, items):
            for kind, value in output:
                self.original_output.write(kind, value)
            progress_bar.update(1)
        pool.close()
        pool.join()


def create_executor(settings: GitentialSettings, **kwargs) -> Executor:
    kwargs.setdefault("show_progress", settings.show_progress)
    if settings.executor == ExecutorSettings.process_pool:
        kwargs.setdefault("pool_size", settings.process_pool_size)
        return ProcessPoolExecutor(**kwargs)
    elif settings.executor == ExecutorSettings.single_tread:
        return SingleThreadExecutor(**kwargs)
    else:
        raise ValueError("Invalid executor settings")