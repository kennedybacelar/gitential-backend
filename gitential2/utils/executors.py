import inspect
import functools
from typing import Iterable, Callable
from multiprocessing import Pool
import abc
from tqdm import tqdm


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
            yield fn_partial(item)


class ProcessPoolExecutor(Executor):
    def __init__(self, **kwargs):
        self.pool_size = kwargs.pop("pool_size", 4)
        super().__init__(**kwargs)

    def _process(self, fn_partial: Callable, items: Iterable, progress_bar):
        pool = Pool(self.pool_size)
        for _ in pool.imap_unordered(fn_partial, items):
            progress_bar.update(1)
        pool.close()
        pool.join()