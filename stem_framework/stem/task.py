
from typing import TypeVar, Union, Tuple, Callable, Optional, Generic, Any, Iterator
from abc import ABC, abstractmethod
from .core import Named
from .meta import Specification, Meta
from functools import reduce

T = TypeVar("T")


class Task(ABC, Generic[T], Named):
    dependencies: Tuple[Union[str, "Task"], ...]
    specification: Optional[Specification] = None
    settings: Optional[Meta] = None

    def check_by_meta(self, meta: Meta):
        pass

    @abstractmethod
    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        pass


class FunctionTask(Task[T]):
    def __init__(self, name: str, func: Callable, dependencies: Tuple[Union[str, "Task"], ...],
                 specification: Optional[Specification] = None,
                 settings: Optional[Meta] = None):
        self._name = name
        self._func = func
        self.dependencies = dependencies
        self.specification = specification
        self.settings = settings

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        return self._func(meta, **kwargs)


class DataTask(Task[T]):
    dependencies = ()

    @abstractmethod
    def data(self, meta: Meta) -> T:
        pass

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        return self.data(meta)


class FunctionDataTask(DataTask[T]):
    def __init__(self, name: str, func: Callable,
                 specification: Optional[Specification] = None,
                 settings: Optional[Meta] = None):
        self._name = name
        self._func = func
        self.specification = specification
        self.settings = settings

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def data(self, meta: Meta) -> T:
        return self._func(meta)


def data(func: Callable[[Meta], T], specification: Optional[Specification] = None, **settings) -> FunctionDataTask[T]:
    def wrap(name):
        return FunctionDataTask(name, func, specification, **settings)

    if func is None:
        return wrap
    else:
        return FunctionDataTask(func.__name__, func, specification, **settings)



def task(func: Callable[[Meta, ...], T], specification: Optional[Specification] = None, **settings) -> FunctionTask[T]:
    def wrap(name, dependencies):
        return FunctionTask(name, func, dependencies, specification, **settings)
    args=[]
    for arg in func.__code__.co_varnames:
        if arg != 'meta':
            args.append(arg)
    args=tuple(args)
    if func is None:
        wrap
    else:
        return FunctionTask(func.__name__, func, args, specification, **settings)


class MapTask(Task[Iterator[T]]):
    def __init__(self, func: Callable, dependence : Union[str, "Task"]):
        self.func=func
        if isinstance(dependence, str):
            self.dependence = dependence
        else:
            self.dependence = dependence.name
        self.name = 'map_' + self.dependence
    def transform(self, meta: Meta, /, **kwargs: Any):
        return map(self.func, kwargs[self.dependence].items())


class FilterTask(Task[Iterator[T]]):
    def __init__(self, key: Callable, dependence: Union[str, "Task"]):
        self.key = key
        
        if isinstance(dependence, str):
            self.dependence = dependence
        else:
            self.dependence = dependence.name

        self._name = 'filter_' + self.dependence

    def transform(self, meta: Meta, /, **kwargs: Any):
        return filter(self.key, kwargs[self.dependence].items())


class ReduceTask(Task[Iterator[T]]):
    def __init__(self, func: Callable, dependence: Union[str, "Task"]):
        self.func = func
        
        if isinstance(dependence, str):
            self.dependence = dependence
        else:
            self.dependence = dependence.name

        self._name = 'reduce_' + self.dependence

    def transform(self, meta: Meta, /, **kwargs: Any):
        return reduce(self.func, kwargs[self.dependence].items())