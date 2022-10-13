
from abc import abstractmethod, ABC, ABCMeta
from types import ModuleType
from typing import Optional, Any, TypeVar, Union
from typing_extensions import Self
from .core import Named
from .meta import Meta
from .task import Task
from importlib import import_module
T = TypeVar("T")


class TaskPath:
    def __init__(self, path: Union[str, list[str]]):
        if isinstance(path, str):
            self._path = path.split(".")
        else:
            self._path = path

    @property
    def is_leaf(self):
        return len(self._path) == 1

    @property
    def sub_path(self):
        return TaskPath(self._path[1:])

    @property
    def head(self):
        return self._path[0]

    @property
    def name(self):
        return self._path[-1]

    def __str__(self):
        return ".".join(self._path)


class ProxyTask(Task[T]):

    def __init__(self, proxy_name, task: Task):
        self._name = proxy_name
        self._task = task

    @property
    def dependencies(self):
        return self._task.dependencies

    @property
    def specification(self):
        return self._task.specification

    def check_by_meta(self, meta: Meta):
        self._task.check_by_meta(meta)

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        return self._task.transform(meta, **kwargs)


class IWorkspace(ABC, Named):

    @property
    @abstractmethod
    def tasks(self) -> dict[str, Task]:
        pass

    @property
    @abstractmethod
    def workspaces(self) -> set["IWorkspace"]:
        pass

    def find_task(cls, task_path: Union[str, TaskPath]) -> Optional[Task]:
        if not isinstance(task_path, TaskPath):
            task_path = TaskPath(task_path)
        if not task_path.is_leaf:
            for w in cls.workspaces:
                if w.name == task_path.head:
                    return w.find_task(task_path.sub_path)
            return None
        else:
            for task_name in cls.tasks:
                if task_name == task_path.name:
                    return cls.tasks[task_name]
            for w in cls.workspaces:
                if t := w.find_task(task_path) is not None:
                    return t
            return None

    def has_task(self, task_path: Union[str, TaskPath]) -> bool:
        return self.find_task(task_path) is not None

    def get_workspace(self, name) -> Optional["IWorkspace"]:
        for workspace in self.workspaces:
            if workspace.name == name:
                return workspace
        return None

    def structure(self) -> dict:
        return {
            "name": self.name,
            "tasks": list(self.tasks.keys()),
            "workspaces": [w.structure() for w in self.workspaces]
        }

    @staticmethod
    def find_default_workspace(task: Task) -> "IWorkspace":
        try:
            return task._stem_workspace
        except AttributeError:
            module = import_module(task.__module__)
            return IWorkspace.module_workspace(module)

    @staticmethod
    def module_workspace(module: ModuleType) -> "IWorkspace":
        try:
            return module.__stem_workspace

        except AttributeError:
            #create
            tasks = {}
            workspaces = set()
    
            for s in dir(module):
                t = getattr(module, s)
                if isinstance(t, Task):
                    tasks[s] = t
                if isinstance(t, IWorkspace):
                    workspaces.add(t)
    
            module.__stem_workspace = LocalWorkspace(
                module.__name__, tasks, workspaces
            )
    
            return module.__stem_workspace


class ILocalWorkspace(IWorkspace):

    @property
    def tasks(self) -> dict[str, Task]:
        return self._tasks

    @property
    def workspaces(self) -> set["IWorkspace"]:
        return self._workspaces


class LocalWorkspace(ILocalWorkspace):

    def __init__(self, name,  tasks=(), workspaces=()):
        self._name = name
        self._tasks = tasks
        self._workspaces = workspaces


class Workspace(ABCMeta, ILocalWorkspace):
    def __new__(mcls: type[Self], name: str, bases: tuple[type, ...],
                namespace: dict[str, Any], **kwargs: Any) -> Self:
        #p.2
        if ILocalWorkspace not in bases:
            bases += (ILocalWorkspace,)
        cls = super().__new__(ABCMeta, name, bases, namespace, **kwargs)
        #need have workspaces p.6
        try:
            workspaces = set(cls.workspaces)
        except AttributeError:
            workspaces = set()
        cls = super().__new__(mcls, name, bases, namespace, **kwargs)


        cls_dict = {s: d for s, d in cls.__dict__.items() if not s.startswith('__')}
#p.3-4
        tasks_to_replace = {
            s: ProxyTask(s, d)
            for s, d in cls_dict.items() 
            if not callable(d) and isinstance(d, Task)
        }

        for s, t in tasks_to_replace.items():
            setattr(cls, s, t)
            cls_dict[s] = t
        # p.5
        for s, d in cls_dict.items():
            if isinstance(d, Task):
                d._stem_workspace = cls 


        tasks_to_show = {
            s: d 
            for s, d in cls_dict.items()
            if isinstance(d, Task)
        }


        cls._tasks = tasks_to_show      
        cls._workspaces = workspaces    
        cls._name = name             
        def __new(userclass, *args, **kwargs):
            return userclass

        cls.__new__ = __new
#p.1
        return cls