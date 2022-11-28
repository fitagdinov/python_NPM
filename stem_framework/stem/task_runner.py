import os
from typing import Generic, TypeVar
from abc import ABC, abstractmethod
import threading
import time
from threading import Thread
import asyncio
import multyprocessing 
from multiprocessing import Process
asyncio.set_event_loop(asyncio.new_event_loop())
from .meta import Meta, get_meta_attr
from .task_tree import TaskNode

T = TypeVar("T")
class TaskRunner(ABC, Generic[T]):
    @abstractmethod
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass


class SimpleRunner(TaskRunner[T]):
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        assert not task_node.has_dependence_errors
        kwargs = {
            t.task.name: self.run(
                get_meta_attr(meta, t.task.name, {}),
                t
            )
            for t in task_node.dependencies
        }
        return task_node.task.transform(meta, **kwargs)


class ThreadingRunner(TaskRunner[T],Thread):
    MAX_WORKERS = 5
    def __init__(self,MAX_WORKERS=MAX_WORKERS):
        self.func_list=[]
        self.args_list=[]
        self.MAX_WORKERS=MAX_WORKERS
        self.threads=[]
    def add_func(self,func,args):
        if len(self.func_list)<self.MAX_WORKERS:
            self.func_list.append(func)
            self.args_list.append(args)
        else: 
            print("numbers funclions more limits.Can't write this function")
    def get_treads(self):
        return(self.threads)
    def thread_run(self):
        for i in range(len(self.func_list)):
            f=self.func_list[i]
            args=self.args_list[i]
            t = threading.Thread(target=f, args=args)
            self.threads.append(t)
            t.start()
        for t in self.threads:
            t.join()

    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        assert not task_node.has_dependence_errors
        
        
        
        


class AsyncRunner(TaskRunner[T]):
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass  # TODO(Assignment 9)


class ProcessingRunner(TaskRunner[T]):
    MAX_WORKERS = os.cpu_count()

    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass  # TODO(Assignment 9)
