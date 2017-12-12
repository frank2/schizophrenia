#!/usr/bin/env python

import functools
import queue
import sys
import threading
import types
import time

MANAGER = None

def find_manager():
    global MANAGER

    return MANAGER

def set_manager(manager):
    if not isinstance(manager, Manager):
        raise ValueError('manager must be a Manager object')

    global MANAGER

    MANAGER = manager

from schizophrenia.result import Result, WouldBlockError
from schizophrenia.task import Task

__all__ = ['ClosedPipeError', 'TID', 'Pipe', 'PipeEnd', 'Manager', 'find_manager', 'set_manager']
    
class ClosedPipeError(Exception):
    pass

class TID(object):
    def __init__(self, manager_obj, tid):
        self.manager = manager_obj

        if not self.manager is None and not isinstance(self.manager, Manager):
            raise ValueError('manager must be a Manager object')

        self.tid = tid

    @property
    def task(self):
        if not self.manager.has_tid(self):
            return
        
        return self.manager.get_task(self)

    def is_alive(self):
        task = self.task
        return not task is None and task.is_alive()

    def join(self, timeout=None):
        if self.task is None:
            raise RuntimeError('no task to join')

        self.task.join(timeout)

    @classmethod
    def join_all(klass, *tids):
        for tid in tids:
            if tid is None:
                continue

            if not isinstance(tid, TID):
                raise ValueError('tid must be a TID object')

        for tid in tids:
            if tid is None:
                continue
            
            if not tid.is_alive():
                continue
            
            tid.join()

    def kill(self, exception=None):
        if self.task is None:
            raise RuntimeError('no task to kill')

        self.task.kill(exception)

    def die(self, exception=None):
        task = self.task

        if task is None:
            raise RuntimeError('no task to kill')

        task.die(exception)

    def get(self, blocking=True, timeout=None):
        task = self.task

        if task is None:
            raise RuntimeError('no task to retrieve return value')

        return task.get(blocking=blocking, timeout=timeout)

    @classmethod
    def get_all(klass, *tids, blocking=True, timeout=None):
        tasks = list()
        
        for tid in tids:
            if tid is None:
                continue

            if not isinstance(tid, TID):
                raise ValueError('tid must be a TID object')

            tasks.append(tid.task)

        return Task.get_all(*tasks, blocking=blocking, timeout=timeout)

    def __hash__(self):
        return hash(self.tid)

    def __int__(self):
        return self.tid

    def __eq__(self, other):
        return int(other) == int(self)

    def __repr__(self):
        task = self.task

        if task is None:
            return '<TID:{}>'.format(self.tid)
        else:
            return '<TID:{} {}>'.format(self.tid, task.thread_name)
        
class Pipe(object):
    def __init__(self, tid_one, tid_two):
        self.open(tid_one, tid_two)

    def open(self, tid_one, tid_two):
        if not isinstance(tid_one, TID) or not isinstance(tid_two, TID):
            raise ValueError('tid must be a TID object')

        if tid_one == tid_two:
            raise ValueError('tid cannot connect to itself')

        self.one = PipeEnd()
        self.two = PipeEnd(send=self.one.recv, recv=self.one.send)
        self.key = frozenset([tid_one, tid_two])
        self.mapping = {tid_one: self.one, tid_two: self.two}

    def close(self):
        self.one.send = None
        self.two.send = None
        self.key = None

class PipeEnd(object):
    def __init__(self, send=None, recv=None):
        if send is None and recv is None:
            self.send = queue.Queue()
            self.recv = queue.Queue()
        elif not send is None and not recv is None:
            self.send = send
            self.recv = recv
        else:
            raise ValueError('both send and recv must be None or not None simultaneously')

    def put(self, item, timeout=None):
        if self.send is None:
            raise ClosedPipeError('the pipe is closed')
        
        self.send.put(item, block=False, timeout=timeout)

    def get(self, block=True, timeout=None):
        if self.send is None:
            if self.empty():
                raise ClosedPipeError('pipe is closed with empty receive buffer')

            return self.recv.get()
        
        return self.recv.get(block, timeout)

    def closed(self):
        return self.send is None

    def empty(self):
        return self.recv.empty()

class Manager(object):
    MAX_TID = 2 ** 16
    
    def __init__(self):
        self.modules = dict()
        self.tasks = dict()
        self.tids = dict()

        self.pipes = dict()
        self.pipe_ends = dict()
        self.tid_lock = threading.RLock()

        self.tid = 0

        self.load_module('schizophrenia')

    def load_module(self, module, load_as=None):
        if '.' in module and load_as is None:
            raise ValueError('submodules should not be loaded without an alias')

        if load_as is None:
            load_as = module

        self.modules[load_as] = __import__(module)

    def reload_module(self, loaded, first_round=True):
        attribute_queue = [self.modules[loaded]]
        reloaded_set = set()

        while len(attribute_queue) > 0:
            dequeued = attribute_queue.pop(0)
            reload(dequeued)
            reloaded_set.add(dequeued)
            
            attrs = dir(dequeued)

            for attribute_name in attrs:
                attribute = getattr(dequeued, attribute_name)
                
                if type(attribute) is ModuleType and not attribute in reloaded_set:
                    attribute_queue.append(attribute)

        # get those pesky circular links too
        if first_round:
            self.reload_module(loaded, False)

    def unload_module(self, loaded):
        self.modules.pop(loaded, None)

    def load_task(self, task_string):
        modules = task_string.split('.')
        task = modules.pop()
        module_root = modules[0]
        module = self.modules.get(module_root)

        if module is None:
            raise ValueError('no such module {}'.format(module_root))

        modules.pop(0)

        while len(modules) > 0:
            next_module = modules.pop(0)
            submodule = getattr(module, next_module, None)

            if submodule is None:
                raise AttributeError('no such submodule {}'.format(next_module))

            module = submodule

        task_class = getattr(module, task, None)

        if task_class is None:
            raise AttributeError('no such object {} in {}'.format(task, module))

        if not issubclass(task_class, Task):
            raise ValueError('task must be a Task class')

        return task_class

    def register_task(self, task_obj):
        if task_obj in self.tasks:
            raise RuntimeError('task already registered with manager')

        with self.tid_lock:
            while self.tid in self.tids:
                self.tid += 1

                if self.tid >= self.MAX_TID:
                    self.tid = 0

            tid_obj = TID(self, self.tid)
            
            self.tids[tid_obj] = task_obj
            self.tasks[task_obj] = tid_obj

        return tid_obj

    def unregister_task(self, task_obj):
        if not task_obj in self.tasks:
            raise RuntimeError('task not registered with manager')

        tid_obj = self.tasks.get(task_obj, None)

        if not tid_obj is None: # race condition, feel free to just let it go.
            self.tids.pop(tid_obj, None)
            self.tasks.pop(task_obj, None)

        if tid_obj in self.pipe_ends:
            ends = self.pipe_ends.pop(tid_obj, None)

            if ends is None:
                return

            for end in list(ends)[:]:
                self.close_pipe(tid_obj, end)

    def kill_task(self, tid, exception=None):
        task = self.tids.get(tid)
        task.on_kill(exception)

    def create_task(self, task_name, *args, **kwargs):
        task_class = self.load_task(task_name)
        obj = functools.partial(task_class, manager=self)(*args, **kwargs)

        return obj

    def launch_task(self, task_obj, *args, **kwargs):
        tid_obj = self.register_task(task_obj)
        task_obj.run(*args, **kwargs)
        return task_obj

    def spawn_task(self, task_name, *args, **kwargs):
        return self.launch_task(self.create_task(task_name), *args, **kwargs)

    def spawn_task_after(self, timeout, taskname, *args, **kwargs):
        task_obj = self.create_task(task_name)
        task_obj.run_after(timeout, *args, **kwargs)

    def has_tid(self, tid):
        return not self.tids.get(tid, None) is None

    def has_task(self, task):
        return not self.tasks.get(task, None) is None

    def get_tid(self, task):
        return self.tasks.get(task, None)

    def get_task(self, tid):
        return self.tids.get(tid, None)

    def get_tids(self):
        return list(self.tids.keys())

    def get_tasks(self):
        return list(self.tasks.keys())

    def create_pipe(self, first, second):
        if not isinstance(first, TID):
            raise ValueError('expected a tid for the first argument')
        
        if not isinstance(second, TID):
            raise ValueError('expected a tid for the second argument')

        if not self.has_tid(first):
            raise RuntimeError('no such tid: {}'.format(repr(first)))

        if not self.has_tid(second):
            raise RuntimeError('no such tid: {}'.format(repr(first)))

        if self.has_pipe_connection(first, second):
            return self.get_pipe(first, second)

        #if first == second:
        #    print(first, second)

        pipe = Pipe(first, second)
        ends = list(pipe.key)
        #print(pipe.key)
        self.pipes[pipe.key] = pipe

        self.pipe_ends.setdefault(ends[0], set()).add(ends[1])
        self.pipe_ends.setdefault(ends[1], set()).add(ends[0])

        if ends[0].task:
            ends[0].task.on_new_pipe(ends[1], pipe)
            
        if ends[1].task:
            ends[1].task.on_new_pipe(ends[0], pipe)

        return pipe

    def has_pipe(self, tid):
        if not isinstance(tid, TID):
            raise ValueError('tid must be a TID object')

        return not self.pipe_ends.get(tid, None) is None

    def has_pipe_connection(self, tid_one, tid_two):
        if not isinstance(tid_one, TID):
            raise ValueError('tid must be a TID object')

        if not isinstance(tid_two, TID):
            raise ValueError('tid must be a TID object')

        return not self.pipes.get(frozenset([tid_one, tid_two]), None) is None

    def get_pipe(self, tid_one, tid_two):
        if not isinstance(tid_one, TID):
            raise ValueError('tid must be a TID object')

        if not isinstance(tid_two, TID):
            raise ValueError('tid must be a TID object')

        key = frozenset([tid_one, tid_two])
        pipe = self.pipes.get(key, None)

        if pipe is None:
            raise ValueError('no such pipe with key {}'.format(key))

        return pipe

    def get_ends(self, tid):
        return self.pipe_ends.get(tid, None)

    def get_end(self, tid_from, tid_to):
        if not isinstance(tid_from, TID):
            raise ValueError('tid must be a TID object')

        if not isinstance(tid_to, TID):
            raise ValueError('tid must be a TID object')

        pipe = self.get_pipe(tid_from, tid_to)
        return pipe.mapping[tid_to]

    def close_pipe(self, tid_from, tid_to):
        pipe_key = frozenset([tid_from, tid_to])
        pipe = self.pipes.get(pipe_key, None)

        if pipe is None:
            raise ValueError('no such pipe with key {}'.format(pipe_key))

        pipe.close()
            
        tid_one, tid_two = list(pipe_key)
        tid_one_task = tid_one.task
        tid_two_task = tid_two.task
        
        if not tid_one_task is None:
            tid_one_task.on_close_pipe(tid_two, pipe)

        if not tid_two_task is None:
            tid_two_task.on_close_pipe(tid_one, pipe)

        end_set = self.pipe_ends.get(tid_one, None)

        if not end_set is None:
            end_set.remove(tid_two)

            if len(end_set) == 0:
                self.pipe_ends.pop(tid_one, None)

        end_set = self.pipe_ends.get(tid_two, None)

        if not end_set is None:
            end_set.remove(tid_one)

            if len(end_set) == 0:
                self.pipe_ends.pop(tid_two, None)

MANAGER = Manager()
