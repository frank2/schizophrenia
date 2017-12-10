#!/usr/bin/env python

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

__all__ = ['ClosedPipeError', 'PID', 'Pipe', 'PipeEnd', 'Manager', 'find_manager', 'set_manager']

class DebuggableRLock(threading._RLock):
    def acquire(self, blocking=True, timeout=-1):
        print('thread {} wants lock {} owned by {}'.format(threading.get_ident(), id(self), self._owner))
        result = super(DebuggableRLock, self).acquire(blocking, timeout)
        print('thread {} got lock result {}'.format(threading.get_ident(), result))

    def release(self, *args):
        if self._owner == threading.get_ident():
            print('thread {} released lock {}'.format(threading.get_ident(), id(self)))
            
        super(DebuggableRLock, self).release(*args)

    __enter__ = acquire
    
class ClosedPipeError(Exception):
    pass

class PID(object):
    def __init__(self, manager_obj, pid):
        self.manager = manager_obj

        if not self.manager is None and not isinstance(self.manager, Manager):
            raise ValueError('manager must be a Manager object')

        self.pid = pid

    @property
    def task(self):
        if not self.manager.has_pid(self):
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
    def join_all(klass, *pids):
        for pid in pids:
            if pid is None:
                continue

            if not isinstance(pid, PID):
                raise ValueError('pid must be a PID object')

        for pid in pids:
            if pid is None:
                continue
            
            if not pid.is_alive():
                continue
            
            pid.join()

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
    def get_all(klass, *pids, blocking=True, timeout=None):
        tasks = list()
        
        for pid in pids:
            if pid is None:
                continue

            if not isinstance(pid, PID):
                raise ValueError('pid must be a PID object')

            tasks.append(pid.task)

        return Task.get_all(*tasks, blocking=blocking, timeout=timeout)

    def __hash__(self):
        return hash(self.pid)

    def __int__(self):
        return self.pid

    def __eq__(self, other):
        return int(other) == int(self)

    def __repr__(self):
        task = self.task

        if task is None:
            return '<PID:{}>'.format(self.pid)
        else:
            return '<PID:{} {}>'.format(self.pid, task.thread_name)
        
class Pipe(object):
    def __init__(self, pid_one, pid_two):
        self.open(pid_one, pid_two)

    def open(self, pid_one, pid_two):
        if not isinstance(pid_one, PID) or not isinstance(pid_two, PID):
            raise ValueError('pid must be a PID object')

        self.one = PipeEnd()
        self.two = PipeEnd(send=self.one.recv, recv=self.one.send)
        self.key = frozenset([pid_one, pid_two])
        self.mapping = {pid_one: self.one, pid_two: self.two}

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
        
        self.send.put(item, timeout=timeout)

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
    MAX_PID = 2 ** 16
    
    def __init__(self):
        global PID_LOCK

        self.modules = dict()
        self.tasks = dict()
        self.pids = dict()

        self.pipes = dict()
        self.pipe_ends = dict()
        self.pipe_lock = DebuggableRLock()

        self.pid = 0
        self.pid_lock = DebuggableRLock()

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
        del self.modules[loaded]

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

    def create_task(self, task_name):
        task_class = self.load_task(task_name)
        obj = task_class(self)

        return obj

    def register_task(self, task_obj):
        with self.pid_lock:
            #print('pid_lock owner: {}'.format(self.pid_lock._owner))

            if task_obj in self.tasks:
                raise RuntimeError('task already registered with manager')

            while self.pid in self.pids:
                self.pid += 1

                if self.pid >= self.MAX_PID:
                    self.pid = 0

            pid_obj = PID(self, self.pid)
            
            self.pids[pid_obj] = task_obj
            self.tasks[task_obj] = pid_obj

        return pid_obj

    def unregister_task(self, task_obj):
        with self.pid_lock:
            #print('pid_lock owner: {}'.format(self.pid_lock._owner))

            if not task_obj in self.tasks:
                raise RuntimeError('task not registered with manager')

            pid_obj = self.tasks[task_obj]

            del self.pids[pid_obj]
            del self.tasks[task_obj]

        with self.pipe_lock:
            #print('pipe_lock owner: {}'.format(self.pipe_lock._owner))

            if pid_obj in self.pipe_ends:
                ends = self.pipe_ends[pid_obj]

                for end in list(ends)[:]:
                    self.close_pipe(pid_obj, end)

    def launch_task(self, task_obj, *args, **kwargs):
        pid_obj = self.register_task(task_obj)
        task_obj.run(*args, **kwargs)
        return pid_obj

    def spawn_task(self, task_name, *args, **kwargs):
        return self.launch_task(self.create_task(task_name), *args, **kwargs)

    def spawn_task_after(self, timeout, taskname, *args, **kwargs):
        task_obj = self.create_task(task_name)
        task_obj.run_after(timeout, *args, **kwargs)

    def has_pid(self, pid):
        with self.pid_lock:
            #print('pid_lock owner: {}'.format(self.pid_lock._owner))
        
            result = pid in self.pids

        return result

    def has_task(self, task):
        with self.pid_lock:
            #print('pid_lock owner: {}'.format(self.pid_lock._owner))

            result = task in self.tasks

        return result

    def get_pid(self, task):
        result = None

        with self.pid_lock:
            #print('pid_lock owner: {}'.format(self.pid_lock._owner))

            if self.has_task(task):
                if task in self.tasks:
                    result = self.tasks[task]

        return result

    def get_task(self, pid):
        result = None

        with self.pid_lock:
            #print('pid_lock owner: {}'.format(self.pid_lock._owner))

            if self.has_pid(pid):
                if pid in self.pids:
                    result = self.pids[pid]

        return result

    def get_pids(self):
        with self.pid_lock:
            #print('pid_lock owner: {}'.format(self.pid_lock._owner))

            result = list(self.pids.keys())

        return result

    def get_tasks(self):
        with self.pid_lock:
            #print('pid_lock owner: {}'.format(self.pid_lock._owner))

            result = list(self.tasks.keys())

        return result

    def create_pipe(self, first, second):
        if not isinstance(first, PID):
            raise ValueError('expected a pid for the first argument')
        
        if not isinstance(second, PID):
            raise ValueError('expected a pid for the second argument')

        if not self.has_pid(first):
            raise RuntimeError('no such pid: {}'.format(repr(first)))

        if not self.has_pid(second):
            raise RuntimeError('no such pid: {}'.format(repr(first)))

        if self.has_pipe_connection(first, second):
            return self.get_pipe(first, second)
            
        pipe = Pipe(first, second)
        ends = list(pipe.key)

        with self.pipe_lock:
            self.pipes[pipe.key] = pipe

            self.pipe_ends.setdefault(ends[0], set()).add(ends[1])
            self.pipe_ends.setdefault(ends[1], set()).add(ends[0])

        if ends[0].task:
            ends[0].task.on_new_pipe(ends[1], pipe)
            
        if ends[1].task:
            ends[1].task.on_new_pipe(ends[0], pipe)

        return pipe

    def has_pipe(self, pid):
        if not isinstance(pid, PID):
            raise ValueError('pid must be a PID object')

        with self.pipe_lock:
            result = pid in self.pipe_ends

        return result

    def has_pipe_connection(self, pid_one, pid_two):
        if not isinstance(pid_one, PID):
            raise ValueError('pid must be a PID object')

        if not isinstance(pid_two, PID):
            raise ValueError('pid must be a PID object')

        with self.pipe_lock:
            result = frozenset([pid_one, pid_two]) in self.pipes

        return result

    def get_pipe(self, pid_one, pid_two):
        if not isinstance(pid_one, PID):
            raise ValueError('pid must be a PID object')

        if not isinstance(pid_two, PID):
            raise ValueError('pid must be a PID object')

        key = frozenset([pid_one, pid_two])

        with self.pipe_lock:
            if not key in self.pipes:
                raise ValueError('no such pipe with key {}'.format(key))

            result = self.pipes[key]

        return result

    def get_ends(self, pid):
        with self.pipe_lock:
            if not self.has_pipe(pid):
                raise ValueError('no pipe end for PID {}'.format(repr(pid)))

            result = self.pipe_ends[pid]

        return result

    def get_end(self, pid_from, pid_to):
        if not isinstance(pid_from, PID):
            raise ValueError('pid must be a PID object')

        if not isinstance(pid_to, PID):
            raise ValueError('pid must be a PID object')

        pipe = self.get_pipe(pid_from, pid_to)

        return pipe.mapping[pid_to]

    def close_pipe(self, pid_from, pid_to):
        pipe_key = frozenset([pid_from, pid_to])

        with self.pipe_lock:
            if not pipe_key in self.pipes:
                raise ValueError('no such pipe with key {}'.format(pipe_key))

            pipe = self.pipes[pipe_key]
            pipe.close()

            pid_one, pid_two = list(pipe_key)
            pid_one_task = pid_one.task
            pid_two_task = pid_two.task

            if not pid_one_task is None:
                pid_one_task.on_close_pipe(pid_two, pipe)

            if not pid_two_task is None:
                pid_two_task.on_close_pipe(pid_one, pipe)

            self.pipe_ends[pid_one].remove(pid_two)

            if len(self.pipe_ends[pid_one]) == 0:
                del self.pipe_ends[pid_one]

            self.pipe_ends[pid_two].remove(pid_one)

            if len(self.pipe_ends[pid_two]) == 0:
                del self.pipe_ends[pid_two]

MANAGER = Manager()
