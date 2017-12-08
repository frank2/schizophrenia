#!/usr/bin/env python

import threading
import types
import time

from schizophrenia.task import Task

__all__ = ['PID', 'Manager']

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
        return not self.task is None and self.task.is_alive()

    def join(self, timeout=None):
        if self.task is None:
            raise RuntimeError('no task to join')

        self.task.join(timeout)

    @classmethod
    def join_all(self, *pids):
        for pid in pids:
            if pid is None:
                continue

            if not isinstance(pid, PID):
                raise ValueError('pid must be a PID object')

        for pid in pids:
            print('Joining PID {}'.format(pid))
            pid.join()

    def kill(self):
        if self.task is None:
            raise RuntimeError('no task to kill')

        self.task.kill()

    def get(self):
        if self.task is None:
            raise RuntimeError('no task to retrieve return value')

        return self.task.get()

    def __hash__(self):
        return hash(self.pid)

    def __int__(self):
        return self.pid

    def __eq__(self, other):
        return int(other) == int(self)

    def __repr__(self):
        if self.task is None:
            return '<PID:{}>'.format(self.pid)
        else:
            return '<PID:{} {}>'.format(self.pid, self.task.thread_name)

class Manager(object):
    MAX_PID = 2 ** 32
    
    def __init__(self):
        self.modules = dict()
        self.tasks = dict()
        self.pids = dict()

        self.pid = 0
        self.pid_lock = threading.Lock()
        self.pid_event = threading.Event()

        self.load_module('schizophrenia')
        # self.launch_task('schizophrenia.manager.TaskMonitor')

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
        print('getting pid lock')
        import traceback
        traceback.print_stack()
        with self.pid_lock:
            self.pid_event.clear()

            if task_obj in self.tasks:
                raise RuntimeError('task already registered with manager')

            while self.pid in self.pids:
                self.pid += 1

                if self.pid >= self.MAX_PID:
                    self.pid = 0

            pid_obj = PID(self, self.pid)
            
            self.pids[pid_obj] = task_obj
            self.tasks[task_obj] = pid_obj

            self.pid_event.set()

        return pid_obj

    def launch_task(self, task_obj, *args, **kwargs):
        pid_obj = self.register_task(task_obj)
        task_obj.run(*args, **kwargs)
        return pid_obj

    def wait_for_pids(self):
        if not self.pid_lock.acquire(False):
            self.pid_event.wait()

    def has_pid(self, pid):
        self.wait_for_pids()
        return pid in self.pids

    def has_task(self, task):
        self.wait_for_pids()
        return task in self.tasks

    def get_pid(self, task):
        if not self.has_task(task):
            return

        self.wait_for_pids()
        return self.tasks[task]

    def get_task(self, pid):
        if not self.has_pid(pid):
            return

        self.wait_for_pids()        
        return self.pids[pid]
