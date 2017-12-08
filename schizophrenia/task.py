#!/usr/bin/env python

import sys
import threading
import time

__all__ = ['Result', 'Task']

class TaskExit(Exception):
    pass

class Result(threading.Event):
    def __init__(self):
        self.value = None
        self.task = None
        super(Result,self).__init__()

    def set(self, value=None):
        self.value = value
        super(Result, self).set()

    def clear(self):
        self.value = None
        super(Result, self).clear()

    def get(self, timeout=None):
        if self.task is None:
            raise RuntimeError('result not bound to a task')

        start = time.time()

        while not self.task.ready():
            time.sleep(0.01)

            if timeout is None:
                continue

            if start + timeout < time.time():
                raise TimeoutError('result timed out')

        time_delta = time.time() - start

        if not timeout is None:
            timeout -= time_delta

        if self.wait(timeout):
            if not self.task.exception is None:
                exc, tb = self.task.exception[1], self.task.exception[2]
                raise exc.with_traceback(tb)
            
            return self.value

        raise TimeoutError('result timed out')

class Task(object):
    def __init__(self, manager=None):
        from schizophrenia import manager

        self.manager = kwargs.setdefault('manager', self.MANAGER)

        if not self.manager is None and not isinstance(self.manager, manager.Manager):
            raise ValueError('manager must be a Manager object')
        
        self.thread = None
        self.result = Result()
        self.result.task = self
        self.exception = None
        self.death = threading.Event()

    @property
    def pid(self):
        if self.ready() or self.manager is None or not self.manager.has_task(self):
            return None

        return self.manager.get_pid(self)

    def link(self, result):
        if not isinstance(result, Result):
            raise ValueError('result must be a Result object')

        if self.is_alive():
            raise RuntimeError('cannot link to running task')
        
        self.result = result
        self.result.task = self

    def ready(self):
        return self.thread is None or not self.thread.is_alive()

    def is_alive(self):
        return not self.thread is None and self.thread.is_alive()

    def successful(self):
        return not self.thread is None and not self.thread.is_alive() and self.exception is None

    def prepare(self, *args, **kwargs):
        self.result.clear()
        self.thread = threading.Thread(target=self.task_runner
                                       ,args=args
                                       ,kwargs=kwargs
                                       ,name='{}#{:08x}'.format(
                                           self.__class__.__name__
                                           ,id(self))
                                       ,daemon=True)

    def start(self):
        if self.manager and not self.manager.has_task(self):
            self.manager.register_task(self)
            
        self.thread.start()
        return self.result

    def run(self, *args, **kwargs):
        self.prepare(*args, **kwargs)
        return self.start()

    def join(self, timeout=None):
        if self.thread is None:
            raise RuntimeError('no thread to join')

        if not self.is_alive():
            raise RuntimeError('thread is not alive to join')

        self.thread.join(timeout)

    def kill(self):
        self.death.set()

    def get(self):
        return self.result.get()

    def task_runner(self, *args, **kwargs):
        try:
            self.result.set(self.task(*args, **kwargs))
        except TaskExit:
            self.result.set()
        except:
            self.exception = sys.exc_info()
            self.result.set()

    def task(self, *args, **kwargs):
        raise NotImplementedError('task undefined')

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)
