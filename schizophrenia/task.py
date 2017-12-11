#!/usr/bin/env python

import queue
import sys
import threading
import time

from schizophrenia.result import Result, WouldBlockError

__all__ = ['TaskExit', 'TaskResult', 'Task']

class TaskExit(Exception):
    pass

class TaskResult(Result):
    def __init__(self, task):
        self.task = task

        if not isinstance(self.task, Task):
            raise ValueError('task must be a Task object')
        
        super(TaskResult, self).__init__()

    def get(self, blocking=True, timeout=None):
        if self.task is None:
            raise RuntimeError('result not bound to a task')

        if not self.task.started.is_set() and blocking:
            raise RuntimeError('task never started, get() would hang forever')

        if self.task.started.is_set() and not blocking:
            if self.task.is_alive() or not self.is_set():
                raise WouldBlockError('the task has not finished yet')

            self.raise_exception()
            return self.value

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

        if not timeout is None and timeout < 0:
            timeout = None

        return super(TaskResult, self).get(blocking, timeout)

class Task(object):
    def __init__(self, manager_obj=None):
        from schizophrenia.manager import Manager, find_manager

        self.manager = manager_obj

        if not self.manager is None and not isinstance(self.manager, Manager):
            raise ValueError('manager must be a Manager object')

        if self.manager is None:
            self.manager = find_manager()

        if self.manager is None:
            raise ValueError('no manager could be found')
        
        self.thread = None
        self.result = TaskResult(self)
        self.started = threading.Event()
        self.exception = None
        self.death = threading.Event()
        self.pipes = dict()

    @property
    def tid(self):
        if self.ready() or not self.manager.has_task(self):
            return None

        return self.manager.get_tid(self)

    @property
    def thread_name(self):
        return '{}#{:08x}'.format(self.__class__.__name__, id(self))

    def on_new_pipe(self, tid, pipe):
        if self.tid is None: # task died before the pipe could be registered
            return

        pipe_end = pipe.mapping[self.tid]
        self.pipes[tid] = pipe_end

    def on_close_pipe(self, tid, pipe):
        if tid in self.pipes:
            del self.pipes[tid]

    def on_kill(self, exception=None):
        if exception:
            self.exception = exception

        self.death.set()

    def get_pipe(self, tid):
        #with self.pipe_lock:
        return self.pipes.get(tid, None)

    def get_pipes(self):
        #with self.pipe_lock:
        return dict(list(self.pipes.items())[:])

    def has_pipe(self, tid):
        #with self.pipe_lock:
        return not self.pipes.get(tid, None) is None

    def create_pipe(self, tid):
        if not self.is_alive():
            raise RuntimeError('cannot create a pipe on a dead task')

        pipe = self.manager.create_pipe(self.tid, tid)
        return pipe.mapping[self.tid]
        
    def close_pipe(self, tid):
        if self.has_pipe(tid):
            self.manager.close_pipe(self.tid, tid)

    def bind(self, manager_obj):
        if not isinstance(manager_obj, manager.Manager):
            raise ValueError('manager must be a Manager object')

        self.manager = manager_obj

    def link(self, result):
        if not isinstance(result, TaskResult):
            raise ValueError('result must be a TaskResult object')

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
        if not self.ready():
            raise RuntimeError("cannot prepare a task that's not ready")

        self.result.clear()
        self.thread = threading.Thread(target=self.task_runner
                                       ,args=args
                                       ,kwargs=kwargs
                                       ,name=self.thread_name)
        self.thread.daemon = True

    def start(self):
        if self.manager and not self.manager.has_task(self):
            self.manager.register_task(self)
            
        self.thread.start()
        return self.result

    def run(self, *args, **kwargs):
        self.prepare(*args, **kwargs)
        return self.start()

    def run_after(self, timeout, *args, **kwargs):
        timer = threading.Timer(timeout, self.run, args=args, kwargs=kwargs)
        timer.start()

    def join(self, timeout=None):
        if self.thread is None:
            raise RuntimeError('no thread to join')

        if not self.is_alive():
            raise RuntimeError('thread is not alive to join')

        self.thread.join(timeout)

    @classmethod
    def join_all(self, *tasks):
        for task in tasks:
            if task is None:
                continue

            if not isinstance(task, Task):
                raise ValueError('task must be a Task object')

        for task in tasks:
            task.join()

    def kill(self, exception=None):
        if not self.is_alive():
            raise RuntimeError('task is not running')

        self.manager.kill_task(self.tid, exception)

    def die(self, exception=None):
        self.kill(exception)
        self.join()

    def is_dead(self):
        return self.death.is_set()

    def dead(self):
        if not self.is_dead():
            return

        if not self.exception is None:
            raise self.exception
        else:
            raise TaskExit('task received a death event')

    def get(self, blocking=True, timeout=None):
        return self.result.get(blocking=blocking, timeout=timeout)

    @classmethod
    def get_all(self, *tasks, blocking=True, timeout=None):
        for task in tasks:
            if not isinstance(task, Task):
                raise ValueError('tasks must be Task objects')

        results = list()
        
        for task in tasks:
            results.append(task.get(blocking=blocking, timeout=timeout))

        return results

    def task_runner(self, *args, **kwargs):
        self.started.clear()
        
        try:
            self.task_prep(*args, **kwargs)
            self.started.set()
            self.result.set(self.task(*args, **kwargs))
        except TaskExit:
            self.result.set()
        except:
            self.result.set_exception()
            import traceback
            traceback.print_exc()

        if self.manager and self.manager.has_task(self):
            self.manager.unregister_task(self)

    def task_prep(self, *args, **kwargs):
        return

    def task(self, *args, **kwargs):
        raise NotImplementedError('task undefined')

    def wait_for_start(self, timeout=None):
        self.started.wait(timeout)

    def sleep(self, timeout):
        current_time = time.time()
        end_time = time.time() + timeout
        delta = end_time - current_time

        if delta <= 0.001:
            sleep_rate = 0.0001
        elif delta <= 0.01:
            sleep_rate = 0.001
        else:
            sleep_rate = 0.01

        while time.time() < end_time:
            self.dead()
            time.sleep(sleep_rate)

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def __repr__(self):
        if self.tid is None:
            return '<{}>'.format(self.thread_name)
        else:
            return '<{}: TID:{}>'.format(self.thread_name, repr(self.tid.tid))
