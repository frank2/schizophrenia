#!/usr/bin/env python

import functools
import inspect
import queue
import re
import shlex
import sys
import threading
import time

from schizophrenia.result import Result, WouldBlockError

__all__ = ['TaskExit', 'TaskPrototypeArgs', 'TaskPrototypeKwargs', 'TaskPrototype', 'TaskPrototypeEnforcer', 'enforce_prototype', 'TaskResult', 'Task']

class TaskExit(Exception):
    pass

class TaskPrototypeArgs(object):
    TYPE_CLASS = None

    def __init__(self, **kwargs):
        self.type_class = kwargs.setdefault('type_class', self.TYPE_CLASS)

        if not self.type_class is None and not callable(self.type_class):
            raise ValueError('type class must be callable')

    def __call__(self, arg):
        if self.type_class is None:
            return arg
        else:
            return self.type_class(arg)

class TaskPrototypeKwargs(object):
    TYPE_MAP = None

    def __init__(self, **kwargs):
        self.type_map = kwargs.setdefault('type_map', self.TYPE_MAP)

        if self.type_map is None:
            self.type_map = dict()

    def __call__(self, key, arg):
        if not key in self.type_map:
            return arg
        else:
            return self.type_map[key](arg)

class TaskPrototype(object):
    def __init__(self, *args):
        self.args = list()

        for arg in args:
            if not callable(arg):
                raise ValueError('prototype arg must be callable')

            self.args.append(arg)

    def parse_args(self, *args, **kwargs):
        args = list(args)
        prototype_args = list(self.args[:])
        args_result = list()
        kwargs_result = dict()

        state_pos = 0
        state_args = 1
        state_kwargs = 2

        parse_state = state_pos

        while len(prototype_args) > 0:
            if parse_state == state_pos:
                proto = prototype_args.pop(0)

                if isinstance(proto, TaskPrototypeArgs):
                    prototype_args.insert(0, proto)
                    parse_state = state_args
                    continue
                elif isinstance(proto, TaskPrototypeKwargs):
                    prototype_kwargs.insert(0, proto)
                    parse_state = state_kwargs
                    continue

                if len(args) == 0:
                    raise RuntimeError('ran out of positional arguments to parse')
                
                args_result.append(proto(args.pop(0)))
            elif parse_state == state_args:
                if len(args) == 0:
                    proto = prototype_args.pop(0)

                    if len(prototype_args) == 0:
                        break

                    if not isinstance(prototype_args[0], TaskPrototypeKwargs):
                        raise ValueError('the only prototype that can follow TaskPrototypeArgs is TaskPrototypeKwargs')

                    parse_state = state_kwargs
                    continue

                proto = prototype_args[0]
                args_result.append(proto(args.pop(0)))
            elif parse_state == state_kwargs:
                proto = prototype_args[0]

                if len(kwargs) == 0:
                    break

                k, v = kwargs.popitem()
                kwargs_result[k] = proto(k, v)

        if len(args) > 0:
            raise RuntimeError('not all arguments parsed')

        return tuple(args_result), kwargs_result

    def from_string(self, string):
        lexed = shlex.split(string)
        args = list()
        kwargs = dict()

        parsing = args

        for elem in lexed:
            if parsing == args:
                if re.match('^[a-zA-Z_][a-zA-Z0-9]+=(.*)$', elem):
                    parsing = kwargs
                else:
                    parsing.append(elem)
                    
            if parsing == kwargs:
                groups = re.match('^(?P<key>[a-zA-Z_][a-zA-Z0-9]+)=(?P<value>.*)$', elem)

                if groups is None:
                    raise ValueError('keyword argument {} is not a keyword argument'.format(elem))

                key = groups.group('key')
                value = groups.group('value')
                value = value.strip("'")
                value = value.strip('"')

                parsing[key] = value

        return self.parse_args(*args, **kwargs)

class TaskPrototypeEnforcer(object):
    def __init__(self, wrapping):
        self.wrapping = wrapping

    def __get__(self, obj, klass=None):
        self.scope = obj # make the object stay in scope without a global
    
        if klass is None:
            klass = type(fuckyou)
        
        @functools.wraps(self.wrapping)
        def wrapper(*args, **kwargs):
            proto = klass.PROTOTYPE
            obj = self.scope

            if obj is None and not klass is None:
                args = list(args)
                obj = args.pop(0)
                args = tuple(args)
                
            parsed_args, parsed_kwargs = proto.parse_args(*args, **kwargs)

            return self.wrapping(obj, *parsed_args, **parsed_kwargs)
                
        return wrapper

def enforce_prototype(wrapping):
    return TaskPrototypeEnforcer(wrapping)
                                  
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
    MANAGER = None
    PROTOTYPE = TaskPrototype(TaskPrototypeArgs(), TaskPrototypeKwargs())
    
    def __init__(self, **kwargs):
        from schizophrenia.manager import Manager, find_manager

        self.manager = kwargs.setdefault('manager', self.MANAGER)

        if not self.manager is None and not isinstance(self.manager, Manager):
            raise ValueError('manager must be a Manager object')

        if self.manager is None:
            self.manager = find_manager()

        if self.manager is None:
            raise ValueError('no manager could be found')

        self.prototype = kwargs.setdefault('prototype', self.PROTOTYPE)

        if self.prototype is None:
            raise ValueError('prototype cannot be None')
        
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
        if not self.is_alive():
            return

        self.kill(exception)
        self.join()

    def die_after(self, timeout, exception=None):
        timer = threading.Timer(timeout, self.die, args=[exception])
        timer.start()

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
            #import traceback
            #traceback.print_exc()

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
