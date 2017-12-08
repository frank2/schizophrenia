#!/usr/bin/env python

import threading

from schizophrenia.task import Task

__all__ = ['Pipe', 'Manager']

class Manager(Task):
    def __init__(self):
        super(Task, self).__init__()

        self.modules = dict()
        self.tasks = dict()
        self.pipes = dict()

        self.module_lock = threading.Lock()
        self.task_lock = threading.Lock()

        self.load_module('schizophrenia')

    def load_module(self, module, load_as=None):
        if load_as is None:
            load_as = module.split('.')[-1]

        self.modules[load_as] = __import__(module)

    def reload_module(self, loaded):
        reload(self.modules[loaded])

    def unload_module(self, loaded):
        del self.modules[loaded]

    def load_task(self, module, task):
        modules = module.split('.')
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
            raise AttributeError('no such object {} in {}'.format(task, next_module))

        if not issubclass(task_class, Task):
            raise ValueError('task must be a Task class')

        return task_class
        
    def launch_task(self, module, task, *args, **kwargs):
        task_class = self.load_task(module, task)
