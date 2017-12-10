#!/usr/bin/env python

import random
import time

import stacktracer

from schizophrenia.task import Task
from schizophrenia.manager import TID, find_manager

class MasterTask(Task):
    def task(self):
        while not self.dead():
            tasks = self.manager.get_tasks()

            # first, look for child tasks without a pipe
            for task in tasks:
                if not isinstance(task, SlaveTask):
                    continue

                tid = task.tid

                if tid is None or tid == self.tid:
                    continue

                if not self.has_pipe(tid) and tid.is_alive():
                    self.create_pipe(tid)

                self.dead()

            # next, loop over our pipes for possible messages, then distribute those messages
            # to the rest of the tasks
            pipes = self.get_pipes()

            for tid in pipes:
                pipe = pipes[tid]

                if pipe.closed():
                    continue
                    
                try:
                    message = pipe.get(block=False)
                except: # queue is empty
                    message = None

                while not message is None:
                    reformatted_message = '<{}> {}'.format(tid, message)

                    for other_tid in pipes:
                        if other_tid == tid or not other_tid.is_alive() or pipes[other_tid].closed():
                            continue
                        
                        pipes[other_tid].put(reformatted_message)
                        self.dead()

                    print(reformatted_message)
                    self.dead()
                    
                    try:
                        message = pipe.get(block=False)
                    except: # queue is empty
                        message = None

                self.dead()

            self.sleep(0.01)

class SlaveTask(Task):
    def task(self):
        # find the master task and create the pipe
        master = list(filter(lambda x: isinstance(x, MasterTask), self.manager.get_tasks()))

        while len(master) == 0:
            self.sleep(0.01)
            master = list(filter(lambda x: isinstance(x, MasterTask), self.manager.get_tasks()))

        master = master[0]
        pipe = self.create_pipe(master.tid)

        while not random.randint(1, 30) == 30 and not pipe is None:
            message = ''.join([random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ') for i in range(10, 50)])
            
            if pipe.closed():
                break

            pipe.put(message)

            wait_time = random.randint(100,5000)
            wait_time /= 1000.0
            start = time.time()
            end = start + wait_time

            while not time.time() > end:
                pipe = self.get_pipe(master.tid)

                if pipe is None or pipe.closed():
                    break

                try:
                    message = pipe.get(block=False)
                except:
                    self.sleep(0.01)
                    continue

                #print('{} got message: {}'.format(self.thread_name, message))
                self.dead()

            self.dead()

        print('{} is done.'.format(self.thread_name))

if __name__ == '__main__':
    stacktracer.trace_start('trace.html', 5, True)
    mgr = find_manager()
    mgr.load_module('__main__')
    master_tid = mgr.spawn_task('__main__.MasterTask')
    children = [mgr.spawn_task('__main__.SlaveTask') for i in range(50)]

    for i in range(10):
        time.sleep(1)

    print('Killing everything.')

    for child in children:
        if child.is_alive():
            child.die()

    master_tid.die()

    print(master_tid.is_alive())

    stacktracer.trace_stop()
