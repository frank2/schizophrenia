#!/usr/bin/env python

import random
import time

import stacktracer

from schizophrenia.task import Task
from schizophrenia.manager import PID, find_manager

class MasterTask(Task):
    def task(self):
        while not self.dead():
            tasks = self.manager.get_tasks()

            # first, look for child tasks without a pipe
            for task in tasks:
                if not isinstance(task, SlaveTask):
                    continue

                pid = task.pid

                if pid is None or pid == self.pid:
                    continue

                if not self.has_pipe(pid) and pid.is_alive():
                    self.create_pipe(pid)

                self.dead()

            # next, loop over our pipes for possible messages, then distribute those messages
            # to the rest of the tasks
            pipes = self.get_pipes()

            for pid in pipes:
                pipe = pipes[pid]

                if pipe.closed() or pipe.empty():
                    continue

                message = pipe.get()
                reformatted_message = '<{}> {}'.format(pid, message)

                for other_pid in pipes:
                    if other_pid == pid or not other_pid.is_alive() or pipes[other_pid].closed():
                        continue
                        
                    pipes[other_pid].put(reformatted_message)

                    self.dead()

                print(reformatted_message)

                self.dead()

            time.sleep(0.01)

class SlaveTask(Task):
    def task(self):
        # find the master task and create the pipe
        master = list(filter(lambda x: isinstance(x, MasterTask), self.manager.get_tasks()))

        while len(master) == 0:
            time.sleep(0.01)
            master = list(filter(lambda x: isinstance(x, MasterTask), self.manager.get_tasks()))

        master = master[0]
        pipe = self.create_pipe(master.pid)

        while not random.randint(1, 30) == 30 and not pipe is None:
            message = ''.join([random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ') for i in range(10, 50)])
            
            if pipe.closed():
                break

            pipe.put(message)

            wait_time = random.randint(1,5)
            start = time.time()
            end = start + wait_time

            while not time.time() > end:
                pipe = self.get_pipe(master.pid)

                if pipe is None:
                    break

                if pipe.empty() or pipe.closed():
                    continue

                message = pipe.get()

                #print('{} got message: {}'.format(self.thread_name, message))
                self.dead()

            self.dead()

        print('{} is done.'.format(self.thread_name))

if __name__ == '__main__':
    stacktracer.trace_start('trace.html', 5, True)
    mgr = find_manager()

    with mgr.pid_lock:
        print('HURRRRRRRR')

    mgr.load_module('__main__')
    master_pid = mgr.spawn_task('__main__.MasterTask')
    children = [mgr.spawn_task('__main__.SlaveTask') for i in range(50)]

    for i in range(10):
        time.sleep(1)

    print('Killing everything.')

    for child in children:
        if child.is_alive():
            child.die()

    master_pid.die()

    print(master_pid.is_alive())

    stacktracer.trace_stop()
