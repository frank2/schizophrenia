#!/usr/bin/env python

import sys
import threading

__all__ = ['WouldBlockError', 'Result']

class WouldBlockError(Exception):
    pass

class Result(threading.Event):
    def __init__(self):
        self.value = None
        self.exception = None
        super(Result, self).__init__()

    def set(self, value=None):
        self.value = value
        super(Result, self).set()

    def clear(self):
        self.value = None
        super(Result, self).clear()

    def get(self, blocking=True, timeout=None):
        if not blocking and not self.is_set():
            raise WouldBlockError('the value is not yet ready')
        
        if self.wait(timeout):
            self.raise_exception()
            return self.value

        raise TimeoutError('result timed out')

    def set_exception(self, exception=None):
        if not exception is None:
            self.exception = (None, exception, None)
        else:
            self.exception = sys.exc_info()

        self.set()

    def raise_exception(self):
        if self.exception is None:
            return

        exc, tb = self.exception[1], self.exception[2]
        raise exc.with_traceback(tb)
