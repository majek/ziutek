'''
>>> er = ExpiratorRunner(lock=threading.Lock(), granularity=0.1)
>>> ex = er.Expirator(0.25, None)
>>> ex.push('a')
>>> ex.push('b')
>>> ex.push('c')
>>> ex.pop()
'a'
>>> ex.pop_all()
['c', 'b']
>>> ex.push('a')
>>> er.sleep(0.3)
>>> ex.pop() is None
True
>>> er.close()
'''
from __future__ import with_statement # 2.5 only

import os
import time
import thread
import threading
from . import peekqueue

import logging
log = logging.getLogger(__name__)


class _Expirator:
    def __init__(self, delay, callback, get_time):
        self.delay = delay
        self.callback = callback
        self.get_time = get_time
        self.queue = peekqueue.PeekQueue()
        self.keys  = set()

    def push(self, key):
        #assert key is not None and key not in self.keys
        self.keys.add(key)
        self.queue.push( (self.get_time() + self.delay, key) )

    def pop(self):
        _, key = self.queue.pop(default=(None, None))
        if key is None:
            return None
        self.keys.remove(key)
        return key

    def pop_all(self):
        keys = list(self.keys)
        self.queue = peekqueue.PeekQueue()
        self.keys  = set()
        return keys

    def _peek_timeout(self):
        timeout, _ = self.queue.peek(default=(None, None))
        return timeout


class ExpiratorRunner:
    def __init__(self, lock, granularity=1.0):
        self.lock = lock
        self._granularity = granularity
        self._t = [time.time()]
        self._expirators = []
        self._exit_event = threading.Event()
        self._ping_event = threading.Event()
        self._exited_event = threading.Event()
        thread.start_new_thread(self._run,())

    def Expirator(self, delay, callback):
        expirator = _Expirator(delay, callback, self.get_time)
        self._expirators.append( expirator )
        return expirator

    def _run(self):
        log.info("Expirator pid:%i" % (os.getpid(), ))
        ''' Executed in dedicated thread. '''
        while self._exit_event.isSet() is False:
            t = time.time()
            self._t[0] = t
            with self.lock:
                for expirator in self._expirators:
                    keys = []
                    while True:
                        to = expirator._peek_timeout() 
                        if to is None or to > t:
                            break
                        keys.append( expirator.pop() )
                    if keys and expirator.callback:
                        try:
                            expirator.callback(keys)
                        except Exception:
                            log.critical("Exception in callback:", exc_info=True)
                            raise
            time.sleep(self._granularity)
            self._ping_event.set()
            self._ping_event.clear()
        self._exited_event.set()

    def get_time(self):
        return self._t[0]

    def close(self):
        self._exit_event.set()
        self._exited_event.wait()

    def sleep(self, delay):
        t_exp = time.time() + delay
        while self.get_time() < t_exp:
            self._ping_event.wait()
