import threading
# Based on: http://mail.python.org/pipermail/tutor/2003-June/023479.html

class PeekQueue:
    '''A sample implementation of a First-In-First-Out data structure.

    >>> a = PeekQueue()
    >>> a.empty.isSet()
    True
    >>> a.push('a')
    >>> a.push('b')
    >>> len(a)
    2
    >>> a.pop()
    'a'
    >>> a.push('c')
    >>> a.empty.isSet()
    False
    >>> a.pop()
    'b'
    >>> a.peek()
    'c'
    >>> a.pop()
    'c'
    >>> len(a)
    0
    >>> a.empty.isSet()
    True
    >>> a.join()
    '''

    def __init__(self):
        self.in_stack = []
        self.out_stack = []
        self.empty = threading.Event()
        self.empty.set()

    def push(self, obj):
        if not self.in_stack and not self.out_stack:
            self.empty.clear()
        self.in_stack.append(obj)

    def pop(self, default=None):
        '''
        >>> a = PeekQueue()
        >>> a.pop() is None
        True
        '''
        if not self.out_stack:
            self.out_stack = list(reversed(self.in_stack))
            self.in_stack = []
        if self.out_stack:
            obj = self.out_stack.pop()
            if not self.in_stack and not self.out_stack:
                self.empty.set()
            return obj
        return default

    def peek(self, default=None):
        '''
        >>> a = PeekQueue()
        >>> a.peek() is None
        True
        '''
        if not self.out_stack:
            self.out_stack = list(reversed(self.in_stack))
            self.in_stack = []
        if self.out_stack:
            return self.out_stack[-1]
        return default

    def join(self, timeout=None):
        self.empty.wait(timeout=timeout)

    def __nonzero__(self):
        '''
        >>> a = PeekQueue()
        >>> True if a else False
        False
        '''
        return True if self.in_stack or self.out_stack else False

    def __len__(self):
        '''
        >>> a = PeekQueue()
        >>> len(a)
        0
        >>> a.push('a')
        >>> a.push('b')
        >>> len(a)
        2
        '''
        return len(self.in_stack) + len(self.out_stack)

