

class LRUCache:
    '''
    >>> l = LRUCache(maxsize=3)
    >>> l.add('a')
    >>> l.add('b')
    >>> l.add('c')
    >>> l.add('d')
    >>> l.to_list()
    ['b', 'c', 'd']
    >>> l.add('b')
    >>> l.to_list()
    ['c', 'b', 'd']
    >>> l.add('b')
    >>> l.to_list()
    ['c', 'd', 'b']
    >>> l.add('b')
    >>> l.to_list()
    ['c', 'd', 'b']
    '''
    def __init__(self, maxsize=100):
        assert maxsize > 0
        self.i = {}
        self.l = [None] * maxsize
        self.p = 0
        self.maxsize = maxsize

    def add(self, key):
        if key not in self.i:
            self.i[key] = self.p
            self.l[self.p] = key
            self.p = (self.p + 1)  % self.maxsize
        else:
            a = self.i[key]
            b = (a+1) % self.maxsize
            if b == self.p:
                return
            keyb = self.l[b]
            self.l[a], self.l[b] = self.l[b], self.l[a]
            self.i[key] = b
            self.i[keyb] = a

    def __contains__(self, key):
        '''
        >>> l = LRUCache(maxsize=3)
        >>> l.add('a')
        >>> 'a' in l
        True
        '''
        return (key in self.i)

    def __len__(self):
        '''
        >>> l = LRUCache(maxsize=3)
        >>> l.add('a')
        >>> len(l)
        1
        '''
        return len(self.i)

    def to_list(self):
        '''
        >>> l = LRUCache(maxsize=3)
        >>> l.add('a')
        >>> l.to_list()
        [None, None, 'a']
        '''
        return [self.l[(self.p+p) % self.maxsize] for p in range(self.maxsize)]

