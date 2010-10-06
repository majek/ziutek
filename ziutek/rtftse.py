'''
>>> from .smalltable_extra import QListClient
>>> mc = QListClient("127.0.0.1:11211")
>>> idx = Indexer(mc, flush_delay=0.5, max_tuples=16, namespace='test1')
>>> idx.put(1, "ala ma kota".split())
3
>>> idx.flush()
(1, 3)
>>> idx.put(2, "ala ma kota robota ale ne kot robot".split()) # 8
8
>>> idx.put(3, "ala ma kota robota ale ne kot robot ojej".split()) # 9
9
>>> time.sleep(1.1)
>>> idx.flush()
(0, 0)
>>> idx.put(4, "kota ma ala".split())
3
>>> idx.flush()
(1, 3)
>>> srch = Searcher(mc, namespace='test1')
>>> srch.materialized_query(["ala"])
(4, [1L, 2L, 3L, 4L])
>>> srch.materialized_query(["ala"], reverse=True)
(4, [4L, 3L, 2L, 1L])
>>> srch.materialized_query(["robota"])
(2, [2L, 3L])
>>> srch.materialized_query("ala AND robot".split())
(2, [2L, 3L])
>>> srch.materialized_query(["ojej"])
(1, [3L])
>>> srch.materialized_query("robota ANDNOT ojej".split())
(1, [2L])
>>> srch.materialized_query("ala AND robota ANDNOT ojej".split())
(1, [2L])
>>> srch.materialized_query("null OR ojej".split())
(1, [3L])

>>> idx.delete(2, "ala ma kota robota ale ne kot robot".split())
8
>>> idx.close()
>>> srch.materialized_query(["ala"])
(3, [1L, 3L, 4L])
>>> srch.materialized_query("ala AND robot".split())
(1, [3L])

>>> srch.materialized_query(["non-exisitng-word"])
(0, [])
>>> mc.close()

Deletion.

>>> mc = QListClient("127.0.0.1:11211")
>>> idx = Indexer(mc, flush_delay=55.5, max_tuples=16, namespace='test2')
>>> srch = Searcher(mc, namespace='test2')
>>> idx.put(1, "ala ma kota".split())
3
>>> idx.delete(1, "ala ma kota".split())
3
>>> idx.flush()
(1, 3)
>>> srch.materialized_query(["ala"])
(0, [])
>>> idx.close()
>>> mc.close()

Many chunks.

>>> mc = QListClient("127.0.0.1:11211")
>>> idx = Indexer(mc, flush_delay=555, block_size=2, max_tuples=32, namespace='test3')
>>> srch = Searcher(mc, namespace='test3', block_size=2)
>>> idx.put(1, "ala ma kota".split())
3
>>> idx.put(2, "ala ma kota w".split())
4
>>> idx.put(3, "ala ma kota w bardzo".split())
5
>>> idx.put(999, "ala ma kota w bardzo duzym sloiku".split())
7
>>> idx.flush()
(4, 19)
>>> srch.materialized_query(["ala"])
(4, [1L, 2L, 3L, 999L])
>>> srch.materialized_query(["ala"], reverse=True)
(4, [999L, 3L, 2L, 1L])

Limit results.
>>> srch.materialized_query(["ala"], limit=3)
(4, [1L, 2L, 3L])
>>> srch.materialized_query(["ala"], limit=3, reverse=True)
(4, [999L, 3L, 2L])
>>> srch.materialized_query(["ala"], limit=2)
(4, [1L, 2L, 3L])
>>> srch.materialized_query(["ala"], limit=1) # 3 chunks, every has 1 item
(3, [1L])

>>> idx.close()
>>> mc.close()

'''
from __future__ import with_statement # 2.5 only

import collections
import array
import time
import Queue as queue
import threading
import itertools

import os
import logging

from . import parsetorpn
from . import qlist
from . import lrucache
from . import expirator

import multiprocessing
import functools

log = logging.getLogger(__name__)
FORMAT_CONS = '%(asctime)s %(name)-12s %(levelname)8s\t%(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT_CONS)

# Maximum queue depth, 0 means unlimited.
CPU_COUNT=multiprocessing.cpu_count()
QUEUE_LIMIT=CPU_COUNT*3
SENDER_CONCURRENCY=max(2, int(CPU_COUNT*1.5))

def with_lock(fun):
    @functools.wraps(fun)
    def wrapper(self, *args, **kwargs):
        with self.lock:
            r = fun(self, *args, **kwargs)
        return r
    return wrapper



class Sender:
    def __init__(self, mc, namespace, metachunk_cache_size):
        self.namespace = namespace
        self._send_queue = multiprocessing.Queue(maxsize=QUEUE_LIMIT)
        self._mc = mc
        self._exit = multiprocessing.Event()
        self._empty = multiprocessing.Event()
        self._empty.set()
        self._parent_pid = os.getpid()
        self.metachunk_cache = lrucache.LRUCache(maxsize=metachunk_cache_size)

        self._processes = []
        for p in range(SENDER_CONCURRENCY):
            process = multiprocessing.Process(target=self._run)
            process.start()
            self._processes.append( process )
        self._transferred = 0

    def push(self, item):
        if self._send_queue.qsize() == 0:
            self._empty.clear()
        self._send_queue.put( item )

    def join(self):
        self._empty.wait()

    def size(self):
        return self._send_queue.qsize()

    def close(self):
        self._empty.wait()
        self._exit.set()
        for process in self._processes:
            process.join()

    def _run(self):
        try:
            import os
            log.info("(Net) Sender pid:%i" % (os.getpid(), ))
            namespace = self.namespace
            mc = self._mc.clone()
            cache = self.metachunk_cache
            normal_chunks = 0
            meta_chunks = 0
            transferred = 0
            while True:
                dd = None
                try:
                    send_cmd, dd = self._send_queue.get(block=True, timeout=2.5)
                    log.info("(Net) Queued:%i  Hitlists: chunks/meta %i/%i  Through_queue_MiB: %.1f" % (
                                self.size()+1,
                                normal_chunks, meta_chunks, 
                                transferred/1048576.0, 
                            ))
                except queue.Empty:
                    if self._exit.is_set():
                        break
                    else:
                        continue
                cmds = {
                    'ADD': {},
                    'DEL': {},
                }
                cmd = cmds[send_cmd]
                meta = collections.defaultdict(lambda:array.array('L'))
                for ((key, chunk_no), hitlist) in dd.iteritems():
                    transferred += len(hitlist)
                    k = namespace + ':' + key + ':' + str(chunk_no)
                    if len(k) > 255:
                        log.error("key %r too long, ignored" % (k,))
                        continue
                    cmd[k] = hitlist # assume it's already packed
                    if k not in cache:
                        meta[key].append( chunk_no )
                    cache.add(k)
                    normal_chunks += 1

                for key, chunk_numbers in meta.iteritems():
                    k = namespace + ':' + key + ':meta'
                    meta_chunks += 1
                    if len(k) > 255:
                        log.error("key %r too long, ignored" % (k,))
                        continue
                    cmds['ADD'][k] = qlist.pack(chunk_numbers, sort=True)

                if cmds['ADD']:
                    mc.qlist_add_multi( cmds['ADD'] )
                if cmds['DEL']:
                    mc.qlist_del_multi( cmds['DEL'] )

                if self._send_queue.qsize() == 0:
                    self._empty.set()
        except:
            log.error("Exception in multiprocessing:", exc_info=True)
            import os
            import signal
            os.kill(self._parent_pid, signal.SIGKILL)


class HitlistDefaultdict(collections.defaultdict):
    def __init__(self, exp):
        self.exp = exp
        collections.defaultdict.__init__(self)

    def __missing__(self, key):
        self.exp.push( key )
        # Let's try list() instead of array. It appears to be twice faster!
        ## a = array.array('L')
        a = []
        self[key] = a
        return a


class Hitlists:
    def __init__(self, max_tuples, sender, block_size, expirator_runner, flush_delay, send_cmd):
        self.max_tuples = max_tuples
        self.sender     = sender
        self.block_size = block_size
        self.expirator  = expirator_runner.Expirator(flush_delay, self._timeouted)
        self.hitlists   = HitlistDefaultdict(self.expirator)
        self.tuples_inmem = 0
        self.total_tuples = 0
        self.total_docids = 0
        self.send_cmd = send_cmd

    def update_counters(self, counter, docids):
        self.total_docids += docids
        self.tuples_inmem += counter
        self.total_tuples += counter

        while self.tuples_inmem > self.max_tuples:
            to_flush = len(self.hitlists)//3
            quarter_keys = [self.expirator.pop() \
                                        for _ in xrange(to_flush)]
            log.info('(Mem)           Tokens: all/in_mem %i/%i  Hitlists: flushing/total: %i/%i' %
                        (self.total_tuples, self.tuples_inmem,
                        to_flush, len(self.hitlists), ))
            self._flush_keys(quarter_keys)

    def _timeouted(self, keys):
        log.info('(T/O) Timeouted %i hitlists.' % (len(keys), ))
        self._flush_keys(keys)

    def _flush_keys(self, keys):
        dd = {}
        counter = 0
        for key in keys: #word, chunk_no = key
            hitlist = self.hitlists[key]
            dd[ key ] = qlist.pack(hitlist, sort=False)
            counter += len(hitlist)
            del self.hitlists[key]
        self.tuples_inmem -= counter
        if dd:
            self.sender.push( (self.send_cmd, dd) )
        return counter

    def flush(self):
        keys = self.expirator.pop_all()
        return self._flush_keys(keys)

    def close(self):
        self.flush()


class DocidRing:
    def __init__(self, expirator_runner, flush_delay):
        self._expirator = expirator_runner.Expirator(flush_delay, self._timeouted)
        self._s = set()

    def push(self, docid):
        self._s.add( docid )
        self._expirator.push( docid )

    def _timeouted(self, items):
        self._s.difference_update( items )

    def flush(self):
        self._s.clear()
        return len(self._expirator.pop_all())

    def __contains__(self, item):
        return item in self._s


class Indexer:
    '''
    putter (main thread):
            * adds items to cache_ring
            * sets mem_overflow
    flusher (thread):
            * waits for timeout
            * or mem_overflow
            * or for total flush
            * and moves items from cache_ring into shared queue
    sender (child process):
            * sends items from shared queue
            * can be scaled to multiple processes
    '''
    def __init__(self, mc, namespace='', flush_delay=600, max_tuples=128000, block_size=16384):
        log.info("Indexer pid:%i" % (os.getpid(), ))
        log.info("Indexer namespace:%r block_size:%i  flush_delay:%i max_tuples=%i" % (
                    namespace,
                    block_size,
                    flush_delay,
                    max_tuples,
                ))
        self.lock = threading.Lock()
        self.block_size = block_size

        self.sender= Sender(mc,
                        namespace = namespace,
                        metachunk_cache_size = max_tuples // 4,)
        self.expirator_runner = expirator.ExpiratorRunner(self.lock)

        self.put_hitlists = Hitlists(flush_delay=flush_delay,
                                    max_tuples=max_tuples,
                                    block_size=block_size,
                                    expirator_runner = self.expirator_runner,
                                    sender=self.sender,
                                    send_cmd='ADD',)
        self.del_hitlists = Hitlists(flush_delay=flush_delay,
                                    max_tuples=max_tuples,
                                    block_size=block_size,
                                    expirator_runner = self.expirator_runner,
                                    sender=self.sender,
                                    send_cmd='DEL',)
        self.put_docs = DocidRing(expirator_runner = self.expirator_runner,
                                    flush_delay=flush_delay*1.1)
        self.del_docs = DocidRing(expirator_runner = self.expirator_runner,
                                    flush_delay=flush_delay*1.1)

    @with_lock
    def flush(self):
        log.info("(Flu) Flushing")
        tokens  = self.put_hitlists.flush()
        docs    = self.put_docs.flush()
        tokens += self.del_hitlists.flush()
        docs   += self.del_docs.flush()
        self.sender.join()
        log.info("(Flu) Done")
        return (docs, tokens)

    def close(self):
        self.expirator_runner.close() # this must be outside the lock
        self.flush()
        self.put_hitlists.close()
        self.del_hitlists.close()
        self.sender.close()

    def put(self, docid, keywords):
        r = self.put_multi([(docid, keywords),])
        return r[1]

    def delete(self, docid, keywords):
        r =  self.delete_multi([(docid, keywords),])
        return r[1]

    def put_multi(self, sequence):
        return self._multi(sequence, self.del_docs, self.del_hitlists,
                                     self.put_docs, self.put_hitlists)

    def delete_multi(self, sequence):
        return self._multi(sequence, self.put_docs, self.put_hitlists,
                                     self.del_docs, self.del_hitlists)

    @with_lock
    def _multi(self, sequence, bad_docs, bad_hitlists, my_docs, my_hitlists):
        hld = my_hitlists.hitlists
        tokens = 0
        docs = 0
        for docid, words in sequence:
            assert isinstance(docid, int) or isinstance(docid, long)
            if docid in bad_docs:
                log.warning("Modifying recently saved doc #%r. That is slow!" % (docid, ))
                bad_hitlists.flush()
                bad_docs.flush()
            docs += 1
            tokens += len(words)

            chunk_no = docid // self.block_size

            for word in words:
                hld[(word, chunk_no)].append( docid )

            my_docs.push(docid)
        my_hitlists.update_counters(tokens, docs)
        return (docs, tokens)

    def stats(self):
        log.info("      Queued:%i  Tokens: all/in_mem %i/%i  Docs: add/del %i/%i" % (
            self.sender.size(),
            self.put_hitlists.total_tuples + self.del_hitlists.total_tuples,
            self.put_hitlists.tuples_inmem + self.del_hitlists.tuples_inmem,
            self.put_hitlists.total_docids, self.del_hitlists.total_docids,
            ))



class Searcher:
    def __init__(self, mc, block_size=16384, namespace=''):
        self.namespace = namespace
        self.mc = mc
        self.block_size = block_size

    def _bind_query(self, query, chunk_number):
        query = query[:]
        keys = []
        for i, token in enumerate(query):
            if token not in ['AND', 'ANDNOT', 'OR']:
                key = '%s:%s:%s' % (self.namespace, token, chunk_number)
                query[i] = key
                keys.append( key )
        metachunks = self.mc.qlist_get_multi(keys)
        return [metachunks.get(token, token)  for token in query]


    def query(self, tokenized_query, limit=1000, reverse=False):
        unbound_query = parsetorpn.parse(tokenized_query)
        meta_query = self._bind_query(unbound_query, 'meta')

        hitlists = []
        found_items = 0
        srchd_items = 0

        chunk_hitlist =  self._execute_bound_query(meta_query, self.meta_actions)
        chunk_numbers = qlist.unpack( chunk_hitlist, reverse=reverse)

        for chunk_number in chunk_numbers:
            bound_query = self._bind_query(unbound_query, chunk_number)
            qbuf = self._execute_bound_query(bound_query, self.normal_actions)
            hitlist = qlist.unpack(qbuf, reverse=reverse)
            hitlists.append(hitlist)
            found_items += len(hitlist)
            srchd_items += self.block_size
            if found_items >= limit:
                break

        if srchd_items == 0:
            results = 0
        else:
            results = int((float(found_items) / float(srchd_items)) * len(chunk_numbers) * self.block_size)
        return (results, itertools.chain(*hitlists))

    def materialized_query(self, *args, **kwargs):
        results, out = self.query(*args, **kwargs)
        return (results, list(out))


    normal_actions = {
        'AND': qlist.do_and,
        'OR': qlist.do_or,
        'ANDNOT': qlist.do_andnot,
    }

    meta_actions = {
        'AND': qlist.do_and,
        'OR': qlist.do_or,
        'ANDNOT': lambda a,b:a,
    }

    def _execute_bound_query(self, q, actions):
        token = q.pop()
        if token not in actions:
            return token # is hitlist

        fun = actions[token]

        b = self._execute_bound_query(q, actions)
        a = self._execute_bound_query(q, actions)
        return fun(a, b)


