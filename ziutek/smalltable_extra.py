'''
>>> True == True
True
'''
import smalltable

from . import qlist

OP_QLIST_ADD=0xF0
OP_QLIST_DEL=0xF1

FLAG_QLIST=0x04

class QListClient(smalltable.Client):
    def _decode(self, value, flags):
        if flags == FLAG_QLIST:
            return value
        return smalltable.Client._decode(self, value, flags)

    @smalltable.code_loader(__name__, ['plugin_qlist.c', 'qlist.c'])
    def qlist_add_multi(self, key_map):
        items = key_map.items()
        def _req(key, value):
            return {
                'opcode':OP_QLIST_ADD,
                'key':key,
                'value': value,
                'reserved':smalltable.RESERVED_FLAG_QUIET,
            }
        self.conn.send_with_noop( _req(key, value) for key, value in items )
        for i, (r_status, r_cas, r_extras, r_key, r_value) in enumerate(self.conn.recv_till_noop()):
            if r_status is not smalltable.STATUS_NO_ERROR:
                raise smalltable.status_exceptions[r_status](key=items[i])
        return True

    @smalltable.code_loader(__name__, ['plugin_qlist.c', 'qlist.c'])
    def qlist_del_multi(self, key_map):
        items = key_map.items()
        def _req(key, value):
            return {
                'opcode':OP_QLIST_DEL,
                'key':key,
                'value': value,
                'reserved':smalltable.RESERVED_FLAG_QUIET,
            }
        self.conn.send_with_noop( _req(key, value) for key, value in items )
        for i, (r_status, r_cas, r_extras, r_key, r_value) in enumerate(self.conn.recv_till_noop()):
            if r_status is not smalltable.STATUS_NO_ERROR:
                raise smalltable.status_exceptions[r_status](key=items[i])
        return True

    def qlist_get_multi(self, keys):
        return self.get_multi(keys, default=qlist.pack([]))

    def qlist_get_many(self, keys):
        keys = list(keys)
        return dict( zip(keys, self.get_multi(keys, default=qlist.pack([]))) )


    def clone(self):
        return QListClient(self.server_addr)

