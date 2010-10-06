#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <smalltable.h>

#define OP_QLIST_ADD 0xF0
#define OP_QLIST_DEL 0xF1

#define FLAG_QLIST (0x04)
#define EMPTY_QLIST_SIZE 10

/* qlist.c */
int qlist_or(uint8_t *qpc_start, int qpc_sz, uint8_t *qpa, uint8_t *qpb);
int qlist_and(uint8_t *qpc_start, int qpc_sz, uint8_t *qpa, uint8_t *qpb);
int qlist_andnot(uint8_t *qpc_start, int qpc_sz, uint8_t *qpa, uint8_t *qpb);

int qlist_pack(uint8_t *qbuf_start, int qbuf_sz, uint64_t *items, int items_sz);
int qlist_unpack(uint64_t *items_start, int items_sz,  uint8_t *qbuf);


extern uint64_t unique_number;

uint8_t buf_a[MAX_VALUE_SIZE];
uint8_t buf_b[MAX_VALUE_SIZE];

/*
   Request:
      MUST NOT have extras.
      MUST have key.
      MUST have value.
   Ignore CAS.
*/
ST_RES *cmd_qlist_add(ST_REQ *req, ST_RES *res) {
	if(req->extras_sz || !req->key_sz || !req->value_sz) {
		return(set_error_code(res, MEMCACHE_STATUS_INVALID_ARGUMENTS));
	}
	MC_METADATA md;
	memset(&md, 0, sizeof(md));

	uint8_t *qla = buf_a;
	int qla_sz = sizeof(buf_a);
	uint8_t *qlb = (uint8_t *)req->value;
	uint8_t *qlc = buf_b;
	int qlc_sz = sizeof(buf_b);
	
	int r = storage_get(&md, (char*)qla, qla_sz, req->key, req->key_sz);
	if(r < 0){
		r = qlist_pack(qla, qla_sz, NULL, 0);
		if(r < 0) {
			set_error_code(res, MEMCACHE_STATUS_ITEM_NOT_STORED);
			goto exit;
		}
		md.flags = FLAG_QLIST;
		md.cas = ++unique_number || ++unique_number;
	}

	if( (md.flags & FLAG_QLIST) == 0) {
		set_error_code(res, MEMCACHE_STATUS_ITEM_NOT_STORED);
		goto exit;
	}
	
	r = qlist_or(qlc, qlc_sz, qla, qlb);
	if(r < 0) {
		set_error_code(res, MEMCACHE_STATUS_ITEM_NOT_STORED);
		goto exit;
	}
	
	md.cas = (md.cas+1) || (md.cas+2);
	r = storage_set(&md, (char*)qlc, r, req->key, req->key_sz);
	if(r < 0) {
		set_error_code(res, MEMCACHE_STATUS_ITEM_NOT_STORED);
		goto exit;
	}

	res->status = MEMCACHE_STATUS_OK;
	res->cas = md.cas;
exit:
	return(res);
}

/*
   Request:
      MUST NOT have extras.
      MUST have key.
      MUST have value.
   Ignore CAS.
*/
ST_RES *cmd_qlist_del(ST_REQ *req, ST_RES *res) {
	if(req->extras_sz || !req->key_sz || !req->value_sz)
		return(set_error_code(res, MEMCACHE_STATUS_INVALID_ARGUMENTS));
	
	MC_METADATA md;
	memset(&md, 0, sizeof(md));

	uint8_t *qla = buf_a;
	int qla_sz = sizeof(buf_a);
	uint8_t *qlb = (uint8_t *)req->value;
	uint8_t *qlc = buf_b;
	int qlc_sz = sizeof(buf_b);

	int r = storage_get(&md, (char*)qla, qla_sz, req->key, req->key_sz);
	if(r < 0)
		goto exit_ok;
		
	if( (md.flags & FLAG_QLIST) == 0) {
		set_error_code(res, MEMCACHE_STATUS_ITEM_NOT_STORED);
		goto exit;
	}
	
	r = qlist_andnot(qlc, qlc_sz, qla, qlb);
	
	if(r == EMPTY_QLIST_SIZE) {
		storage_delete(req->key, req->key_sz);
	}else{
		md.cas = (md.cas+1) || (md.cas+2);
		r = storage_set(&md, (char*)qlc, r, req->key, req->key_sz);
		if(r < 0) {
			set_error_code(res, MEMCACHE_STATUS_ITEM_NOT_STORED);
			goto exit;
		}
	}

exit_ok:
	res->status = MEMCACHE_STATUS_OK;
	res->cas = md.cas;
exit:
	return(res);
}


struct commands_pointers commands_pointers[] = {
	[OP_QLIST_ADD]	{&cmd_qlist_add, CMD_FLAG_PREFETCH},
	[OP_QLIST_DEL]	{&cmd_qlist_del, CMD_FLAG_PREFETCH}
};

int main(int argc, char **argv) {
	setvbuf(stdout, (char *) NULL, _IONBF, 0);
	
	simple_commands_loop(commands_pointers, NELEM(commands_pointers));
	
	return(0);
}


