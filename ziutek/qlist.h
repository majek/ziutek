/* qlist.c */
int qlist_or(u_int8_t *qpc_start, int qpc_sz, u_int8_t *qpa, u_int8_t *qpb);
int qlist_and(u_int8_t *qpc_start, int qpc_sz, u_int8_t *qpa, u_int8_t *qpb);
int qlist_andnot(u_int8_t *qpc_start, int qpc_sz, u_int8_t *qpa, u_int8_t *qpb);

int qlist_pack(u_int8_t *qbuf_start, int qbuf_sz, u_int64_t *items, int items_sz);
int qlist_unpack(u_int64_t *items_start, int items_sz,  u_int8_t *qbuf);
