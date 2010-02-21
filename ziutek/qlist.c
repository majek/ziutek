/*
Included both from backdend and python. Beware on dependencies.
*/
#include <stdlib.h>
#include <event.h>

#ifndef likely
#define likely(x)	__builtin_expect(!!(x), 1)
#endif

#ifndef unlikely
#define unlikely(x)	__builtin_expect(!!(x), 0)
#endif

#ifndef COVERAGE_TEST
#define inline		inline	__attribute__((always_inline))
#endif

#define QLIST_MAGIC 0xDEADBEEFDEADBAAFLL


static inline int qlist_put_delta(u_int8_t *buf, u_int64_t delta) {
	if(likely(delta < (1<<7))) {
		buf[0] = (1<<7) | delta;
		return(1);
	} else if(delta < (1<<14)) {
		buf[0] = (1<<6) | ((delta >> 8) & 0xFF);
		buf[1] = 	  ((delta >> 0) & 0xFF);
		return(2);
	}else if(delta < (1<<21)) {
		buf[0] = (1<<5) | ((delta >> 16) & 0xFF);
		buf[1] = 	  ((delta >> 8) & 0xFF);
		buf[2] = 	  ((delta >> 0) & 0xFF);
		return(3);
	}

	buf[0] = (1<<4);
	buf[1] = 	  ((delta >> 56) & 0xFF);
	buf[2] = 	  ((delta >> 48) & 0xFF);
	buf[3] = 	  ((delta >> 40) & 0xFF);
	buf[4] = 	  ((delta >> 32) & 0xFF);
	buf[5] = 	  ((delta >> 24) & 0xFF);
	buf[6] = 	  ((delta >> 16) & 0xFF);
	buf[7] = 	  ((delta >> 8) & 0xFF);
	buf[8] = 	  ((delta >> 0) & 0xFF);
	return(9);
}

static inline int qlist_put_stop(u_int8_t *buf) {
	buf[0] = 0;
	return(1);
}

/*
	-1: qbuf_sz too small
	-2: not sorted
*/
int qlist_pack(u_int8_t *qbuf_start, int qbuf_sz, u_int64_t *items_start, int items_sz) {
	/* We might have 9 bytes overcommit, assume the bufffer is smaller. */
	qbuf_sz -= 9;
	if(qbuf_sz < 0)
		return(-1);

	u_int8_t *qbuf = qbuf_start;
	u_int8_t *qbuf_end = qbuf + qbuf_sz;
	u_int64_t *items = items_start;
	u_int64_t *items_end = items + items_sz;

	qbuf += qlist_put_delta(qbuf, QLIST_MAGIC);
	if(unlikely(qbuf >= qbuf_end))
		return(-1);

	u_int64_t last_item = 0;
	u_int64_t delta;
	while(items < items_end) {
		if(unlikely(*items < last_item)) /* unsorted? */
			return(-2);
		delta = *items - last_item;
		last_item = *items;
		items++;
		if(unlikely(0 == delta && (items-1) != items_start))
			continue;
		qbuf += qlist_put_delta(qbuf, delta);
		if(unlikely(qbuf >= qbuf_end))
			return(-1);
	}
	qbuf += qlist_put_stop(qbuf);
	
	return(qbuf - qbuf_start);
}

static inline int qlist_get_delta(u_int8_t **qbuf, u_int64_t *delta_ptr) {
	u_int8_t *buf = *qbuf;
	
	u_int64_t delta;
	if(likely(buf[0] & (1<<7))) {
		delta = (buf[0] & ~(1<<7)) << 0;
		*delta_ptr = delta;
		*qbuf += 1;
		return(0);
	}else if(buf[0] & (1<<6)){
		delta  = (buf[0] & ~(1<<6)) << 8;
		delta |= (buf[1] << 0);
		*delta_ptr = delta;
		*qbuf += 2;
		return(0);
	}else if(buf[0] & (1<<5)) {
		delta  = (buf[0] & ~(1<<5)) << 16;
		delta |= (buf[1] << 8);
		delta |= (buf[2] << 0);
		*delta_ptr = delta;
		*qbuf += 3;
		return(0);
	}else if(buf[0] & (1<<4)) {
		// ignore buf[0]
		delta  = ((u_int64_t)buf[1] << 56);
		delta |= ((u_int64_t)buf[2] << 48);
		delta |= ((u_int64_t)buf[3] << 40);
		delta |= ((u_int64_t)buf[4] << 32);
		delta |= ((u_int64_t)buf[5] << 24);
		delta |= ((u_int64_t)buf[6] << 16);
		delta |= ((u_int64_t)buf[7] << 8);
		delta |= ((u_int64_t)buf[8] << 0);
		*delta_ptr = delta;
		*qbuf += 9;
		return(0);
	}
	/* finito, stop */
	*qbuf += 0; /* rollback :) */
	return(-1);
}

/*
static inline int qlist_get_item(u_int8_t **qbuf, u_int64_t *last_item_ptr) {
	u_int64_t delta = 0;
	int d = qlist_get_delta(qbuf, &delta);
	*last_item_ptr += delta;
	return(d);
}
*/
#define qlist_get_item(qbuf, last_item_ptr)		\
	({						\
		u_int64_t delta = 0;			\
		int d = qlist_get_delta(qbuf, &delta);	\
		*last_item_ptr += delta;		\
		(d);					\
	})

/*
	-1: items_sz too small
	-2: bad magic
*/
int qlist_unpack(u_int64_t *items_start, int items_sz,  u_int8_t *qbuf) {
	u_int64_t magic = 0;
	qlist_get_delta(&qbuf, &magic);
	if(magic != QLIST_MAGIC)
		return(-2);
	
	u_int64_t last_item = 0;
	u_int64_t *items = items_start;
	u_int64_t *items_end = items + items_sz;
	while( 1 ) {
		if(unlikely(items >= items_end))
			return(-1);
		if(unlikely(-1 == qlist_get_item(&qbuf, &last_item)))
			break;
		*items = last_item;
		items++;
	}
	return(items - items_start);
}

/*
static inline int qlist_put_item(u_int8_t *qbuf, u_int64_t *last_item, u_int64_t item) {
	u_int64_t delta = item - *last_item;
	*last_item = item;
	return(qlist_put_delta(qbuf, delta));
}
*/
#define qlist_put_item(qbuf, last_item, item) 		\
	({						\
		u_int64_t delta = item - *last_item;	\
		*last_item = item;			\
		(qlist_put_delta(qbuf, delta));		\
	})

#define PREFIX						\
	/* We might have 9 bytes overcommit, assume the bufffer is smaller. */	\
	qpc_sz -= 9;					\
	if(qpc_sz < 0)					\
		return(-1);				\
							\
	u_int8_t *qpc = qpc_start;			\
	u_int8_t *qpc_end = qpc + qpc_sz;		\
	u_int64_t magic = 0;				\
							\
	qpc += qlist_put_delta(qpc, QLIST_MAGIC);	\
	if(unlikely(qpc >= qpc_end))			\
		return(-1);				\
							\
	qlist_get_delta(&qpa, &magic);			\
	if(magic != QLIST_MAGIC)			\
		return(-2);				\
							\
	qlist_get_delta(&qpb, &magic);			\
	if(magic != QLIST_MAGIC)			\
		return(-2);				\
							\
	int a_d = 0;					\
	int b_d = 0;					\
	u_int64_t a = 0;				\
	u_int64_t b = 0;				\
	u_int64_t c = 0;				\
							\
	a_d = qlist_get_item(&qpa, &a);			\
	b_d = qlist_get_item(&qpb, &b);

#define SUFFIX_A_D					\
	while( a_d != -1 ) {				\
		qpc += qlist_put_item(qpc, &c, a);	\
		if(qpc >= qpc_end)			\
			return(-1);			\
		a_d = qlist_get_item(&qpa, &a);		\
	}

#define SUFFIX_B_D					\
	while( b_d != -1 ) {				\
		qpc += qlist_put_item(qpc, &c, b);	\
		if(qpc >= qpc_end)			\
			return(-1);			\
		b_d = qlist_get_item(&qpb, &b);		\
	}

#define SUFFIX_RET					\
	qpc += qlist_put_stop(qpc);			\
	return(qpc - qpc_start);

/*
	-1: qpc_sz too small
	-2: bad magic
*/
int qlist_or(u_int8_t *qpc_start, int qpc_sz, u_int8_t *qpa, u_int8_t *qpb) {
	PREFIX;

	while(a_d != -1 && b_d != -1) {
		if(a < b) {
			qpc += qlist_put_item(qpc, &c, a);
			if(qpc >= qpc_end)
				return(-1);
			a_d = qlist_get_item(&qpa, &a);
		}else if(a > b) {
			qpc += qlist_put_item(qpc, &c, b);
			if(qpc >= qpc_end)
				return(-1);
			b_d = qlist_get_item(&qpb, &b);
		}else{
			qpc += qlist_put_item(qpc, &c, a);
			if(qpc >= qpc_end)
				return(-1);
			a_d = qlist_get_item(&qpa, &a);
			b_d = qlist_get_item(&qpb, &b);
		}
	}
	
	SUFFIX_A_D;
	SUFFIX_B_D;
	SUFFIX_RET;
}

/*
	-1: qpc_sz too small
	-2: bad magic
*/
int qlist_and(u_int8_t *qpc_start, int qpc_sz, u_int8_t *qpa, u_int8_t *qpb) {
	PREFIX;

	while(a_d != -1 && b_d != -1) {
		if(a < b) {
			a_d = qlist_get_item(&qpa, &a);
		}else if(a > b) {
			b_d = qlist_get_item(&qpb, &b);
		}else{
			qpc += qlist_put_item(qpc, &c, a);
			if(qpc >= qpc_end)
				return(-1);
			a_d = qlist_get_item(&qpa, &a);
			b_d = qlist_get_item(&qpb, &b);
		}
	}
	
	SUFFIX_RET;
}

/*
	-1: qpc_sz too small
	-2: bad magic
*/
int qlist_andnot(u_int8_t *qpc_start, int qpc_sz, u_int8_t *qpa, u_int8_t *qpb) {
	PREFIX;

	while(a_d != -1 && b_d != -1) {
		if(a < b) {
			qpc += qlist_put_item(qpc, &c, a);
			if(qpc >= qpc_end)
				return(-1);
			a_d = qlist_get_item(&qpa, &a);
		}else if(a > b) {
			b_d = qlist_get_item(&qpb, &b);
		}else{
			a_d = qlist_get_item(&qpa, &a);
			b_d = qlist_get_item(&qpb, &b);
		}
	}
	
	SUFFIX_A_D;
	SUFFIX_RET;
}

