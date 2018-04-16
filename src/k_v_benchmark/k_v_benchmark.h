#ifndef __K_V_BENCHMARK_H
#define __K_V_BENCHMARK_H

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <event.h>
#include <stdint.h>
#include <inttypes.h>

typedef enum {
	BM_NONE,				//0
	BM_PRINT,				//1
	BM_DIRECT_FILE,			//2
	BM_TO_QUEUE,			//3
	BM_TO_LOCK_FREE_QUEUE,	//4
	BM_TO_ZEROMQ,			//5
	BM_TO_ZEROMQ_X,			//6
} bm_type_t;

typedef enum {
	BM_PROCESS_DUMMY,		//0
	BM_PROCESS_ADD,	 		//1
	BM_PROCESS_SPIN,		//2
	BM_PROCESS_PRINT,		//3
} bm_process_op_t;

typedef enum {
    BM_READ_OP,
    BM_WRITE_OP,
} bm_op_type_t;

typedef struct {
    bm_op_type_t type;
    uint64_t	 key_hv;
	int 		port;
} bm_op_t;

void bm_init();
void* bm_loop_in_thread(void* args);
void bm_record_op(bm_op_t op);

#endif