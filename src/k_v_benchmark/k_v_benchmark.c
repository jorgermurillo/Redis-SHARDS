#include "k_v_benchmark.h"
//#include "waitfree-mpsc-queue/mpscq.h"
//#include "waitfree-mpsc-queue/mpsc.c"
// #include "ringbuf/src/ringbuf.h"
// #include "ringbuf/src/ringbuf.c"
#include "liblfds7.1.1/liblfds7.1.1/liblfds711/inc/liblfds711.h"
#include "shards_monitor.h"
#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
 
// @ Gus: OP QUEUE
typedef struct bm_oq_item_t bm_oq_item_t;
struct bm_oq_item_t {
	bm_op_t op;
	bm_oq_item_t* next;
};

typedef struct bm_oq_t bm_oq_t;
struct bm_oq_t {
	bm_oq_item_t* head;
	bm_oq_item_t* tail;
	pthread_mutex_t lock;
};

static
bm_oq_item_t* malloc_oq_item() {
	bm_oq_item_t* item = malloc(sizeof(bm_oq_item_t));
	return item;
}

static
void free_oq_item(bm_oq_item_t* oq_item) {
	free(oq_item);
}

static
void bm_oq_init(bm_oq_t* oq) {
	pthread_mutex_init(&oq->lock, NULL);
	oq->head = NULL;
	oq->tail = NULL;
}

static
void bm_oq_push(bm_oq_t* oq, bm_oq_item_t* item) {
    item->next = NULL;

    pthread_mutex_lock(&oq->lock);
    if (NULL == oq->tail)
        oq->head = item;
    else
        oq->tail->next = item;
    oq->tail = item;
    pthread_mutex_unlock(&oq->lock);
}

static
bm_oq_item_t* bm_oq_pop(bm_oq_t* oq) {
    bm_oq_item_t* item;

    pthread_mutex_lock(&oq->lock);
    item = oq->head;
    if (NULL != item) {
        oq->head = item->next;
        if (NULL == oq->head)
            oq->tail = NULL;
    }
    pthread_mutex_unlock(&oq->lock);

    return item;
}

// @ Gus: bm settings overrided by config file
bm_type_t bm_type = BM_NONE;

char bm_output_filename[] = "benchmarking_output.txt";
int  bm_output_fd = -1;

bm_oq_t bm_oq;
struct mpscq* bm_mpsc_oq;
int BM_MPSC_OQ_CAP = -1; // @ Gus: capacity must be set right because mpsc is NOT a ring buffer
void* zmq_context = NULL;
void* zmq_sender = NULL;
// #define MAX_WORKERS 2
// static size_t ringbuf_obj_size = 0;
// ringbuf_t* bm_ringbuf;
// ringbuf_worker_t* w1;
// ssize_t off1 = -1;
// unsigned char* buf;
enum lfds711_misc_flag overwrite_occurred_flag;
struct lfds711_ringbuffer_element *re;
struct lfds711_ringbuffer_state rs;

bm_process_op_t bm_process_op_type = BM_PROCESS_DUMMY;
int SPIN_TIME = -1;
int random_accum = 0;

// The process number fro the current process
int pid = 0;

// @ Gus: bm functions
/*
static
bool bm_mpsc_oq_enqueue(bm_op_t op) {
	bm_op_t* op_ptr = malloc(sizeof(bm_op_t));
	*op_ptr = op;
	return mpscq_enqueue(bm_mpsc_oq, op_ptr);
}
*/
int get_and_set_config_from_file() {
    static char* filename = "./bm_config.txt";
    FILE* bm_config_fptr = fopen(filename, "r");
    if (bm_config_fptr == NULL) {
        fprintf(stderr, "%s does NOT exist or it is Wrong.\n", filename);
        return -1;
    }

    char line[50];
    fgets(line, 50, bm_config_fptr);
    bm_type = atoi(line);                   //  Queue type
    fgets(line, 50, bm_config_fptr);
    BM_MPSC_OQ_CAP = atoi(line);            //  Queue length
    fgets(line, 50, bm_config_fptr);
    bm_process_op_type = atoi(line);        //  Process type
    fgets(line, 50, bm_config_fptr);
    SPIN_TIME = atoi(line);                 //  Spin time
    fclose(bm_config_fptr);
    return 0;
}



void bm_init() {

    pid = getpid();
    int rc = get_and_set_config_from_file();
	if (rc < 0 || bm_type == BM_NONE) return;
    srand(time(NULL));
	fprintf(stderr, "----------------------->GUS: Init Benchmarking, %d, %d\n", bm_type, BM_MPSC_OQ_CAP);
	switch(bm_type) {
    	case BM_NONE: {
    		//returns if BM_NONE is chosen above
            ;
    	} break;
    	case BM_PRINT: {
    		;
    	} break;
    	case BM_DIRECT_FILE: {
    		bm_output_fd = open(bm_output_filename, 
    						 O_WRONLY | O_CREAT | O_TRUNC,
    						 S_IRUSR, S_IWUSR);
    	} break;
    	case BM_TO_QUEUE: {
    		bm_oq_init(&bm_oq);
    	} break;
    	case BM_TO_LOCK_FREE_QUEUE: {
            // {
            //     bm_mpsc_oq = mpscq_create(NULL, BM_MPSC_OQ_CAP);
            // }
            // {
            //     size_t buf_len = BM_MPSC_OQ_CAP * sizeof(bm_op_t);
            //     buf = malloc(buf_len);
            //     memset(buf, 0, buf_len);
            //     ringbuf_get_sizes(MAX_WORKERS, &ringbuf_obj_size, NULL);
            //     bm_ringbuf = malloc(ringbuf_obj_size);
            //     memset(bm_ringbuf, 0, ringbuf_obj_size);
            //     ringbuf_setup(bm_ringbuf, MAX_WORKERS, buf_len);
            //     w1 = ringbuf_register(bm_ringbuf, 0);
            // }
            {
                // @ Gus: plus one extra for the necessary dummy element
                fprintf (stderr,"QUEUE LENGTH: %d\n", BM_MPSC_OQ_CAP);
                re = malloc( sizeof(struct lfds711_ringbuffer_element) * (BM_MPSC_OQ_CAP + 1 ));
                lfds711_ringbuffer_init_valid_on_current_logical_core( &rs, re, BM_MPSC_OQ_CAP +1, NULL );
            }
    	} break;
    	case BM_TO_ZEROMQ: {
		    zmq_context = zmq_ctx_new ();
		    zmq_sender = zmq_socket (zmq_context, ZMQ_PUB);
		    int rc = zmq_bind(zmq_sender, "tcp://127.0.0.1:5555");
		    fprintf (stderr, "Started zmq server...\n");
            return;
    	} break;
        case BM_TO_ZEROMQ_X: {
    		zmq_context = zmq_ctx_new ();
		    zmq_sender = zmq_socket (zmq_context, ZMQ_XPUB);
		    int rc = zmq_connect(zmq_sender, "tcp://127.0.0.1:5555");
            int err = zmq_errno();
            
            if(rc==0){
                fprintf(stderr, "Error: %s .\n", zmq_strerror(err));
            }
            assert(rc==0);
		    fprintf (stderr, "Started zmq server as Extended PUB-SUB...\n");
            return;
    	} break;
    }

    init_shards();

}

static
void bm_write_line_op(int fd, bm_op_t op) {
	size_t str_buffer_length = 3 + 10;
	char* str_buffer = malloc(str_buffer_length);
	sprintf(str_buffer, "%d %"PRIu64"\n", op.type, op.key_hv);
	write(fd, str_buffer, strlen(str_buffer));
	free(str_buffer);
}

static
void bm_write_op_to_oq(bm_oq_t* oq, bm_op_t op) {
	bm_oq_item_t* item = malloc_oq_item();
	item->op = op;
	bm_oq_push(oq, item);
}

void bm_process_op(bm_op_t op) {
    switch(bm_process_op_type) {
        case BM_PROCESS_DUMMY: {
            ;
        } break;
        case BM_PROCESS_ADD: {
            random_accum += rand();
        } break;
        case BM_PROCESS_SPIN: {
            struct timeval t1, t2;
            gettimeofday(&t1, NULL);
            double elapsed = 0;
            do {
                gettimeofday(&t2, NULL);
                elapsed = t2.tv_usec - t1.tv_usec;
            } while(elapsed < SPIN_TIME);
        } break;
        case BM_PROCESS_PRINT: {
            fprintf(stderr, "type: %d, key: %"PRIu64"\n", op.type, op.key_hv);
        } break;
    }
    //sprintf("Key: %25"PRIu64" Op: %2d\n", op.key_hv, op.type);

    uint64_t *obj = malloc(sizeof(uint64_t));
    *obj = op.key_hv;

    shards_process_object( obj);

}

static
void bm_consume_ops() {
	switch(bm_type) {
		case BM_NONE: {
    		;
    	} break;
    	case BM_PRINT: {
    		;
    	} break;
    	case BM_DIRECT_FILE: {
 			;
    	} break;
    	case BM_TO_QUEUE: {
    		bm_oq_item_t* item = bm_oq_pop(&bm_oq);
			while(NULL != item) {
				bm_process_op(item->op);
				free_oq_item(item);
				item = bm_oq_pop(&bm_oq);
			}
    	} break;
    	case BM_TO_LOCK_FREE_QUEUE: {
            // {
            //     void* item = mpscq_dequeue(bm_mpsc_oq);
            //     while(NULL != item) {
            //         bm_op_t* op_ptr = item;
            //         bm_process_op(*op_ptr);
            //         free(op_ptr);
            //         item = mpscq_dequeue(bm_mpsc_oq);
            //     }    
            // }
    		// {
      //           size_t len, woff;
      //           len = ringbuf_consume(bm_ringbuf, &woff);
      //           size_t n_op = len/sizeof(bm_op_t);
      //           for (int i = 0; i < n_op; ++i) {
      //               bm_op_t op;
      //               memcpy(&op, &buf[woff], sizeof(bm_op_t));
      //               bm_process_op(op);
      //               woff += sizeof(bm_op_t);
      //           }
      //           ringbuf_release(bm_ringbuf, len);
      //       }
            {
                void* item;
                int rv = lfds711_ringbuffer_read( &rs, &item, NULL );
                while(1 == rv) {
                    bm_op_t* op_ptr = item;
                    bm_process_op(*op_ptr);
                    free(op_ptr);
                    rv = lfds711_ringbuffer_read( &rs, &item, NULL );
                }
            }
    	} break;
    	case BM_TO_ZEROMQ: {
    		;
    	} break;
        case BM_TO_ZEROMQ_X: {
    		;
    	} break;
	}
}

// @ Gus: LIBEVENT
static
struct event_base* bm_event_base;

static
void bm_clock_handler(evutil_socket_t fd, short what, void* args) {
	// fprintf(stderr, "Tick\n");
	bm_consume_ops();
}

static
void bm_libevent_loop() {
	bm_event_base = event_base_new();

	struct event* timer_event = event_new(bm_event_base,
								   -1,
								   EV_TIMEOUT | EV_PERSIST,
								   bm_clock_handler,
								   NULL);
	struct timeval t = {2, 0};
    event_add(timer_event, &t);

	event_base_dispatch(bm_event_base);
}

void* bm_loop_in_thread(void* args) {
	bm_output_fd = open(bm_output_filename, 
						O_WRONLY | O_CREAT | O_TRUNC,
						S_IRUSR | S_IWUSR);
	bm_libevent_loop();
	// while(true) bm_consume_ops();
    fprintf(stderr, "Accumulated Random Value: %d\n", random_accum);
	return NULL;
}

void bm_record_op(bm_op_t op) {
    char* command = op.type == BM_READ_OP ? "GET" : "SET";
    switch(bm_type) {
        case BM_NONE: {
            ;
        } break;
        case BM_PRINT: {
            fprintf(stderr, "----------------------->GUS: PROCESS %s COMMAND WITH KEY HASH: %"PRIu64"\n", command, op.key_hv);
        } break;
        case BM_DIRECT_FILE: {
            bm_write_line_op(bm_output_fd, op);
        } break;
        case BM_TO_QUEUE: {
            bm_write_op_to_oq(&bm_oq, op);
        } break;
        case BM_TO_LOCK_FREE_QUEUE: {
            // {
            //     bm_mpsc_oq_enqueue(op);    
            // }
            
            // {
            //     ssize_t off = ringbuf_acquire(bm_ringbuf, w1, sizeof(bm_op_t));
            //     memcpy(&buf[off], &op, sizeof(bm_op_t));
            //     ringbuf_produce(bm_ringbuf, w1);
            // }
            {
                bm_op_t* op_ptr = malloc(sizeof(bm_op_t));
                *op_ptr = op;
                lfds711_ringbuffer_write( &rs, 
                                        op_ptr, 
                                        NULL, 
                                        &overwrite_occurred_flag, 
                                        NULL, 
                                        NULL );
                if( overwrite_occurred_flag == LFDS711_MISC_FLAG_RAISED ) {
                    fprintf(stderr, "----------------------->GUS: OVERRIDED ELEMENT IN RING BUFFER\n");
                }
            }
        } break;
        case BM_TO_ZEROMQ: {
            // fprintf(stderr, "sending op: %d, hv: %"PRIu64"\n", op.type, op.key_hv);
            //op.pid = pid;
            //printf("Operation : %s Object Hash: %22"PRIu64" PID: %7d\n", command, op.key_hv, op.port);
            zmq_send(zmq_sender, &op, sizeof(bm_op_t), ZMQ_DONTWAIT);
        } break;
        case BM_TO_ZEROMQ_X: {
            // fprintf(stderr, "sending op: %d, hv: %"PRIu64"\n", op.type, op.key_hv);
            //op.pid = pid;
            //printf("Operation : %s Object Hash: %22"PRIu64" PORT: %7d\n", command, op.key_hv, op.port);
            zmq_send(zmq_sender, &op, sizeof(bm_op_t), ZMQ_DONTWAIT);
        } break;
    }
}
/*
static
void bm_record_read_op(char* key, size_t key_length) {
	bm_op_t op = {BM_READ_OP, hash(key, key_length)};
    switch(bm_type) {
    	case BM_NONE: {
    		;
    	} break;
    	case BM_PRINT: {
    		fprintf(stderr, "----------------------->GUS: PROCESS GET COMMANDD WITH KEY: %s (%"PRIu64")\n", key, op.key_hv);
    	} break;
    	case BM_DIRECT_FILE: {
 			bm_write_line_op(bm_output_fd, op);
    	} break;
    	case BM_TO_QUEUE: {
    		bm_write_op_to_oq(&bm_oq, op);
    	} break;
    	case BM_TO_LOCK_FREE_QUEUE: {
    		bm_mpsc_oq_enqueue(op);
    	} break;
    	case BM_TO_ZEROMQ: {
    		// fprintf(stderr, "sending op: %d, hv: %"PRIu64"\n", op.type, op.key_hv);
    		zmq_send(zmq_sender, &op, sizeof(bm_op_t), ZMQ_DONTWAIT);
    	} break;
    }
}

static
void bm_record_write_op(char* command, char* key, size_t key_length) {
	bm_op_t op = {BM_WRITE_OP, hash(key, key_length)};
	switch(bm_type) {
		case BM_NONE: {
    		;
    	} break;
    	case BM_PRINT: {
    		fprintf(stderr, "----------------------->GUS: PROCESS %s COMMANDD WITH KEY: %s (%"PRIu64")\n", command, key, op.key_hv);
    	} break;
    	case BM_DIRECT_FILE: {
    		bm_write_line_op(bm_output_fd, op);
    	} break;
    	case BM_TO_QUEUE: {
    		bm_write_op_to_oq(&bm_oq, op);
    	} break;
    	case BM_TO_LOCK_FREE_QUEUE: {
    		bm_mpsc_oq_enqueue(op);
    	} break;
    	case BM_TO_ZEROMQ: {
    		// fprintf(stderr, "sending op: %d, hv: %"PRIu64"\n", op.type, op.key_hv);
    		zmq_send(zmq_sender, &op, sizeof(bm_op_t), ZMQ_DONTWAIT);
    	} break;
    }
}
*/