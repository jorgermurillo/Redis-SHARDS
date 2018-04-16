# Key-Value store benchmark utilities

## Dependencies

* ZeroMQ, http://zeromq.org/

## How do I compile the lib and make it available to get linked?
gcc -std=gnu11 -c k_v_benchmark.c -o k_v_benchmark.o

ar rcs libkvbenchmark.a k_v_benchmark.o

sudo cp libkvbenchmark.a /usr/local/lib

## op_consumer_main.c
It is a subscriber process which receives operations from another process.

gcc -std=gnu11 op_consumer_main.c -lzmq -o consumer
