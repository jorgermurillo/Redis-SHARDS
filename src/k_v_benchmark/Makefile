CC=gcc
CC2=g++
CFLAGS= -Wall  -I. `pkg-config --cflags --libs glib-2.0`
VERSION=-std=c11
ODIR=obj
AR=ar
ARFLGAS=rcs

libk_v_benchmark.a: k_v_benchmark.o shards_monitor/shards_monitor.o
	$(AR) $(ARFLAGS) ../libk_v_benchmark.a $^

k_v_benchmark.o: k_v_benchmark.c 
	$(CC) -g -c $(CFLAGS) $^ -o $@

shards_monitor/shards_monitor.o: shards_monitor/shards_monitor.c
	$(CC) -g -c $(CFLAGS) $^ -o $@

redis-proxy: redis-proxy.c
	$(CC) -g  -lzmq -Wall $^ -o $@

ZeroMQ_SHARDS: ZeroMQ_SHARDS.c
	$(CC) -g $(CFLAGS)  -lzmq -lSHARDS -Wall $^ -o $@

ZeroMQ_SHARDS2: ZeroMQ_SHARDS2.c
	$(CC) -g $(CFLAGS)  -lzmq -lSHARDS -Wall $^ -o $@