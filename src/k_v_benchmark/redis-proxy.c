#include <zmq.h>
#include <assert.h>


int main (int argc, char** argv){
    printf("Hello...\n");
    int rc = 0;
    void *context = zmq_ctx_new ();

    //  This is where the Redis instances sit
    void *frontend = zmq_socket (context, ZMQ_XSUB);
    rc = zmq_bind (frontend, "tcp://127.0.0.1:5555");
    assert(rc==0);

    //  This is our public endpoint for subscribers (The monitoring functions)
    void *backend = zmq_socket (context, ZMQ_XPUB);
    rc = zmq_bind (backend, "tcp://127.0.0.1:5556");
    assert(rc==0);

    printf("Redis-proxy started...\n");
    //  Run the proxy until the user interrupts us
    zmq_proxy (frontend, backend, NULL);
    
    zmq_close (frontend);
    zmq_close (backend);
    zmq_ctx_destroy (context);
    return 0;
}