#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <zmq.h>
#include <assert.h>
#include "k_v_benchmark.h"
#include <inttypes.h>
#include <glib.h>
#include "SHARDS.h"



void print_MRCs( GHashTable *shards_table, int current_epoch, int totalmemory ){

            GList *shards_list = g_hash_table_get_keys(shards_table);
            shards_list = g_list_sort(shards_list, (GCompareFunc) intcmp);
            GList *first = shards_list;
            SHARDS *shards_tmp = NULL;
            
            int frequency = 0;
            const char* mrc_path = "./ZeroMQ_Results/";
            int lenght_name_file = 49;
            char file_name[lenght_name_file];
            unsigned int number_of_files = g_list_length(shards_list);
            FILE *mrc_file;
            //Obtain the size of the variable totalmemory in bytes
            int size = snprintf( NULL, 0, "%d",  totalmemory);
            char totalmemory_str[size+2];
            snprintf(totalmemory_str,size+2,"%d ",totalmemory);

            char command[  64 + size + 1  + (lenght_name_file*number_of_files)  ] ;
            snprintf(command, 64 ,"python3.6  ~/optimization/Hill_Climbing/__init__.py "); 

            
            strcat(command, totalmemory_str);
            //strcat(command, "python3.6 ");

            while(shards_list!=NULL){

                
                // This loop goes through each SHARDS structure
                shards_tmp = (SHARDS* ) g_hash_table_lookup(shards_table, shards_list->data);
                frequency = shards_tmp->total_objects;
                GHashTable *mrc = (GHashTable*) MRC_empty( shards_tmp)   ;
                
                if(mrc!=NULL){ //Check if there IS a MRC. If a particular SHARDS astructure doesn't have any object referenes stored, the MRC will return NULL.
                    GList *keys = g_hash_table_get_keys(mrc);
                    keys = g_list_sort(keys, (GCompareFunc) intcmp);
                    GList *first_key = keys;
                    int port_tmp = *(int*)(shards_list->data);

                    // shards_list->data is the number of the port of the redis instance
                    snprintf(file_name,lenght_name_file,"%sMRC_epoch_%05d_port_%d.csv",mrc_path ,current_epoch , port_tmp);    
                    strcat(command, file_name);
                    strcat(command, " ");
                    printf("File path: %s\n", file_name);
                    mrc_file = fopen(file_name, "w");

                    if(mrc_file==NULL){
                        fprintf(stderr, "Error opening the file: %s.\n", strerror(errno));

                    }
                    fprintf(mrc_file,"port:%d\n",port_tmp );
                    fprintf(mrc_file,"Request_frequency:%d\n",frequency );
                    
                    

                    while(keys!=NULL){
                        //This loop writes the MRCs
                        //printf("%d,%1.7f\n",*(int*)keys->data, *(double*)g_hash_table_lookup(mrc, keys->data) );
                        fprintf(mrc_file,"%7d,%1.7f\n",*(int*)keys->data, *(double*)g_hash_table_lookup(mrc, keys->data) );

                        keys=keys->next; 
                    }

                    fclose(mrc_file);
                    g_list_free(first_key);
                    g_hash_table_destroy(mrc);
                }


                shards_list = shards_list->next;

            }
            strcat(command, "\n");
            g_list_free(first);
            first = NULL;
            printf("Working directory: ");
            system("pwd");
            printf(command);
            system(command);



            return;

}

/*
*
*
*
*/

int main(int argc, char** argv){
	
	printf("PID: %d\n", getpid());
    /*
        argv[1] = max objects per epoch
        argv[2] = Value of R for each SHARDS data structure
        argv[3] = SHARDS set size
        argv[4] = Total memory among the Redis instances (in bytes)
    */


	void *context = zmq_ctx_new ();
    void *subscriber = zmq_socket (context, ZMQ_SUB);
    int rc = zmq_connect (subscriber, "tcp://localhost:5556");
    assert (rc == 0);

    int zeromq_socket_opt_value = 0;
  
    //rc = zmq_setsockopt (subscriber, ZMQ_SUBSCRIBE, filter, strlen (filter));
    rc = zmq_setsockopt (subscriber, ZMQ_SUBSCRIBE, NULL, 0);			// This operation allows us to subscribe to a topic. Right now we are subscribed to all topics.
    assert (rc == 0);
    rc =  zmq_setsockopt (subscriber, ZMQ_RCVHWM,&zeromq_socket_opt_value , sizeof(int)); 

    // SHARDS stuff
    GHashTable *shards_table = g_hash_table_new_full(g_int_hash, g_int_equal, NULL, ( GDestroyNotify )SHARDS_free);


    int epoch_limit = strtol(argv[1],NULL,10);//1000000;
    int current_epoch = 0; 
    
    double R = strtod(argv[2],NULL);
    unsigned int shards_set_size = strtol(argv[3],NULL,10);
    unsigned int bucket_size = 10;
    int total_memory = strtol(argv[4],NULL,10);


    bm_op_t op = {BM_READ_OP, 0, 0}; //dummy obj to receive the tace from Redis

    uint64_t *obj = NULL;

    int cnt = 0;
    // We use this pointer as the key for each SHARDS data structure. It is the TCP port number that the Redis instance is listening to.
    int *Redis_instance = NULL;

	while( 1 ){
        obj = (uint64_t*) malloc(sizeof(uint64_t));
        zmq_recv(subscriber,&op , sizeof(op), 0);
        // This two lines are for lookiing at the incoming requests
        const char* command = op.type == BM_READ_OP ? "GET" : "SET";
        //printf("Object Hash: %20"PRIu64"  OP: %s PORT:%10d\n",  op.key_hv, command, op.port);
        
        *obj = op.key_hv;

        SHARDS *shards_tmp = (SHARDS*) g_hash_table_lookup(shards_table, &(op.port));

        if(shards_tmp == NULL){

            shards_tmp = SHARDS_fixed_size_init_R(shards_set_size, R, bucket_size, Uint64);
            SHARDS_feed_obj(shards_tmp, obj, sizeof(uint64_t));
            Redis_instance = (int*) malloc(sizeof(int));
            // Insert the SHARDS structure to the table of SHARDS
            *Redis_instance = op.port;
            g_hash_table_insert(shards_table, Redis_instance, shards_tmp);
            Redis_instance = NULL;
            shards_tmp = NULL;

        }else{
            
            SHARDS_feed_obj(shards_tmp, obj, sizeof(uint64_t));
            shards_tmp = NULL;

        }


        cnt++;
        
        if(cnt==epoch_limit){
            fprintf(stderr, "Current epoch: %d.\n", current_epoch);
            print_MRCs(shards_table, current_epoch, total_memory);
            current_epoch++;
            cnt = 0;

        }
        

	}


}


