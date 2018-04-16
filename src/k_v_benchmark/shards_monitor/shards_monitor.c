#include "shards_monitor.h"
#include "SHARDS.h"


 
// SHARDS Stuff
unsigned int epoch =1;
int number_of_objects=0;
int PER_EPOCH_OBJECT_LIMIT= 1000000;     // epoch length
// MRC stuff
char* mrc_path = "./Results/";
char file_name[40];
FILE *mrc_file;

//static SHARDS *shards_array[2];
 SHARDS *shards;

int max_set_size = 16000;
double R_initialize = 0.1;
int bucket_size = 10;



int shards_config() {
    static char* filename = "./shards_config.txt";
    FILE* shards_config_file = fopen(filename, "r");
    if (shards_config_file == NULL) {
        fprintf(stderr, "%s does NOT exist or it is Wrong.\n", filename);
        return -1;
    }

    char line[50];
    fgets(line, 50, shards_config_file);
    max_set_size = atoi(line);                    //  Maximum number of individual objects in the Shards data structure

    fgets(line, 50, shards_config_file);
    R_initialize = strtod(line, NULL);            //  Initial value for R (THIS IS A FLOATING POINT VALUE)
    fprintf(stderr, "R value: %f\n", R_initialize);


    fgets(line, 50, shards_config_file);
    bucket_size = atoi(line);                     //  Size of bucket in reuse distance histogram

    fgets(line, 50, shards_config_file);
    PER_EPOCH_OBJECT_LIMIT = atoi(line);          //  Length of epoch

    fclose(shards_config_file);
    return 0;
}

void init_shards(){

        int rc = shards_config();

        if (rc < 0 ){
            fprintf(stderr,"Default values used for SHARDS config.\n");
            
        } 
        
        shards = SHARDS_fixed_size_init_R(max_set_size, R_initialize, bucket_size, Uint64);
        printf("SHARDS initialized.\n");
        
}

void calculate_miss_rate_curve(void){

        GHashTable *mrc = MRC_empty(shards);

        if(mrc==NULL){
            fprintf(stderr,"NO MRC FOR YOU \n" );
            return;
        }

        GList *keys = g_hash_table_get_keys(mrc);

        keys = g_list_sort(keys, (GCompareFunc) intcmp);
        GList *first = keys;

        //File
        snprintf(file_name,40,"%sMRC_epoch_%05d.csv",mrc_path, epoch);    
        mrc_file = fopen(file_name,"w");
        while(keys!=NULL){
            fprintf(mrc_file,"%7d,%1.7f\n",*(int*)keys->data, *(double*)g_hash_table_lookup(mrc, keys->data) );            
            keys=keys->next;
        }

        fclose(mrc_file);
        g_list_free(first);
        g_hash_table_destroy(mrc);
        epoch+=1;
}

void shards_process_object(uint64_t *object){
        //printf("Shards address: %p\n", shards);
        //printf("Object address: %p\n", object);
        //printf("HASHED OBJECT: %"PRIu64"\n", *object);
        SHARDS_feed_obj(shards, object, sizeof(uint64_t));
        number_of_objects+=1;

        if(number_of_objects==PER_EPOCH_OBJECT_LIMIT){
            calculate_miss_rate_curve();
            number_of_objects=0;
        }

}