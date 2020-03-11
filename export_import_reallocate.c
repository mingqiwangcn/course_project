#include <iostream>
#include <vector>
#include <unordered_map>
#include <string>
#include <chrono>
#include "storage.h"
using namespace std;
using namespace std::chrono;

extern DB* db_open(char* path, DBOpt* opt);
extern vector<DataItem*>* db_get(DB*db, vector<string>* key_lst);
extern void db_put(DB* db, vector<DataItem*>* data_items);
extern void db_close(DB* db);

void do_import(DB* new_db, vector<DataItem*>* data_items) {
    db_put(new_db, data_items);
    int N = data_items->size();
    for (int i = 0; i < N; i++) {
        DataItem* item = (*data_items)[i];
        free(item->key);
        free(item->value);
        free(item);
    }
    free(data_items);
}

int export_import(int argc, char *argv[]) {
    char* input_db = argv[1];
    char* output_db = argv[2];
    DBOpt opt_1;
    opt_1.max_index_buffer_size = 1;
    opt_1.max_data_buffer_size = 1;
    DB* db_1 = db_open(input_db, &opt_1);
    DBOpt opt_2;
    opt_2.max_index_buffer_size = 500;
    opt_2.max_data_buffer_size = 1000; 
    DB* db_2 = db_open(output_db, &opt_2);
    int batch_size = 500;
    vector<string> batch_keys;
    unordered_map<string, IndexItem*>:: iterator idx_itr;

    int total = db_1->index_map->size();
    int num_batch = (total / batch_size) + (total % batch_size > 0); 

    int num = 0;
    high_resolution_clock::time_point t1 = high_resolution_clock::now();
    high_resolution_clock::time_point t2;
    double total_time = 0;
    duration<double> time_used;
    size_t index_key_count = db_1->index_keys->size();
    for (int i = 0; i < index_key_count; i++) {
        string map_key = (*(db_1->index_keys))[i];
        batch_keys.push_back(map_key);
        if (batch_keys.size() == batch_size) {
            vector<DataItem*>* data_items = db_get(db_1, &batch_keys);
            do_import(db_2, data_items);
            batch_keys.clear();
            
            num += 1;
            t2 = high_resolution_clock::now();
            time_used = duration_cast<duration<double>>(t2 - t1);
            total_time += time_used.count();
            t1 = t2;
            printf("time %.2f toltal time %.2f %d/%d\n", time_used.count(), total_time, num, num_batch);

        }
    }

    if (batch_keys.size() > 0) {
        vector<DataItem*>* data_items = db_get(db_1, &batch_keys);
        do_import(db_2, data_items);
        batch_keys.clear();
    
        t2 = high_resolution_clock::now();
        time_used = duration_cast<duration<double>>(t2 - t1);
        total_time += time_used.count(); 
        num += 1;
        printf("time %.2f toltal time %.2f %d/%d\n", time_used.count(), total_time, num, num_batch);
    }

    db_close(db_1);
    db_close(db_2);

}

int main(int argc, char *argv[]) {
    try { 
        export_import(argc, argv);
    } catch (const char* str) {
        cout << "error " << str << endl;
    }

}
