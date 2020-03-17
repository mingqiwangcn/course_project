#include <iostream>
#include <fstream>
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

void read_keys(char* path, vector<string>& keys) {
    ifstream key_file(path);
    string line;
    while (getline(key_file, line)) {
       keys.push_back(line); 
    }
    key_file.close();
}

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

void import_by_keys(int db_no, DB* export_db, DB* import_db, char* key_file_path) {

    printf("start to import from db %d\n", db_no);

    int batch_size = 200;
    vector<string> input_keys;
    read_keys(key_file_path, input_keys);
    vector<string> batch_keys;
    int total = input_keys.size();
    int num_batch = (total / batch_size) + (total % batch_size > 0); 
    int num = 0;
    high_resolution_clock::time_point t1 = high_resolution_clock::now();
    high_resolution_clock::time_point t2;
    double total_time = 0;
    duration<double> time_used;
    size_t index_key_count = input_keys.size();
   
    int total_num = 0;
    
    for (int i = 0; i < index_key_count; i++) {
        string map_key = input_keys[i];
        batch_keys.push_back(map_key);
        if (batch_keys.size() == batch_size) {
            vector<DataItem*>* data_items = db_get(export_db, &batch_keys);
            total_num += data_items->size();
            do_import(import_db, data_items);
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
        vector<DataItem*>* data_items = db_get(export_db, &batch_keys);
        total_num += data_items->size();
        do_import(import_db, data_items);
        batch_keys.clear();
    
        t2 = high_resolution_clock::now();
        time_used = duration_cast<duration<double>>(t2 - t1);
        total_time += time_used.count(); 
        num += 1;
        printf("time %.2f toltal time %.2f %d/%d\n", time_used.count(), total_time, num, num_batch);
    }
    printf("import from db %d %d items\n", db_no, total_num);
}

int export_import(int argc, char *argv[]) {
    DBOpt opt_export;
    opt_export.max_index_buffer_size = 1;
    opt_export.max_data_buffer_size = 1;

    DBOpt opt_import;
    opt_import.max_index_buffer_size = 500;
    opt_import.max_data_buffer_size = 1000; 
    
    char import_db_path[] = "/home/cc/data/part_10/part_10-632";
    DB* db_import = db_open(import_db_path, &opt_import);
    
    for (int i = 1; i <= 10; i++) {
        char export_db_path[300];
        char export_key_file_path[300]; 
        sprintf(export_db_path, "/home/cc/md0/passage_db/passage_db_%d", i);
        sprintf(export_key_file_path, "/home/cc/code/course_project/keys_realloc/part_%d_no_keep_keys.txt", i);
        
        DB* db_export = db_open(export_db_path, &opt_export);

        import_by_keys(i, db_export, db_import, export_key_file_path);

        printf("Now  %d data items and %lu index items\n", 
               db_import->total_items, db_import->index_keys->size());
        
        db_close(db_export); 
    }

    db_close(db_import);

}

int main(int argc, char *argv[]) {
    try { 
        export_import(argc, argv);
    } catch (const char* str) {
        cout << "error " << str << endl;
    }

}
