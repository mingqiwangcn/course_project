#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <string>
#include <algorithm>
#include "storage.h"
using namespace std;

int pct_95 = 645168;  //data item that need more space than this would be in ssd disk

extern DB* db_open(char* path, DBOpt* opt);
extern vector<DataItem*>* db_get(DB*db, vector<string>* key_lst);
extern void db_close(DB* db);

void get_db_stat(char* db_path, vector<string>& keep_keys, vector<string>& no_keep_keys) {
    DBOpt opt;
    opt.max_index_buffer_size = 1;
    opt.max_data_buffer_size = 1;
    DB* db = db_open(db_path, &opt);
    int item_count =  db->index_keys->size();
    for (int i = 0; i < item_count; i++) {
        string key = (*db->index_keys)[i];
        IndexItem* idx_item = (*(db->index_map))[key];
        int needed_space = sizeof(int) + idx_item->key_size + sizeof(int) + idx_item->data_size;
        if (needed_space > pct_95) {
            no_keep_keys.push_back(key);
        } else {
            keep_keys.push_back(key);
        }
    }
    db_close(db);
}

void allocate_keys(vector<string>& all_keys, int num_parts, vector<vector<string>>& key_parts) {
    int i = 0;
    int key_count = all_keys.size();
    int batch_size = key_count / num_parts;
    vector<string> keys;
    while (i < key_count) {
       keys.push_back(all_keys[i]);
       if (keys.size() == batch_size) {
           key_parts.push_back(keys);
           keys.clear();
       }
       i += 1;
    }
    if (keys.size() > 0) {
        key_parts[num_parts-1].insert(key_parts[num_parts-1].end(), keys.begin(), keys.end());
    }
}


int main(int argc, char *argv[]) {
    char path[300];
    char file_name[100];
    vector<string> keep_keys;
    vector<string> no_keep_keys;
    int i = 0;
    int j = 0;
    int item_count = 0;
    for (i = 1; i <= 9; i++) {
        sprintf(path, "/home/cc/md0/passage_db/passage_db_%d/", i);
        
        get_db_stat(path, keep_keys, no_keep_keys);

        sprintf(file_name, "keys_realloc/part_%d_keep_keys.txt", i);
        ofstream keep_keys_file(file_name);
        j = 0;
        item_count = keep_keys.size();
        for (j = 0 ; j < item_count; j++) {
            keep_keys_file << keep_keys[j] << endl;
        }
        keep_keys_file.close();

        sprintf(file_name, "keys_realloc/part_%d_no_keep_keys.txt", i);
        ofstream no_keep_keys_file(file_name);
        item_count = no_keep_keys.size();
        for (j = 0 ; j < item_count; j++) {
            no_keep_keys_file << no_keep_keys[j] << endl;
        }
        no_keep_keys_file.close();
        
        keep_keys.clear();
        no_keep_keys.clear();
    }

    sprintf(path, "/home/cc/md0/passage_db/passage_db_%d/", 10);
   
    get_db_stat(path, keep_keys, no_keep_keys);
    vector<vector<string>> key_parts;
    allocate_keys(keep_keys, 9, key_parts);  

    for (i = 1; i <= 9; i++) {
        sprintf(file_name, "keys_realloc/part_10_keep_keys_allocated_%d.txt", i);
        ofstream key_file(file_name);
        for (j = 0; j < key_parts[i-1].size(); j++) {
            key_file << key_parts[i-1][j] << endl;    
        }
        key_file.close();
    }

    sprintf(file_name, "keys_realloc/part_%d_no_keep_keys.txt", 10);
    ofstream no_keep_keys_file_10(file_name);
    item_count = no_keep_keys.size();
    for (j = 0 ; j < item_count; j++) {
        no_keep_keys_file_10 << no_keep_keys[j] << endl;
    }
    no_keep_keys_file_10.close();
}
