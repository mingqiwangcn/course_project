#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <string>
#include <chrono>
#include <algorithm>
#include "storage.h"
using namespace std;
using namespace std::chrono;

extern DB* db_open(char* path, DBOpt* opt);
extern vector<DataItem*>* db_get(DB*db, vector<string>* key_lst);
extern void db_close(DB* db);

void stat_db(char* db_path, vector<int>& item_spaces) {
    DBOpt opt;
    opt.max_index_buffer_size = 1;
    opt.max_data_buffer_size = 1;
    DB* db = db_open(db_path, &opt);
    int item_count =  db->index_keys->size();
    for (int i = 0; i < item_count; i++) {
        string key = (*db->index_keys)[i];
        IndexItem* idx_item = (*(db->index_map))[key];
        int needed_space = sizeof(int) + idx_item->key_size + sizeof(int) + idx_item->data_size;
        item_spaces.push_back(needed_space); 
    }
    db_close(db);
}

int main(int argc, char *argv[]) {
    vector<int> item_spaces;
    for (int i = 1; i <= 10; i++) {
        char path[300];
        sprintf(path, "/home/cc/data/part_%d/part_%d-2500/", i, i);
        stat_db(path, item_spaces);
    }
    
    ofstream stat_file("stat.txt");
    for (int i = 0; i < item_spaces.size(); i++) {
        stat_file << item_spaces[i] << endl;
    }
    stat_file.close();
    
    cout << "ok" << endl;

}
