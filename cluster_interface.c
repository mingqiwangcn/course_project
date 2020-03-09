//#define PYTHON_INTERFACE

#ifdef PYTHON_INTERFACE
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#endif

#include <string.h>
#include <fstream>
#include "storage.h"

using namespace std;


extern void init_DBOpt(DBOpt& opt);
extern DB* db_open(char* path, DBOpt* opt);
extern vector<DataItem*>* db_get(DB*db, vector<string>* key_lst);
extern void db_put(DB* db, vector<DataItem*>* data_items);
extern void db_close(DB* db);
extern void join_path(char* full_path, char* path, char* file_name);
extern string get_opt(unordered_map<string, string>& options, string opt_name);

typedef struct DBCluster {
    vector<DB*>* db_lst;
    unordered_map<string, DB*>* key_map;    
} DBCluster;

DBCluster* db_cluster = NULL;

vector<string> get_db_lst(char* path) {
    char full_path[MAX_PATH_SIZE];
    char file_name[100] = "meta.cfg";
    join_path(full_path, path, file_name);
    ifstream cfg_file(full_path);
    string line;
    vector<string> db_lst;
    while (getline(cfg_file, line)) {
       db_lst.push_back(line); 
    }
    return db_lst;
}

void new_DBCluster() {
   db_cluster = (DBCluster*)malloc(sizeof(DBCluster));
   db_cluster->db_lst = new vector<DB*>();
   db_cluster->key_map = new unordered_map<string, DB*>(); 
}

void free_DBCluster() {
    delete db_cluster->db_lst;
    delete db_cluster->key_map;
    free(db_cluster);
    db_cluster = NULL;
}

void cluster_db_open(char* path, unordered_map<string, string>& options) {
    DBOpt opt;
    init_DBOpt(opt); 
    if (options.size() > 0) {
        unordered_map<string, string>::iterator itr;
        string opt_max_index_buffer_size = get_opt(options, "max_index_buffer_size");
        if (opt_max_index_buffer_size != "") {
            opt.max_data_buffer_size = atoi(opt_max_index_buffer_size.c_str());
        }
        string opt_max_data_buffer_size = get_opt(options, "max_data_buffer_size");
        if (opt_max_data_buffer_size != "") {
            opt.max_data_buffer_size = atoi(opt_max_data_buffer_size.c_str());
        }
    }
    
    new_DBCluster();
    vector<string> part_name_lst = get_db_lst(path);
    int db_count = part_name_lst.size();
    unordered_map<string, IndexItem*>::iterator idx_itr;
    for (int i = 0; i < db_count; i++) {
        string part_db_path = path + part_name_lst[i];
        char db_path[MAX_PATH_SIZE];
        strcpy(db_path, part_db_path.c_str()); 
        DB* db = db_open(db_path, &opt);
        db_cluster->db_lst->push_back(db);
        for (idx_itr = db->index_map->begin(); idx_itr != db->index_map->end(); ++idx_itr) {
            string index_key = idx_itr->first;
            (*(db_cluster->key_map))[index_key] = db;
        }
    }
}

vector<vector<double>> cluster_db_get(vector<string>& key_lst) {
    unordered_map<string, vector<double>> result_map;
    unordered_map<DB*, vector<string>> db_key_map;
    unordered_map<DB*, vector<string>>::iterator itr;
    int key_count = key_lst.size();
    for (int i = 0; i < key_count; i++) {
        string key = key_lst[i];
        DB* db = (*(db_cluster->key_map))[key];
        itr = db_key_map.find(db);
        if (itr == db_key_map.end()) {
            vector<string> part_keys;
            part_keys.push_back(key);
            db_key_map[db] = part_keys;
        } else {
            itr->second.push_back(key);
        }
    }
    vector<DataItem*>::iterator data_item_itr;
    for (itr = db_key_map.begin(); itr != db_key_map.end(); ++itr) {
        DB* db = itr->first;
        vector<DataItem*>* data_items = db_get(db, &(itr->second));
        for (data_item_itr = data_items->begin(); data_item_itr != data_items->end(); ++data_item_itr) {
            DataItem* item = *data_item_itr;
            double* double_values = (double*)item->value;
            int double_item_size = item->data_size / sizeof(double);
            vector<double> float_vec(double_values, double_values + double_item_size);
            result_map[string(item->key)] = float_vec;

            free(item->key);
            free(item->value);
            free(item);
        }
        delete data_items;
    }
    
    vector<vector<double>> data_lst;
    for (int i = 0; i < key_lst.size(); i++) {
        data_lst.push_back(result_map[key_lst[i]]); 
    } 
    return data_lst;
}

void cluster_db_close() {
    if (db_cluster != NULL) {
        int db_count = db_cluster->db_lst->size();
        for (int i =0; i< db_count; i++) {
            db_close((*db_cluster->db_lst)[i]);
        }
        free_DBCluster();
    }
}

#ifdef PYTHON_INTERFACE
PYBIND11_MODULE(db_storage, m) {
    m.doc() = "db_cluster"; // optional module docstring
    m.def("open", &cluster_db_open, "open database");
    m.def("get", &cluster_db_get, "get data");
    m.def("close", &cluster_db_close, "clode database");
}
#endif


