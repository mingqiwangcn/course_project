//#define PYTHON_INTERFACE

#ifdef PYTHON_INTERFACE
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#endif

#include <string.h>
#include "storage.h"

using namespace std;

extern void init_DBOpt(DBOpt& opt);
extern DB* db_open(char* path, DBOpt* opt);
extern vector<DataItem*>* db_get(DB*db, vector<string>* key_lst);
extern void db_put(DB* db, vector<DataItem*>* data_items);
extern void db_close(DB* db);
extern string get_opt(unordered_map<string, string>& options, string opt_name);


DB* g_db = NULL;

void interop_db_open(char* path, unordered_map<string, string>& options) {
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
    g_db = db_open(path, &opt); 
}

vector<vector<double>> interop_db_get(vector<string>& key_lst) {
    vector<string>* data_key_lst = &key_lst;
    vector<DataItem*>* data_items = db_get(g_db, data_key_lst);
    vector<vector<double>> data_lst;
    vector<DataItem*>::iterator itr;
    for (itr = data_items->begin(); itr != data_items->end(); ++itr) {
        DataItem* item = *itr;
        double* double_values = (double*)item->value;
        int double_item_size = item->data_size / sizeof(double);
        vector<double> float_vec(double_values, double_values + double_item_size);
        data_lst.push_back(float_vec);
        
        free(item->key);
        free(item->value);
        free(item);
    }
    delete data_items;
    return data_lst;
}

void interop_db_put(vector<pair<string, vector<double>>>& data) {
    vector<DataItem*> data_items;
    vector<pair<string, vector<double>>>::iterator itr;
    for (itr = data.begin(); itr != data.end(); ++itr) {
        string map_key = itr->first;
        vector<double> double_values = itr->second;
        DataItem* item = (DataItem*)malloc(sizeof(DataItem));
        data_items.push_back(item);
        item->key_size = strlen(map_key.c_str()) + 1;
        item->key = (char*)malloc(item->key_size);
        memcpy(item->key, map_key.c_str(), item->key_size);
        item->data_size = double_values.size() * sizeof(double);
        item->value = (char*)malloc(item->data_size);
        char* byte_values = (char*)(double_values.data());
        memcpy(item->value, byte_values, item->data_size); 
    }
    db_put(g_db, &data_items);
    int i = 0;
    int item_count = data_items.size();
    while (i < item_count) {
        DataItem* item = data_items[i];
        free(item->key);
        free(item->value);
        free(item);
        i += 1;
    }
}

unordered_map<string, int> interop_db_stats() {
    unordered_map<string, int> map_stats;
    map_stats["total_items"] = g_db->total_items;
    map_stats["total_index_pages"] = g_db->total_index_pages;
    map_stats["total_data_pages"] = g_db->total_data_pages;
    return map_stats;
}

void interop_db_close() {
    if (g_db != NULL) {
        db_close(g_db);
        g_db = NULL;
    }
}

#ifdef PYTHON_INTERFACE
PYBIND11_MODULE(db_storage, m) {
    m.doc() = "db_storage"; // optional module docstring
    m.def("open", &interop_db_open, "open database");
    m.def("get", &interop_db_get, "get data");
    m.def("put", &interop_db_put, "put data");
    m.def("stats", &interop_db_stats, "stat data");
    m.def("close", &interop_db_close, "clode database");
}
#endif


