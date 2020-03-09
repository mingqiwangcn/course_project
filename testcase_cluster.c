#include <string.h>
#include <iostream>
#include <vector>
#include "storage.h"
#include <unordered_map>
#include <random>
#include <utility>

extern void interop_db_open(char* path, unordered_map<string, string>& options);
extern vector<vector<double>> interop_db_get(vector<string>& key_lst);
extern void interop_db_put(vector<pair<string, vector<double>>>& data);
extern void interop_db_close();

void batch_put(char* key_prefix, int batch, int N, vector<string>& key_lst, unordered_map<string, 
    vector<double>>& data,std::uniform_real_distribution<>& dis, std::mt19937& gen) {
    vector<pair<string, vector<double>>> batch_data;
    int i = 0;
    int j = 0;
    for (i = 0; i < N; i++) {
        char key[100];
        sprintf(key, "%s-%d-%d", key_prefix, batch, i);
        string map_key(key);
        key_lst.push_back(map_key);
        int M =  rand() % 3  + 1;
        vector<double> value;
        for (j = 0; j < M; j++) {
            double item =  dis(gen);
            value.push_back(item);
        }
        data[map_key] = value;
        
        batch_data.push_back(std::make_pair(map_key, value));
    }
    interop_db_put(batch_data);
}

void sequential_get(vector<string>& key_lst, unordered_map<string, vector<double>>& data) {
    int i;
    int N = key_lst.size();
    for (i = 0; i < N; i++) {
        vector<string> qry_list;
        qry_list.push_back(key_lst[i]); 
        vector<vector<double>> query_result = interop_db_get(qry_list);
        string qry_key = key_lst[i];
        vector<double> item_1 = data[qry_key];
        vector<double> item_2 = query_result[0];
        if (item_1 != item_2) {
            throw "data is not equal";
        }
    }
}

void random_get(vector<string>& key_lst, unordered_map<string, vector<double>>& data) {
    int i;
    int j;
    int N = key_lst.size(); 
    for (i=0; i < 100; i++) {
        vector<string> query_key_lst;
        for (j = 0; j < 100; j++) {
            int pos = rand() % N;
            query_key_lst.push_back(key_lst[pos]);
        }
        vector<vector<double>> query_result = interop_db_get(query_key_lst);
        for (j =0; j < 100; j++) {
            string qry_key = query_key_lst[j];
            vector<double> item_1 = data[qry_key];
            vector<double> item_2 = query_result[j];
            if (item_1 != item_2) {
                throw "data is not euqal";
            }
        }
    }
}

void test_python_interface(char* path, char* key_prefix) {
    unordered_map<string, string> opt_map;
    interop_db_open(path, opt_map);
    srand(1);
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(1.0, 10.0);
    unordered_map<string, vector<double>> data;
    vector<string> key_lst;

    int num_batch = 3;
    int batch_size = 10000;
    int i;
    for (i = 0; i < num_batch; i++) {
        batch_put(key_prefix, i, batch_size, key_lst, data, dis, gen); 
    }

    interop_db_close();
    
    interop_db_open(path, opt_map);

    sequential_get(key_lst, data);
    
    random_get(key_lst, data); 

    std::cout << "OK" << std::endl;
    interop_db_close();
}


void test_put() {
    char path[100] = "/home/qmwang/code/course_project/example_db2";
    DBOpt* opt = NULL;
    DB* db = db_open(path, opt);  
    int N = 10000;
    int i = 0;
    int j = 0;
    srand(1);

    DataItem* p_items = (DataItem*)malloc(sizeof(DataItem)*N);
    vector<string> key_lst;
    vector<DataItem*> data_item_lst;
    unordered_map<string, DataItem*> item_map; 
    for (i = 0; i < N; i++) {
        char key[20];
        sprintf(key, "%d", i);
        int key_size = strlen(key) + 1;
        p_items[i].key = (char*)malloc(key_size);
        strcpy(p_items[i].key, key);
        string item_key(key); 
        key_lst.push_back(item_key);
        int M =  rand() % 2000 + 20;
        p_items[i].key_size = strlen(key) + 1;
        p_items[i].value = (char*)malloc(M);
        for (j = 0; j < M; j++) {
            p_items[i].value[j] = (rand() % 128);
        }
        p_items[i].value[M-1] = '\0';
        p_items[i].data_size = M;
        data_item_lst.push_back(p_items + i);
        item_map[item_key] = p_items + i;
    }
    
    db_put(db, &data_item_lst);
    db_close(db);
    db = NULL;
    // open
    db = db_open(path, opt);
    std::cout << "total items: " << db->total_items << std::endl;
   
    for (i=0; i < 100; i++) {
        vector<string> query_key_lst;
        for (j = 0; j < 100; j++) {
            int pos = rand() % N;
            query_key_lst.push_back(key_lst[pos]);
        }
        vector<DataItem*>* query_result = db_get(db, &query_key_lst);
        for (j = 0; j < 100; j++) {
            string map_key = query_key_lst[j];
            DataItem* item_1 = item_map.find(map_key)->second;
            DataItem* item_2 = (*query_result)[j];
            if (strcmp(item_1->key, item_2->key) != 0) {
                throw "key not euqal";
            }
            if (item_1->data_size != item_2->data_size) {
                throw "size not equal";
            }
            if (memcmp(item_1->value, item_2->value, item_1->data_size) != 0) {
                throw "value not equal";
            }
            free(item_2->key);
            free(item_2->value);
            free(item_2);
        }
        delete query_result;
    }
    std::cout << "OK" << std::endl;
    for (i =0; i < N; i++) {
        free(p_items[i].key);
        free(p_items[i].value);
    }
    free(p_items);
    db_close(db);
}

int main() {
    try {
        char path_1[] = "/home/qmwang/code/course_project/example_db/part_1";  
        char key_prefix_1[] = "key1";
        test_python_interface(path_1, key_prefix_1);
        
        char path_2[] = "/home/qmwang/code/course_project/example_db/part_2";
        char key_prefix_2[] = "key2";
        test_python_interface(path_2, key_prefix_2);

        char path_3[] = "/home/qmwang/code/course_project/example_db/part_3";
        char key_prefix_3[] = "key3";
        test_python_interface(path_3, key_prefix_3);

    }
    catch (char const* msg) {
        std::cout << "Error: " << msg << std::endl;
    }
    catch(const std::runtime_error& re) {
        std::cerr << "Runtime error: " << re.what() << std::endl;
    }
    catch (const std::exception& ex) {
        std::cerr << "Error occurred: " << ex.what() << std::endl;
    }
    return 0;
}


