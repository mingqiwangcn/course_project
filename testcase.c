#include <string.h>
#include <iostream>
#include <vector>
#include "storage.h"
#include <unordered_map>

void test_put() {
    char path[100] = "/home/qmwang/code/course_project/passage_db";
    DB* db = db_open(path);  
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
        int M =  rand() % 2000;
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
    db = db_open(path);
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
        }
    }
    std::cout << "OK" << std::endl;
}

int main() {
    try {
        test_put();
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


