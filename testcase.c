#include <string.h>
#include <iostream>
#include <vector>
#include "storage.h"
#include <unordered_map>
#include <random>
#include <utility>
using namespace std;

extern void interop_db_open(char* path, unordered_map<string, string>& options);
extern vector<vector<double>> interop_db_get(vector<string>& key_lst);
extern void interop_db_put(vector<pair<string, vector<double>>>& data);
extern void interop_db_close();
extern void cluster_db_open(char* path, unordered_map<string, string>& options);
extern vector<vector<double>> cluster_db_get(vector<string>& key_lst);
extern void cluster_db_close();

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

unordered_map<string, vector<double>> put_data(char* path, char* key_prefix) {
    unordered_map<string, string> opt_map;
    interop_db_open(path, opt_map);
    
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

    interop_db_close();

    std::cout << "OK" << std::endl;

    return data;
}

void cluster_query(unordered_map<string, vector<double>> data) {
    char path[] = "/home/qmwang/code/course_project/example_db/";
    unordered_map<string, string> options;
    cluster_db_open(path, options);
    vector<string> qry_keys;
    int N = 10000;
    for (int i =0 ; i< 100; i++) {
        char key[100];
        int m = rand() % N;
        sprintf(key, "%s-%d-%d", "key1", 0, m);
        qry_keys.push_back(key);

        m = rand() % N;
        sprintf(key, "%s-%d-%d", "key2", 0, m);
        qry_keys.push_back(key);

        m = rand() % N;
        sprintf(key, "%s-%d-%d", "key3", 0, m);
        qry_keys.push_back(key);
    }
    vector<vector<double>> query_result = cluster_db_get(qry_keys);
    
    for (int j =0; j < 100; j++) {
        string qry_key = qry_keys[j];
        vector<double> item_1 = data[qry_key];
        vector<double> item_2 = query_result[j];
        if (item_1 != item_2) {
            throw "data is not euqal";
        }
    }
}
           
int main() {
    srand(1);
    try {
        unordered_map<string, vector<double>> data_1;
        unordered_map<string, vector<double>> data_2;
        unordered_map<string, vector<double>> data_3; 

        char path_1[] = "/home/qmwang/code/course_project/example_db/part_1";  
        char key_prefix_1[] = "key1";
        data_1 = put_data(path_1, key_prefix_1);
        
        char path_2[] = "/home/qmwang/code/course_project/example_db/part_2";
        char key_prefix_2[] = "key2";
        data_2 = put_data(path_2, key_prefix_2);

        char path_3[] = "/home/qmwang/code/course_project/example_db/part_3";
        char key_prefix_3[] = "key3";
        data_3 = put_data(path_3, key_prefix_3);

        data_1.insert(data_2.begin(), data_2.end());
        data_1.insert(data_3.begin(), data_3.end());

        cluster_query(data_1); 

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


