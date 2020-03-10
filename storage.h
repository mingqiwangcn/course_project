#ifndef STORAGE_H
#define STORAGE_H
#include <stdio.h>
#include <unordered_map>
#include <vector>
#include <list>
#define MAX_KEY_SIZE 116
#define MAX_PATH_SIZE 200

using namespace std;

enum PageType {index_page, data_page};

typedef struct IndexItem {
    char* key;
    int key_size;
    int page_no;
    int offset;
    int data_size;
} IndexItem;

typedef struct Page {
    char* data;
    int page_no;
} Page;

typedef struct PageBuffer {
    int capacity;
    unordered_map<int, Page*>* page_map;
    list<Page*>* enter_queue;
    list<Page*>* free_pages;
    list<Page*>* written_pages; //these pages are already indexed in page_map.
    int last_write_page;
} PageBuffer;

typedef struct DB {
    char path[MAX_PATH_SIZE];
    FILE* f_meta;
    FILE* f_index;
    FILE* f_data;
    
    int total_index_pages;
    int total_data_pages;
    int total_items;
    Page* meta_page;

    unordered_map<string, IndexItem*>* index_map;

    PageBuffer* index_buffer;
    PageBuffer* data_buffer;
    
    size_t PAGE_SIZE;               // page size for index and data page
    int PAGE_META_OFFSET;           // (PAGE_SIZE-sizeof(int)*2)
    int META_PAGE_SIZE;             // page size for meta page, smaller than index and dats page.
    int MAX_INDEX_BUFFER_SIZE;      //buffer capacity for index pages
    int MAX_DATA_BUFFER_SIZE;       //buffer capacity for data pages
     
} DB;

typedef struct DataItem {
    int key_size;
    char* key;
    int data_size;
    char* value;
} DataItem;

typedef struct DBOpt {
   int max_index_buffer_size;
   int max_data_buffer_size;
} DBOpt;

DB* db_open(char* path, DBOpt* opt);

vector<DataItem*>* db_get(DB*db, vector<string>* key_lst);

void db_put(DB* db, vector<DataItem*>* data_items);

void db_close(DB* db);

#endif
