#ifndef STORAGE_H
#define STORAGE_H
#include <stdio.h>
#include <unordered_map>
#include <list>
#define MAX_KEY_SIZE 116
#define MAX_PATH_SIZE 200
#define PAGE_SIZE 4096 // 32768 2M
#define PAGE_META_OFFSET (PAGE_SIZE-sizeof(int)*2)
#define META_PAGE_SIZE 4096 
using namespace std;

enum PageType {index_page, data_page};

typedef struct IndexItem {
    char key[MAX_KEY_SIZE];
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
    
     
} DB;

typedef struct DataItem {
    char key[MAX_KEY_SIZE];
    char* value;
    int size;
} DataItem;

DB* db_open(char* path);

DataItem** db_get(DB* db, char** keys, int key_size);

void db_put(DB* db, DataItem* db_items, int item_size);

void db_close(DB* db);

#endif
