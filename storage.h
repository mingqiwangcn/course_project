#ifndef STORAGE_H
#define STORAGE_H
#include <stdio.h>
#include <map>
#include <list>
#define MAX_KEY_SIZE 116
#define MAX_PATH_SIZE 200
#define PAGE_SIZE 32768 //32M
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
    list<Page*>* container;
    char* data;
    int page_no;
} Page;

typedef struct PageBuffer {
    list<Page*>* in_use_pages;
    list<Page*>* free_pages;
} PageBuffer;

typedef struct DB {
    char path[MAX_PATH_SIZE];
    FILE* f_meta;
    FILE* f_index;
    FILE* f_data;
    
    int total_index_pages;
    int total_data_pages;

    map<string, IndexItem>* index_map;

    PageBuffer* page_buffer;

     
} DB;

typedef struct DataItem {
    char key[MAX_KEY_SIZE];
    char* value;
    int size;
} DataItem;

DB* db_open(char* path);

void db_get(DB* db, char** keys, int key_size);

void db_put(DB* db, DataItem* db_items, int item_size);

void db_close(DB* db);

#endif
