#ifndef STORAGE_H
#define STORAGE_H

#include <stdio.h>
#include <map>
#include <list>

#define MAX_KEY_SIZE 116
#define MAX_PATH_SIZE 200
#define PAGE_SIZE 32768 //32M 

using namespace std;

typedef struct IndexItem {
    int page_no;
    int offset;
    int size;
} IndexItem;

typedef struct Page {
    char* data;
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
    int index_offset;

    int total_data_pages;
    int data_offset;

    map<string, IndexItem>* index_map;

    PageBuffer* page_buffer;

     
} DB;


typedef struct DataItem {
    char key[MAX_KEY_SIZE];
    char* value;
} DataItem;

DB* db_open(char* path);

void db_batch_get(DB* db, char** keys, int key_size);

void db_batch_put(DB* db, DataItem* db_items, int item_size);

void db_close(DB* db);

#endif
