#ifndef STORAGE_H
#define STORAGE_H
#include <stdio.h>

#define MAX_KEY_SIZE 116
#define MAX_PATH_SIZE 200

typedef struct DB {
    char path[MAX_PATH_SIZE];
    FILE* f_meta;
    FILE* f_data; 
} DB;

typedef struct DataItem {
    char key[MAX_KEY_SIZE];
    char* value;
} DataItem

DB* db_open(char* path);

void db_batch_get(DB* db, char** keys, int key_size);

void db_batch_put(DB* db, DataItem* db_items, int item_size);

void db_close(DB* db);

#endif
