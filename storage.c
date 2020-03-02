#include <stdlib.h>
#include <string.h>
#include <map>
#include "storage.h"
#define MAX_FULL_PATH_SIZE 300

using namespace std;

void join_path(char* full_path, char* path, char* file_name);
void read_meta(DB* db);
extern void append_page(DB* db, Page* page);

DB* db_open(char* path) {
    if (strlen(path) >= MAX_PATH_SIZE)
        return 0;

    DB* db = (DB*)malloc(sizeof(DB));
    strcpy(db->path, path);

    char meta_file_path[MAX_FULL_PATH_SIZE];
    char meta_file_name[20] = "db.meta";
    join_path(meta_file_path, path, meta_file_name);
    db->f_meta = fopen(meta_file_path, "a+");

    read_meta(db);

    char data_file_path[MAX_FULL_PATH_SIZE];
    char data_file_name[20] = "db.data";
    join_path(data_file_path, path, data_file_name);
    db->f_data = fopen(data_file_path, "a+");

    return db;
}

void join_path(char* full_path, char* path, char* file_name) {
    strcpy(full_path, path);
    strcpy(full_path, "/");
    strcpy(full_path, file_name);
}

void read_meta(DB* db) {
    FILE* f = db->f_meta;
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    if (size == 0)
        return;
    fseek(f, 0, SEEK_SET);
    char* meta =(char*) malloc(size);
    fread(meta, 1, size, f);
   
    int offset = 0;
    memcpy(&(db->total_index_pages), meta+offset, sizeof(int));
    offset += sizeof(int);
   
    memcpy(&(db->total_index_pages), meta+offset, sizeof(int));
    

}

void read_index(DB* db){
    FILE* f = db->f_index;
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fseek(f, 0, SEEK_SET);
    char* meta =(char*) malloc(size);
    fread(meta, 1, size, f);
    int offset = 0;
    map<string, IndexItem>* index_map = new map<string, IndexItem>();
    db->index_map = index_map;
    while (offset < size) {
        IndexItem item;
        
        char* key = (char*)malloc(sizeof(MAX_KEY_SIZE));
        memcpy(key, meta+offset, MAX_KEY_SIZE);
        offset += MAX_KEY_SIZE;
        
        int page_no = -1;
        memcpy(&page_no, meta+offset, sizeof(int));
        offset += sizeof(int);

        int offset_in_page = -1;
        memcpy(&offset_in_page, meta+offset, sizeof(int));
        offset += sizeof(int);

        int item_size = -1;
        memcpy(&item_size, meta+offset, sizeof(int));
        offset += sizeof(int);

        string map_key(key);
        item.page_no = page_no;
        item.offset = offset_in_page;
        item.size = item_size;
        
        index_map->insert(pair<string, IndexItem>(map_key, item));

    }
}

void db_batch_put(DB* db, DataItem* db_items, int item_size) {
    
}


void db_close(DB* db) {
    return;
}
