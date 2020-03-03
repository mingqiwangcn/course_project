#include <stdlib.h>
#include <string.h>
#include <list>
#include <map>
#include "storage.h"
#define MAX_FULL_PATH_SIZE 300

using namespace std;

void join_path(char* full_path, char* path, char* file_name);
void read_meta(DB* db);
void read_index(DB* db);
extern Page* new_buffer_page(DB* db);
extern Page* read_page(PageType page_type, DB* db, int page_no);
void append_page(PageType page_type, DB* db, Page* page);
extern void free_buffer_page(DB* db, Page* page);
void write_index_data(DB* db, list<IndexItem> index_item_lst);

extern char* read_meta_page(DB* db);


void init_db(DB* db) {
    db->path[0] = '\0';
    db->f_meta = NULL;
    db->f_index = NULL;
    db->f_data = NULL;
    db->total_index_pages = 0;
    db->total_data_pages = 0;
    db->index_map = NULL;
    db->page_buffer = (PageBuffer*)malloc(sizeof(PageBuffer));
    db->page_buffer->in_use_pages = new list<Page*>();
    db->page_buffer->free_pages = new list<Page*>();
    
}

DB* db_open(char* path) {
    if (strlen(path) >= MAX_PATH_SIZE)
        return NULL;

    DB* db = (DB*)malloc(sizeof(DB));
    strcpy(db->path, path);

    char meta_file_path[MAX_FULL_PATH_SIZE];
    char meta_file_name[20] = "db.meta";
    join_path(meta_file_path, path, meta_file_name);
    db->f_meta = fopen(meta_file_path, "a+");

    read_meta(db);

    char index_file_path[MAX_FULL_PATH_SIZE];
    char index_file_name[20] = "db.index";
    join_path(index_file_path, path, index_file_name);
    
    read_index(db);

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
    char* meta = read_meta_page(db);
    int offset = 0;
    memcpy(&(db->total_index_pages), meta+offset, sizeof(int));
    offset += sizeof(int);
    memcpy(&(db->total_data_pages), meta+offset, sizeof(int));
    
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
    int last_page_no = db->total_data_pages;
    Page* page = read_page(PageType::data_page, db, last_page_no);

    int i = 0;
    int space = PAGE_SIZE - sizeof(int); //reserve the last few bytes to store the number of items in the page
    int num_items= 0;
    int offset = 0;

    list<IndexItem> index_item_lst; 
    
    while (i < item_size) {
        DataItem* cur_item = db_items + i;
        if (cur_item->size <= space) {
            memcpy(page->data+offset, cur_item->value, cur_item->size); 

            IndexItem idx_item;
            strcpy(idx_item.key, cur_item->key);
            idx_item.page_no = db->total_data_pages;
            idx_item.offset = offset;
            idx_item.size = cur_item->size;
            
            index_item_lst.push_back(idx_item);

            offset += cur_item->size;
            space -= cur_item->size;
            num_items += 1;

            i += 1;
        } else {
            //fulli
            if (num_items <= 0)
                throw "error";

            append_page(PageType::data_page, db, page);
            
            db->total_data_pages += 1; 
            num_items = 0;
            offset = 0;
            space = PAGE_SIZE - sizeof(int);
        } 
    }

    //append index page
    
    write_index_data(db, index_item_lst); 
     
    free_buffer_page(db, page);
}

void write_index_data(DB* db, list<IndexItem> index_item_lst){
    int item_count = index_item_lst.size();
    int tola_size = sizeof(IndexItem*) * item_count;
    IndexItem** p_idx_items= (IndexItem**)malloc(tola_size);
    int i = 0;
    int last_page_no = db->total_index_pages;
    Page* page = read_page(PageType::meta_page, db, last_page_no);

    int space = PAGE_SIZE - sizeof(int); //reserve the last few bytes to store the number of items in the page
    int num_items= 0;
    int offset = 0;

    while (i < item_count) {
         
    }
    
}


void db_close(DB* db) {
    return;
}
