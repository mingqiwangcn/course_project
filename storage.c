#include <stdlib.h>
#include <string.h>
#include <vector>
#include <list>
#include <map>
#include <unistd.h>
#include "storage.h"
#define MAX_FULL_PATH_SIZE 300
#define MAX_INDEX_BUFFER_SIZE 3
#define MAX_DATA_BUFFER_SIZE  3

using namespace std;

void join_path(char* full_path, char* path, char* file_name);
void read_meta(DB* db);
void read_index(DB* db);
void write_index(DB* db, list<IndexItem*>* index_item_lst);
extern int get_page_offset(Page* page);
extern void set_page_offset(Page* page, int offset);
extern bool fit_page(Page* page, int space_needed);
extern void reset_page(Page* page);
extern void flush_written_pages(PageBuffer* buffer, FILE* f);
extern Page* request_new_page_to_append(PageBuffer* buffer, FILE* f, int new_page_no);
extern Page* read_index_page(DB* db, int page_no);
extern Page* read_data_page(DB* db, int page_no);
extern Page* read_meta_page(FILE* f); 
extern void write_meta_page(FILE* f, Page* page); 
extern PageBuffer* new_page_buffer(int capacity);
extern Page* new_meta_page();
extern void free_page_buffer(PageBuffer* buffer);
extern void free_page(Page* page);

void init_db(DB* db) {
    db->path[0] = '\0';
    db->f_meta = NULL;
    db->f_index = NULL;
    db->f_data = NULL;
    db->total_index_pages = 0;
    db->total_data_pages = 0;
    db->meta_page = NULL;
    db->index_map = NULL;
    db->index_buffer = new_page_buffer(MAX_INDEX_BUFFER_SIZE);
    db->data_buffer = new_page_buffer(MAX_DATA_BUFFER_SIZE);
}

bool file_exist(char *filename)
{
    return (access(filename, F_OK ) != -1);
}

DB* db_open(char* path) {
    if (strlen(path) >= MAX_PATH_SIZE)
        throw "db path is too long.";

    DB* db = (DB*)malloc(sizeof(DB));
    init_db(db);
    strcpy(db->path, path);

    char meta_file_path[MAX_FULL_PATH_SIZE];
    char meta_file_name[20] = "db.meta";
    join_path(meta_file_path, path, meta_file_name);

    if (file_exist(meta_file_path)) {
        db->f_meta = fopen(meta_file_path, "a+");
        read_meta(db);
    } else {
        db->f_meta = fopen(meta_file_path, "a+");
        db->meta_page = new_meta_page();
    }

    char index_file_path[MAX_FULL_PATH_SIZE];
    char index_file_name[20] = "db.index";
    join_path(index_file_path, path, index_file_name);
    
    db->f_index = fopen(index_file_path, "a+");
    if (db->total_index_pages > 0) {
        read_index(db);
    }

    char data_file_path[MAX_FULL_PATH_SIZE];
    char data_file_name[20] = "db.data";
    join_path(data_file_path, path, data_file_name);

    db->f_data = fopen(data_file_path, "a+");

    return db;
}

void join_path(char* full_path, char* path, char* file_name) {
    strcpy(full_path, path);
    strcat(full_path, "/");
    strcat(full_path, file_name);
}

void read_meta(DB* db) {
    db->meta_page = read_meta_page(db->f_meta);
    char* meta = db->meta_page->data;
    int offset = 0;
    memcpy(&(db->total_items), meta+offset, sizeof(int));
    offset += sizeof(int);
    memcpy(&(db->total_index_pages), meta+offset, sizeof(int));
    offset += sizeof(int);
    memcpy(&(db->total_data_pages), meta+offset, sizeof(int));
}

bool is_meta_changed(DB* db) {
    int offset = 0;
    int total_items = 0;
    char* meta = db->meta_page->data;
    memcpy(&total_items, meta+offset, sizeof(int));
    if (total_items != db->total_items)
        return true;
    else
        return false;
}

void write_meta(DB* db) {
    int offset = 0;
    char* meta = db->meta_page->data;
    memcpy(meta+offset, &(db->total_items), sizeof(int));
    offset += sizeof(int);
    memcpy(meta+offset, &(db->total_index_pages), sizeof(int));
    offset += sizeof(int);
    memcpy(meta+offset, &(db->total_data_pages), sizeof(int));

    write_meta_page(db->f_meta, db->meta_page);
}


void build_index(DB* db, Page* page) {
    int max_offset = get_page_offset(page);
    int offset = 0;
    while (offset < max_offset) {
        IndexItem* index_item = (IndexItem*)malloc(sizeof(IndexItem));
        memcpy(index_item->key, page->data+offset, MAX_KEY_SIZE);
        offset += MAX_KEY_SIZE;
        memcpy(&(index_item->page_no), page->data+offset, sizeof(int));
        offset += sizeof(int); 
        memcpy(&(index_item->offset), page->data+offset, sizeof(int));
        offset += sizeof(int);
        memcpy(&(index_item->data_size), page->data+offset, sizeof(int));
        offset += sizeof(int);
        string map_key(index_item->key);
        (*(db->index_map))[map_key] = index_item;
    }
}


void read_index(DB* db){
    db->index_map = new unordered_map<string, IndexItem*>();
    int page_no = 0;
    for (page_no = 0; page_no < db->total_index_pages; page_no++) {
        Page* page = read_index_page(db, page_no);
        build_index(db, page); 
    }
}

IndexItem* create_IndexItem(char* key, int page_no, int offset, int data_size) {
    IndexItem* item = (IndexItem*)malloc(sizeof(IndexItem));
    strcpy(item->key, key);
    item->page_no = page_no;
    item->offset = offset;
    item->data_size = data_size;
    return item;
}

DataItem* create_DataItem(int data_size) {
    DataItem* item = (DataItem*)malloc(sizeof(DataItem));
    item->value = (char*)malloc(data_size);
    item->size = data_size;
    return item; 
}

DataItem* get_data_item(Page* page, int offset, int data_size) {
    DataItem* item = create_DataItem(data_size);
    memcpy(item->value, page->data+offset+sizeof(int), data_size);
    return item;
}

vector<DataItem*>* db_get(DB*db, vector<string>* key_lst) {
    vector<DataItem*>* data_item_lst = new vector<DataItem*>();
    vector<string>::iterator lst_itr;
    unordered_map<string, IndexItem*>::iterator map_itr;
    for (lst_itr = key_lst->begin(); lst_itr != key_lst->end(); ++lst_itr) {
        string map_key = *lst_itr;
        map_itr = db->index_map->find(map_key);
        if (map_itr != db->index_map->end()) {
            IndexItem* index_item = map_itr->second;
            Page* data_page = read_data_page(db, index_item->page_no);
            DataItem* data_item = get_data_item(data_page, index_item->offset, index_item->data_size);
            strcpy(data_item->key, map_key.c_str());
            data_item_lst->push_back(data_item);
        } else {
           throw "key not found."; 
        }
    }
    return data_item_lst;
}

/*
1) reserve the last 8 bytes in a data page to store meta info
    [space_offset]: the start offset that contains no data items
    [num_data_items]:number of items in this page
2) data item is store as
    [item_1_size][item_1_data][item_2_size][item_2_data]... 
*/
void db_put(DB* db, vector<DataItem*>* data_items) {
    int item_size = data_items->size();
    int size_len = sizeof(int);
    int new_page_no = db->total_data_pages; 
    Page* page = NULL;
    if (db->total_data_pages == 0) {
        page = request_new_page_to_append(db->data_buffer, db->f_data, new_page_no);
    } else {
        int last_page_no = db->total_data_pages - 1;
        Page* last_page = read_data_page(db, last_page_no);
        int min_space = size_len + (*data_items)[0]->size;
        bool fit_flag = fit_page(last_page, min_space);
        if (fit_flag) {
            page = last_page;
        } else {
            page = request_new_page_to_append(db->data_buffer, db->f_data, new_page_no);
        }
    }
    int i = 0;
    int max_space = PAGE_META_OFFSET;
    int space = max_space; 
    int num_items= 0;
    int offset = get_page_offset(page);
    list<IndexItem*>* index_item_lst = new list<IndexItem*>(); 
    while (i < item_size) {
        DataItem* cur_item = (*data_items)[i];
        int request_size = size_len + cur_item->size;
        if (request_size <= space) {
            IndexItem* idx_item = create_IndexItem(cur_item->key, page->page_no, offset, cur_item->size);
            index_item_lst->push_back(idx_item);
            memcpy(page->data+offset, &(cur_item->size), size_len);
            offset += size_len; 
            memcpy(page->data+offset, cur_item->value, cur_item->size); 
            offset += cur_item->size;
            space -= request_size;
            num_items += 1;
            i += 1;
        } else {
            //fulli
            if (num_items <= 0)
                throw "error";
             
            set_page_offset(page, offset);
            //set num of items in this page
            memcpy(page->data+PAGE_META_OFFSET+sizeof(int), &num_items, sizeof(int));
            //put this page in written_pages queue
            db->data_buffer->written_pages->push_back(page);
            db->total_data_pages += 1;
            new_page_no = db->total_data_pages; 
            page = request_new_page_to_append(db->data_buffer, db->f_data, new_page_no);
            num_items = 0;
            offset = 0;
            space = max_space;
        } 
    }
    if (num_items > 0) {
        set_page_offset(page, offset);
        memcpy(page->data+PAGE_META_OFFSET+sizeof(int), &num_items, sizeof(int));
        db->data_buffer->written_pages->push_back(page);
        db->total_data_pages += 1;
    }
    flush_written_pages(db->data_buffer, db->f_data);
    //write index page
    write_index(db, index_item_lst);
    delete index_item_lst;
    db->total_items += item_size;
    write_meta(db);    
}

void write_index(DB* db, list<IndexItem*>* index_item_lst){
    int index_item_size = MAX_KEY_SIZE + sizeof(int) * 3;
    int min_space = index_item_size;
    int new_page_no = db->total_index_pages;
    Page* page = NULL;
    if (db->total_index_pages == 0) {
        page = request_new_page_to_append(db->index_buffer, db->f_index, new_page_no);
    } else {
        int last_page_no = db->total_index_pages - 1;
        Page* last_page = read_index_page(db, last_page_no);
        bool fit_flag = fit_page(last_page, min_space);
        if (fit_flag) {
            page = last_page;
        } else {
            page = request_new_page_to_append(db->index_buffer, db->f_index, new_page_no);
        }
    }

    int item_count = index_item_lst->size();
    int tola_size = sizeof(IndexItem*) * item_count;
    IndexItem** p_idx_items= (IndexItem**)malloc(tola_size);
    list<IndexItem*>::iterator itr;
    int max_space = PAGE_META_OFFSET;
    int space = max_space; 
    int num_items= 0;
    int offset = get_page_offset(page);
    int i = 0;

    for (itr = index_item_lst->begin(); itr != index_item_lst->end(); ++itr) {
        p_idx_items[i] = (*itr);
        i += 1;
    }

    i = 0;
    while (i < item_count) {
        if (index_item_size <= space) {
            memcpy(page->data+offset, p_idx_items[i]->key, MAX_KEY_SIZE);
            offset += MAX_KEY_SIZE;
            memcpy(page->data+offset, &(p_idx_items[i]->page_no), sizeof(int));
            offset += sizeof(int);
            memcpy(page->data+offset, &(p_idx_items[i]->offset), sizeof(int));
            offset += sizeof(int);
            memcpy(page->data+offset, &(p_idx_items[i]->data_size), sizeof(int));
            offset += sizeof(int);
            space -= index_item_size;
            num_items += 1;
            i += 1;

        } else {
            //page is full.
            if (num_items <= 0)
                throw "error";
            
            set_page_offset(page, offset);
            memcpy(page->data+PAGE_META_OFFSET+sizeof(int), &num_items, sizeof(int));

            db->index_buffer->written_pages->push_back(page);
            db->total_index_pages += 1;
            new_page_no = db->total_index_pages; 
            page = request_new_page_to_append(db->index_buffer, db->f_index, new_page_no);
            
            num_items = 0;
            offset = 0;
            space = max_space;
        }
    }

    free(p_idx_items);

    if (num_items > 0) {
        set_page_offset(page, offset);
        memcpy(page->data+PAGE_META_OFFSET+sizeof(int), &num_items, sizeof(int));
        db->index_buffer->written_pages->push_back(page);
        db->total_index_pages += 1;
    }

    flush_written_pages(db->index_buffer, db->f_index);
}

void db_close(DB* db) {
    if (is_meta_changed(db)) {
        write_meta(db);
    }

    free_page_buffer(db->index_buffer);
    free_page_buffer(db->data_buffer);

    free_page(db->meta_page);
    
    fclose(db->f_meta);
    fclose(db->f_index);
    fclose(db->f_data);
    
    free(db);

    return;
}





