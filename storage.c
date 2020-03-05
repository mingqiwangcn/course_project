#include <stdlib.h>
#include <string.h>
#include <list>
#include <map>
#include <unistd.h>
#include "storage.h"
#define MAX_FULL_PATH_SIZE 300
#define MAX_INDEX_BUFFER_SIZE 100
#define MAX_DATA_BUFFER_SIZE  1000

using namespace std;

void join_path(char* full_path, char* path, char* file_name);
void read_meta(DB* db);
void read_index(DB* db);
extern Page* new_buffer_page(DB* db);
extern Page* read_page(PageType page_type, DB* db, int page_no);
extern void write_page(PageType page_type, DB* db, Page* page);
extern void free_buffer_page(DB* db, Page* page);
void write_index_data(DB* db, list<IndexItem> index_item_lst);
extern Page* get_data_buffer_page(DB* db);
extern Page* get_index_buffer_page(DB* db);
extern char* read_meta_page(DB* db);
extern void reset_page(Page* page);

void new_page_buffer(int capacity) {
    PageBuffer* buffer = (PageBuffer*)malloc(sizeof(PageBuffer));
    buffer->capacity = capacity;
    buffer->page_map = new map<int, Page*>();
    buffer->free_pages = new list<Page*>();
    buffer->dirty_pages = new list<Page*>();
    return buffer;
}

void init_db(DB* db) {
    db->path[0] = '\0';
    db->f_meta = NULL;
    db->f_index = NULL;
    db->f_data = NULL;
    db->total_index_pages = 0;
    db->total_data_pages = 0;
    db->index_map = NULL;
    db->index_buffer = new_page_buffer(MAX_INDEX_BUFFER_SIZE);
    db->page_buffer = new_page_buffer(MAX_DATA_BUFFER_SIZE);
}

bool file_exist(char *filename)
{
    return (access(filename, F_OK ) != -1);
}

DB* db_open(char* path) {
    if (strlen(path) >= MAX_PATH_SIZE)
        throw "db path is too long.";

    DB* db = (DB*)malloc(sizeof(DB));
    strcpy(db->path, path);

    char meta_file_path[MAX_FULL_PATH_SIZE];
    char meta_file_name[20] = "db.meta";
    join_path(meta_file_path, path, meta_file_name);

    if (file_exist(meta_file_path)) {
        db->f_meta = fopen(meta_file_path, "a+");
        read_meta(db);
    } else {
        db->f_meta = fopen(meta_file_path, "a+");
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
        item.data_size = item_size;
        
        index_map->insert(pair<string, IndexItem>(map_key, item));

    }
}




/*
If the last data page is not full, return it, otherwise create a new data page to put
*/
Page* get_page_to_put(PageType page_type, DB* db, int total_pages, int min_space) {
    Page* page = NULL;
    if (total_pages == 0) {
        page = new_buffer_page(db);
    } else {
        int last_page_no = total_pages - 1;
        page = read_page(page_type, db, last_page_no);
        int space = PAGE_META_OFFSET - get_page_offset(page);
        if (space < min_space) {
            page = new_buffer_page(db);
        }
    }
    return page;
}


/*
1) reserve the last 8 bytes in a data page to store meta info
    [space_offset]: the start offset that contains no data items
    [num_data_items]:number of items in this page
2) data item is store as
    [item_1_size][item_1_data][item_2_size][item_2_data]... 
*/
void db_put(DB* db, DataItem* db_items, int item_size) {
    int size_len = sizeof(int); 
    Page* page = NULL;
    if (db->total_data_pages == 0) {
        page = request_new_page(db->data_buffer);
    } else {
        int last_page_no = db->total_data_pages - 1;
        Page* last_page = read_data_page(db, last_page_no);
        int min_space = size_len + db_items[0].size;
        bool fit_flag = fit_page(last_page, min_space);
        if (fit_flag) {
            page = last_page;
        } else {
            page = request_new_page(db->data_buffer);
        }
    }

    int i = 0;
    int max_space = PAGE_META_OFFSET;
    int space = max_space; 
    int num_items= 0;
    int offset = get_page_offset(page);
    
    list<IndexItem> index_item_lst; 
     
    while (i < item_size) {
        DataItem* cur_item = db_items + i;
        int request_size = size_len + cur_item->size;
        if (request_size <= space) {
            IndexItem idx_item;
            strcpy(idx_item.key, cur_item->key);
            idx_item.page_no = db->total_data_pages;
            idx_item.offset = offset;
            idx_item.data_size = cur_item->size;
            index_item_lst.push_back(idx_item);
            
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
            
            memset(page->data+PAGE_META_OFFSET, offset, sizeof(int));
            memset(page->data+PAGE_META_OFFSET+sizeof(int), num_items, sizeof(int));

            write_page(PageType::data_page, db, page);
            if (page->page_no == -1) {
                db->total_data_pages += 1; 
            }

            reset_page(page);
            num_items = 0;
            offset = 0;
            space = max_space;
        } 
    }

    //write index page
    write_index_data(db, index_item_lst); 
     
}

void write_index_data(DB* db, list<IndexItem> index_item_lst){
    int index_item_size = MAX_KEY_SIZE + sizeof(int) * 3;
    int min_space = index_item_size;
    Page* page = get_page_to_put(PageType::index_page, db, db->total_index_pages, min_space);
    int item_count = index_item_lst.size();
    int tola_size = sizeof(IndexItem*) * item_count;
    IndexItem** p_idx_items= (IndexItem**)malloc(tola_size);
    list<IndexItem>::iterator itr;
    int max_space = PAGE_META_OFFSET;
    int space = max_space; 
    int num_items= 0;
    int offset = get_page_offset(page);
    int i = 0;

    for (itr = index_item_lst.begin(); itr != index_item_lst.end(); ++itr) {
        p_idx_items[i] = &(*itr);
        
        if (index_item_size < space) {
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
            
            memset(page->data+PAGE_META_OFFSET, offset, sizeof(int));
            memset(page->data+PAGE_META_OFFSET+sizeof(int), num_items, sizeof(int));

            write_page(PageType::index_page, db, page);
            if (page->page_no == -1) { 
                db->total_index_pages += 1;
            }
            reset_page(page);
            num_items = 0;
            offset = 0;
            space = max_space;
        }
    }

}


void db_close(DB* db) {
    return;
}
