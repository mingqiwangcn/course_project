#include "storage.h"
#include <list>
#include <stdio.h>
#include <string.h>

using namespace std;

int get_page_offset(Page* page) {
    int offset = 0;
    memcpy(&offset, page->data+PAGE_META_OFFSET, sizeof(int));
    return offset;
}

void set_page_offset(Page* page, int offset) {
    memcpy(page->data+PAGE_META_OFFSET, &offset, sizeof(int));
}

bool fit_page(Page* page, int space_needed) {
    int space = PAGE_META_OFFSET - get_page_offset(page);
    return space >= space_needed;
}

void reset_page(Page* page) {
    page->page_no = -1;
    set_page_offset(page, 0)
}

Page* new_page() {
    Page* page = (Page*)malloc(sizeof(Page*));
    page->data = (char*)malloc(PAGE_SIZE);
    reset_page(page); 
    return page;
}

Page* request_new_page(PageBuffer* buffer) {
    Page* page = NULL;
    if (buffer->free_pages.size() > 0) {
       page = buffer->free_pages.front();
       buffer->free_pages.pop_front(); 
    } else {
        if (buffer->page_map->size() == buffer->capacity) {
            throw "buffer is full.";
        }
        page = new_page();
    }
    return page;
}

Page* read_page(PageBuffer* buffer, FILE* f, int total_pages, int page_no) {
    if (page_no < 0 || page_no >= total_pages) {
        throw "invalid page_no";
    }

    Page* page = NULL;
    unordered_map<int, Page*>::iterator itr = buffer->page_map.find(page_no);
    if (itr != buffer->page_map.end()) {
        page = *itr;
        return page;
    }
    page = request_new_page(buffer);
    int offset = page_no * PAGE_SIZE;
    fseek(f, offset, SEEK_SET);
    fread(page->data, 1, PAGE_SIZE, f);
    page->page_no = page_no;
    buffer->page_map[page_no] = page;
    return page;
}


Page* read_index_page(DB* db, int page_no) {
    Page* page = read_page(db->index_buffer, db->f_index, db->total_index_pages, page_no);
    return page;
}

Page* read_data_page(DB* db, int page_no) {
    Page* page = read_page(db->data_buffer, db->f_data, db->total_data_pages, page_no);
    return page;
}


void write_index_page(DB* db, Page* page) {
    write_page(db->index_buffer, page);
}

void write_data_page(DB* db, Page* page) {
    write_page(db->data_buffer, page);
}

void append_page(PageBuffer* buffer, Page* page) {
    
    buffer->dirty_pages.push_back(page);
}

void commit_write() {
    
}


char* read_meta_page(DB* db) {
    FILE* f = db->f_meta;
    fseek(f, 0, SEEK_SET);
    char* meta_page = (char*)malloc(sizeof(META_PAGE_SIZE));
    fread(meta_page, 1, META_PAGE_SIZE, f);
    return meta_page;
}


void write_meta_page(DB* db, char* meta_page) {
    FILE* f = db->f_meta;
    fseek(f, 0, SEEK_SET);
    fwrite(meta_page, 1, META_PAGE_SIZE, f); 
}


