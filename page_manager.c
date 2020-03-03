#include "storage.h"
#define MAX_NUM_PGAES 1000
#include <list>

using namespace std;

Page* new_buffer_page(DB* db) {
    Page* page = NULL;

    list<Page*>* in_use_pages = db->page_buffer->in_use_pages;
    list<Page*>* free_pages = db->page_buffer->free_pages;

    if (free_pages->size() > 0) {
        page = free_pages->front();
        free_pages->pop_front();
        in_use_pages->push_back(page);

    } else {
        int buffer_count = free_pages->size() + in_use_pages->size();
        if (buffer_count >= MAX_NUM_PGAES) {
            throw "Buffer is full and all pages are in use."; 
        }
        page = (Page*)malloc(sizeof(Page));
        page->data = (char*)malloc(sizeof(PAGE_SIZE));
        in_use_pages->push_back(page);
    }

    page->container = in_use_pages;
    return page;
}


void free_buffer_page(DB* db, Page* page) {
    page->container->remove(page);
}

FILE* get_fp(PageType page_type, DB* db) {
    FILE* f = NULL;
    if (page_type == PageType::meta_page)
        f = db->f_meta;
    else if (page_type == PageType::data_page)
        f = db->f_data;
    else {}
    return f;
}

Page* read_page(PageType page_type, DB* db, int page_no) {
    FILE* f = get_fp(page_type, db);
    fseek(f, 0, SEEK_SET); 
    Page* page = new_buffer_page(db);
    fread(page->data, 1, PAGE_SIZE, f);
    return page;
}


void append_page(PageType page_type, DB* db, Page* page) {
    FILE* f = get_fp(page_type, db);
    fseek(f, 0, SEEK_END);  
    fwrite(page->data, 1, PAGE_SIZE, f);
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


