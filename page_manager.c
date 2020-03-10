#include "storage.h"
#include <list>
#include <stdio.h>
#include <string.h>

using namespace std;

PageBuffer* new_page_buffer(int capacity) {
    PageBuffer* buffer = (PageBuffer*)malloc(sizeof(PageBuffer));
    buffer->capacity = capacity;
    buffer->page_map = new unordered_map<int, Page*>();
    buffer->enter_queue = new list<Page*>();
    buffer->free_pages = new list<Page*>();
    buffer->written_pages = new list<Page*>();
    buffer->last_write_page = -1;
    return buffer;
}


int get_page_offset(DB* db, Page* page) {
    int offset = 0;
    memcpy(&offset, page->data+(db->PAGE_META_OFFSET), sizeof(int));
    return offset;
}

void set_page_offset(DB* db, Page* page, int offset) {
    memcpy(page->data+(db->PAGE_META_OFFSET), &offset, sizeof(int));
}

void set_page_item_count(DB* db, Page* page, int item_count) {
      memcpy(page->data+(db->PAGE_META_OFFSET)+sizeof(int), &item_count, sizeof(int));
}

bool fit_page(DB* db, Page* page, int space_needed) {
    int space = (db->PAGE_META_OFFSET) - get_page_offset(db, page);
    return space >= space_needed;
}

void reset_page(DB* db, Page* page) {
    page->page_no = -1;
    set_page_offset(db, page, 0);
    set_page_item_count(db, page, 0);
}

Page* new_page(DB* db) {
    Page* page = (Page*)malloc(sizeof(Page));
    page->data = (char*)malloc(db->PAGE_SIZE);
    reset_page(db, page); 
    return page;
}

void free_page(Page* page) {
    free(page->data);
    free(page);
}

void free_page_list(list<Page*>* page_lst) {
    list<Page*>::iterator itr;
    for (itr = page_lst->begin(); itr != page_lst->end(); ++itr) {
        Page* page = *itr;
        free_page(page);
    }
    delete page_lst;
}

void free_page_buffer(PageBuffer* buffer) {
    unordered_map<int, Page*>::iterator itr;
    for (itr = buffer->page_map->begin(); itr != buffer->page_map->end(); ++itr) {
        Page* page = itr->second;
        free_page(page);
    }
    delete buffer->page_map;
    free_page_list(buffer->free_pages);
    delete buffer->enter_queue;
    delete buffer->written_pages;
    free(buffer);
}

void flush_written_pages(DB* db, PageBuffer* buffer, FILE* f) {
    list<Page*>* written_pages = buffer->written_pages;
    list<Page*>::iterator itr;
    unordered_map<int, Page*>::iterator map_itr;
    Page* first_page = written_pages->front();
    
    size_t sz_page_no = first_page->page_no;
    size_t offset = sz_page_no * (db->PAGE_SIZE);
    fseek(f, offset, SEEK_SET);
    Page* last_page = written_pages->back();
    for (itr = written_pages->begin(); itr != written_pages->end(); ++itr) {
        Page* page = *itr;
        fwrite(page->data, 1, db->PAGE_SIZE, f);
        if (page != last_page) {
            buffer->free_pages->push_back(page);
            map_itr = buffer->page_map->find(page->page_no);
            buffer->page_map->erase(map_itr);  
        }
    }
    if (buffer->last_write_page >= 0 && (buffer->last_write_page != last_page->page_no)) {
        map_itr = buffer->page_map->find(buffer->last_write_page);
        //the page may be written again and thus already removed and put in free page list.
        if (map_itr != buffer->page_map->end()) {
            Page* pre_written_page = map_itr->second;
            buffer->page_map->erase(map_itr);
            buffer->free_pages->push_back(pre_written_page);
        }
    }
    buffer->last_write_page = last_page->page_no;
    written_pages->clear();
}

Page* request_new_page_to_append(DB* db, PageBuffer* buffer, FILE* f, int new_page_no) {
    Page* page = NULL;
    if (buffer->free_pages->size() > 0) {
       page = buffer->free_pages->front();
       reset_page(db, page);
       buffer->free_pages->pop_front(); 
    } else {
        int num_allocated = buffer->page_map->size();
        if (num_allocated < buffer->capacity) {
            page = new_page(db);
        } else {
            if (buffer->written_pages->size() > 0) {
                //write to disk for more free pages
                flush_written_pages(db, buffer, f);
                //allocate one from free_pages
                if (buffer->free_pages->size() == 0)
                    throw "buffer is too small.";
                page = buffer->free_pages->front();
                reset_page(db, page);
                buffer->free_pages->pop_front();
            } else {
                throw "buffer is full.";
            }
        }
    }
    page->page_no = new_page_no;
    if (buffer->page_map->find(new_page_no) != buffer->page_map->end()) {
        throw "buffer error";
    }
    (*(buffer->page_map))[new_page_no] = page;
    return page;
}

Page* read_page(DB* db, PageBuffer* buffer, FILE* f, int total_pages, int page_no) {
    if (page_no < 0 || page_no >= total_pages) {
        throw "invalid page_no";
    }

    Page* page = NULL;
    unordered_map<int, Page*>::iterator itr = buffer->page_map->find(page_no);
    if (itr != buffer->page_map->end()) {
        page = itr->second;
        return page;
    }
   
    if (buffer->page_map->size() < buffer->capacity) {
        page = new_page(db);
        page->page_no = page_no;
    } else {
        //need to evict some page by enter_queue
        page = buffer->enter_queue->front();
        buffer->enter_queue->pop_front();
        itr = buffer->page_map->find(page->page_no);
        buffer->page_map->erase(itr);
        page->page_no = page_no; 
    }
    size_t sz_page_no = page_no; 
    size_t offset = sz_page_no * (db->PAGE_SIZE);
    fseek(f, offset, SEEK_SET);
    fread(page->data, 1, db->PAGE_SIZE, f);
    (*(buffer->page_map))[page_no] = page;
    buffer->enter_queue->push_back(page);
    return page;
}


Page* read_index_page(DB* db, int page_no) {
    Page* page = read_page(db, db->index_buffer, db->f_index, db->total_index_pages, page_no);
    return page;
}

Page* read_data_page(DB* db, int page_no) {
    Page* page = read_page(db, db->data_buffer, db->f_data, db->total_data_pages, page_no);
    return page;
}


Page* new_meta_page(DB* db) {
    Page* page = (Page*)malloc(sizeof(Page));
    page->data = (char*)malloc(db->META_PAGE_SIZE);
    size_t N = sizeof(int) * 3;
    memset(page->data, 0, N);
    page->page_no = 0;
    return page;
}

Page* read_meta_page(DB* db, FILE* f) {
    fseek(f, 0, SEEK_SET);
    Page* page = new_meta_page(db);
    fread(page->data, 1, db->META_PAGE_SIZE, f);
    return page;
}

void write_meta_page(DB* db, FILE* f, Page* page) {
    fseek(f, 0, SEEK_SET);
    fwrite(page->data, 1, db->META_PAGE_SIZE, f);
}


