#include "storage.h"
#define MAX_NUM_PGAES 1000


Page* new_buffer_page(DB* db) {
    return 0;
}

Page* read_page(DB* db, int page_no) {
    
    return 0;
}

void append_page(DB* db, Page* page) {
    fwrite(page->data, 1, PAGE_SIZE, db->f_data);
}
