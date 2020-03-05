#include <string.h>
#include "storage.h"


void test_put() {
    char path[100] = "/home/qmwang/code/course_project/passage_db";
    DB* db = db_open(path);  
    int N = 10000;
    int i = 0;
    
    srand(1);

    DataItem* p_items = (DataItem*)malloc(sizeof(DataItem)*N);
    for (i = 0; i < N; i++) {
        char key[20];
        sprintf(key, "%d", i);
        strcpy(p_items[i].key, key);
        int M =  rand() % 2000;
        char C = (rand() % 26) + 'a';
        p_items[i].value = (char*)malloc(M);
        memset(p_items[i].value, C, M);
        p_items[i].value[M-1] = '\0';
        printf("%s", p_items[i].value);
        p_items[i].size = M;
    }

    db_put(db, p_items, N);

    db_close(db);
}

int main() {
    test_put();
    return 0;
}


