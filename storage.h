#ifndef STORAGE_H
#define STORAGE_H

#define KEY_SIZE 116
#define MAX_PATH_SIZE 200

typedef struct DB {
    char path[MAX_PATH_SIZE];
      
} DB;


DB* db_open(char* path);

void db_close(DB* db);

#endif
