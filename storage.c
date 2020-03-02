#include <stdlib.h>
#include <string.h>
#include "storage.h"

DB* db_open(char* path) {
    if (strlen(path) >= MAX_PATH_SIZE)
        return 0;

    DB* db = (DB*)malloc(sizeof(DB));
    strcpy(db->path, path);

    return db;
}

void db_close(DB* db) {
    return;
}
