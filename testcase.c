#include "storage.h"

int main() {
    char path[100] = "/home/cc/md0/passage_db";
    DB* db = db_open(path);
    db_close(db);
    return 0;
}
