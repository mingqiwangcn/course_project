import db_storage as db;
import random

def test_put():
    opts = {"page_size":"4096"}
    db.open("/home/qmwang/code/course_project/example_db2", opts)

    N = 10000;
    data = {}
    key_lst = []
    for i in range(N):
        key = str(i)
        key_lst.append(key)
        M = random.randint(10, 100);
        value = [random.uniform(1.0, 9.9) for _ in range(M)]
        data[key] = value;

    db.put(data);
    
    db.close()

    # open again to read
    db.open("/home/qmwang/code/course_project/example_db2", opts)
    qry_keys = [];
    for i in range(100):
        pos = random.randint(0, N-1); 
        qry_keys.append(key_lst[pos]);

    query_result = db.get(qry_keys)
    
    for i in range(100):
        key = qry_keys[i]
        value1 = data[key]
        value2 = query_result[i]

        if value1 != value2:
            print(error);

    db.close()    
    print("OK")

def main():
    test_put()

if __name__ == '__main__':
    main();
