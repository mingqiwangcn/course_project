import argparse;
import db_storage as db;
import random

def test_put(args):
    opts = {}
    db.open(args.db_path, opts)

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
 
    stat_data = db.stats();
    print(stat_data)
    
    db.close()

    # open again to read
    db.open(args.db_path, opts)

    stat_data = db.stats();
    print(stat_data)

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

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db_path', type=str)
    args = parser.parse_args()
    return args


def main():
    args = get_args();
    test_put(args)

if __name__ == '__main__':
    main();

