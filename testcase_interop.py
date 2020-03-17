import argparse;
import db_storage as db
import db_cluster as db_cls
import random
import json
import time

def test_put(args):
    opts = {}
    db.open(args.db_path, opts)

    N = 10000;
    data = []
    data_map = {}
    key_lst = []
    for i in range(N):
        key = str(i)
        key_lst.append(key)
        M = random.randint(10, 100);
        value = [random.uniform(1.0, 9.9) for _ in range(M)]
        pair = (key, value)
        data.append(pair);
        data_map[key] = value

    db.put(data);
 
    stat_data = db.stats();
    
    db.close()

    # open again to read
    db.open(args.db_path, opts)

    stat_data = db.stats();

    qry_keys = [];
    for i in range(100):
        pos = random.randint(0, N-1); 
        qry_keys.append(key_lst[pos]);

    query_result = db.get(qry_keys)
    
    for i in range(100):
        key = qry_keys[i]
        value1 = data_map[key]
        value2 = query_result[i]

        if value1 != value2:
            print("error");
            raise

    db.close()    
    print("OK")

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db_path', type=str)
    args = parser.parse_args()
    return args

def cls_get():
    
    db.open('/home/cc/data/part_10/large_page_data/', {})

    db.close()
'''
    db_cls.open('/home/cc/data/', {})
    
    with open('/home/cc/code/fabric-qa-local/expts/keys_4.txt') as f:
        item_keys = json.load(f)

    t1 = time.time()
    values = db_cls.get(item_keys);
    t2 = time.time()

    print('time %.2f' % (t2-t1))

    db_cls.close()
'''

def main():
    args = get_args();
    #test_put(args)
    cls_get()

if __name__ == '__main__':
    main();

