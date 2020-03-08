import argparse;
import os;
import db_storage as db;
from tqdm import tqdm
import pickle
from multiprocessing import Pool as ProcessPool
import queue
import threading

def get_input_files(input_path):
    file_path_lst = []
    part_num = 10
    N = 7845
    for i in range(1, part_num+1):
        part_name = ('part_%d' % i)
        for j in range(N):
            file_name = 'encoded.part.%d' % j
            file_path = os.path.join(input_path, part_name, file_name)
            file_path_lst.append(file_path)
    return file_path_lst;

data_queue = queue.Queue(5)

def db_put_data():
    N = 0
    while True:
        batch_data = data_queue.get()
        if batch_data is None:
            break
        N += len(batch_data)
        db.put(batch_data)
   
    msg = 'total put db %d items \n' % N 
    print(msg)


def parse_data(file_path):
    data_item_lst = []
    with open(file_path, 'rb') as f:
        data = pickle.load(f)
        for item in data:
            doc_id = item[0]
            pid = item[1]
            max_pid = item[2]
            tensor = item[3]
            item_id = "%s,%d" % (doc_id, pid)
            pair = (item_id, tensor)
            data_item_lst.append(pair)
    return data_item_lst

def import_data(args):
    input_path = args.input_path;
    db_path = args.db_path

    opts = {'max_index_buffer_size':'100', 'max_data_buffer_size':'100'}
    db.open(db_path, opts)

    put_worker = threading.Thread(target=db_put_data)
    put_worker.start()

    file_path_lst = get_input_files(input_path);
    
    batch_size = args.batch_size

    N = len(file_path_lst);
    
    work_pool = ProcessPool(args.num_workers)

    data_item_lst = []
    for idx in tqdm(range(0, N, batch_size)):
        batch_path_lst = file_path_lst[idx:(idx+batch_size)]
        M = len(batch_path_lst)
        for encode_data_lst in tqdm(work_pool.imap_unordered(parse_data, batch_path_lst), total=M):
            data_item_lst.extend(encode_data_lst)
            if len(data_item_lst) >= 500:
                data_queue.put(data_item_lst)
                data_item_lst = []

    if len(data_item_lst) > 0:
        data_queue.put(data_item_lst)

    data_queue.put(None)
    put_worker.join()

    db.close()



def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', type=str)
    parser.add_argument('--batch_size', type=int, default=10)
    parser.add_argument('--num_workers', type=int)
    parser.add_argument('--db_path', type=str)
    args = parser.parse_args()
    return args

def main():
    args = get_args()
    import_data(args)


if __name__ == '__main__':

    main()


