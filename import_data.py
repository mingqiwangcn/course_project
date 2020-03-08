import argparse;
import os;
import db_storage as db;
from tqdm import tqdm
import pickle
import queue
import threading

batch_size = 0

def get_input_files(input_path):
    file_path_lst = []
    N = 7845
    for i in range(N):
        file_name = 'encoded.part.%d' % i
        file_path = os.path.join(input_path, file_name)
        file_path_lst.append(file_path)
    return file_path_lst;

data_queue = queue.Queue(2)

def db_put_data():
    N = 0
    num_batch_items = batch_size * 100
    while True:
        batch_data = data_queue.get()
        if batch_data is None:
            break

        N += (len(batch_data) / num_batch_items)
        db.put(batch_data)
        print('N=%d' % N);
   
    msg = 'total put db %d blocks \n' % N 
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
    global batch_size
    batch_size = args.batch_size
    input_path = args.input_path;
    db_path = args.db_path

    opts = {'max_index_buffer_size':'100', 'max_data_buffer_size':'100'}
    db.open(db_path, opts)

    put_worker = threading.Thread(target=db_put_data)
    put_worker.start()

    file_path_lst = get_input_files(input_path);
    N = len(file_path_lst);
    
    for idx in tqdm(range(0, N, batch_size)):
        data_item_lst = []
        batch_path_lst = file_path_lst[idx:(idx+batch_size)]
        for file_path in batch_path_lst:
            item_list = parse_data(file_path)
            data_item_lst.extend(item_list)
        data_queue.put(data_item_lst)

    data_queue.put(None)
    put_worker.join()
    db.close()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', type=str)
    parser.add_argument('--batch_size', type=int, default=10)
    parser.add_argument('--db_path', type=str)
    args = parser.parse_args()
    return args

def main():
    args = get_args()
    import_data(args)


if __name__ == '__main__':

    main()


