import argparse;
import os;
import db_storage as db;
from tqdm import tqdm
import pickle

def get_input_files():
    file_path_lst = []
    part_num = 10
    N = 7845;
    for i in range(1, part_num+1):
        part_name = ('part_%d' % i)
        for j in range(N):
            file_name = 'encoded.part.%d' % j
            file_path = os.path.join(part_name, file_name)
            file_path_lst.append(file_path)
    return file_path_lst;

def import_data(args):
    input_path = args.input_path;
    db_path = args.db_path
    opts = {}
    db.open(db_path, opts)
    file_path_lst = get_input_files();
    for file_path in tqdm(file_path_lst):
        file_full_path = os.path.join(input_path, file_path)
        map_items = {}
        with open(file_full_path, 'rb') as f:
            data = pickle.load(f)
            for item in data:
                doc_id = item[0]
                pid = item[1]
                max_pid = item[2]
                tensor = item[3]
                item_id = "%s,%d" % (doc_id, pid)
                map_items[item_id] = tensor
        db.put(map_items)

    db.close()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', type=str)
    parser.add_argument('--db_path', type=str)
    args = parser.parse_args()
    return args

def main():
    args = get_args()
    import_data(args)


if __name__ == '__main__':

    main()


