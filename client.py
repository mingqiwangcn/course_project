import requests
import json
import time

def get_item_values(item_keys):
    r = requests.post('http://localhost:8080', json=item_keys)
    item_values = r.json()
    return item_values

def main():
    with open('item_keys.json') as f:
        item_keys = json.load(f)

    t1 = time.time()
    item_values = get_item_values(item_keys)
    t2 = time.time()
    
    print('time %.2f' % (t2 - t1))

    print(len(item_values)) 

if __name__ == '__main__':

    main()

