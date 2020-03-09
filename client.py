import requests
import json


def get_item_values(item_keys):
    r = requests.post('http://localhost:8080', json=item_keys)
    item_values = r.json()
    return item_values

def main():
    item_keys = ['0-1', '1-2', '0-3']
    item_values = get_item_values(item_keys)
    
    for value in item_values:
        print(value)
     

if __name__ == '__main__':

    main()

