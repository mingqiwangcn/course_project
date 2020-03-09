import requests
import json

def main():
    data = ['01', '02', '03']
    r = requests.post('http://localhost:8080', json=data)
    print(r.json())
    

if __name__ == '__main__':

    main()

