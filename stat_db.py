import numpy as np

def stat():
    data = []
    with open('./stat.txt') as f:
        for line in f:
            data.append(int(line))

    pct = np.percentile(data, 95)

    total_size = 0
    data_2 = []
    for item in data:
        if item > pct:
            data_2.append(item)

    
    sum_size = np.sum(data_2);
    
    unit = 1024 * 1024 * 1024
    print("pct_95 %d B %d K, data_2 count %d, space %.2f G" % (pct ,pct / 1024, len(data_2), sum_size / unit ))

if __name__ == '__main__':

    stat()
