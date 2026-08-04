import numpy as np
import matplotlib.pyplot as plt
from torrent.piece_pickers import (
    get_policy, rarest_random, sequential, cascading, hybrid, sequence_random
)

import pdb

def download(arr, mask):
    # sim libtorrent piece picker
    # among highest priority, random selection
    arr = np.array(arr)
    max_indices = np.where(arr == arr.max())[0]  # Returns array([1, 3, 5])
    r_max = np.random.choice(max_indices)
    arr[r_max] = 0
    mask[r_max] = True

    return arr, mask

policies = ['sequential', 'random-rarest', 'hybrid', 'sequence-random']

if __name__ == "__main__":
    for pol in policies:
        policy = get_policy(pol)
        print(f"pol : {policy.__name__}")
        n = 100 # number of pieces
        s = 0.2 # probability for hybrid method
        N = 15 # number of buckers TODO: handle remainders
        priorities = np.ones(n) * 4
        mask = [False] * n
        res = np.zeros((n,n))

        new_priorities = priorities
        count = 0
        while(np.sum(mask) != len(mask)):
            if pol == 'hybrid':
                new_priorities = policy(new_priorities, mask, s)
            elif pol == 'sequence-random':
                new_priorities = policy(new_priorities, mask, N)
            else:
                new_priorities = policy(new_priorities, mask, -1)
            res[count,:] = new_priorities
            new_priorities, mask = download(new_priorities, mask)
            count += 1

        print(res)
        plt.imshow(res)
        plt.xlabel('piece index')
        plt.ylabel('download round')
        plt.title(pol)
        # plt.xticks(range(0,n))
        # plt.yticks(range(0,n))
        plt.colorbar()
        plt.show()



