import numpy as np
import matplotlib.pyplot as plt
from torrent.piece_pickers import (
    get_policy, ones_filter, rarest_random, sequential, cascading, hybrid, sequence_random
)

import pdb

def download(arr, mask):
    arr = np.array(arr)
    mask = np.array(mask)
    
    # Only consider pieces that have a priority > 0
    valid_indices = np.where(arr > 0)[0]
    
    if len(valid_indices) == 0:
        # No available pieces to download in this round
        return arr, mask
        
    # Find the maximum priority among valid pieces
    max_val = arr[valid_indices].max()
    max_indices = np.where(arr == max_val)[0]
    
    r_max = np.random.choice(max_indices)
    arr[r_max] = 0
    mask[r_max] = True

    return arr, mask

policies = ['sequential', 'random-rarest', 'hybrid', 'sequence-random']
# policies = ['sequence-random']

if __name__ == "__main__":
    for pol in policies:
        policy = get_policy(pol)
        print(f"pol : {policy.__name__}")
        n = 100 # number of pieces
        s = 0.5 # probability for hybrid method
        b = 10 # bucket size
        priorities = np.ones(n) * 4
        mask = [False] * n
        res = np.zeros((n,n))

        new_priorities = priorities
        count = 0

        # download rounds
        while(count < n):
            if pol == 'hybrid':
                new_priorities = policy(new_priorities, mask, s)
            elif pol == 'sequence-random':
                new_priorities = policy(new_priorities, mask, b)
            else:
                new_priorities = policy(new_priorities, mask, -1)
            new_priorities = ones_filter(new_priorities)
            res[count,:] = (new_priorities)
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



