import numpy as np
import random
import pdb

"""
Define Piece Picking policies
"""

def rarest_random(priorities, mask, _):
    """
    Vanilla Rarest-First (all priorities=4)
    """
    # reset all priorities to 4
    new_priorities = np.array([4 * bool(x) for x in priorities])
    return new_priorities.astype(int).tolist()

def sequential(priorities, mask, _):
    """
    Assign priorities based on upcoming missing sequential file
    """
    new_priorities = np.zeros_like(priorities)
    # find first 6 undownloaded pieces
    p = 7 # start with max priority
    for i, m in enumerate(mask):
        if m: # downloaded already
            continue

        new_priorities[i] = p
        if p > 1:
            p -= 1

    return new_priorities.astype(int).tolist()

def cascading(priorities, mask):
    pass

def hybrid(priorities, mask, thresh=0.5):
    # with probability s use sequential, (1-s) use rarest_first 
    s = random.random()
    if s < thresh:
        new_priorities = sequential(priorities, mask, _)
    else:
        new_priorities = rarest_random(priorities, mask, _)

    return new_priorities.astype(int).tolist()

def sequence_random(priorities, mask, n=10):
    b = int(len(mask) / n) # bucket size
    new_priorities = np.array([1 * bool(x) for x in priorities])
    # apply sequential to buckets
    n_seq = np.ones(n)
    n_mask = []
    for m in range(n):
        if np.sum(mask[m*b:(m+1)*b]) == b: # all pieces in this seq downloaded
            n_mask.append(True)
        else:
            n_mask.append(False)

    buckets = sequential(n_seq, n_mask, _)
    idx = np.argmax(buckets) # max bucket
    low = idx*b
    upp = (idx+1)*b
    sub_bucket = priorities[low:upp]
    sub_mask = mask[low:upp]

    new_bucket = rarest_random(sub_bucket, sub_mask, _)
    new_priorities[low:upp] = new_bucket
    return new_priorities.astype(int).tolist()

def get_policy(arg : str):
    if arg == 'sequential' or arg == 's':
        return sequential 
    elif arg == 'cascading'  or arg == 'c':
        return cascading
    elif arg == 'hybrid'  or arg == 'rs':
        return hybrid
    elif arg == 'sequence-random'  or arg == 'sr':
        return sequence_random
    else:
        return rarest_random # default