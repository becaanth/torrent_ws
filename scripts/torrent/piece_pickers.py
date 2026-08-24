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
    new_priorities = np.array([4 * (not bool(m)) for m in mask])
    return new_priorities

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

    return new_priorities

def cascading(priorities, mask):
    pass

def hybrid(priorities, mask, thresh=0.5):
    # with probability s use sequential, (1-s) use rarest_first 
    s = random.random()
    if s < thresh:
        new_priorities = sequential(priorities, mask, -1)
    else:
        new_priorities = rarest_random(priorities, mask, -1)

    return new_priorities

def sequence_random(priorities, mask, b=10):
    b = int(b)
    n = len(mask) // b

    new_priorities = np.zeros(len(mask), dtype=int)  # everything cancels/stays 0 by default

    # apply sequential to buckets
    n_seq = np.ones(n)
    n_mask = [
        np.sum(mask[m*b:(m+1)*b]) == b
        for m in range(n)
    ]

    buckets = sequential(n_seq, n_mask, -1)
    idx = np.argmax(buckets) # max bucket

    low = idx*b
    upp = (idx + 1) * b if idx < n - 1 else len(mask)
    sub_bucket = priorities[low:upp]
    sub_mask = mask[low:upp]

    new_bucket = rarest_random(sub_bucket, sub_mask, -1)
    new_priorities[low:upp] = new_bucket
    return new_priorities

def ones_filter(prio):
    """
    floor all priority=1 to =0 to cancel in-flight requests
    need this to cancel in-flight
    """
    prio_copy = prio.copy()
    prio_copy[np.where(prio_copy == 1)] = 0
    return prio_copy


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

def eval_seq(downloaded_mask):
    """
    return sequentiality metric defined by Fan et al.
    S = (U_0 + U_1 + ... + U_N)/M,
    where U_i denotes a 'useful' chunk, that is in-order,
    M denotes the total number of chunks

    downloaded_mask: list<bool>
    """
    M = len(downloaded_mask)
    U = 0

    l = len(downloaded_mask)
    if l < 1: # guard
        return -1,-1,-1
    
    for i in range(l):
        if downloaded_mask[i]:
            U+=1
        else:
            return U/M, U, M