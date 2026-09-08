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
    print(priorities)
    print(mask)
    # new_priorities = np.array([4 * (not bool(m)) for m in mask])
    new_priorities = np.where(~mask, np.random.uniform(1.0, 5.0, size=len(mask)), 0.0)
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
    return sequential(priorities, mask, _)

def hybrid(priorities, mask, thresh=0.5):
    # with probability s use sequential, (1-s) use rarest_first 
    s = random.random()
    if s < thresh:
        return sequential(priorities, mask, -1)
    return rarest_random(priorities, mask, -1)

def sequence_random(priorities, mask, b=10):
    b = int(b)
    mask = np.asarray(mask, dtype=bool)
    L = len(mask)
    if L == 0 or b < 1:
        return np.zeros(L, dtype=int)
    
    n = max(1, L // b)
    new_priorities = np.zeros(L, dtype=int)  # everything cancels/stays 0 by default

    # apply sequential to buckets
    n_seq = np.ones(n)
    n_mask = [
        np.sum(mask[m*b:(m+1)*b]) == min(b, L-m*b)
        for m in range(n)
    ]

    buckets = sequential(n_seq, n_mask, -1)
    if not np.any(buckets):
        # every bucket already complete
        return new_priorities
    
    idx = np.argmax(buckets) # max bucket
    low = idx*b
    upp = (idx + 1) * b if idx < n - 1 else L
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
    l = len(downloaded_mask)
    M = np.sum(downloaded_mask)
    if l < 1 or M < 1:
        return -1,-1,-1,1
    U = 0
    for i in range(M):
        if downloaded_mask[i]:
            U+=1
        else:
            return U / M, U, M, l

    # perfect contiguity
    return 1.0, U, M, l

def sequentiality_trajectory(download_order, M):
    """
    Paper-faithful S metric (Eq. 10): average of U(x)/x over the whole
    download trajectory, where U(x) counts pieces in the contiguous run
    from piece 0 after x pieces have arrived (in *arrival* order, not index
    order -- a piece can arrive that isn't yet part of the useful prefix).

    download_order: list of piece indices in the order they completed
    M: total pieces in the file (for reference/normalization by caller)
    Returns S in [0,1], or -1 if nothing has been downloaded yet.
    """
    n = len(download_order)
    if n == 0:
        return -1
    seen = set()
    useful_prefix = 0
    total = 0.0
    for x, piece_idx in enumerate(download_order, start=1):
        seen.add(piece_idx)
        while useful_prefix in seen:
            useful_prefix += 1
        total += useful_prefix / x
    return total / n

def eval_robustness(source_counts, p=0.5):
    """
    R = 1 - p^r̄ (Eq. 9). source_counts: per-piece count of currently-
    connected agents holding FULL_DATA for that piece, for one session.
    """
    source_counts = np.asarray(source_counts, dtype=float)
    if len(source_counts) == 0:
        return -1
    r_bar = float(np.mean(source_counts))
    return 1.0 - (p ** r_bar)