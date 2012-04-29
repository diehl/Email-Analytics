from social_signaling.util.suffix_array import *

def lcs_dist(s1,s2,lcs=None):
    """Returns the fraction of the maximum string length for s1 and s2 that is not 
    attributed to the longest common substring. The distance d ranges over [0,1].
    If d = 0, s1 = s2. If d = 1, s1 and s2 share no characters in common. Returns 0
    if both strings are empty."""
    if lcs == None:
        lcs = compute_longest_common_substrings([s1,s2])[0]
    lcs_length = len(lcs)
    max_length = float(max(len(s1),len(s2)))
    if max_length == 0.0:
        return 0.0
    else:
        return 1 - ( lcs_length / float(max(len(s1),len(s2))) )
    
    