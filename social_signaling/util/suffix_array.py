def construct_suffix_array(lst):
    """Given a list lst of integers of length n, returns a list of indices indicating the
    order of the sublists lst[k:]."""
    sarray = range(len(lst))
    sarray.sort(key=lambda i: lst[i:])    
    return sarray

def longest_common_prefix(lst1,lst2):
    """Returns the longest common list lst1[0:k] for the given lists of integers."""
    k = 0
    min_len = min(len(lst1),len(lst2))
    while k < min_len and lst1[k] == lst2[k]:
        k += 1
    return lst1[0:k]
    
def construct_lcp_array(lst,sarray):
    """Given a list of integers lst of length n along with its corresponding suffix 
    array, returns a list of length n-1 with the lengths of the longest common 
    prefixes for consecutive pairs of suffixes as ordered in the suffix array."""
    
    # Compute the inverse of the suffix array mapping
    isarray = [-1] * len(sarray)
    j = 0
    for i in sarray:
        isarray[i] = j
        j += 1
    
    lcp = [-1] * (len(lst)-1)
    for i in range(len(lst)):
    
        rank_i = isarray[i]
        rank_im1 = isarray[i-1]
    
        # If the ith suffix is ranked last in the ordering, there's 
        # nothing to compute. move to the next iteration. 
        if rank_i == len(lst)-1:
            continue
    
        # Determine the suffix that follows i in the ordering
        j = sarray[rank_i+1]
        
        # If it's not the first iteration and the i-1th suffix wasn't the last
        # in the ordering...
        if i > 0 and rank_im1 != len(lst)-1:
            
            # Skip the first max(lcp[rank_im1]-1,0) integers since we know 
            # lcp[rank_i] >= lcp[rankim1]-1
            l = max(lcp[rank_im1]-1,0)
    
        # Otherwise...    
        else:
            l = 0
        
        # Perform the comparison
        lst1 = lst[i+l:]
        lst2 = lst[j+l:]        
        lcp[rank_i] = len(longest_common_prefix(lst1,lst2)) + l
            
    return lcp

def construct_bwt_array(lst,sarray):
    """Given a list of integers lst and the corresponding suffix array sarray, returns an array
    containing the Burrows and Wheeler transformation. Each entry in the array contains
    the integer directly preceeding the suffix in lst referenced by the corresponding entry 
    in sarray."""
    bwt_array = []
    for i in sarray:
        if i > 0:
            bwt_array.append(lst[i-1])
        else:
            bwt_array.append(None)
    return bwt_array

def print_ordered_suffixes(lst,sarray,lcp_array):
    """Prints out the sublists lst[k:] of the list of integers lst in order."""
    j = 0
    llen = len(lst)
    print "SA\tLCP\tSuffix"
    for i in sarray:
        if j < llen-1:
            lcp = lcp_array[j]
        else:
            lcp = -1 
        print "%d\t%d\t%s" % (i,lcp,str(lst[i:]))
        j += 1

def convert_string_to_list_of_ints(s):
    """Converts each character in the string s to its ordinal value. Returns the resulting 
    list of integers."""
    return map(lambda c : ord(c),s)
    
def convert_list_of_ints_to_string(ilist):
    """Converts the list of integer representation for a string to the corresponding string."""
    return ''.join(map(lambda i : unichr(i),ilist))

def represent_concatenated_strings_with_sentinels(slist):
    """Given a list of strings, returns a list of integers representing all strings in slist
    concatenated together and separated by sentinel characters that are lexicographically
    less than any of the characters in the given strings."""
    
    # Convert all of the strings to lists of integers 
    ilist = [convert_string_to_list_of_ints(s) for s in slist]

    # Concatenate lists of integers together with negative integers representing the sentinels
    ulist = []
    i = -len(slist)
    for srep in ilist:
        srep.append(i)
        ulist.extend(srep)
        i += 1
        
    return ulist

def compute_suffix_type(pos,slengths):
    """Returns the suffix type given the position and the string lengths.
    Assumes the string lengths do not include the sentinel characters."""
    type = 0
    total = slengths[type]+1
    while pos >= total:
        type += 1
        total += slengths[type]+1
    return type

def compute_longest_common_substrings(slist, verbose=False):
    """Returns a list of the longest common substrings for the given list of strings."""
    
    # Number of strings
    num_strings = len(slist)
    
    # Compute the lengths of the given substrings
    slengths = [len(s) for s in slist]
    
    # Check for any zero length strings. Return the empty string if a zero length string was given.
    if 0 in slengths:
        return ['']
    
    # Construct the list of integers from the list of strings
    ulist = represent_concatenated_strings_with_sentinels(slist)
        
    # Construct the suffix array for ulist
    sa = construct_suffix_array(ulist)
    
    # Construct the lcp array
    lcp = construct_lcp_array(ulist,sa)
    
    # Print the suffix array and longest common prefix array as necessary
    if verbose:
        print_ordered_suffixes(ulist,sa,lcp)
        print
    
    # Compute the suffix types
    suffix_types = [compute_suffix_type(sa[i],slengths) for i in range(len(sa))]

    # Begin scan through the suffix array. we can skip the first num_string entries 
    # since those will correspond to the sentinel characters.
    win_begin = num_strings
    win_end = num_strings
    
    # Define the limit for win_begin and win_end
    ulist_len = len(ulist)
    win_begin_limit = ulist_len-num_strings+1
    win_end_limit = ulist_len-1

    # Collection of lcp values for the window
    lcp_win = []
    
    # Minimum lcp value for the window
    min_lcp_win = ulist_len
    
    # Type count array for the window
    type_counts = [0] * num_strings
    
    # Types represented in the window
    type_counts[suffix_types[win_begin]] += 1
    num_types_present = 1

    # Maximum of the minimum window lcp's seen so far
    max_lcp = -1
    
    # Longest common substrings seen so far
    lcs = []

    if verbose:
        print "win\twin\t\tmin"
        print "begin\tend\tlcp_win\tlcp_win"

    while win_begin < win_begin_limit:
    
        # Enlarge the window until num_string types are present or win_end_limit
        # is reached
        while num_types_present < num_strings and win_end < win_end_limit:
        
            win_end += 1
            
            # Update the lcp array for the window
            lcp_win.append(lcp[win_end-1])
            
            # Update the minimum lcp value for the window
            min_lcp_win = min(lcp[win_end-1],min_lcp_win)
            
            # Update the type counts and the number of types present
            type_counts[suffix_types[win_end]] += 1
            num_types_present = sum([i > 0 for i in type_counts])
        
        # If the window has all types represented...
        if num_types_present == num_strings:
        
            if verbose:
                print "%d\t%d\t%s\t%d" % (win_begin,win_end,str(lcp_win),min_lcp_win)

            # Check to see if the minimum exceeds max_lcp
            if min_lcp_win > max_lcp:
                max_lcp = min_lcp_win
                lcs = [ulist[sa[win_begin]:sa[win_begin]+max_lcp]]
            
            # If the minimum equals max_lcp, check to see if we should save this substring
            if min_lcp_win == max_lcp:
                new_s = ulist[sa[win_begin]:sa[win_begin]+max_lcp]
                if not new_s in lcs:
                    lcs.append(new_s)
                
            # Shift the beginning of the window and update lcp_win and min_lcp_win
            win_begin += 1
            lcp_win.pop(0)
            if lcp[win_begin-1] == min_lcp_win:
                if len(lcp_win) > 0:
                    min_lcp_win = min(lcp_win)
                else:
                    min_lcp_win = ulist_len
        
            # Update type counts
            type_counts[suffix_types[win_begin-1]] -= 1
            if type_counts[suffix_types[win_begin-1]] == 0:
                num_types_present -= 1
    
        # If the window end is at its limit...
        elif win_end == win_end_limit:
        
            # There is no more searching to be done, since advancing the beginning of the 
            # window will not make num_types_present = num_strings. So we can end the search.
            win_begin = win_begin_limit
               
    # Convert the longest common substrings from their integer representation to strings
    for i in range(len(lcs)):
        lcs[i] = convert_list_of_ints_to_string(lcs[i])
    
    if verbose:
        print
        print "max_lcp\tlongest common substring"
        print "%d\t%s" % (max_lcp,str(lcs))
    
    return lcs
    
def compute_supermaximal_repeats(slist, verbose=False):
    
    # Number of strings
    num_strings = len(slist)
    
    # Compute the lengths of the given substrings
    slengths = [len(s) for s in slist]    
    
    # Construct the list of integers from the list of strings
    ulist = represent_concatenated_strings_with_sentinels(slist)    
        
    # Construct the suffix array for ulist
    sa = construct_suffix_array(ulist)
    
    # Construct the lcp array
    lcp = construct_lcp_array(ulist,sa)
    
    # Construct the bwt array
    bwt = construct_bwt_array(ulist,sa)
    
    # Print the suffix array and longest common prefix array as necessary
    if verbose:
        print_ordered_suffixes(ulist,sa,lcp)
        print
        print "((win_begin,win_end),num_types_present)"
    
    # Compute the suffix types
    suffix_types = [compute_suffix_type(sa[i],slengths) for i in range(len(sa))]

    # Begin scan through the suffix array. We can skip the first num_string entries 
    # since those will correspond to the sentinel characters.
    win_begin = num_strings
    
    # Define the limits for the beginning and end of the window
    ulist_len = len(ulist)
    win_begin_limit = ulist_len-num_strings+1
    win_end_limit = ulist_len-1
    
    # Supermaximal repeats
    smr = []

    while win_begin < win_begin_limit:
    
        # Advance win_begin until lcp[win_begin-1] < lcp[win_begin] which 
        # signifies the start of a potential local maximum
        while lcp[win_begin-1] >= lcp[win_begin] and win_begin < win_begin_limit:
            win_begin += 1
            
        # Check to see if we've reached the end
        if win_begin == win_begin_limit:
            continue
            
        # Set the initial position for the end of the window
        win_end = win_begin+1
        
        # Initialize lcp local maximum value over the window
        lcp_local_max = lcp[win_begin]
        
        # Type count array for the window
        type_counts = [0] * num_strings
        type_counts[suffix_types[win_begin]] = 1
        type_counts[suffix_types[win_end]]   = 1
        num_types_present = sum([i > 0 for i in type_counts])
        
        # Enlarge the window until a local maximum is found or win_end_limit
        # is reached
        while win_end < win_end_limit and lcp[win_end-1] == lcp[win_end]:
            win_end += 1
            
            # Update the type counts and the number of types present
            type_counts[suffix_types[win_end]] += 1
            num_types_present = sum([i > 0 for i in type_counts])
        
        # If the window is a local maximum...
        if win_end == win_end_limit or lcp[win_end-1] > lcp[win_end]:
        
            # Check to see if it is a supermaximal repeat
            supermax = True
            prev_chars = set()
            for i in range(win_begin,win_end+1):
                if bwt[i] in prev_chars:
                    supermax = False
                    break
                else:
                    prev_chars.add(bwt[i])    
                    
            # If it is a supermaximal repeat, add it to the list
            if supermax:
                prefix = convert_list_of_ints_to_string(ulist[sa[win_begin]:sa[win_begin]+lcp[win_begin]])
                new_smr = ((win_begin,win_end),num_types_present,prefix)
                smr.append(new_smr)
                
                if verbose:
                    print "%s\t\'%s\'" % (str(new_smr),prefix)
                
        # Move win_begin forward
        win_begin = win_end
        
    return smr
