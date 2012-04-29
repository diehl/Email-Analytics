import time
from datetime                           import timedelta
from social_signaling.db_access         import DB
from social_signaling.email_analysis    import CommRelationship, MessageCollection
from social_signaling.util.suffix_array import *

# Connect to the database
print "Connecting to the database."
db = DB.DB()
db.startRedisServer()
db.createRedisConnection()

# Delete any previous LCS data
print "Clearing previous LCS data."
db.clearMessagePairLCSubstrings()

# Get all of the message ids
print "Fetching all of the message ids."
mids = db.getAllMessageIDs()

# Define two message collections to iterate through
print "Creating the message collections."
mc1 = MessageCollection.MessageCollection(mids)
mc1.timeOrder()
mc2 = mc1.copy()

num_msgs = len(mc1)
print "Total number of messages: %d" % num_msgs
comparisons = 0
prev_comparisons = 0
start = time.clock()
for i in range(num_msgs-1):

    msgA = mc1[i]
    
    # If there's no datetime associated with the message, skip ahead
    if msgA.Datetime == None:
        continue
    
    # Recipients of message A
    recips = msgA.TO
    recips.extend(msgA.CC)
    recips.extend(msgA.BCC)
    
    # Scan ahead to compare with messages in a two day window
    j = i+1
    msgB = mc2[j]
    while j < num_msgs and msgB.Datetime - msgA.Datetime <= timedelta(days=2):
    
        # If the sender of message B is one of the recipients of message A...
        if msgB.Sender in recips:
    
            # Compute the longest common substrings between the message subjects
            lcs = compute_longest_common_substrings([msgA.Subject,msgB.Subject])
            pair = [msgA.MessageID,msgB.MessageID]
            db.setMessagePairLCSubstring(pair,'Subject',lcs)    
            comparisons += 1

        j += 1
        msgB = mc2[j]

    if (i+1) % 500 == 0:
        elapsed = time.clock() - start
        if comparisons > prev_comparisons:
            rate = elapsed / float(comparisons-prev_comparisons)
        prev_comparisons = comparisons
        print "%d messages processed. %d comparisons. %f seconds / comparison." % (i+1,comparisons,rate)
        start = time.clock()