import marshal
from social_signaling.db_access           import DB
from social_signaling.email_analysis      import Message
from social_signaling.util.string_metrics import *

# Connect to the database
print "Connecting to the database."
db = DB.DB()
db.startRedisServer()
db.createRedisConnection()

# Get all message pairs for which a longest common substring has been computed
print "Getting all message pairs with a computed longest common substring."
mps = db.getAllLCSMessagePairs()

# Compute the message pair subject line distances 
print "Compute the message pair subject line distances."
i = 0
dlist = []
msg = Message.Message()
while i<1000000:
    mp = mps[i]
    slist = []
    for mid in mp:
        msg.setMessageID(mid)
        slist.append(msg.Subject)
    lcs = db.getMessagePairLCSubstring(mp,'Subject')
    dlist.append(lcs_dist(slist[0],slist[1],lcs[0]))
    i += 1

    if i % 1000 == 0:
        print "%d distances computed." % i
        
# Save the results
f = open('subject_line_distances.txt','w')
f.write(marshal.dumps(dlist))
f.close()

        