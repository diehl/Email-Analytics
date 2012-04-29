import marshal
from social_signaling.db_access import DB
from social_signaling.email_analysis import Message

# connect to redis
db = DB.DB()
db.startRedisServer()
db.createRedisConnection()
r = db.redisDB

# get all of the message strings
orig_msgs = r.hvals('messages')

# remove message_id from the message dictionaries
i = 0
dup_dict = {}
for mstr in orig_msgs:
    mdict = marshal.loads(mstr)
    mid = mdict['message_id']
    del mdict['message_id']
    mstr = marshal.dumps(mdict)
    if dup_dict.has_key(mstr):
        dup_dict[mstr].append(mid)
    else:
        dup_dict[mstr] = [mid]
    i += 1
    if i % 500 == 0:
        print "%d messages processed." % i    

# compare counts    
print "%d original messages." % len(orig_msgs)
print "%d unique messages." % len(dup_dict.keys())

# count the sets of message duplicates and print out duplicate messages
msg = Message.Message()
msg_sets = 0
for mids in dup_dict.values(): 
    if len(mids) > 1:
        msg_sets += 1
        msg.setMessageID(mids[0])
        #print msg
print "%d sets of duplicate messages." % msg_sets

