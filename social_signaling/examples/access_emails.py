from social_signaling.db_access import DB
from social_signaling.email_analysis import MessageCollection

# start the Redis server
db = DB.DB()
db.startRedisServer()

# create the connection to the Redis server
db.createRedisConnection(DB.ENRON)

# get all of the message ids
mids = db.getAllMessageIDs()

# get the first 10 messages from the data
msgs = MessageCollection.MessageCollection(mids[0:10])

# print out the messages
for msg in msgs:
    print msg

# stop the Redis server
db.stopRedisServer()
