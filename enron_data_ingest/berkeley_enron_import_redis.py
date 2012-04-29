# import the modules
import cgi
import marshal
import MySQLdb
import redis
from calendar          import timegm
from dateutil          import tz
from dateutil.parser   import parse

# create a connection to the MySQL databases
UCBdb = MySQLdb.connect(host='localhost',user='root',passwd='',db='berkeley_enron')
ISIdb = MySQLdb.connect(host='localhost',user='root',passwd='',db='isi_enron')

# create a connection to the redis database
rdb = redis.Redis(host='localhost', port=6379, db=0)

# delete previous keys if they exist
rdb.delete('fully_observed_addresses','messages','social_relationships','sorted_message_ids',
           'mids_per_directed_comm_relationship', 'recipients_per_sender_address', 
           'senders_per_recipient_address')

# fetch message information from the UC Berkeley MySQL database
print "Pulling message data from the UC Berkeley MySQL database."
cursor = UCBdb.cursor()
cursor.execute("select t2.email, t1.date, t1.timezone, t1.smtpid, t1.subject, t1.body from msgs as t1, addresses as t2 where t1.eid = t2.eid;")
results = cursor.fetchall()

# fetch email addresses corresponding to the 151 email inboxes that make up the dataset
print "Pulling email address data from the USC/ISI MySQL database."
cursor = ISIdb.cursor()
cursor.execute("select Email_id as email from employeelist;")
addressTuples = cursor.fetchall()
fullyObservedAddresses = [e[0] for e in addressTuples]

# find and modify the specific address that appears to be incorrectly represented in the ISI database
for i in range(len(fullyObservedAddresses)):
    if fullyObservedAddresses[i] == 'paul.y barbo@enron.com':
        break
fullyObservedAddresses[i] = "paul.y'barbo@enron.com"

# add the set of fully observed addresses to the redis database
for address in fullyObservedAddresses:
    rdb.sadd('fully_observed_addresses',address)

# for each message...
numMsgs = len(results)
for i in xrange(numMsgs):

    # get the message
    msg = results[i]

    # get the message datetime
    msgDT = msg[1]

    # get the message timezone
    msgTZ = msg[2]

    if msgDT != None:

        # create new datetime object with time normalized to UTC
        msgDT = parse(msgDT.strftime('%Y/%m/%d %H:%M:%S') + ' ' + msgTZ[:5]).astimezone(tz.tzoffset('',0))
        
        # convert the datetime into seconds since the epoch
        msgDTsecs = timegm(msgDT.utctimetuple())

        # message datetime string
        msgDTstring = str(msgDT)

    else:

        # message datetime string
        msgDTstring = 'None'

        # seconds since the epoch
        msgDTsecs = 0

    # get the message ID
    msgID = msg[3]

    # get the message subject
    msgSubj = msg[4]

    # get the message body
    msgBody = msg[5]

    # get the message sender's email address
    sender = msg[0]

    # decode the address from ISO-8859-1 and encode in ASCII preserving extended characters
    sender = cgi.escape(sender.decode('iso-8859-1')).encode('ascii','xmlcharrefreplace')

    # create a message dictionary
    mDict = { 'message_id' : msgID, 'datetime' : msgDTstring, 'epoch_secs' : msgDTsecs, 
              'subject' : msgSubj, 'body' : msgBody, 'sender' : sender, 'to' : [],
              'cc' : [], 'bcc' : [] }

    # add message dictionary to the redis hash
    rdb.hset('messages',msgID,marshal.dumps(mDict))

    if (i+1) % 500 == 0:
        print "%d messages processed." % (i+1)

# fetch recipient information from the MySQL database
print "Pulling recipient data from the MySQL database."
cursor = UCBdb.cursor()
cursor.execute("select t1.smtpid, t2.reciptype, t2.reciporder, t3.email from msgs as t1, recip_info as t2, addresses as t3 where t1.mid = t2.mid and t2.eid = t3.eid;")
results = cursor.fetchall()

# for each recipient...
numRecips = len(results)
for i in xrange(numRecips):

    # get the recipient information
    recip = results[i]

    # get the message ID
    msgID = recip[0]

    # get the recipient type
    recipType = recip[1]

    # get the recipient order
    recipOrder = int(recip[2])

    # get the recipient email address
    recipEmail = recip[3]
    
    # decode the address from ISO-8859-1 and encode in ASCII preserving extended characters
    recipEmail = cgi.escape(recipEmail.decode('iso-8859-1')).encode('ascii','xmlcharrefreplace')

    # fetch message dictionary from redis
    mDictStr = rdb.hget('messages',msgID)
    mDict = marshal.loads(mDictStr)

    # add recipient information to the message
    rTup = (recipEmail,recipOrder)
    if mDict.has_key(recipType):
        mDict[recipType].append(rTup)
    else:
        mDict[recipType] = [rTup]

    # push message dictionary back to redis
    rdb.hset('messages',msgID,marshal.dumps(mDict))

    if (i+1) % 5000 == 0:
        print "%d recipients processed." % (i+1)

# generate ordered lists for the message recipients and a sorted set of message ids
i = 0
for msgID in rdb.hkeys('messages'):

    # fetch message dictionary from redis
    mDictStr = rdb.hget('messages',msgID)
    mDict = marshal.loads(mDictStr)

    # sort the recipients by their position info and reduce the lists down to the addresses
    # only, now in the proper order
    fields = ['to','cc','bcc']
    for field in fields:
        mDict[field].sort(key=lambda tup: tup[1])
        mDict[field] = [tup[0] for tup in mDict[field]]

    # push message dictionary back to redis
    rdb.hset('messages',msgID,marshal.dumps(mDict))
    
    # add message id to the sorted set
    rdb.zadd('sorted_message_ids',mDict['message_id'],mDict['epoch_secs'])
                    
    i += 1
    if i % 500 == 0:
        print "%d messages processed." % i

# fetch manager-subordinate relationship information from the MySQL database
print "Pulling manager-subordinate relationship information from the MySQL database."
cursor = UCBdb.cursor()
cursor.execute("select * from sub_mgr_pairs_in_collection;")
results = cursor.fetchall()

# for each relationship pair
print "Adding manager-subordinate relationships to the redis database."
for rpair in results:

   # create the relationship in the redis database
   relDict = { 'type' : 'directly reported to', 'target' : rpair[1], 'evidence_type' : 'interval',
               'start_time' : '2000-01-01 00:00:00 +00:00', 'end_time' : '2001-11-31 23:59:59 +00:00',
               'provenance' : 'http://tinyurl.com/cfsooc' }
   rdb.hset('social_relationships',rpair[0],marshal.dumps([relDict]))

# generate three hash tables: 
# - one mapping (sender email address, recipient email address) tuples to lists of
#   (message id, epoch secs) tuples
# - one mapping sender email addresses to sets of (recipient email address,
#   (min epoch secs, max epoch secs)) tuples
# - one mapping recipient email addresses to sets of (sender email address,
#   (min epoch secs, max epoch secs)) tuples
i = 0
for msgID in rdb.hkeys('messages'):

    # fetch message dictionary from redis
    mDictStr = rdb.hget('messages',msgID)
    mDict = marshal.loads(mDictStr)
    
    # aggregate all of the recipient addresses into a set to avoid duplicates. some email 
    # addresses are added to more than one recipient field (to/cc/bcc).
    recips = set()
    fields = ['to','cc','bcc']    
    for field in fields:
        for recip in mDict[field]:
            recips.add(recip)
        
    for recip in recips:
    
        # if the sender and recipient email addresses are the same, skip this relationship
        if mDict['sender'] == recip:
            continue
    
        # relationship
        rel = (mDict['sender'],recip)
    
        # grab existing relationship information if it exists
        if rdb.exists('mids_per_directed_comm_relationship'):

            if rdb.hexists('mids_per_directed_comm_relationship',str(rel)):
    
                # get the list of (message id, epoch secs) tuples
                tList = marshal.loads(rdb.hget('mids_per_directed_comm_relationship',str(rel)))
        
            else:
                tList = []
        
        else:
            tList = []
    
        # push (message id, epoch secs) tuple into list such that tuples are in ascending order
        # relative to epoch secs
        k = 0
        numTups = len(tList)
        while k < numTups and tList[k][1] < mDict['epoch_secs']:
            k += 1
        tList.insert(k,(mDict['message_id'],mDict['epoch_secs']))
                
        # save the relationship list
        rdb.hset('mids_per_directed_comm_relationship',str(rel),marshal.dumps(tList))

        # grab the set of recipients for the sender email address if it exists
        if rdb.hexists('recipients_per_sender_address',mDict['sender']):
        
            # get the set of recipients
            rSet = marshal.loads(rdb.hget('recipients_per_sender_address',mDict['sender']))
            
        else:
        
            rSet = set()

        # check to see if there is an element for the recipient
        el = next((tup for tup in rSet if tup[0] == recip),None)
        
        # if no element exists
        if el == None:
        
            # add the recipient to the set
            rSet.add((recip,(mDict['epoch_secs'],mDict['epoch_secs'])))
            
        # otherwise, check to see if modifications to the relationship time interval are 
        # needed
        else:
        
            # remove the original element from the set
            rSet.remove(el)
        
            # modify the element as needed
            minTime = el[1][0]
            maxTime = el[1][1]
            msgTime = mDict['epoch_secs']
            if msgTime < minTime:
                el = (el[0],(msgTime,maxTime))
            if msgTime > maxTime:
                el = (el[0],(minTime,msgTime))
                
            # add the new element to the set
            rSet.add(el)
        
        # push the set back into redis
        rdb.hset('recipients_per_sender_address',mDict['sender'],marshal.dumps(rSet))
        
        # grab the set of senders for the recipient email address if it exists
        if rdb.hexists('senders_per_recipient_address',recip):
        
            # get the set of senders
            sSet = marshal.loads(rdb.hget('senders_per_recipient_address',recip))
            
        else:
        
            sSet = set()
            
        # check to see if there is an element for the sender
        el = next((tup for tup in sSet if tup[0] == mDict['sender']),None)
        
        # if no element exists
        if el == None:
        
            # add the recipient to the set
            sSet.add((mDict['sender'],(mDict['epoch_secs'],mDict['epoch_secs'])))
            
        # otherwise, check to see if modifications to the relationship time interval are 
        # needed
        else:
        
            # remove the original element from the set
            sSet.remove(el)
        
            # modify the element as needed
            minTime = el[1][0]
            maxTime = el[1][1]
            msgTime = mDict['epoch_secs']
            if msgTime < minTime:
                el = (el[0],(msgTime,maxTime))
            if msgTime > maxTime:
                el = (el[0],(minTime,msgTime))
                
            # add the new element to the set
            sSet.add(el)
        
        # push the set back into redis
        rdb.hset('senders_per_recipient_address',recip,marshal.dumps(sSet))            
                
    i += 1
    if i % 500 == 0:
        print "%d messages processed." % i