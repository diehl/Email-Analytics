# import the modules
import cgi
import marshal
import MySQLdb
import pymongo
from calendar          import timegm
from dateutil          import tz
from dateutil.parser   import parse

# create a connection to the MySQL databases
ucb_db = MySQLdb.connect(host='localhost',user='root',passwd='root',db='berkeley_enron')
isi_db = MySQLdb.connect(host='localhost',user='root',passwd='root',db='isi_enron')

# create a connection to the mongo database
mongo_connection = pymongo.Connection()

# drop the previous enron db
mongo_connection.drop_database('enron_db')

# create a new database
mdb = mongo_connection.enron_db

# fetch message information from the UC Berkeley MySQL database
print "Pulling message data from the UC Berkeley MySQL database."
cursor = ucb_db.cursor()
cursor.execute("select t2.email, t1.date, t1.timezone, t1.smtpid, t1.subject, t1.body from msgs as t1, addresses as t2 where t1.eid = t2.eid;")
results = cursor.fetchall()

# fetch email addresses corresponding to the 151 email inboxes that make up the dataset
print "Pulling email address data from the USC/ISI MySQL database."
cursor = isi_db.cursor()
cursor.execute("select Email_id as email from employeelist;")
address_tuples = cursor.fetchall()
fully_observed_addresses = [e[0] for e in address_tuples]

# find and modify the specific address that appears to be incorrectly represented in the ISI database
for i in range(len(fully_observed_addresses)):
    if fully_observed_addresses[i] == 'paul.y barbo@enron.com':
        break
fully_observed_addresses[i] = "paul.y'barbo@enron.com"

# add the set of fully observed addresses to the mongo database
foa = mdb.fully_observed_addresses
for address in fully_observed_addresses:
    foa.insert({'address' : address})
    
# for each message...
print "Constructing the corpus dictionary."
corpus_dict = {}
num_msgs = len(results)
for i in xrange(num_msgs):

    # get the message
    msg = results[i]

    # get the message datetime
    msg_dt = msg[1]

    # get the message timezone
    msg_tz = msg[2]

    if msg_dt != None:

        # create new datetime object with time normalized to UTC
        msg_dt = parse(msg_dt.strftime('%Y/%m/%d %H:%M:%S') + ' ' + msg_tz[:5]).astimezone(tz.tzoffset('',0))
        
        # convert the datetime into seconds since the epoch
        msg_dt_secs = timegm(msg_dt.utctimetuple())

    else:

        # seconds since the epoch
        msg_dt_secs = 0

    # get the message ID
    msg_id = msg[3]

    # get the message subject
    msg_subj = msg[4]

    # get the message body
    msg_body = msg[5]

    # get the message sender's email address
    sender = msg[0]

    # decode the address from ISO-8859-1 and encode in ASCII preserving extended characters
    sender = cgi.escape(sender.decode('iso-8859-1')).encode('ascii','xmlcharrefreplace')

    # create a message dictionary
    m_dict = { 'message_id' : msg_id, 'datetime' : msg_dt, 'epoch_secs' : msg_dt_secs, 
               'subject' : msg_subj, 'body' : msg_body, 'sender' : sender, 'to' : [],
               'cc' : [], 'bcc' : [] }

    # add to the corpus dictionary
    corpus_dict[msg_id] = m_dict

    if (i+1) % 500 == 0:
        print "%d messages processed." % (i+1)

# fetch recipient information from the MySQL database
print "Pulling recipient data from the MySQL database."
cursor = ucb_db.cursor()
cursor.execute("select t1.smtpid, t2.reciptype, t2.reciporder, t3.email from msgs as t1, recip_info as t2, addresses as t3 where t1.mid = t2.mid and t2.eid = t3.eid;")
results = cursor.fetchall()

# for each recipient...
print "Inserting the recipient data into the corpus dictionary."
num_recips = len(results)
for i in xrange(num_recips):

    # get the recipient information
    recip = results[i]

    # get the message ID
    msg_id = recip[0]

    # get the recipient type
    recip_type = recip[1]

    # get the recipient order
    recip_order = int(recip[2])

    # get the recipient email address
    recip_email = recip[3]
    
    # decode the address from ISO-8859-1 and encode in ASCII preserving extended characters
    recip_email = cgi.escape(recip_email.decode('iso-8859-1')).encode('ascii','xmlcharrefreplace')

    # fetch message dictionary 
    m_dict = corpus_dict[msg_id]

    # add recipient information to the message
    r_tup = (recip_email,recip_order)
    if m_dict.has_key(recip_type):
        m_dict[recip_type].append(r_tup)
    else:
        m_dict[recip_type] = [r_tup]

    # save message dictionary
    corpus_dict[msg_id] = m_dict

    if (i+1) % 5000 == 0:
        print "%d recipients processed." % (i+1)

# generate ordered lists for the message recipients
print "Ordering the message recipients in the corpus dictionary."
i = 0
for msg_id in corpus_dict.keys():

    m_dict = corpus_dict[msg_id]

    # sort the recipients by their position info and reduce the lists down to the addresses
    # only, now in the proper order
    fields = ['to','cc','bcc']
    for field in fields:
        m_dict[field].sort(key=lambda tup: tup[1])
        m_dict[field] = [tup[0] for tup in m_dict[field]]

    # save message dictionary
    corpus_dict[msg_id] = m_dict
    
    i += 1
    if i % 500 == 0:
        print "%d messages processed." % i

# push the message data to mongo
print "Pushing the message data into MongoDB."
i = 0
messages = mdb.messages
for m_dict in corpus_dict.values():
    messages.insert(m_dict)
    i += 1
    if i % 500 == 0:
        print "%d messages processed." % i

# fetch manager-subordinate relationship information from the MySQL database
print "Pulling manager-subordinate relationship information from the MySQL database."
cursor = ucb_db.cursor()
cursor.execute("select * from sub_mgr_pairs_in_collection;")
results = cursor.fetchall()

# for each relationship pair
print "Adding manager-subordinate relationships to MongoDB."
social_relationships = mdb.social_relationships
rel_dicts = []
for rpair in results:

   # create the relationship in mongo
   rel_dict = { 'source' : rpair[0], 'type' : 'directly reported to', 'target' : rpair[1], 'evidence_type' : 'interval',
                'start_time' : parse('2000-01-01 00:00:00 +00:00'), 'end_time' : parse('2001-11-30 23:59:59 +00:00'),
                'provenance' : 'http://tinyurl.com/cfsooc' }
   rel_dicts.append(rel_dict)
   
# push the relationship data into mongo   
social_relationships.insert(rel_dicts)

