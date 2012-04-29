# import the modules
import cgi
import neo4j
import MySQLdb
from neo4j.util      import Subreference
from calendar        import timegm
from dateutil        import tz
from dateutil.parser import parse

# create a connection to the MySQL databases
UCBdb = MySQLdb.connect(host='localhost',user='root',passwd='',db='berkeley_enron')
ISIdb = MySQLdb.connect(host='localhost',user='root',passwd='',db='isi_enron')

# create a connection to the neo4j database
graphdb = neo4j.GraphDatabase("/Users/diehl4/neodb/UCB_Enron_v2")
with graphdb.transaction:
    
    # create index for email address nodes
    addIdx = graphdb.index("address", create=True)

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

# create dictionary to map email IDs to message nodes
msgNodeDict = {}

# message batch size - number of messages to process for each graph database transaction
msgBatchSize = 500

# number of batches
numBatches = len(results) / msgBatchSize + 1

# for each batch...
i = 0
for b in range(numBatches):

    with graphdb.transaction:

        # for each message in the batch...
        for m in range(msgBatchSize):

            # get the message
            msg = results[i]

            # get the message datetime
            msgDT = msg[1]

            # get the message timezone
            msgTZ = msg[2]

            # get the message ID
            msgID = msg[3]

            # get the message subject
            msgSubj = msg[4]

            # get the message body
            msgBody = msg[5]
            
            if msgDT != None:

                # create new datetime object with time normalized to UTC
                msgDT = parse(msgDT.strftime('%Y/%m/%d %H:%M:%S') + ' ' + msgTZ[:5]).astimezone(tz.tzoffset('',0))

                # convert the datetime into seconds since the epoch
                msgDTsecs = timegm(msgDT.utctimetuple())

                # create new message node - assume there are no duplicate messages in the collection
                # so no need to check for matching nodes that already exist.
                m = graphdb.node(year=msgDT.year, month=msgDT.month, day=msgDT.day, hour=msgDT.hour, 
                                 minute=msgDT.minute, second=msgDT.second, epochSecs=msgDTsecs, subject=msgSubj, 
                                 body=msgBody, emailID = msgID, type="Message")

            else:

                # create new message node with zeros for time attributes.
                # assume there are no duplicate messages in the collection
                # so no need to check for matching nodes that already exist.
                m = graphdb.node(year=0, month=0, day=0, hour=0, 
                                 minute=0, second=0, epochSecs=0, subject=msgSubj, 
                                 body=msgBody, emailID = msgID, type="Message")

            # add message node to the dictionary
            msgNodeDict[msgID] = m

            # get the message sender's email address
            sender = msg[0]

            # decode the address from ISO-8859-1 and encode in ASCII preserving extended characters
            sender = cgi.escape(sender.decode('iso-8859-1')).encode('ascii','xmlcharrefreplace')

            # check to see if an email address node exists for this address
            e = addIdx[sender]
            if e is None:

                # check to see if this address is fully observed
                fO = (sender in fullyObservedAddresses)

                # if not, create a new email address node
                e = graphdb.node(address=sender, fullyObserved=fO, type="Email Address")

                # add the node to the index
                addIdx[sender] = e

            # create a link from the new email address node to the new message node
            e.SENT(m)

            i += 1

            # check to see if we are at the end of the results list and need to stop iterating
            if i == len(results):
                break

    print "%d messages processed." % i

# fetch recipient information from the MySQL database
print "Pulling recipient data from the MySQL database."
cursor = UCBdb.cursor()
cursor.execute("select t1.smtpid, t2.reciptype, t2.reciporder, t3.email from msgs as t1, recip_info as t2, addresses as t3 where t1.mid = t2.mid and t2.eid = t3.eid;")
results = cursor.fetchall()

# recipient batch size - number of recipients to process for each graph database transaction
recipBatchSize = 10000

# number of batches
numBatches = len(results) / recipBatchSize + 1

# for each batch...
i = 0
for b in range(numBatches):

    with graphdb.transaction:

        # for each recipient in the batch...
        for r in range(recipBatchSize):

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

            # check to see if an email address node exists for this address
            e = addIdx[recipEmail]
            if e is None:

                # check to see if this address is fully observed
                fO = (recipEmail in fullyObservedAddresses)

                # if not, create a new email address node
                e = graphdb.node(address=recipEmail, fullyObserved=fO, type="Email Address")

                # add the node to the index
                addIdx[recipEmail] = e

            # retrieve the message node
            m = msgNodeDict[msgID]

            # create a link from the message node to the email address node
            m.RECEIVED_BY(e,type=recipType,order=recipOrder)

            i += 1

            # check to see if we are at the end of the results list and need to stop iterating
            if i == len(results):
                break

    print "%d recipients processed." % i

# close connection to the graph database
graphdb.shutdown()
