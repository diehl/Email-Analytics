# import the modules
import cgi
import MySQLdb
import redis
from calendar          import timegm
from dateutil          import tz
from dateutil.parser   import parse
from social_signaling.db_access.neo4j_rest_client import GraphDatabase

# create a connection to the MySQL databases
UCBdb = MySQLdb.connect(host='localhost',user='root',passwd='',db='berkeley_enron')
ISIdb = MySQLdb.connect(host='localhost',user='root',passwd='',db='isi_enron')

# create a connection to the neo4j database
graphdb = GraphDatabase("http://localhost:7474/db/data")

# create a connection to the redis database
rdb = redis.Redis(host='localhost', port=6379, db=0)

# delete previous keys if they exist
rdb.delete('neo4j_message_node_index','neo4j_address_node_index',
           'neo4j_fully_observed_address_nodes','neo4j_people_node_index')

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

# message batch size - number of messages to process for each graph database transaction
msgBatchSize = 500

# number of batches
numBatches = len(results) / msgBatchSize + 1

# for each batch...
i = 0
for b in range(numBatches):

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

            # message datetime string
            msgDTstring = str(msgDT)

        else:

            # seconds since the epoch
            msgDTsecs = 0

            # message datetime string
            msgDTstring = 'None'

        # create new message node - assume there are no duplicate messages in the collection
        # so no need to check for matching nodes that already exist.
        m = graphdb.node(datetime=msgDTstring, epochSecs=msgDTsecs, subject=msgSubj, 
                         body=msgBody, emailID=msgID, type="Message")

        # add message node to the index
        rdb.hset('neo4j_message_node_index',msgID,str(m.id))

        # get the message sender's email address
        sender = msg[0]

        # decode the address from ISO-8859-1 and encode in ASCII preserving extended characters
        sender = cgi.escape(sender.decode('iso-8859-1')).encode('ascii','xmlcharrefreplace')

        # check to see if an email address node exists for this address
        addNodeID = rdb.hget('neo4j_address_node_index',sender)
        if addNodeID != None:
            e = graphdb.node[int(addNodeID)]
        else:

            # check to see if this address is fully observed
            fO = (sender in fullyObservedAddresses)

            # create a new email address node
            e = graphdb.node(address=sender, fullyObserved=fO, type="Email Address")

            # add the node to the address index
            rdb.hset('neo4j_address_node_index',sender,str(e.id))

            # if this node is fully observed, add the index to the list of fully observed nodes
            if fO:
                rdb.sadd('neo4j_fully_observed_address_nodes',str(e.id))                            

        # create a link from the new email address node to the new message node
        e.SENT(m, datetime=msgDTstring, epochSecs=msgDTsecs)

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
        addNodeID = rdb.hget('neo4j_address_node_index',recipEmail)
        if addNodeID != None:
            e = graphdb.node[int(addNodeID)]
        else:

            # check to see if this address is fully observed
            fO = (recipEmail in fullyObservedAddresses)

            # create a new email address node
            e = graphdb.node(address=recipEmail, fullyObserved=fO, type="Email Address")

            # add the node to the address index
            rdb.hset('neo4j_address_node_index',recipEmail,str(e.id))

            # if this node is fully observed, add the index to the list of fully observed nodes
            if fO:
                rdb.sadd('neo4j_fully_observed_address_nodes',str(e.id))                            

        # retrieve the message node
        msgNodeID = rdb.hget('neo4j_message_node_index',msgID)
        m = graphdb.node[int(msgNodeID)]

        # create a link from the message node to the email address node
        m.RECEIVED_BY(e, datetime=m['datetime'], epochSecs=m['epochSecs'], type=recipType, order=recipOrder)

        i += 1

        # check to see if we are at the end of the results list and need to stop iterating
        if i == len(results):
            break

    print "%d recipients processed." % i

# fetch manager-subordinate relationship information from the text file
print "Pulling manager-subordinate relationship information from the text file."
f = open('enron_manager_subordinate_relationships.txt','r')
relationships = f.readline().split('\r')

# helper function to check for the existence of an edge of the specified type 
# going from node na to node nb
def has_outgoing_relationship(na,nb,relType):
    return nb in [r.end for r in na.relationships.outgoing(types=[relType])]

# for each relationship pair
print "Adding person nodes and manager-subordinate relationships to the neo4j database."
personNodeDict = {}
for rel in relationships:

    rel = rel.split(',')

    # subordinate name
    subName = rel[0:2]
    subNameKey = subName[1] + ' ' + subName[0]

    # manager name
    mgrName = rel[2:4]
    mgrNameKey = mgrName[1] + ' ' + mgrName[0]

    # subordinate email address
    subEmail = rel[4]

    # manager email address
    mgrEmail = rel[5]

    # fetch the subordinate email address node
    addNodeID = rdb.hget('neo4j_address_node_index',subEmail)
    subAddNode = graphdb.node[int(addNodeID)]

    # fetch the manager email address node
    addNodeID = rdb.hget('neo4j_address_node_index',mgrEmail)
    mgrAddNode = graphdb.node[int(addNodeID)]
   
    # if a person node for the subordinate has been created, retrieve it
    personNodeID = rdb.hget('neo4j_people_node_index',subNameKey)
    if personNodeID != None:
        subPersonNode = graphdb.node[int(personNodeID)]

    # otherwise, create a person node along with a relationship to indicate
    # their use of the given email address
    else:
        subPersonNode = graphdb.node(lastName=subName[0], firstName=subName[1], 
                                     provenance='http://tinyurl.com/4faotq4', type="Person")
        rdb.hset('neo4j_people_node_index',subNameKey,str(subPersonNode.id))
 
    # link the subordinate person node to the email address if necessary
    if not has_outgoing_relationship(subPersonNode,subAddNode,'USED_EMAIL_ADDRESS'):
        subPersonNode.USED_EMAIL_ADDRESS(subAddNode)

    # if a person node for the manager has been created, retrieve it
    personNodeID = rdb.hget('neo4j_people_node_index',mgrNameKey)
    if personNodeID != None:
        mgrPersonNode = graphdb.node[int(personNodeID)]

    # otherwise, create a person node along with a relationship to indicate
    # their use of the given email address
    else:
        mgrPersonNode = graphdb.node(lastName=mgrName[0], firstName=mgrName[1], 
                                     provenance='http://tinyurl.com/4faotq4', type="Person")
        rdb.hset('neo4j_people_node_index',mgrNameKey,str(mgrPersonNode.id))

    # link the manager person node to their email address if necessary
    if not has_outgoing_relationship(mgrPersonNode,mgrAddNode,'USED_EMAIL_ADDRESS'):
        mgrPersonNode.USED_EMAIL_ADDRESS(mgrAddNode)

    # create the manager-subordinate relationship in the neo4j database if it does not exist already
    if not has_outgoing_relationship(subPersonNode,mgrPersonNode,'DIRECTLY_REPORTED_TO'):
        subPersonNode.DIRECTLY_REPORTED_TO(mgrPersonNode, type='Social Relationship', evidenceType='Interval', 
                                           startDatetime='2000-01-01 00:00:00 +00:00',
                                           endDatetime='2001-11-30 23:59:59 +00:00',
                                           provenance='http://tinyurl.com/4faotq4')
