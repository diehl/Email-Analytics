import redis
from social_signaling.db.neo4j_rest_client import GraphDatabase

# create a connection to the neo4j database
graphdb = GraphDatabase('http://localhost:7474/db/data')

# create a connection to the redis database
rdb = redis.Redis(host='localhost', port=6379, db=0)

# iterate over the vertices
# there is no mechanism in the Python REST client to discover the total number
# of vertices available. so we must know the total number beforehand. 
for i in xrange(343112):

    # get the vertex from neo4j
    v = graphdb.node[i]

    # if it's an email address vertex
    if v.properties.has_key('type') and v['type'] == "Email Address":

        # add a key-value pair to the redis index mapping from the address 
        # to the vertex id
        rdb.hset('addresses',v['address'],i)

        # if the email address is fully observed
        if v['fullyObserved'] == True:

            # add the vertex id to the set of ids corresponding to fully observed
            # email addresses
            rdb.sadd('fullyObserved',i)

    if (i+1) % 500 == 0:
        print "%d vertices processed." % (i+1)

print "%d vertices processed." % (i+1)


