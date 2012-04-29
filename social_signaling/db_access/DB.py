import os
import redis
import marshal
import subprocess
from time            import sleep
from datetime        import datetime
from dateutil.parser import parse
from social_signaling.db_access.neo4j_rest_client import GraphDatabase

# Redis databases used for various datasets
ENRON = 0
SWITCHBOARD = 1

class Borg(object):
    _shared_state = {}
    def __new__(cls, *a, **k):
        obj = object.__new__(cls, *a, **k)
        obj.__dict__ = cls._shared_state
        return obj

class DB(Borg):
    """Class encapsulating connections to both the Neo4j and Redis databases.
    Global state ensures that each instance of DB will share the same
    connections.
    """
    
    # In the near-term, we'll leave these as public variables for debugging purposes
    neo4j_path = None
    redis_path = None
    neo4j_running = False
    redis_running = False
    neo4jDB = None
    redisDB = None    

    #========== Methods for interacting with the database servers ==========#

    def startNeo4jServer(self):
        """Checks to see that the environment variable NEO4J_HOME is defined. If it is 
        specified, the Neo4j server is started.
        """
        if self.neo4j_running == False:
            if self.neo4j_path == None:
    
                # Get the path to the Neo4j database
                self.neo4j_path = os.getenv('NEO4J_HOME')
                if self.neo4j_path == None:
                    raise Exception("NEO4J_HOME is undefined. Exiting.")

            # Start the Neo4j server
            if self.neo4j_path[-1] != '/':
                self.neo4j_path += '/'
            subprocess.Popen([self.neo4j_path+'bin/neo4j','start']).wait()
            self.neo4j_running = True
        else:
            print "Neo4j Server is already running."

    def createNeo4jConnection(self):
        """Instantiates a Neo4j database object."""    
        if self.neo4j_running:
            try:
                self.neo4jDB = GraphDatabase("http://localhost:7474/db/data")
            except Exception:
                raise Exception("Unable to establish connection to the Neo4j server.")
        else: 
            raise Exception("Neo4j server is not running.")

    def checkNeo4jConnection(self):
        """Raises an exception if there is no Neo4j database connection."""
        if self.neo4jDB == None:
            raise Exception("No Neo4j database connection exists.")

    def startRedisServer(self):
        """Checks to see that the environment variable REDIS_HOME is defined. If it is 
        specified, the Redis server is started.
        """
        if self.redis_running == False:
            if self.redis_path == None:
    
                # Get the path to the Redis database
                self.redis_path = os.getenv('REDIS_HOME')
                if self.redis_path == None:
                    raise Exception("REDIS_HOME is undefined. Exiting.")
    
            # Start the Redis server
            print "Starting Redis Server..."
            if self.redis_path[-1] != '/':
                self.redis_path += '/'
            subprocess.Popen([self.redis_path+'redis-server',self.redis_path+'redis.conf'])
            self.redis_running = True

        else:
            print "Redis Server is already running."

    def createRedisConnection(self,rdb=0):
        """Instantiates a Redis database object."""
        if self.redis_running:
            try:
                # Note: the database defaults to zero for now
                self.redisDB = redis.Redis(host='localhost', port=6379, db=rdb)
            except Exception:
            
                # if it failed, wait five seconds and try again
                sleep(5)
                try:
                    self.redisDB = redis.Redis(host='localhost', port=6379, db=rdb)
                except Exception:
                    raise Exception("Unable to establish connection to the Redis server.")
        else:
            raise Exception("Redis server is not running.")

    def checkRedisConnection(self):
        """Raises an exception if there is no Redis database connection."""
        if self.redisDB == None:
            raise Exception("No Redis database connection exists.")

    def startDBServers(self):
        """Checks to see that the environment variables NEO4J_HOME and REDIS_HOME
        are defined. If they are specified, the Neo4j and Redis servers are started
        and the database objects are instantiated.
        """
        self.startNeo4jServer()
        self.startRedisServer()

    def createDBConnections(self,rdb=0):
        """Instantiates the Neo4j and Redis database objects."""
        self.createNeo4jConnection()
        self.createRedisConnection(rdb)

    def stopNeo4jServer(self):
        """Stops the Neo4j server if it is running. Sets the database object reference 
        to None.
        """
        if self.neo4j_running == True:
            
            # Shutdown the Neo4j server
            subprocess.Popen([self.neo4j_path+'bin/neo4j','stop']).wait()
            self.neo4j_running = False
            self.neo4jDB = None
        else:
            print "Neo4j server is already shut down."
            
    def stopRedisServer(self):
        """Stops the Redis server if it is running. Sets the database object reference
        to None.
        """
        if self.redis_running == True:
    
            # Shutdown the Redis server
            print "Stopping Redis Server..."
            subprocess.Popen([self.redis_path+'redis-cli','shutdown']).wait()
            self.redis_running = False
            self.redisDB = None
        else:
            print "Redis server is already shut down."

    def stopDBServers(self):
        """Stops all database servers that are running. Sets the database
        object references to None.
        """
        self.stopNeo4jServer()
        self.stopRedisServer()
    
    #========== Methods for accessing message data ==========#
    
    def checkRedisKey(self,key):
        """Raises an exception if the specified key does not exist in Redis."""
        if self.redisDB.exists(key) == False:
            err = "%s hash does not exist." % key
            raise Exception(err)
    
    def getAllMessageIDs(self):
        """Returns a list of all message ids if it exists or None otherwise. Throws 
        an exception if no database connection exists.
        """
        self.checkRedisConnection()
        self.checkRedisKey('neo4j_message_node_index')
        msg_ids = self.redisDB.hkeys('neo4j_message_node_index')
        if len(msg_ids) == 0:
            return None
        else:
            return msg_ids
    
    def _lookup_neo4j_message_node_index(self,message_id):
        """Returns the message node index in Neo4j for the given message id."""
        self.checkRedisConnection()
        self.checkRedisKey('neo4j_message_node_index')
        return self.redisDB.hget('neo4j_message_node_index',message_id)
    
    def _lookup_neo4j_node(self,idx):
        """Returns the Neo4j node for the given node index."""
        self.checkNeo4jConnection()
        return self.neo4jDB.node[idx]
    
    def _get_message_content_properties_from_neo4j(self,message_id):
        """Returns a dictionary of message properties minus the sender and recipient 
        information from Neo4j.
        """
        self.checkNeo4jConnection()
    
        # Fetch the property information from the message node
        idx = self._lookup_neo4j_message_node_index(message_id)
        msg_node = self._lookup_neo4j_node(idx)
        msg = msg_node.properties
        del msg['type']
        
        return msg

    def _get_message_from_neo4j(self,message_id):
        """Returns a dictionary of message properties from Neo4j for the referenced message."""
        self.checkNeo4jConnection()
                
        # Fetch the property information from the message node
        idx = self._lookup_neo4j_message_node_index(message_id)
        msg_node = self._lookup_neo4j_node(idx)
        msg = msg_node.properties
        
        # Convert the datetime from a string to a datetime object
        if msg['datetime'] == 'None':
            msg['datetime'] = None
        else:
            msg['datetime'] = parse(msg['datetime'])
        
        # Fetch the sender information from the address node
        sender_node = msg_node.relationships.incoming(types=["SENT"])[0].start
        msg['sender'] = sender_node['address']       
        
        # Fetch the recipient information from the RECEIVED_BY edges and address nodes
        recipient_rels = msg_node.relationships.outgoing(types=["RECEIVED_BY"])
        msg['to'] = []
        msg['cc'] = []
        msg['bcc'] = []
        for rel in recipient_rels:
            rel_type  = rel.properties['type']
            rel_order = rel.properties['order']
            recip_node = rel.end
            msg[rel_type].append((recip_node['address'],rel_order))
            
        # Sort the recipients by their position info and reduce the lists down to the addresses
        # only, now in the proper order
        fields = ['to','cc','bcc']
        for field in fields:
            msg[field].sort(key=lambda tup: tup[1])
            msg[field] = [tup[0] for tup in msg[field]]     

        # Define properties to rename and delete
        prop_map = { 'emailID' : 'MessageID', 'datetime' : 'Datetime', 'epochSecs' : 'EpochSecs',
                     'sender' : 'Sender', 'to' : 'TO', 'cc' : 'CC', 'bcc' : 'BCC', 
                     'subject' : 'Subject', 'body' : 'Body'}
        prop_del = ['type']
        
        # Rename and delete properties
        for key1, key2 in prop_map.items():
            msg[key2] = msg[key1]
            del msg[key1]
        for key in prop_del:
            del msg[key]
        
        return msg  
        
    def _get_message_from_redis(self,message_id):
        """Returns a dictionary of message properties from Redis for the referenced message."""
        self.checkRedisConnection()
        self.checkRedisKey('messages')
        
        # Get the message property data
        sdict = self.redisDB.hget('messages',message_id)
        msg = marshal.loads(sdict)
        
        # Convert the datetime from a string to a datetime object
        if msg['datetime'] == 'None':
            msg['datetime'] = None
        else:
            msg['datetime'] = parse(msg['datetime'])
            
        # Define properties to rename
        prop_map = { 'message_id' : 'MessageID', 'datetime' : 'Datetime', 'epoch_secs' : 'EpochSecs',
                     'sender' : 'Sender', 'to' : 'TO', 'cc' : 'CC', 'bcc' : 'BCC', 
                     'subject' : 'Subject', 'body' : 'Body'}
        
        # Rename properties
        for key1, key2 in prop_map.items():
            msg[key2] = msg[key1]
            del msg[key1]            

        return msg
        
    def getMessage(self,message_id):
        """Returns a dictionary of message properties for the referenced message."""
        return self._get_message_from_redis(message_id)
                
    #========== Methods for accessing communication relationship data ==========#                
                
    def fullyObserved(self,address):
        """Returns True if the given email address is fully observed."""
        return self.redisDB.sismember('fully_observed_addresses',address)
        
    def getAllFullyObservedAddresses(self):
        """Returns a list of all fully observed email addresses."""
        return self.redisDB.smembers('fully_observed_addresses')
    
    def getAllDirectedCommRelationshipIDs(self):
        """Returns a list of all directed communication relationship ids if it exists or 
        None otherwise.
        """
        self.checkRedisConnection()        
        self.checkRedisKey('mids_per_directed_comm_relationship')
        rel_ids = self.redisDB.hkeys('mids_per_directed_comm_relationship')
        if len(rel_ids) == 0:
            return None
        else:
            # Convert strings back to tuples before returning
            return [eval(rel_id) for rel_id in rel_ids]
            
    def getDirectedCommRelationship(self,rel_id):
        """Returns a list of (message id, epoch secs) tuples for the directed communication
        relationship rel_id = (sender address,recipient address) if it exists or None otherwise.
        """
        self.checkRedisConnection()                   
        self.checkRedisKey('mids_per_directed_comm_relationship')
        if self.redisDB.hexists('mids_per_directed_comm_relationship',str(rel_id)):
            return marshal.loads(self.redisDB.hget('mids_per_directed_comm_relationship',str(rel_id)))
        else:
            return None
            
    def getSendersForRecipient(self,recip_address):
        """Returns a set of (sender address, (beginning epoch secs, end epoch secs)) tuples
        specifying the directed communication relationships and their temporal extent.
        """
        return marshal.loads(self.redisDB.hget('senders_per_recipient_address',recip_address))
        
    def getRecipientsForSender(self,sender_address):
        """Returns a set of (recipient address, (beginning epoch secs, end epoch secs)) tuples
        specifying the directed communication relationships and their temporal extent.
        """
        return marshal.loads(self.redisDB.hget('recipients_per_sender_address',sender_address))

    #========== Methods for storing and accessing longest common substring data ==========#
    
    def _get_lcsubstring_dict_from_redis(self,mids):
        """Returns the longest common substring dictionary from Redis for the message id
        pair list, if it exists. Otherwise an empty dictionary is returned.
        """

        # Ensure that mids is a list
        if type(mids) != list:
            raise Exception("mids is not of type list.")
            
        # Sort mids to avoid ordering issue
        mids.sort()
        
        # Check to see if a dictionary exists currently for the message id pair
        if self.redisDB.hexists('lcsubstrings',str(mids)):
        
            # get the dictionary
            d = marshal.loads(self.redisDB.hget('lcsubstrings',str(mids)))
            
        else:
            d = {}
            
        return d
    
    def _set_lcsubstring_dict_in_redis(self,mids,d):
        """Inserts the given longest common substring dictionary d for the message id pair mids 
        into Redis. mids should be of type list. Otherwise an exception will be thrown.
        """
        if type(mids) == list:
            
            # Sort mids to avoid ordering issue
            mids.sort()
            
            # Get the dictionary
            self.redisDB.hset('lcsubstrings',str(mids),marshal.dumps(d))
        else:
            raise Exception("mids is not of type list.")
    
    def setMessagePairLCSubstring(self,mids,field,lcs):
        """The longest common substring hash maps from message id pairs to a dictionary of 
        computed longest common substrings for various message fields (such as 'Subject' and
        'Body'). mids is a list capturing the pair of message IDs. field is the string label
        corresponding to the message field of interest. lcs is the longest common substring
        between the message fields. The given information is inserted into the hash.
        """
        
        # Get the dictionary for mids
        d = self._get_lcsubstring_dict_from_redis(mids)
            
        # Add the data to the dictionary
        d[field] = lcs
        
        # Insert the updated dictionary into Redis
        self._set_lcsubstring_dict_in_redis(mids,d)
        
    def getMessagePairLCSubstring(self,mids,field):
        """Returns the longest common substring for the message field associated with the 
        pair of message ids if it exists in the hash. Otherwise None is returned. mids should
        be of type list.
        """
        
        # Get the dictionary for mids
        d = self._get_lcsubstring_dict_from_redis(mids)
        
        # If the longest common substring for the field is available, return it
        if d.has_key(field):
            return d[field]
        else:
            return None
            
    def clearMessagePairLCSubstrings(self):
        """Deletes the hash containing longest common substring data."""
        self.redisDB.delete('lcsubstrings')
        
    def getAllLCSMessagePairs(self):
        """Returns a list of the message id pairs in the longest common substring hash."""
        return map(eval,self.redisDB.hkeys('lcsubstrings'))
        
    #========== Methods for storing and accessing Mechanical Turk annotations ==========#
    
    def setMTurkMessageAnnotation(self,mid,results,description):
        """Saves the (results,description) tuple representing a Mechanical Turk 
        message annotation associated with the given message id mid.
        """
        
        # Check to see if there exists a current list of annotations for message id mid 
        if self.redisDB.hexists('mturk_msg_annotations',mid):
        
            # Get the list of annotations for mid
            annotations = marshal.loads(self.redisDB.hget('mturk_msg_annotations',mid))
            
        else:
            annotations = []
        
        # Add the annotation if necessary
        if not (results,description) in annotations:
            annotations.append((results,description))
        
            # Push the list back to the database
            self.redisDB.hset('mturk_msg_annotations',mid,marshal.dumps(annotations))

    def getMTurkMessageAnnotations(self,mid):
        """Returns a list of (results,description) tuples for the given message id if 
        annotation data is available. Otherwise None is returned.
        """
        
        # Check to see if there exists a current list of annotations for message id mid 
        if self.redisDB.hexists('mturk_msg_annotations',mid):
        
            # Return the list of annotations for mid
            return marshal.loads(self.redisDB.hget('mturk_msg_annotations',mid))
            
        else:
            return None
            
    def getAllMTurkMessageAnnotations(self):
        """Returns a dictionary mapping from message ids to lists of (results,description)
        tuples if annotation data is available. Otherwise None is returned.
        """
        
        # Get the message ids for which annotation data is available
        mids = self.redisDB.hkeys('mturk_msg_annotations')
        
        # Build the dictionary if annotation data is available
        if len(mids) > 0:
            data = {}
            for mid in mids:
                data[mid] = self.getMTurkMessageAnnotations(mid)
        else:
            data = None
            
        return data
        
    #========== Methods for storing and accessing social relationship metadata ==========#        
    
    def getSocialRelationshipMetadata(self,address):
        """Returns a list of dictionaries characterizing the known directed social 
        relationships associated with the given email address. Otherwise None is returned.
        """
        if self.redisDB.hexists('social_relationships',address):
            rels = marshal.loads(self.redisDB.hget('social_relationships',address))
        else:
            rels = None
        return rels
            