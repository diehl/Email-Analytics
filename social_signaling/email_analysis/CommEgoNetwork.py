from social_signaling.db_access.DB   import DB
from social_signaling.email_analysis import CommRelationship

class CommEgoNetwork(object):

    def __init__(self,ego_address):
        self._db = DB()
        self._ego_address = ego_address
        self._alters = self.getSenders()
        self._alters.extend(self.getRecipients())
        self._alters = list(set(self._alters))    
        
    def fullyObserved(self):
        """Returns True if the ego network is fully observed."""
        return self._db.fullyObserved(self._ego_address)
    
    def setEgoAddress(self,ego_address):
        """Sets the ego network email address to the address given."""
        self._ego_address = ego_address
        
    def getSenders(self):
        """Returns a list of email addresses that sent email to the ego."""
        sender_tups = self._db.getSendersForRecipient(self._ego_address)
        senders = [t[0] for t in sender_tups]
        return senders
        
    def getRecipients(self):
        """Returns a list of email addresses that received email from the ego."""
        recip_tups = self._db.getRecipientsForSender(self._ego_address)
        recips = [t[0] for t in recip_tups]
        return recips
        
    def getAlters(self):
        """Returns a list of all email addresses that exchanged email with the ego."""
        return self._alters
        
    def getCommRelationships(self):
        """Returns a list of all communication relationships involving the ego."""
        return [CommRelationship.CommRelationship((a,self._ego_address)) for a in self._alters]
        
    def getRelationshipEmailCounts(self):
        """Returns a dictionary containing relationship email count data. The top level 
        dictionary has two keys: 'ego' and 'data'. The value associated with 'ego' is the ego's
        email address. The value associated with 'data' is a list of dictionaries containing
        data for each communication relationship in the ego network. The relationship 
        dictionaries have two keys: 'alter' and 'counts'. The value associated with 'alter' is 
        the alter's email address. The value associated with counts is a list containing
        [# of messages sent by the ego,
         # of direct messages sent by the ego,
         # of indirect messages sent by the ego,
         # of threaded messages sent by the ego,
         # of messages sent by the alter,
         # of direct messages sent by the alter,
         # of indirect messages sent by the alter,
         # of threaded messages sent by the alter].
        """
        ego = self._ego_address
        ego_data = {'ego' : ego, 'data' : []}
        rels = self.getCommRelationships()
        for rel in rels:
            
            # Get the relationship participants
            rel_id = rel.getRelationshipID()
            
            # Identify the alter address
            if rel_id[0] == ego:
                alter = rel_id[1]
            else:
                alter = rel_id[0]
                
            # Construct the dictionary of relationship data
            rel_data = {'alter' : alter}
            rel_data['counts'] = [rel.getNumberOfMsgsFromSender(ego),
                                  rel.getNumberOfDirectMsgsFromSender(ego),
                                  rel.getNumberOfIndirectMsgsFromSender(ego),            
                                  rel.getNumberOfThreadedMsgsFromSender(ego),
                                  rel.getNumberOfMsgsFromSender(alter),
                                  rel.getNumberOfDirectMsgsFromSender(alter),
                                  rel.getNumberOfIndirectMsgsFromSender(alter),
                                  rel.getNumberOfThreadedMsgsFromSender(alter)]
            ego_data['data'].append(rel_data)
                   
        return ego_data        
        
    def getNumberOfThreadedMsgs(self):
        """Returns the total number of threaded messages exchanged in the ego network."""
        
        # Get the relationships associated with the ego network
        rels = self.getCommRelationships()
        
        # Collect the message ids of the threaded messages
        mids = set([])
        for rel in rels:        
            threads = rel.getConversationThreads()
            for thread in threads:
                thread_mids = thread.getMessageIDs()
                mids = mids.union(set(thread_mids))
                
        return len(mids)