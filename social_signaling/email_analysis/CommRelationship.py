from social_signaling.db_access.DB   import DB
from social_signaling.email_analysis import MessageCollection, MessageThreading
from social_signaling.util.time_interval import *

class DirectedCommRelationship(object):

    def __init__(self,rel_id=None):
        """Initializes the object for the directed communication relationship
        rel_id = (sender address,recipient address) if it is specified.
        """
        self._db = DB()
        self._rel_id = None
        self._messages = MessageCollection.MessageCollection()
        if rel_id != None:
            self.setRelationshipID(rel_id)
            
    def __str__(self):
        """Creates a display string for the relationship id."""
        return str(self._rel_id)
        
    def setRelationshipID(self,rel_id):
        """Sets the message collection to those messages associated with the given 
        directed relationship.
        """
        
        # Set the relationship id
        self._rel_id = rel_id
        
        # Get the list of (message id, epoch secs) tuples for the directed relationship
        tlist = self._db.getDirectedCommRelationship(rel_id)
        
        # Check to ensure we're dealing with a valid relationship
        if tlist != None:

            # Extract the list of message ids
            mids = [tup[0] for tup in tlist]
        
            # Set the list of message ids in the MessageCollection object
            self._messages.setMessageIDs(mids)
            
    def getRelationshipID(self):
        """Returns the relationship ID for the directed relationship."""
        return self._rel_id
            
    def getAllMessages(self):
        """Returns the message collection associated with the directed relationship."""
        return self._messages
        
    def getSenderTokens(self,time_interval=None):
        """Returns a list of all sender tokens from the set of messages in the directed
        relationship. If a time interval is specified through a tuple of datetimes
        (interval_begin, interval_end), the sender tokens returned correspond to the 
        messages that were sent within the time interval.
        """
        
        # If a time interval is specified, check to see if it is valid
        if time_interval != None and not valid_time_interval(time_interval):
            raise Exception("The specificed time interval is not valid!")

        # Extract the tokens
        tokens = []
        for msg in self._messages:
            if time_interval == None or within_time_interval(msg.Datetime,time_interval):
                tokens.extend(msg.getSenderTokens())
        return tokens
        
    def getNumberOfMsgs(self,time_interval=None):
        """Returns the number of messages associated with the directed relationship.
        If a time interval is specified through a tuple of datetimes (interval_begin, 
        interval_end), the count returned corresponds to the number of messages that 
        were sent within the time interval.
        """
        return self._messages.getNumberOfMsgs(time_interval)
        
    def getNumberOfDirectMsgs(self,time_interval=None):
        """Returns the number of messages where the recipient is listed in the TO field.
        If a time interval is specified through a tuple of datetimes (interval_begin, 
        interval_end), the count returned corresponds to the number of relevant messages 
        that were sent within the time interval.
        """

        # If a time interval is specified, check to see if it is valid
        if time_interval != None and not valid_time_interval(time_interval):
            raise Exception("The specificed time interval is not valid!")

        # Count the relevant messages
        count = 0
        recip = self._rel_id[1]
        for msg in self._messages:
            
            # If a time interval was specified and the message is not in the time interval,
            # continue to the next message
            if time_interval != None and not within_time_interval(msg.Datetime,time_interval):
                continue

            if recip in msg.TO:
                count += 1
                
        return count
        
    def getNumberOfIndirectMsgs(self,time_interval=None):
        """Returns the number of messages where the recipient is listed in either the CC
        or BCC field. If the recipient is listed in the TO field as well, the TO field takes
        precedent. If a time interval is specified through a tuple of datetimes (interval_begin, 
        interval_end), the count returned corresponds to the number of relevant messages 
        that were sent within the time interval.
        """

        # If a time interval is specified, check to see if it is valid
        if time_interval != None and not valid_time_interval(time_interval):
            raise Exception("The specificed time interval is not valid!")

        count = 0
        recip = self._rel_id[1]
        for msg in self._messages:
        
            # If a time interval was specified and the message is not in the time interval,
            # continue to the next message
            if time_interval != None and not within_time_interval(msg.Datetime,time_interval):
                continue        
        
            if (not recip in msg.TO) and (recip in msg.CC or recip in msg.BCC):
                count += 1
        
        return count

class CommRelationship(object):

    def __init__(self,rel_id=None):
        """Initializes the object for the undirected communication relationship
        rel_id = (participant address 1,paritipant address 2) if it is specified.
        """
        self._db = DB()
        self._rel_id = None
        self._directed_relationships = {}
        self._all_messages = MessageCollection.MessageCollection()
        self._thread_factory = MessageThreading.MessageThreadFactory()
        self._threads = None
        if rel_id != None:
            self.setRelationshipID(rel_id)
        
    def __str__(self):
        """Creates a display string for the relationship id."""
        return str(self._rel_id)
        
    def setRelationshipID(self,rel_id):
        """Sets the message collection to those messages associated with the given 
        undirected communication relationship.
        """
        
        # Sort the email address pair so that there is a unique representation of the
        # relationship id
        rel_id = list(rel_id)
        rel_id.sort()
        rel_id = tuple(rel_id)
        
        # Set the relationship id
        self._rel_id = rel_id
        
        # Get the directed relationship for each side of the undirected relationship
        for i in range(2):
        
            self._directed_relationships[rel_id[0]] = DirectedCommRelationship(rel_id)
            dir_rel = self._directed_relationships[rel_id[0]]
            
            # If there's data for this side...
            if dir_rel.getNumberOfMsgs() > 0:
                
                # Extract the list of message ids
                mids = dir_rel.getAllMessages().getMessageIDs()                
                
                # Add to the collection of all messages
                self._all_messages.addMessageIDs(mids)

            # Reverse the rel_id tuple for the next iteration
            rel_id = tuple(reversed(rel_id))
            
        # Reset the thread list
        self._threads = None
            
    def getRelationshipID(self):
        """Returns the tuple (participant 1 address, participant 2 address)."""
        return self._rel_id 
            
    def getAllMessages(self):
        """Returns a MessageCollection object capturing all the message ids for the
        relationship.
        """
        return self._all_messages
        
    def getSenderTokens(self,time_interval=None):
        """Returns a list of tuples containing the participant address and the list of
        tokens in the messages from the participant address if the relationship ID is 
        specified. Otherwise None is returned. If a time interval is specified through 
        a tuple of datetimes (interval_begin, interval_end), the sender tokens returned 
        correspond to the messages that were sent within the time interval.
        """
        if self._rel_id == None:
            return None
        else:
            tokens = []
            for sender in self._rel_id:
                rel = self.getOutboundRelationship(sender)
                tokens.append((sender,rel.getSenderTokens(time_interval)))
            return tokens
                
    def _construct_conversation_threads(self):
        """Construct the conversation threads from the relationship's messages."""
        if self._threads == None:
            self._threads = self._thread_factory.constructConversationThreads(self.getAllMessages())
        
    def getConversationThreads(self):
        """Returns a list of Thread objects for the relationship."""
        if self._threads == None:
            self._construct_conversation_threads()
        return self._threads
        
    def _participant_check(self,address):
        """Throws an exception if address is not one of the relationship participants."""
        if not address in self._rel_id:
            err = "%s is not one of the relationship participants." % address
            raise Exception(err)  
            
    def getOutboundRelationship(self,sender):
        """Returns a DirectedCommRelationship object for the specified sender. If sender 
        is not one of the participating email addresses in the communication relationship, 
        an exception is thrown.
        """
        self._participant_check(sender)
        return self._directed_relationships[sender]
            
    def getNumberOfMsgsFromSender(self,sender,time_interval=None):
        """Returns the number of messages sent by the specified sender to the other participant.
        If sender is not one of the participating email addresses in the communication 
        relationship, an exception is thrown. If a time interval is specified through 
        a tuple of datetimes (interval_begin, interval_end), the count returned corresponds to 
        the number of messages that were sent within the time interval.
        """
        return self.getOutboundRelationship(sender).getNumberOfMsgs(time_interval)
        
    def getNumberOfDirectMsgsFromSender(self,sender,time_interval=None):
        """Returns the number of messages sent by the specified sender with the other participant
        in the TO field. If sender is not one of the participating email addresses in the 
        communication relationship, an exception is thrown. If a time interval is specified through 
        a tuple of datetimes (interval_begin, interval_end), the count returned corresponds to 
        the number of relevant messages that were sent within the time interval.
        """
        return self.getOutboundRelationship(sender).getNumberOfDirectMsgs(time_interval)
        
    def getNumberOfIndirectMsgsFromSender(self,sender,time_interval=None):
        """Returns the number of messages sent by the specified sender with the other participant
        in the CC/BCC field. If sender is not one of the participating email addresses in the 
        communication relationship, an exception is thrown. If a time interval is specified through 
        a tuple of datetimes (interval_begin, interval_end), the count returned corresponds to 
        the number of relevant messages that were sent within the time interval.
        """
        return self.getOutboundRelationship(sender).getNumberOfIndirectMsgs(time_interval)
        
    def getNumberOfThreadedMsgsFromSender(self,sender,time_interval=None):
        """Returns the number of messages sent by the specified sender to the other participant
        that are part of an email thread. If sender is not one of the participating email
        addresses in the communication relationship, an exception is thrown. If a time interval 
        is specified through a tuple of datetimes (interval_begin, interval_end), the count 
        returned corresponds to the number of relevant messages that were sent within the time 
        interval.
        """
        self._participant_check(sender)
        threads = self.getConversationThreads()
        count = 0
        for thread in threads:
            count += thread.getNumberOfMsgsFromSender(sender,time_interval)
        return count