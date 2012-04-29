from collections                             import Sequence
from social_signaling.email_analysis.Message import Message
from social_signaling.util.time_interval     import *

class MessageCollection(Sequence):

    def __init__(self,message_ids=[],message_epoch_secs={}):
        self._msg = Message()
        self._message_ids = []
        self._message_epoch_secs = message_epoch_secs
        if message_ids != []:
            self.setMessageIDs(message_ids)
        
    def __len__(self):
        """Returns the number of messages in the collection."""
        return len(self._message_ids)
        
    def __getitem__(self,key):
        """Returns a single message object referenced by key."""
        mid = self._message_ids[key]
        if type(mid) == str:
            self._msg.setMessageID(mid)
            return self._msg
        else:
            raise Exception("Slice operations are not supported.")
            
    def _extract_message_epoch_secs(self,mids):
        """Adds message EpochSecs to the internal dictionary for the referenced messages."""
        msg = Message()
        for mid in mids:
            msg.setMessageID(mid)
            self._message_epoch_secs[mid] = msg.EpochSecs
        
    def copy(self):
        """Returns a new MessageCollection object with the same message ids as the
        current instance."""
        return MessageCollection(self._message_ids,self._message_epoch_secs)
        
    def timeOrder(self,time_reverse=False):
        """Reorders the message ids such that they are in ascending order based on the
        time sent. If time_reverse=True, the message ids are sorted in descending time order."""
        
        # Get the keys from the epoch secs dictionary
        es_keys = self._message_epoch_secs.keys()
        
        # Remove any additional keys that are no longer needed
        keys_to_delete = set(es_keys) - set(self._message_ids)
        for k in keys_to_delete:
            del self._message_epoch_secs[k]
            
        # Get any new times that are needed and not available
        mids_to_fetch = set(self._message_ids) - set(es_keys)
        self._extract_message_epoch_secs(mids_to_fetch)
        
        # Sort the message ids
        self._message_ids.sort(key=lambda mid : self._message_epoch_secs[mid],reverse=time_reverse)
            
    def setMessageIDs(self,mids):
        """Sets the object's list of message ids to mids."""
        self._message_ids = mids
         
    def addMessageIDs(self,mids):
        """Adds mids to the object's list of message ids."""
        self._message_ids.extend(mids)
        
    def getMessageIDs(self):
        """Returns the object's list of message ids."""
        return self._message_ids
        
    def getNumberOfMsgs(self,time_interval=None):
        """Returns the number of message ids. If a time interval is specified through 
        a tuple of datetimes (interval_begin, interval_end), the count returned corresponds to 
        the number of messages that were sent within the time interval.
        """

        # If a time interval is specified, check to see if it is valid
        if time_interval != None and not valid_time_interval(time_interval):
            raise Exception("The specificed time interval is not valid!")
    
        if time_interval == None:
            count = len(self._message_ids)
        else:
            count = 0
            for msg in self:
                if within_time_interval(msg.Datetime,time_interval):
                    count += 1
                    
        return count
            
    