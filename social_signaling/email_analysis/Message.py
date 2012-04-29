import re,nltk
from social_signaling.db_access.DB import DB

class Message(object):

    def __init__(self, message_id=None):
        self._db = DB()
        if message_id != None:
            self.setMessageID(message_id)
        
    def __str__(self):
        """Creates a display string for printing the specified message attributes."""
    
        # Define the property order for display
        disp_props = ['MessageID', 'Datetime', 'EpochSecs', 'Sender', 'TO', 'CC', 
                      'BCC', 'Subject', 'Body']
    
        # Assemble the display string
        disp_string = ''
        for prop in disp_props:
        
            # Get the attribute if it exists
            try:
                attr = getattr(self,prop)
            except AttributeError:
                attr = 'not defined'
            
            # If the attribute is a list, build the string representation
            if type(attr) == list:
                astr = ''
                for item in attr:
                    astr += str(item) + ', '
                attr = astr[:-2]
            
            # Append to the display string
            disp_string += prop + ' : ' + str(attr) + '\n'
   
        return disp_string
        
    def setMessageID(self,message_id):
        """Sets the message object attributes to those returned by db.getMessage()."""
        
        # Get the message properties
        msg = self._db.getMessage(message_id)
        
        # Set the object properties
        for k,v in msg.items():
            setattr(self, k, v)
            
    def _trim_at_first_substring(self,sub,s):
        """Finds the first occurrence of sub in s. If sub is present, s is trimmed at the 
        starting location of sub and returned."""
        idx = s.find(sub)
        if idx > -1:
            s = s[:idx]
        return s
    
    def getSenderText(self):
        """Returns the filtered message body with text from previous messages removed.""" 
        
        # Get the message body
        body = self.Body

        # The following are heuristics for identifying sender text in the Enron email corpus
        
        # Remove the original message text if present
        body = self._trim_at_first_substring('-----Original Message-----',body)
        
        # Remove forwarded message text if present
        body = self._trim_at_first_substring('---------------------- Forwarded by',body)
        body = self._trim_at_first_substring('From:',body)
        body = self._trim_at_first_substring('To:',body)
        
        # Remove meeting text
        body = self._trim_at_first_substring('-----Original Appointment-----',body)
        
        # Remove the BlackBerry signature if present
        body = self._trim_at_first_substring('--------------------------\nSent from my BlackBerry Wireless Handheld',body)
        
        # remove random =20 entries in the message body
        body = re.sub(r'=20','',body)
    
        # remove random = that appear in the middle, at the beginning and at
        # the end of words
        body = re.sub(r'\b=\b','',body)
        body = re.sub(r'=\b','',body)
        body = re.sub(r'\b=','',body)        
        
        return body
        
    def getSenderTokens(self,lower=True):
        """Returns a list of tokens derived from the sender's text in the message body.
        If lower = True, the tokens will be returned in all lowercase."""
        
        # The regular expression defining the tokenizer.
        # Extracts sequences with <one or more letters>'<one or more letters> OR
        # <one or more letters>
        regexp = r"([a-zA-Z]+'[a-zA-Z]+)|([a-zA-Z]+)"  
        
        # Extract the tokens
        tokens = nltk.regexp_tokenize(self.getSenderText(),regexp)        
        
        # Lowercase the tokens if necessary
        if lower:
            tokens = map(lambda s : s.lower(),tokens)
        
        return tokens
        