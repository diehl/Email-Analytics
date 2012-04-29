import datetime

def valid_time_interval(time_interval):
    """Returns True if the time_interval[0] < time_interval[1] and both are of type 
    datetime.
    """
    if time_interval[0] < time_interval[1]:
        if (type(time_interval[0]) == datetime.datetime and 
            type(time_interval[1]) == datetime.datetime):
            return True
    return False
            
def within_time_interval(dt,time_interval):
    """Returns True if dt is within the specified time interval and False otherwise."""
    if type(dt) == datetime.datetime and time_interval[0] <= dt and dt <= time_interval[1]:
        return True
    else:
        return False