import logging 
from functools import wraps
# from .mod_exceptions import FalseDivisionError



def logger_config(): 
	
	# create a logger object 
	logger = logging.getLogger('e_logger') 
	logger.setLevel(logging.DEBUG) 
	
	#create a file to store all the logged exceptions 
	logfile = logging.FileHandler('e_logger.log') 
	
	log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
	formatter = logging.Formatter(log_format) 
	
	logfile.setFormatter(formatter) 
	logger.addHandler(logfile) 
	
	return logger 


# print(logger) 

# Decorator for Logger
def record_exception(logger):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except:
                issue ="\n============================================\n"
                issue = issue+"exception in "+func.__name__+"\n"
                issue = issue+"============================================\n"
                logger.exception(issue)
                raise 
        return wrapper
    return decorator 

