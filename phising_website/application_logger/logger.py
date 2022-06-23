from datetime import datetime
from pathlib import Path


def abstractfunc(func): 
  """
    This will create a abstract class for us, to use in the interface
  """
  func.__isabstract__ = True
  return func


class Singleton(type):
  """
    This Metaclass is the blueprint for the Logger class, to make it a singleton design pattern.
  """
  _instance = {}
  def __call__(cls, *args, **kwargs): 
    if cls not in cls._instance:
      cls._instance[cls]=super(Singleton,cls).__call__(*args, **kwargs)
    return cls._instance[cls]


class ILogger(metaclass=Singleton): 
  @abstractfunc
  def log(self):
    pass


class Logger(ILogger) :
    """
    This class will be used to log the message into the logs folder 
    """
    def __init__(self) :
        pass
    
    def log(self,folder : str, filename : str, level : str, message : str) -> None :
        """
        Folder - Need not to create a new folder if the folder is not exist.
        filename : filename for the log.
        level : what is the level of the log(info, warning, error, critical).
        message : pass the log message.
        """
        path = Path(f"logs/{folder}")
        path.mkdir(parents = True, exist_ok = True)
        current_moment = datetime.now()
        date = current_moment.date()
        current_time = current_moment.strftime("%H:%M:%S")
        with open(path/filename, 'a+') as file :
            file.write(f"{level} : {str(date)} : {str(current_time)} : {message} \n" )
        
            
