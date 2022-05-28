from datetime import datetime
from pathlib import Path
import os 
class Logger(object) :
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
        
      #  os.chdir("/home/adminuser/internship_projects/phising_website/logs")
        #print(os.getcwd())
        #path = Path("logs", folder)

        #print(path.exists())

        #if not path.exists() :
         #   print("ues")
          #  path.mkdir(parent = True)
                
        path = Path(f"logs/{folder}")
        path.mkdir(parents = True, exist_ok = True)
        current_moment = datetime.now()
        date = current_moment.date()
        current_time = current_moment.strftime("%H:%M:%S")
        with open(path/filename, 'a+') as file :
            file.write(f"{level} : {str(date)} : {str(current_time)} : {message} \n" )
        
            
