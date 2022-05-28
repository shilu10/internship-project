from application_logger import logger
import pandas as pd 
from pathlib import Path
import shutil, os

class File_Handler : 
    LOGGER = logger.Logger()

    @classmethod
    def save_file(cls, file_obj, filename, type_of_data) :
        if type_of_data == "training" : 
            folder = 'client_files_training/'
        else :
            folder = "client_files_testing/"
        
        _file = pd.read_csv(file_obj)
        path_for_file = Path(folder)
        path_for_file.mkdir(parents = True, exist_ok = True)
        _file.to_csv(filename, index = False)

        path = Path(folder + filename)
        if path.is_file():
            os.remove(filename)
            pass
        else :
            shutil.move(filename, folder)
        cls.LOGGER.log("general_logs", "general.log", "info", f"Moving the Client file to the {folder} directory....")

        