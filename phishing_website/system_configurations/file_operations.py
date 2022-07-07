import glob
from pathlib import Path
import shutil, os
from application_logger import logger
from abc import ABC, abstractmethod



class IFileOperations(ABC): 
    @abstractmethod
    def directory_creation(self): 
        pass

    @abstractmethod
    def deletion_of_good_files(self): 
        pass

    @abstractmethod
    def moving_bad_files_to_archive(self):
        pass


class FileOperationsContext(IFileOperations): 
    def __init__(self, state): 
        print(state)
        self.state = state 
    
    def set_state(self, state): 
        self.state = state

    def directory_creation(self):
        self.state.directory_creation()
    
    def deletion_of_good_files(self):
        self.state.deletion_of_good_files()

    def moving_bad_files_to_archive(self):
        return self.state.moving_bad_files_to_archive()


class Training(FileOperationsContext):

    def __init__(self):
        self.logger = logger.Logger()
    
    def directory_creation(self) -> None:
            """
            Directory Creation for the Good data and for the bad data, after segregating them after validation.
            Folder Names -> good_Data/, bad_Data/
            """
            try :
                self.logger.log("file_operations_logs", "file_operations.log", "info", "Started creating directories of training data")
                training_good_dir = "data_segregation/training/good_data/"
                path = Path(training_good_dir)
                path.mkdir(parents=True, exist_ok=True)

                training_bad_dir = "data_segregation/training/bad_data/"
                path = Path(training_bad_dir)
                path.mkdir(parents=True, exist_ok=True)

            except OSError as error:
                self.logger.log("file_operations_logs", "file_operations.log", "error", f"Error while creating directories of training")
                raise error

            else :
                self.logger.log("file_operations_logs", "file_operations.log", "info", f"Created the Directories successfully!! for training data")
                
    def deletion_of_good_files(self) -> None:
        """
        This method will delete the file of current good data and bad data in their folders to make a space optimization.
        The files will be deleted only for the good data. Once after the data is moved into the database table.
        The bad files will be put into the archive dir, for the later use.
        """
        try :
            self.logger.log("file_operations_logs", "file_operations.log", "info", f"Started deletion the good files of the training data!!")
            parent_directory = "data_segregation/training/good_data/"
            for file in glob.glob(parent_directory + "*"):
                os.remove(file)

        except OSError as error:
            self.logger.log("file_operations_logs", "file_operations.log", "error", f"Error occured while deleting the good file of training data")
            raise error
        
        else :
            self.logger.log("file_operations_logs", "file_operations.log", "info", f"Successfully deleted the good files of training data")
             
    def moving_bad_files_to_archive(self):
        """
        This Method is used to move the bad files into the archive directory from the bad directory. 
        """
        try:
            self.logger.log("file_operations_logs", "file_operations.log", "info", f"Started moving bad files of training to archive directory")
            des_path = "bad_data_achrive/training/"
            path = Path(des_path)
            parent_path = "data_segregation/training/bad_data/"
            path.mkdir(parents=True, exist_ok=True)
            files = os.listdir(parent_path)
            
            for file in files:
                shutil.move(parent_path + file, des_path)
        
        except OSError as error:
            self.logger.log("file_operations_logs", "file_operations.log", "error", f"Error occured while moving the bad files of training to archive directory!!")
            
        else:
            self.logger.log("file_operations_logs", "file_operations.log", "info", f"Successfully moved the bad files of training to archive")


class Preprocessing(FileOperationsContext):
    
    def __init__(self):
        self.logger = logger.Logger()

    def directory_creation(self) -> None:
            """
            Directory Creation for the Good data and for the bad data, after segregating them after validation.
            Folder Names -> good_Data/, bad_Data/
            """
            try :
                self.logger.log("file_operations_logs", "file_operations.log", "info", "Started creating directories of training data")
                training_good_dir = "data_segregation/preprocessing/good_data/"
                path = Path(training_good_dir)
                path.mkdir(parents=True, exist_ok=True)

                training_bad_dir = "data_segregation/preprocessing/bad_data/"
                path = Path(training_bad_dir)
                path.mkdir(parents=True, exist_ok=True)

            except OSError as error:
                self.logger.log("file_operations_logs", "file_operations.log", "error", f"Error while creation a directories of preprocessing")
                raise error

            else :
                self.logger.log("file_operations_logs", "file_operations.log", "info", f"Created the Directories successfully!! for training data")
                
    def deletion_of_good_files(self) -> None:
        """
        This method will delete the file of current good data and bad data in their folders to make a space optimization.
        The files will be deleted only for the good data. Once after the data is moved into the database table.
        The bad files will be put into the archive dir, for the later use.
        """
        try :
            self.logger.log("file_operations_logs", "file_operations.log", "info", f"Started deletion the good files of the training data!!")
            parent_directory = "data_segregation/preprocessing/good_data/"
            for file in glob.glob(parent_directory + "*"):
                os.remove(file)

        except OSError as error:
            self.logger.log("file_operations_logs", "file_operations.log", "error", f"Error occured while deleting the good file of preprocessing data")
            raise error
        
        else :
            self.logger.log("file_operations_logs", "file_operations.log", "info", f"Successfully deleted the good files of preprocessing data")
             
    def moving_bad_files_to_archive(self):
        """
        This Method is used to move the bad files into the archive directory from the bad directory. 
        """
        try:
            self.logger.log("file_operations_logs", "file_operations.log", "info", f"Started moving bad files of preprocessing to archive directory")
            des_path = "bad_data_achrive/preprocessing/"
            path = Path(des_path)
            parent_path = "data_segregation/preprocessing/bad_data/"
            path.mkdir(parents=True, exist_ok=True)
            files = os.listdir(parent_path)
            
            for file in files:
                shutil.move(parent_path + file, des_path)
        
        except OSError as error:
            self.logger.log("file_operations_logs", "file_operations.log", "error", f"Error occured while moving the bad files of preprocessing to archive directory!!")
            
        else:
            self.logger.log("file_operations_logs", "file_operations.log", "info", f"Successfully moved the bad files of preprocessing to archive")


class Testing(FileOperationsContext):
    
    def __init__(self):
        self.logger = logger.Logger()

    def directory_creation(self) -> None:
            """
            Directory Creation for the Good data and for the bad data, after segregating them after validation.
            Folder Names -> good_Data/, bad_Data/
            """
            try :
                self.logger.log("file_operations_logs", "file_operations.log", "info", "Started creating directories of testing data")
                training_good_dir = "data_segregation/testing/good_data/"
                path = Path(training_good_dir)
                path.mkdir(parents=True, exist_ok=True)

                training_bad_dir = "data_segregation/testing/bad_data/"
                path = Path(training_bad_dir)
                path.mkdir(parents=True, exist_ok=True)

            except OSError as error:
                self.logger.log("file_operations_logs", "file_operations.log", "error", f"Error while creation a directories of testing")
                raise error

            else :
                self.logger.log("file_operations_logs", "file_operations.log", "info", f"Created the Directories successfully!! for testing data")
                
    def deletion_of_good_files(self) -> None:
        """
        This method will delete the file of current good data and bad data in their folders to make a space optimization.
        The files will be deleted only for the good data. Once after the data is moved into the database table.
        The bad files will be put into the archive dir, for the later use.
        """
        try :
            self.logger.log("file_operations_logs", "file_operations.log", "info", f"Started deletion the good files of the training data!!")
            parent_directory = "data_segregation/testing/good_data/"
            for file in glob.glob(parent_directory + "*"):
                os.remove(file)

        except OSError as error:
            self.logger.log("file_operations_logs", "file_operations.log", "error", f"Error occured while deleting the good file of testing data")
            raise error
        
        else :
            self.logger.log("file_operations_logs", "file_operations.log", "info", f"Successfully deleted the good files of testing data")
             
    def moving_bad_files_to_archive(self):
        """
        This Method is used to move the bad files into the archive directory from the bad directory. 
        """
        try:
            self.logger.log("file_operations_logs", "file_operations.log", "info", f"Started moving bad files of testing to archive directory")
            des_path = "bad_data_achrive/testing/"
            path = Path(des_path)
            parent_path = "data_segregation/testing/bad_data/"
            path.mkdir(parents=True, exist_ok=True)
            files = os.listdir(parent_path)
            
            for file in files:
                shutil.move(parent_path + file, des_path)
        
        except OSError as error:
            self.logger.log("file_operations_logs", "file_operations.log", "error", f"Error occured while moving the bad files of testing to archive directory!!")
            
        else:
            self.logger.log("file_operations_logs", "file_operations.log", "info", f"Successfully moved the bad files of testing to archive")