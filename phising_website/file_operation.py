import glob
import shutil, os
from application_logger import logger

class FileOperation:

    def __init__(self, type_of_data):
        self.type_of_data = type_of_data
        self.logger = logger.Logger()
    
    def directory_creation(self) -> None:
            """
            Directory Creation for the Good data and for the bad data, after segregating them after validation.
            Folder Names -> good_Data/, bad_Data/
            """
            try :
                if self.type_of_data == "training":
                    parent_directory = "training_data_segregation/"
                else :
                    parent_directory = "testing_data_segregation/"

                path = os.path.join(parent_directory, "good_data/")
                if not os.path.isdir(path):
                    os.makedirs(path)

                path = os.path.join(parent_directory, "bad_data/")
                if not os.path.isdir(path):
                    os.makedirs(path)

            except OSError as error:
                self.logger.log("file_operations_logs", "file_operations.log", "error", f"Error while creating the Folder : {str(error)} in {self.type_of_data} phase")
                raise error

            else :
                self.logger.log("file_operations_logs", "file_operations.log", "info", f"Created the Directories successfully!! in {self.type_of_data} phase")
                
    def deletion_of_good_files(self) -> None:
        """
        This method will delete the file of current good data and bad data in their folders to make a space optimization.
        The files will be deleted only for the good data. Once after the data is moved into the database table.
        The bad files will be put into the archive dir, for the later use.
        """
        try :
            if self.type_of_data == "training":
                parent_directory = "training_data_segregation/good_data/"
            else :
                parent_directory = "testing_data_segregation/good_data/"

            for file in glob.glob(parent_directory + "*"):
                os.remove(file)

        except OSError as error:
            self.logger.log("file_operations_logs", "file_operations.log", "error", f"Error occured while file in {self.type_of_data} phase!!")
            raise error
        
        else :
            self.logger.log("file_operations_logs", "file_operations.log", "info", f"Deleted the file successfully in {self.type_of_data} phase!!")
             
    def moving_bad_files_to_archive(self):
        """
        This Method is used to move the bad files into the archive directory from the bad directory. 
        """
        try:
            if self.type_of_data == "training":
                parent_folder = "training_data_segregation/bad_data/"
                destination_path = "bad_training_data_archive/"
            else:
                parent_folder = "testing_data_segregation/bad_Data/"
                destination_path = "bad_testing_data_archive/"
            
            if not os.path.isdir(destination_path):
                os.makedirs(destination_path)
                
            files = os.listdir(parent_folder)
            
            for file in files:
                shutil.move(parent_folder + file, destination_path)
        
        except OSError as error:
            self.logger.log("file_operations_logs", "file_operations.log", "error", f"Error occured while moving the bad files to archive directories in {self.type_of_data} phase!!")
            
        else:
            self.logger.log("file_operations_logs", "file_operations.log", "info", f"moving the bad files to archive directories in {self.type_of_data} phase!!")

