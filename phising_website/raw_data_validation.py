from application_logger import logger
import os, shutil
import pandas as pd
import deepdiff
import re, glob

# this is a polymorphism class. Because it can able to do samething for different data.
class RawDataValidator:
    
    def __init__(self, file_path, filename, number_of_columns, filename_pattern, length_of_datestamp, length_of_timestamp, type_of_data, columns_name):
        self.logger = logger.Logger()
        self.file_path = file_path
        self.filename = filename
        self.number_of_columns = number_of_columns
        self.filename_pattern = filename_pattern 
        self.length_of_datestamp = length_of_datestamp
        self.length_of_timestamp = length_of_timestamp
        self.type_of_data = type_of_data
        self.columns_name = columns_name
    
    #phising_08012020_120000.csv
    def validate_file_properties(self):
        
        if re.match(self.filename_pattern, self.filename):
            
            splitting_at_dot = self.filename.split(".")
            name, date_stamp, time_stamp = splitting_at_dot[0].split('_')
            _error = False
            
            client_dic = { }
            client_file = pd.read_csv(self.file_path + self.filename)

            for column in client_file.columns: 
                if client_file[column].dtype == "O":
                    client_dic[column] = "OBJECT"
                else :
                    client_dic[column] = "INTEGER"
                    
            if splitting_at_dot[1] != "csv":
                self.logger.log("general_logs", "general.log", "error", f"Filename extension is not .csv. Moving the file into bad directory of {self.type_of_data}")
                _error = True
                    
                    
            if name != "phising" or len(time_stamp) != self.length_of_timestamp or len(date_stamp) != self.length_of_datestamp:
                self.logger.log("general_logs", "general.log", "error", f"Filename schema is not valid. Moving the file into bad directory {self.type_of_data}")
                _error = True
                
            if self.number_of_columns != len(pd.read_csv(self.file_path + self.filename).columns): 
                print(self.number_of_columns != len(pd.read_csv(self.file_path + self.filename).columns))
                self.logger.log("general_logs", "general.log", "error", f"Number of Columns in the file are not valid. Moving the file into bad Directory {self.type_of_data}")
                _error = True
                    
            if len(deepdiff.DeepDiff(self.columns_name, client_dic)) > 0:
                print(deepdiff.DeepDiff(self.columns_name, client_dic))
                self.logger.log("general_logs", "general.log", "error", f"Number of Columns in the file are not valid. Moving the file into bad Directory {self.type_of_data}")
                _error = True
                    
            if _error == False:
                self.logger.log("general_logs", "general.log", "info", f"Filename schema is valid. Moving the file into good directory {self.type_of_data}")
                
                if self.type_of_data == "training":
                    shutil.copy(glob.glob(self.file_path + self.filename)[0], 'training_data_segregation/good_data/')
                else:
                    shutil.copy(glob.glob(self.file_path + self.filename)[0], 'testing_data_segregation/good_data/')
                
            else:
                if self.type_of_data == "training":
                    shutil.copy(glob.glob(self.file_path + self.filename)[0], 'training_data_segregation/bad_data/')
                else :
                    shutil.copy(glob.glob(self.file_path + self.filename)[0], 'testing_data_segregation/bad_data/')
                    
        else:
            if self.type_of_data == "training":
              #  print(file.read())
                shutil.copy(glob.glob(self.file_path + self.filename)[0], 'training_data_segregation/bad_data/')
            else:
                shutil.copy(glob.glob(self.file_path + self.filename)[0], 'testing_data_segregation/bad_data/')

            self.logger.log("general_logs", "general.log", "error", f"Filename schema is not valid. Moving the file into bad directory {self.type_of_data}")
            
            

