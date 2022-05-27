from Application_logger import logger
import os, shutil
import pandas as pd
import deepdiff
import re, glob

# this is a polymorphism class. Because it can able to do samething for different data.
class RawDataValidator :
    
    def __init__(self) :
        self.logger = logger.Logger()
    
    #phising_08012020_120000.csv
    def validate_file_properties(self, file_path, filename, number_of_columns, filename_pattern, length_of_datestamp, length_of_timestamp, type_of_data, columns_name):
        
        if re.match(filename_pattern, filename) :
            
            splitting_at_dot = filename.split(".")
            name, date_stamp, time_stamp = splitting_at_dot[0].split('_')
            _error = False
            
            client_dic = { }
            client_file = pd.read_csv(file_path + filename)

            for column in client_file.columns : 
                if client_file[column].dtype == "O" :
                    client_dic[column] = "OBJECT"
                else :
                    client_dic[column] = "INTEGER"
                    
            if splitting_at_dot[1] != "csv" :
                self.logger.log("general_logs", "general.log", "error", f"Filename extension is not .csv. Moving the file into bad directory of {type_of_data}")
                _error = True
                    
                    
            if name != "phising" or len(time_stamp) != length_of_timestamp or len(date_stamp) != length_of_datestamp :
                self.logger.log("general_logs", "general.log", "error", f"Filename schema is not valid. Moving the file into bad directory {type_of_data}")
                _error = True
                
            if number_of_columns != len(pd.read_csv(file_path + filename).columns) : 
                print(number_of_columns != len(pd.read_csv(file_path + filename).columns))
                self.logger.log("general_logs", "general.log", "error", f"Number of Columns in the file are not valid. Moving the file into bad Directory {type_of_data}")
                _error = True
                    
            if len(deepdiff.DeepDiff(columns_name, client_dic)) > 0 :
                print(deepdiff.DeepDiff(columns_name, client_dic))
                self.logger.log("general_logs", "general.log", "error", f"Number of Columns in the file are not valid. Moving the file into bad Directory {type_of_data}")
                _error = True
                    
                    
            if _error == False :
                self.logger.log("general_logs", "general.log", "info", f"Filename schema is valid. Moving the file into good directory {type_of_data}")
                
                if type_of_data == "training" :
                    shutil.copy(glob.glob(file_path + filename)[0], 'Training_data_segregation/Good_Data/')
                else :
                    shutil.copy(glob.glob(file_path + filename)[0], 'Testing_data_segregation/Good_Data/')
                
            else :
                if type_of_data == "training" :
                    shutil.copy(glob.glob(file_path + filename)[0], 'Training_data_segregation/Bad_Data/')
                else :
                    shutil.copy(glob.glob(file_path + filename)[0], 'Testing_data_segregation/Bad_Data/')
                    
        else :
            if type_of_data == "training" :
              #  print(file.read())
                shutil.copy(glob.glob(file_path + filename)[0], 'Training_data_segregation/Bad_Data/')
            else :
                shutil.copy(glob.glob(file_path + filename)[0], 'Testing_data_segregation/Bad_Data/')

            self.logger.log("general_logs", "general.log", "error", f"Filename schema is not valid. Moving the file into bad directory {type_of_data}")
            
            

