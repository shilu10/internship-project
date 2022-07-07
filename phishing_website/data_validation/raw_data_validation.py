from application_logger import logger
import os, shutil
import pandas as pd
import deepdiff
import re, glob

# builder pattern for rawdatavalidator
class ValidatorBuilder:
    def __init__(self, type_of_data): 
        self.validator = RawDataValidator(type_of_data)

    def add_filepath(self, filepath): 
        self.validator.file_path = filepath
        return self 
    
    def add_filename(self, filename): 
        self.validator.filename = filename
        return self

    def add_no_of_columns(self, number): 
        self.validator.number_of_columns = number
        return self

    def add_filename_pattern(self, pattern):
        self.validator.filename_pattern = pattern
        return self 
    
    def add_length_of_datestamp(self, length_of_datestamp): 
        self.validator.length_of_datestamp = length_of_datestamp
        return self
    
    def add_length_of_timestamp(self, length_of_timestamp): 
        self.validator.length_of_timestamp = length_of_timestamp
        return self 
    
    def add_column_name(self, name): 
        self.validator.columns_name = name
        return self

    def add_logger(self, logger): 
        self.validator.logger = logger
        return self

    def build(self): 
        return self.validator


class RawDataValidator:
    
    def __init__(self, type_of_data):
        self.type_of_data = type_of_data
        
    #phising_08012020_120000.csv
    def validate_file_properties(self):
        try: 
            self.logger.log("validation_logs", "validation.log", "info", "Validation of the Training Data Started..")
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
                    self.logger.log("validation_logs", "validation.log", "error", f"Filename extension is not .csv. Moving the file into bad directory of {self.type_of_data}")
                    _error = True
                        
                        
                if name != "phising" or len(time_stamp) != self.length_of_timestamp or len(date_stamp) != self.length_of_datestamp:
                    self.logger.log("validation_logs", "validation.log", "error", f"Filename schema is not valid. Moving the file into bad directory {self.type_of_data}")
                    _error = True
                    
                if self.number_of_columns != len(pd.read_csv(self.file_path + self.filename).columns): 
                    print(self.number_of_columns != len(pd.read_csv(self.file_path + self.filename).columns))
                    self.logger.log("validation_logs", "validation.log", "error", f"Number of Columns in the file are not valid. Moving the file into bad Directory {self.type_of_data}")
                    _error = True
                        
                if len(deepdiff.DeepDiff(self.columns_name, client_dic)) > 0:
                    print(deepdiff.DeepDiff(self.columns_name, client_dic))
                    self.logger.log("validation_logs", "validation.log", "error", f"Number of Columns in the file are not valid. Moving the file into bad Directory {self.type_of_data}")
                    _error = True
                        
                if _error == False:
                    self.logger.log("validation_logs", "validation.log", "info", f"Filename schema is valid. Moving the file into good directory {self.type_of_data}")
                    
                    if self.type_of_data == "training":
                        shutil.copy(glob.glob(self.file_path + self.filename)[0], 'data_segregation/training/good_data/')
                    else:
                        shutil.copy(glob.glob(self.file_path + self.filename)[0], 'data_segregation/testing/good_data/')
                    
                else:
                    if self.type_of_data == "training":
                        shutil.copy(glob.glob(self.file_path + self.filename)[0], 'data_segregation/training/bad_data/')
                    else :
                        shutil.copy(glob.glob(self.file_path + self.filename)[0], 'data_segregation/testing/bad_data/')
                        
            else:
                if self.type_of_data == "training":
                #  print(file.read())
                    shutil.copy(glob.glob(self.file_path + self.filename)[0], 'data_segregation/training/bad_data/')
                else:
                    shutil.copy(glob.glob(self.file_path + self.filename)[0], 'data_segregation/testing/bad_data/')

                self.logger.log("validation_logs", "validation.log", "error", f"Filename schema is not valid. Moving the file into bad directory {self.type_of_data}")
                
        except Exception as error: 
            self.logger.log("validation_logs", "validation.log", "error", f"Error during the validation of the training data {error}")

        else: 
            self.logger.log("validation_logs", "validation.log", "info", f"Completed the raw data validation")
