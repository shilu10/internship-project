from Application_logger import logger
import os 
import json


class Client_Rawdata :
    
    #  file : str
    def __init__(self, type_of_data : str) -> None :
        self.type_of_data = type_of_data
        if self.type_of_data == "training" :
            self.schema_file_path = "schema_for_training_data.json"
        else :
            self.schema_file_path = "schema_for_testing_data.json"
        self.logger = logger.Logger()
        
    def values_for_validation(self) -> tuple :
        
        try : 
            self.logger.log("Rawdata_logs/", "schema_data.log", "info", f"Start reading the pre-defined schema file of {self.type_of_data}")         
            with open(self.schema_file_path) as file : 
                schema_data = json.load(file)

            number_of_columns = schema_data.get('Number_of_columns')
            name = schema_data.get('Name_of_file')
            length_of_datestamp = schema_data.get('Length_of_datestamp_in_filename')
            length_of_timestamp = schema_data.get('Length_of_timestamp_in_filename')
            columns_name = schema_data.get('Columns_name')
            message = f"Reading of the file is successfull values are, Number of columns={number_of_columns}, Length of datestamp={length_of_datestamp}, Length of timestamp={length_of_timestamp}, Pattern={name}"
            self.logger.log("Rawdata_logs", "rawdata.log", "info", message)
            self.logger.log("Rawdata_logs/", "schema_data.log", "info", f"Reading of the pre-defined schema file of {self.type_of_data} is successull..")

            return number_of_columns, columns_name, length_of_datestamp, length_of_timestamp
        
        except ValueError as error :
            self.logger.log("Rawdata_logs", "rawdata.log", "error", "Value for the key is not presented inside the json file.")
            raise error
        
        except KeyError as error :
            self.logger.log("Rawdata_logs", "rawdata.log", "error", "Key is not found in the json file")
            raise error
        
        except Exception as error :
            self.logger.log("Rawdata_logs", "rawdata.log", "error", str(error))
            raise error
        
        else :
            self.logger.log("Rawdata_logs", "rawdata.log", "info", "Reading of the schema file is SuccessFull!!")

    def regex_pattern_creation(self) -> str :
        pattern = "['phising']+['\_'']+[\d_]+[\d]+\.csv"
        return pattern
    
   