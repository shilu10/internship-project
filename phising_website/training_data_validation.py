from Application_logger import logger
from schema_values import Client_Rawdata
from file_operation import File_Operation
from raw_data_validator import RawDataValidator
from clientdatatransformation import DataTransformation
from db import *

class Training_data_validator :

    def __init__(self) :
        self.type_of_data = "training" 
        self.logger = logger.Logger()
        self.file_path = "Client_files/"

    def validate_training_data(self, filename) :
         # Getting the predefined schema values from the schema files.
        schema_obj = Client_Rawdata(self.type_of_data)

        #number_of_columns, columns_name, length_of_datestamp, length_of_timestamp = schema_obj.values_for_validation()
        number_of_columns, columns_name, length_of_datestamp, length_of_timestamp = schema_obj.values_for_validation()
        filename_pattern = schema_obj.regex_pattern_creation()

        file_operation_obj = File_Operation(self.type_of_data)

        # creation of good data and bad data folder.
        file_operation_obj.directory_creation()


        # validates the data.
        validator_obj = RawDataValidator()

        self.logger.log("general_logs", "general.log", "error", "Validation of the Training Data Started..")

        # Validating the schema_values and the file values
        validator_obj.validate_file_properties(self.file_path, filename, number_of_columns, filename_pattern, length_of_datestamp, length_of_timestamp, self.type_of_data, columns_name)

        # doing composition.
        client_data_transformer = Transform_Client_Data()
        client_data_transformer.transform_client_data(filename)

        # db connection and insertion of the data. 
        
        db = DBConnection(filename)
        connection = db.connect()[0]
        db_name = db.connect()[1]

        db_operations = DataBaseOperations(connection, columns_name, db_name)
        db_operations.create_table()


class Transform_Client_Data :

    def transform_client_data(self, filename) :
        data_transformer = DataTransformation(filename)
        data_transformer.change_hypen()


