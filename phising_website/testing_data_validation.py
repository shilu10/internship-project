from application_logger import logger
from schema_values import ClientRawData
from file_operation import FileOperation
from raw_data_validation import RawDataValidator
from client_testing_data_transformation import DataTransformation
from testing_db import DataBaseOperations

class TestingDataValidator :
    # Class attributes.
    def __init__(self):
        self.type_of_data = "testing" 
        self.logger = logger.Logger()
        self.file_path = "client_files_testing/"

    
    def validate_testing_data(self, filename):
        try: 
            self.logger.log("general_logs", "general.log", "info", f"Started the client testing data validation")
            # Getting the predefined schema values from the schema files.
            schema_obj = ClientRawData(self.type_of_data)

            #number_of_columns, columns_name, length_of_datestamp, length_of_timestamp = schema_obj.values_for_validation()
            number_of_columns, columns_name, length_of_datestamp, length_of_timestamp = schema_obj.values_for_validation()
            filename_pattern = schema_obj.regex_pattern_creation()

            file_operation_obj = FileOperation(self.type_of_data)

            # creation of good data and bad data folder.
            file_operation_obj.directory_creation()

            # validates the data.
            validator_obj = RawDataValidator()

            self.logger.log("general_logs", "general.log", "error", "Validation of the Testing Data Started..")

            # Validating the schema_values and the file values
            validator_obj.validate_file_properties(self.file_path, filename, number_of_columns, filename_pattern, length_of_datestamp, length_of_timestamp, self.type_of_data, columns_name)

            # transforming the client data and takes care of the database operations.
            client_data_transformer = TransformClientData()
            client_data_transformer.transform_client_data(filename)

            db_operations = DataBaseOperations(columns_name, filename, "testing_files_from_db/")
            db_operations.create_table()

            # for insertion of values into the table 
            db_operations.insert_values()

            # convert the table values into csv 
            db_operations.create_csv()
        
        except Exception as error:
            self.logger.log("general_logs", "general.log", "error", f"Error during the client testing data validation {error}") 

        else: 
            self.logger.log("general_logs", "general.log", "info", f"Completed the client testing data validation")


class TransformClientData:
    
    def transform_client_data(self, filename):
        data_transformer = DataTransformation(filename)
        data_transformer.change_hypen()

