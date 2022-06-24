from application_logger import logger
from schema_values import ClientRawData
from file_operation import FileOperation
from raw_data_validation import ValidatorBuilder
from client_data_transformation import DataTransformation
from db import *


class TrainingDataValidator:

    def __init__(self):
        self.type_of_data = "training" 
        self.logger = logger.Logger()
        self.file_path = "client_files_training/"

    def validate_training_data(self, filename):
        try: 
            self.logger.log("general_logs", "general.log", "info", f"Started the client training data validation")
            #Getting the predefined schema values from the schema files.
            schema_obj = ClientRawData(self.type_of_data)

            #number_of_columns, columns_name, length_of_datestamp, length_of_timestamp = schema_obj.values_for_validation()
            number_of_columns, columns_name, length_of_datestamp, length_of_timestamp = schema_obj.values_for_validation()
            filename_pattern = schema_obj.regex_pattern_creation()

            # Data Ingestion Processing!!!!
            validator = (ValidatorBuilder(self.type_of_data).add_column_name(columns_name).add_filename(filename)
                        .add_filename_pattern(filename_pattern).add_filepath(self.file_path).add_length_of_datestamp(length_of_datestamp)
                        .add_length_of_timestamp(length_of_timestamp).add_no_of_columns(number_of_columns).add_logger(self.logger)
                        .build()
                        )

            # Validating the schema_values and the file values
            validator.validate_file_properties()

            # doing composition.
            client_data_transformer = TransformClientData()
            client_data_transformer.transform_client_data(filename)

            db_operations = DataBaseOperations(columns_name, filename, "training_files_from_db/")
            db_operations.create_table()

            # for insertion of values into the table 
            db_operations.insert_values()

            # convert the table values into csv 
            db_operations.create_csv()
        
        except Exception as error:
            self.logger.log("general_logs", "general.log", "error", f"Error during the client training data validation {error}") 

        else: 
            self.logger.log("general_logs", "general.log", "info", f"Completed the client training data validation")

class TransformClientData:

    def transform_client_data(self, filename):
        data_transformer = DataTransformation(filename)
        data_transformer.change_hypen()


