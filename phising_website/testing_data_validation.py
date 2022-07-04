from application_logger import logger
from schema_values import ClientRawData
from file_operation import FileOperation
from raw_data_validation import ValidatorBuilder
from client_testing_data_transformation import DataTransformation
from db import DataBaseOperationsContext, TestingDBConnection, TestingDataBaseOperations

class TestingDataValidator :
    # Class attributes.
    def __init__(self):
        self.type_of_data = "testing" 
        self.logger = logger.Logger()
        self.file_path = "client_files/testing/"

    
    def validate_testing_data(self, filename):
        print("called")
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
            validator = (ValidatorBuilder(self.type_of_data).add_column_name(columns_name).add_filename(filename)
                        .add_filename_pattern(filename_pattern).add_filepath(self.file_path).add_length_of_datestamp(length_of_datestamp)
                        .add_length_of_timestamp(length_of_timestamp).add_no_of_columns(number_of_columns).add_logger(self.logger)
                        .build()
                        )
                        
            # Validating the schema_values and the file values
            validator.validate_file_properties()

            # transforming the client data and takes care of the database operations.
            client_data_transformer = TransformClientData()
            client_data_transformer.transform_client_data(filename)

            print(filename, "for the testing")
            testing_db_operations = DataBaseOperationsContext(state = TestingDataBaseOperations(columns_name, filename, "files_from_db/testing/"))

            testing_db_operations.create_dir()
            testing_db_operations.create_table()

            # for insertion of values into the table 
            testing_db_operations.insert_value()

            # convert the table values into csv 
            testing_db_operations.create_csv()
        
        except Exception as error:
            self.logger.log("general_logs", "general.log", "error", f"Error during the client testing data validation {error}") 

        else: 
            self.logger.log("general_logs", "general.log", "info", f"Completed the client testing data validation")


class TransformClientData:
    
    def transform_client_data(self, filename):
        data_transformer = DataTransformation(filename)
        data_transformer.change_hypen()

