application logger/ logger.py - this file contains the logger code.

schema_for_training_data.json - this file has the pre-defined schema, we have signed with the client for training data.

schema_for_testing_data.json - this file has the pre-defined schema, we have signed with the client for testing data.

schema_values - this file has a code gives the schema values from the schema files of both training and testing data.

file_operations - this file has a code for moving the data to the directory of good and bad seperately. this file code is used to delete the pre-existing directory of good and bad, creating new dir for both.

raw_data_validator - this will have the methods that will do all of the validations for both training and testing data.

storing_to_db - this file code is used to store the current good data in the database. and deleting the file for the storage optimization .

