import sqlite3
from pathlib import Path
from application_logger import logger 
import pandas as pd 
import shutil

# we will have different database file for each csv client files.
class DBConnection :
    def __init__(self, filename) :
        self.db_name = filename.split('.')[0] + ".db"
        self.path = Path("training_databases/")
        self.path.mkdir(parents = True, exist_ok = True)
        self.logger = logger.Logger()
    
    def connect(self) :

        try :
            self.logger.log("db_logs", "db.log", "info", f"Started the db connection to {self.db_name}")
            self.connection = sqlite3.connect(Path(f"training_databases/{self.db_name}"))
            return self.connection

        except ConnectionError as error :
            self.logger.log("db_logs", "db.log", "error", f"Error While connecting to the {self.db_name} Database")
            raise error 

class DataBaseOperations :
    
    def __init__(self, column_names, filename) :

        self.column_names = column_names
        self.filename = filename
        self.logger = logger.Logger()
        self.db_connection = DBConnection(self.filename)
        self.good_file_folder = "training_data_segregation/good_data/"
        self.bad_file_folder = "training_data_segregation/bad_data/"
        self.path = Path("db_training_files/")
        self.path.mkdir(parents = True, exist_ok = True)
        self.training_file_from_db = "db_training_files/"

    def create_table(self) :
        connection = self.db_connection.connect()
        cursor = connection.cursor()
        table_name = "training"

        try :
            query = f"SELECT count(name)  FROM sqlite_master WHERE type = 'table'AND name = '{table_name}'"
            cursor.execute(query)
            connection.commit()

            res = cursor.fetchone()[0]
            print(res == 1 )
            if res == 1 :
                self.logger.log("db_logs", "db.log", "info", f"Table named {table_name} created successfully in {self.filename} Database")
            
            else :
                for col_name in self.column_names.keys() : 
                    col_type = self.column_names[col_name]                
                    try :
                        
                        query = f'ALTER TABLE {table_name} ADD COLUMN "{col_name}" {col_type}'
                        cursor.execute(query)
                        connection.commit()
                        
                    except Exception as error:
                        query = f"CREATE TABLE {table_name} ({col_name} {col_type})"
                        cursor.execute(query)
                        connection.commit()
                        
        except Exception as error :
            self.logger.log("db_logs", "db.log", "error", f"Error while creating {table_name} in {self.filename} Database")

        else :
            self.logger.log("db_logs", "db.log", "info", f"Table named {table_name} created successfully in {self.filename}")

        finally :
            connection.close()
            self.logger.log("db_logs", "db.log", "info", f"Closed the db connection to {self.filename}")

    def insert_values(self) :

        try :
            connection = self.db_connection.connect()
            data = pd.read_csv(self.good_file_folder + self.filename)
            data.to_sql("training", connection, if_exists = 'replace', index = False)
            connection.commit()
            self.logger.log("db_logs", "db.log", "info", f"Values are inserted into the table of training in {self.filename}")
        
        except Exception as error :
            connection.rollback()
            shutil.move(self.good_file_folder + self.filename, self.bad_file_folder)
            self.logger.log("general_logs", "general.log", "error", f"Error while working with the good data file during the database operation so moving it into bad data directory")
            self.logger.log("db_logs", "db.log", "error", f"Error while inserting the values into training new in {self.filename}")

        finally :
            connection.close()
            self.logger.log("db_logs", "db.log", "info", f"Closed the db connection to {self.filename}")

    def create_csv(self) :

        try : 
            connection = self.db_connection.connect()
            db_df = pd.read_sql_query("SELECT * FROM training", connection)
            db_df.to_csv(self.training_file_from_db + self.filename, index = False)
            self.logger.log("db_logs", "db.log", "info", f"Successfully converted the table values into csv file and saved in the db_training_dir")
        
        except Exception as error :
            self.logger.log("db_logs", "db.log", "error", f"Error during the creation of csv file from the values of the db table")

        finally :
            connection.close()
            self.logger.log("db_logs", "db.log", "info", f"Closed the db connection to {self.filename}")





