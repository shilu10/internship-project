import sqlite3
from pathlib import Path
from application_logger import logger 
import pandas as pd 
import shutil


def abstractfunc(func): 
  """
    This will create a abstract class for us, to use in the interface
  """
  func.__isabstract__ = True
  return func


class Singleton(type):
  """
    This Metaclass is the blueprint for the Logger class, to make it a singleton design pattern.
  """
  _instance = {}
  def __call__(cls, *args, **kwargs): 
    if cls not in cls._instance:
      cls._instance[cls]=super(Singleton,cls).__call__(*args, **kwargs)
    return cls._instance[cls]


class IDBConnection(metaclass=Singleton): 
    @abstractfunc
    def connect(self): 
        pass
    
    @abstractfunc
    def create_dir(self): 
        pass

class IDBOperarion(): 
    @abstractfunc
    def create_table(self): 
        pass 
    
    @abstractfunc
    def insert_value(self): 
        pass 

    @abstractfunc
    def create_csv(self): 
        pass


# we will have different database file for each csv client files.
class DBConnectionContext(IDBConnection):
    def __init__(self, state):
        self.state = state
    
    def connect(self, filename):
        return self.state.connect(filename)
    
    def create_dir(self):
        return self.state.create_dir()

    def set_state(self, state): 
        self.state = state
        return self.state


class DataBaseOperationsContext(IDBOperarion):
    
    def __init__(self, state):
        self.state = state
   #     print(self.state, "state in database")
    
    def create_dir(self): 
        self.state.create_dir()

    def set_state(self, state): 
        self.state = state
        return self.state

    def create_table(self):
        self.state.create_table()

    def insert_value(self):
        self.state.insert_value()
        
    def create_csv(self):
        self.state.create_csv()
       

class TrainingDataBaseOperations(IDBOperarion): 

    def __init__(self, column_names, filename, dir_to_save):
        self.column_names = column_names
        self.filename = filename
        self.dir_to_save = dir_to_save
        self.good_file_folder = "training_data_segregation/good_data/"
        self.bad_file_folder = "training_data_segregation/bad_data/"
        self.training_file_from_db = None
        self.db_connection = DBConnectionContext(TrainingDBConnection()) 
       
        self.logger = logger.Logger()

    def create_dir(self): 
        path = Path(self.dir_to_save)
      
        path.mkdir(parents = True, exist_ok = True)
        self.training_file_from_db = self.dir_to_save
   
    def create_table(self): 
        connection = self.db_connection.connect(self.filename)
       
        cursor = connection.cursor()
        table_name = "training"

        try :
            query = f"SELECT count(name)  FROM sqlite_master WHERE type = 'table'AND name = '{table_name}'"
            cursor.execute(query)
            connection.commit()

            res = cursor.fetchone()[0]
           
            if res == 1:
                self.logger.log("db_logs", "db.log", "info", f"Table named {table_name} created successfully in {self.filename} Database")
            
            else :
                
                for col_name in self.column_names.keys(): 
                    col_type = self.column_names[col_name]                
                    try :      
                        query = f'ALTER TABLE {table_name} ADD COLUMN "{col_name}" {col_type}'
                        cursor.execute(query)
                        connection.commit()
                        
                    except Exception as error:
                        query = f"CREATE TABLE {table_name} ({col_name} {col_type})"
                        cursor.execute(query)
                        connection.commit()
                        
        except Exception as error:
            self.logger.log("db_logs", "db.log", "error", f"Error while creating {table_name} in {self.filename} Database")

        else:
            self.logger.log("db_logs", "db.log", "info", f"Table named {table_name} created successfully in {self.filename}")

        finally:
            connection.close()
            self.logger.log("db_logs", "db.log", "info", f"Closed the db connection to {self.filename}")

    def insert_value(self): 
        try:
            connection = self.db_connection.connect(self.filename)
            data = pd.read_csv(self.good_file_folder + self.filename)
            data.to_sql("training", connection, if_exists = 'replace', index = False)
            connection.commit()
            self.logger.log("db_logs", "db.log", "info", f"Values are inserted into the table of training in {self.filename}")
        
        except Exception as error:
            connection.rollback()
            shutil.move(self.good_file_folder + self.filename, self.bad_file_folder)
            self.logger.log("general_logs", "general.log", "error", f"Error while working with the good data file during the database operation so moving it into bad data directory")
            self.logger.log("db_logs", "db.log", "error", f"Error while inserting the values into training new in {self.filename}")

        finally:
            connection.close()
            self.logger.log("db_logs", "db.log", "info", f"Closed the db connection to {self.filename}") 

   
    def create_csv(self): 
        try: 
            connection = self.db_connection.connect(self.filename)
            db_df = pd.read_sql_query("SELECT * FROM training", connection)
            db_df.to_csv(self.training_file_from_db + self.filename, index = False)
            self.logger.log("db_logs", "db.log", "info", f"Successfully converted the table values into csv file and saved in the db_training_dir")
        
        except Exception as error:
            self.logger.log("db_logs", "db.log", "error", f"Error during the creation of csv file from the values of the db table")

        finally:
            connection.close()
            self.logger.log("db_logs", "db.log", "info", f"Closed the db connection to {self.filename}")


class PreproccessingDataBaseOperations(IDBOperarion): 

    def __init__(self, column_names, filename, dir_to_save):
        self.column_names = column_names
        self.filename = filename
        self.dir_to_save = dir_to_save
        self.good_file_folder = "ml_preprocessed_data/training_files/"
        self.bad_file_folder = "training_data_segregation/bad_data/"
        self.training_file_from_db = None
        self.db_connection = DBConnectionContext(PreprocessingDBConnection()) 
        self.logger = logger.Logger()
      

    def create_dir(self): 
        path = Path(self.dir_to_save)
      
        path.mkdir(parents = True, exist_ok = True)
        self.training_file_from_db = self.dir_to_save
   
    def create_table(self): 
        connection = self.db_connection.connect(self.filename)
        
        cursor = connection.cursor()
        table_name = "preprocessing"

        try :
            query = f"SELECT count(name)  FROM sqlite_master WHERE type = 'table'AND name = '{table_name}'"
            cursor.execute(query)
            connection.commit()

            res = cursor.fetchone()[0]
            print(res == 1 )
            if res == 1:
                self.logger.log("db_logs", "db.log", "info", f"Table named {table_name} created successfully in {self.filename} Database")
            
            else :
                print(self.column_names, "in preprocessing")
                for col_name in self.column_names: 

                    col_type = self.column_names[col_name]                
                    try :      
                        query = f'ALTER TABLE {table_name} ADD COLUMN "{col_name}" {col_type}'
                        cursor.execute(query)
                        connection.commit()
                        
                    except Exception as error:
                        query = f"CREATE TABLE {table_name} ({col_name} {col_type})"
                        cursor.execute(query)
                        connection.commit()
                        
        except Exception as error:
            self.logger.log("db_logs", "db.log", "error", f"Error while creating table named{table_name} in {self.filename} Database {error}")

        else:
            self.logger.log("db_logs", "db.log", "info", f"Table named {table_name} created successfully in {self.filename}")

        finally:
            connection.close()
            self.logger.log("db_logs", "db.log", "info", f"Closed the db connection to {self.filename}")

    def insert_value(self): 
        try:
            connection = self.db_connection.connect(self.filename)
            data = pd.read_csv(self.good_file_folder + self.filename)
            data.to_sql("preprocessing", connection, if_exists = 'replace', index = False)
            connection.commit()
            self.logger.log("db_logs", "db.log", "info", f"Values are inserted into the table of training in {self.filename}")
        
        except Exception as error:
            connection.rollback()
            shutil.move(self.good_file_folder + self.filename, self.bad_file_folder)
            self.logger.log("general_logs", "general.log", "error", f"Error while working with the good data file during the database operation so moving it into bad data directory")
            self.logger.log("db_logs", "db.log", "error", f"Error while inserting the values into training new in {self.filename}")

        finally:
            connection.close()
            self.logger.log("db_logs", "db.log", "info", f"Closed the db connection to {self.filename}") 

   
    def create_csv(self): 
        try: 
            connection = self.db_connection.connect(self.filename)
            db_df = pd.read_sql_query("SELECT * FROM preprocessing", connection)
            db_df.to_csv(self.training_file_from_db + self.filename, index = False)
            self.logger.log("db_logs", "db.log", "info", f"Successfully converted the table values into csv file and saved in the db_training_dir")
        
        except Exception as error:
            self.logger.log("db_logs", "db.log", "error", f"Error during the creation of csv file from the values of the db table")

        finally:
            connection.close()
            self.logger.log("db_logs", "db.log", "info", f"Closed the db connection to {self.filename}")


class TestingDataBaseOperations(IDBOperarion): 

    def __init__(self, column_names, filename, dir_to_save):
        self.column_names = column_names
        self.filename = filename
        self.dir_to_save = dir_to_save
        self.good_file_folder = "testing_data_segregation/good_data/"
        self.bad_file_folder = "testing_data_segregation/bad_data/"
        self.testing_files_from_db = None
        self.db_connection = DBConnectionContext(TestingDBConnection())
        self.logger = logger.Logger()

    def create_dir(self): 
        path = Path(self.dir_to_save)
        path.mkdir(parents = True, exist_ok = True)
        self.testing_files_from_db = self.dir_to_save

    def create_table(self):
        connection = self.db_connection.connect(self.filename)
        cursor = connection.cursor()
        table_name = "testing"

        try :
            query = f"SELECT count(name)  FROM sqlite_master WHERE type = 'table'AND name = '{table_name}'"
            cursor.execute(query)
            connection.commit()

            res = cursor.fetchone()[0]
            print(res == 1 )
            if res == 1:
                self.logger.log("testing_db_logs", "db.log", "info", f"Table named {table_name} created successfully in {self.filename} Database")
            
            else :
                for col_name in self.column_names: 
                    col_type = self.column_names[col_name]                
                    try :
                        
                        query = f'ALTER TABLE {table_name} ADD COLUMN "{col_name}" {col_type}'
                        cursor.execute(query)
                        connection.commit()
                        
                    except Exception as error:
                        print(error, "erro in testing")
                        query = f"CREATE TABLE {table_name} ({col_name} {col_type})"
                        cursor.execute(query)
                        connection.commit()
                        
        except Exception as error:
            self.logger.log("testing_db_logs", "db.log", "error", f"Error while creating table {table_name} in {self.filename} Database {error}")

        else:
            self.logger.log("testing_db_logs", "db.log", "info", f"Table named {table_name} created successfully in {self.filename}")

        finally:
            connection.close()
            self.logger.log("testing_db_logs", "db.log", "info", f"Closed the db connection to {self.filename}")

    def insert_value(self):

        try:
            print("insert value called")
            connection = self.db_connection.connect(self.filename)
            data = pd.read_csv(self.good_file_folder + self.filename)
            print(data, "data from testing")
            data.to_sql("testing", connection, if_exists = 'replace', index = False)
            connection.commit()
            self.logger.log("testing_db_logs", "db.log", "info", f"Values are inserted into the table of training in {self.filename}")
        
        except Exception as error:
            connection.rollback()
            shutil.move(self.good_file_folder + self.filename, self.bad_file_folder)
            self.logger.log("general_logs", "general.log", "error", f"Error while working with the good data file during the database operation so moving it into bad data directory")
            self.logger.log("testing_db_logs", "db.log", "error", f"Error while inserting the values into training new in {self.filename}")

        finally:
            connection.close()
            self.logger.log("testing_db_logs", "db.log", "info", f"Closed the db connection to {self.filename}")

    def create_csv(self):

        try: 
            #print(self.filename, "in testing")
            connection = self.db_connection.connect(self.filename)
            db_df = pd.read_sql_query("SELECT * FROM testing", connection)
            db_df.to_csv(self.testing_files_from_db + self.filename, index = False)
            self.logger.log("testing_db_logs", "db.log", "info", f"Successfully converted the table values into csv file and saved in the testing_files_from_db directory")
        
        except Exception as error:
            self.logger.log("testing_db_logs", "db.log", "error", f"Error during the creation of csv file from the values of the db table {error}")

        finally:
            connection.close()
            self.logger.log("testing_db_logs", "db.log", "info", f"Closed the db connection to {self.filename}")


class TrainingDBConnection(IDBConnection): 
    def __init__(self):
        self.logger = logger.Logger()
        self.create_dir()

    def create_dir(self):
        self.path = Path("training_databases/")
        self.path.mkdir(parents = True, exist_ok = True)

    def connect(self, filename):
        
        try :
            self.db_name = filename.split('.')[0] + ".db"
            self.logger.log("db_logs", "db.log", "info", f"Started the db connection to {self.db_name} in training")
            self.connection = sqlite3.connect(Path(f"training_databases/{self.db_name}"))
            return self.connection

        except ConnectionError as error:
            self.logger.log("db_logs", "db.log", "error", f"Error While connecting to the {self.db_name} Database")
            raise error 
        
        finally: 
            self.logger.log("db_logs", "db.log", "info", f"closed the db connection to {self.db_name} in training")


class TestingDBConnection(IDBConnection):
    def __init__(self):
        self.logger = logger.Logger() 
        self.create_dir()
        
    def create_dir(self):
        self.path = Path("testing_databases/")
        self.path.mkdir(parents = True, exist_ok = True)
        

    def connect(self,  filename):
        try :
            self.db_name = filename.split('.')[0] + ".db"
            self.logger.log("testing_db_logs", "db.log", "info", f"Started the db connection to {self.db_name} in testing")
            self.connection = sqlite3.connect(Path(f"testing_databases/{self.db_name}"))
            return self.connection

        except ConnectionError as error:
            self.logger.log("testing_db_logs", "db.log", "error", f"Error While connecting to the {self.db_name} Database")
            raise error

        finally:   
            self.logger.log("db_logs", "db.log", "info", f"closed the db connection to {self.db_name} in testing") 


class PreprocessingDBConnection(IDBConnection): 
    def __init__(self):
        self.logger = logger.Logger()
        self.create_dir()
        
    def create_dir(self):
        self.path = Path("preprocessing_databases/")
        self.path.mkdir(parents = True, exist_ok = True)

    def connect(self, filename):
        
        try :
            self.db_name = filename.split('.')[0] + ".db"
            self.logger.log("db_logs", "db.log", "info", f"Started the db connection to {self.db_name} in testing")
            self.connection = sqlite3.connect(Path(f"preprocessing_databases/{self.db_name}"))
           # print(self.connection, "in preprocessing...")
            return self.connection

        except ConnectionError as error:
            self.logger.log("db_logs", "db.log", "error", f"Error While connecting to the {self.db_name} Database")
            raise error 
        
        finally:
            self.logger.log("db_logs", "db.log", "info", f"closed the db connection to {self.db_name} in testing")