import sqlite3
from pathlib import Path
from Application_logger import logger 

# we will have different database file for each csv client files.
class DBConnection :
    def __init__(self, db_name) :
        self.db_name = db_name.split('.')[0] + ".db"
        self.path = Path("Training_DB/")
        self.path.mkdir(parents = True, exist_ok = True)
        self.logger = logger.Logger()
    
    def connect(self) :

        try :
            self.logger.log("db_logs", "db.log", "info", f"Started the db connection to {self.db_name}")
            self.connection = sqlite3.connect(Path(f"Training_DB/{self.db_name}"))
            return self.connection, self.db_name 

        except ConnectionError as error :
            self.logger.log("db_logs", "db.log", "error", f"Error While connecting to the {self.db_name} Database")
            raise error 

class DataBaseOperations :
    
    def __init__(self, connection, column_names, db_name) :
        self.column_names = column_names
        self.connection = connection 
        self.db_name = db_name
        self.logger = logger.Logger()

    def create_table(self) :
        cursor = self.connection.cursor()
        table_name = "training"

        try :
            query = f"SELECT count(name)  FROM sqlite_master WHERE type = 'table'AND name = '{table_name}'"
            cursor.execute(query)
            self.connection.commit()

            res = cursor.fetchone()[0]
            print(res == 1 )
            if res == 1 :
                print("yes")
                self.logger.log("db_logs", "db.log", "info", f"Table named {table_name} created successfully in {self.db_name} Database")
            
            else :
                for col_name in self.column_names.keys() : 
                    col_type = self.column_names[col_name]                
                    try :
                        
                        query = f'ALTER TABLE {table_name} ADD COLUMN "{col_name}" {col_type}'
                        cursor.execute(query)
                        self.connection.commit()
                        print("this is failuer")

                    except :
                        query = f"CREATE TABLE {table_name} ({col_name} {col_type})"
                        cursor.execute(query)
                        self.connection.commit()
                        print("this is success")
                        
        except Exception as error :
            print(error)
            self.logger.log("db_logs", "db.log", "error", f"Error while creating {table_name} in {self.db_name} Database")

        else :
            self.logger.log("db_logs", "db.log", "info", f"Table named {table_name} created successfully in {self.db_name}")

        finally :
            self.connection.close()
            self.logger.log("db_logs", "db.log", "info", f"Closed the db connection to {self.db_name}")


        


