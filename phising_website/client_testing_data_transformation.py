from application_logger import logger 
import pandas as pd 

class DataTransformation:
    def __init__(self ,filename):
        self.logger = logger.Logger()
        self.path = f'testing_data_segregation/good_data/{filename}'

    def change_hypen(self):
        """
        This method is used to change the column name if it contains hypen to normal 
        column name.
        """
        try:
            self.logger.log("general_logs", "general.log", "info", f"Started the data transformation for the Client Data...")
            client_data = pd.read_csv(self.path)
            columns = client_data.columns

            for column in columns:
                new_colname = column.replace('-', '')
                client_data.rename({column : new_colname}, inplace = True)

            self.logger.log("general_logs", "general.log", "info", f"Successfully completed the data transformation for the Client Data...")
        
        except Exception as error:
            self.logger.log("general_logs", "general.log", "error", f"Probelm occured during the  data transformation for the Client Data...")
