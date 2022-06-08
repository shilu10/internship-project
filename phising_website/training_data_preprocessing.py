import pickle
import pandas as pd
from sklearn.cluster import KMeans
from kneed import KneeLocator
from application_logger import logger 
from sklearn.feature_selection import mutual_info_classif, SelectKBest
from db import DataBaseOperations

class TrainingDataPreprocessing: 

    def __init__(self, filename):
        self.filename = filename 
        self.X = None
        self.y = None 
        self.selected_cols = None
        self.cluster_labels = None
        self.logger = logger.Logger()

    def feature_selection(self): 
        self.logger.log("general_logs", "general.log", "info", "Feature Selection of Validated Client Training Data Started!!")
        try: 
            # We could use this for new data for the feature selection.
            # selector = SelectKBest(mutual_info_classif, k=60)
            # selected_features = selector.fit_transform(X, y)

            with open('objects/selected_cols.obj', 'rb') as f: 
                selected_cols = pickle.load(f)
                self.selected_cols = selected_cols
                # reading the file from the db_training_files.
            validated_client_data = pd.read_csv(f'training_files_from_db/{self.filename}')
            y = validated_client_data['phishing']
            validated_client_data = validated_client_data[selected_cols]
            X = pd.DataFrame(validated_client_data)
            
            self.X = X
            self.y = y
            self.logger.log("general_logs", "general.log", "info", "Completed the process of Feature Selection")
            return True

        except Exception as error: 
            self.logger.log("general_logs", "general.log", "error", f"Error While Selecting the features in feature selection {error}")
 
    # clustering using kneelde algorithm(package: KneeLocator)
    def find_k(self, df, increment=0, decrement=0):
        self.logger.log("general_logs", "general.log", "info", "Started finding the optimal k value")
        try: 
            wcss = {}    
            for k in range(1, 21):
                kmeans = KMeans(n_clusters=k, random_state=1)
                kmeans.fit(df)
                wcss[k] = kmeans.inertia_
            kn = KneeLocator(x=list(wcss.keys()), 
            y = list(wcss.values()), 
            curve='convex', 
            direction='decreasing')
            k = kn.knee + increment - decrement
            self.logger.log("general_logs", "general.log", "info", "Completed the finding of optimal k value")
            return k

        except Exception as error: 
            self.logger.log("general_logs", "general.log", "error", f"Error while selecting the k values {error}") 

    # applying a K-Means Clustering.
    def clustering(self): 
        self.logger.log("general_logs", "general.log", "info", "Started the clustering process")
        try: 
            # finding a optimal k values takes a time, so predefined value is 3.
            k = 3
            # k = find_k(X)
            kmeans = KMeans(n_clusters=k, random_state=1).fit(self.X)
            self.cluster_labels = kmeans.labels_
            self.logger.log("general_logs", "general.log", "info", "Completed the clustering process")
            return True

        except Exception as error: 
            self.logger.log("general_logs", "general.log", "error", "Error during the process of clustering") 

    def csv_from_preprocessed_data(self): 
        self.logger.log("general_logs", "general.log", "info", "Creating a csv file from the preprocessed client data")
        try: 
            self.X['cluster_labels'] = self.cluster_labels
            self.X['labels'] = self.y 
            self.X.to_csv("training_data_segregation/good_data/preprocessed_data.csv", index=False)

        except Exception as error: 
            self.logger.log("general_logs", "general.log", "error", "Error while creating a csv file from the preprocessed client data")

    # this method will create the table and insert the value into it and also create a csv file out of it.
    def db_operations(self): 
        self.logger.log("general_logs", "general.log", "info", "Started the db operations for preprocessed client data")
        try: 
            db_opr = DataBaseOperations(self.selected_cols, 'preprocessed_data.csv', "preprocessed_files_from_db/") 
            db_opr.create_table()
            db_opr.insert_values()
            db_opr.create_csv()
            self.logger.log("general_logs", "general.log", "info", "Successfully created the training data from the client data.")
        except Exception as error: 
            self.logger.log("general_logs", "general.log", "error", f"error during db operations for preprocessed client data {error}")