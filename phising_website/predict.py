from application_logger import logger
from os.path import exists
import shutil
import pandas as pd 
import pickle 
import numpy as np
from pathlib import Path


class Predict: 
    """
        If the file for the prediction has been upto the schema, then it will in the 
        testing_data_segregation/good_data/ directory. if only we need to do the prediction.
    """
    def __init__(self, filename): 
        self.logger = logger.Logger()
        self.filename = filename 
        self.filepath = f"testing_files_from_db/{self.filename}"
        path = Path('prediction_files/')
        path.mkdir(parents=True, exist_ok=True)
        
    def run(self): 
        try: 
            self.logger.log("testing_logs", "testing.log", "info", "Started the prediction!!")
            with open("objects/cluster0_model.obj", 'rb') as f: 
                cluster0_model = pickle.load(f)
            with open("objects/cluster1_model.obj", 'rb') as f: 
                cluster1_model = pickle.load(f)
            with open("objects/cluster2_model.obj", 'rb') as f: 
                cluster2_model = pickle.load(f)
            with open("objects/kmeans_model.obj", 'rb') as f: 
                kmeans_predictor = pickle.load(f)
            with open("objects/selected_cols.obj", 'rb') as f: 
                selected_columns = pickle.load(f)
            with open("objects/standard_scaler.obj", 'rb') as f: 
                standard_scaler = pickle.load(f)

            predict_data = pd.read_csv(self.filepath)
            predict_data = predict_data[selected_columns]
            stand_data = standard_scaler.transform(predict_data)
            stand_predict_data = pd.DataFrame(data=stand_data, columns=selected_columns)
            cluster_nos = kmeans_predictor.predict(stand_predict_data)

            cluster0 = np.where(cluster_nos==0)
            cluster1 = np.where(cluster_nos==1)
            cluster2 = np.where(cluster_nos==2)

            cluster0_data = stand_predict_data.iloc[cluster0]
            cluster1_data = stand_predict_data.iloc[cluster1]
            cluster2_data = stand_predict_data.iloc[cluster2]

            cluster0_pred = cluster0_model.predict(cluster0_data)
            cluster1_pred = cluster1_model.predict(cluster1_data)
            cluster2_pred = cluster2_model.predict(cluster2_data)

            cluster0_pred_df = pd.DataFrame(data=cluster0_pred, columns=['prediction'], index=cluster0_data.index)
            cluster1_pred_df = pd.DataFrame(data=cluster1_pred, columns=['prediction'], index=cluster1_data.index)
            cluster2_pred_df = pd.DataFrame(data=cluster2_pred, columns=['prediction'], index=cluster2_data.index)                       

            pred_df = pd.concat([cluster0_pred_df, cluster1_pred_df, cluster2_pred_df], axis=0)
            pred_df = pred_df.sort_index()
            pred_df.to_csv(f"{self.filename.split('.')[0]}_prediction.csv", index=True)
            
            if not exists(f"prediction_files/{self.filename.split('.')[0]}_prediction.csv"): 
                shutil.move(f"{self.filename.split('.')[0]}_prediction.csv", "prediction_files")
            self.logger.log("testing_logs", "testing.log", "info", "Completed the prediction of the client data and also moved the csv file to prediction_files/ directory")
            return True

        except Exception as error:
            self.logger.log("testing_logs", "testing.log", "error", f"Error while prediction {error}")
             
