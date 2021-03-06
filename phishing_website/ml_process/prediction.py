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
        self.filepath = f"files_from_db/testing/{self.filename}"
        path = Path('prediction_files/')
        path.mkdir(parents=True, exist_ok=True)
        
    def run(self): 
        try: 
            self.logger.log("ml_process_logs", "testing.log", "info", "Started the prediction!!")
            with open("artifacts/cluster0_model.obj", 'rb') as f: 
                cluster0_model = pickle.load(f)
            with open("artifacts/cluster1_model.obj", 'rb') as f: 
                cluster1_model = pickle.load(f)
            with open("artifacts/cluster2_model.obj", 'rb') as f: 
                cluster2_model = pickle.load(f)
            with open("artifacts/kmeans_model.obj", 'rb') as f: 
                kmeans_predictor = pickle.load(f)
            with open("artifacts/selected_cols.obj", 'rb') as f: 
                selected_columns = pickle.load(f)
            with open("artifacts/standard_scaler.obj", 'rb') as f: 
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
            prediction_file = f"{self.filename.split('.')[0]}_prediction.csv"
            pred_df.to_csv(prediction_file, index=True)
            
            try: 
                if exists(prediction_file): 
                    shutil.move(prediction_file, "prediction_files")
            except Exception as error: 
                print(error, "in moving files")
            self.logger.log("ml_process_logs", "testing.log", "info", "Completed the prediction of the client data and also moved the csv file to prediction_files/ directory")
            return prediction_file

        except Exception as error:
            self.logger.log("ml_process_logs", "testing.log", "error", f"Error while prediction {error}")
             
