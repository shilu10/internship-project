from application_logger import logger 
import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt 
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import GridSearchCV
import pickle

class ModelBuilding: 

    def __init__(self, training_filepath) -> None:
        self.training_filepath = training_filepath
        self.logger = logger.Logger()
        self.results = {}
        self.cluster_dic = {}
        self.best_models = {}

    def prepare_data_from_clusters(self) -> None: 
        try:
            self.logger.log("training_logs", "train.log", "info", "preparation of data for each clusters is started!!!")
            main_df = pd.read_csv("preprocessed_files_from_db/preprocessed_data.csv")
            clusters = main_df.cluster_labels.unique()
            
            for cluster in clusters: 
                self.cluster_dic["df_"+str(cluster)] = pd.DataFrame(main_df[main_df.cluster_labels==cluster])
            self.cluster_dic["df_0"].drop("cluster_labels", inplace=True, axis=1)
            self.cluster_dic["df_1"].drop("cluster_labels", inplace=True, axis=1)
            self.cluster_dic["df_2"].drop("cluster_labels", inplace=True, axis=1)
        
        except Exception as error:
            self.logger.log("training_logs", "train.log", "error", f"Error while preparation of data for each {error}")

        else:
            self.logger.log("training_logs", "train.log", "info", "Successfully created data for each clusters!")


    def hyperparameter_tuning(self) -> None:
        try: 
            self.logger.log("training_logs", "train.log", "info", "HyperParameter Tuning for each clusters data is started")
            with open("objects/selected_cols.obj", 'rb') as f: 
                selected_cols = pickle.load(f)
            models_params_dict = {
                    "rf_classifier": {
                        "object": RandomForestClassifier(), 
                        "params": {
                            "criterion": ["gini", "entropy"],
                            "n_estimators": [100, 150, 200],
                            "max_depth": [20, 40, 60], 
                            "oob_score": [True],
                            "random_state": [32],
                        }
                    }, 
                    "gb_classifier": {
                        "object": GradientBoostingClassifier(), 
                        "params": {
                            "loss": ["exponential"],
                            "n_estimators": [100, 150, 200],
                            "criterion": ["friedman_mse", "squared_error"], 
                            "max_depth": [3, 6, 9], 
                            "random_state": [32], 
                        }
                    }
                }
            for cluster_no, cluster_df in self.cluster_dic.items(): 
                for classifier in models_params_dict: 
                    grid_search_cv = GridSearchCV(models_params_dict[classifier].get("object"), models_params_dict[classifier].get("params"), n_jobs=15)
                    grid_search_cv.fit(cluster_df[(selected_cols)], cluster_df.labels)
                    if not self.results.get(cluster_no): 
                        self.results[cluster_no] = {}
                    
                    if not self.results.get(cluster_no).get(classifier):  
                        self.result[cluster_no][classifier] = {}
                        
                    self.results[cluster_no][classifier]["best_params"] = grid_search_cv.best_params_
                    self.results[cluster_no][classifier]["best_score"] = grid_search_cv.best_score_
        
        except Exception as error: 
            self.logger.log("training_logs", "train.log", "error", f"Error during the hyperparameter tuning {error}")

        else:
            self.logger.log("training_logs", "train.log", "info", "Successfully compeleted the hyperparameter tuning")
    
    def select_best_model(self): 
        try:
            self.logger.log("training_logs", "train.log", "info", "Started the selection of best models for each clusters from models!!")
            for cluster_no, models in self.results.items():
                max_accuracy = 0
                for model in models: 
                    if models[model].get("best_score") >= max_accuracy: 
                        max_accuracy = models[model].get("best_score")
                        if not self.best_models.get(cluster_no): 
                            self.best_models[cluster_no] = {}
                        self.best_models[cluster_no]["model_name"] = model
                        self.best_models[cluster_no]["params"] = models[model].get("best_params")
        
        except Exception as error:
            self.logger.log("training_logs", "train.log", "error", f"Error during the selection of best model from models {error}")

        else:
            self.logger.log("training_logs", "train.log", "info", "Successfully selected the best model for each clusters")

    def train(self): 
        try:
            self.logger.log("training_logs", "train.log", "info", "Model Building of Client Training Data Started!!")
        
        except Exception as error: 
            pass 

        else:
            pass