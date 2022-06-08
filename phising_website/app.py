from flask import Flask, render_template, request
from file_handler import FileHandler
from training_data_validation import TrainingDataValidator 
from testing_data_validation import TestingDataValidator
from file_operation import FileOperation
from training_data_preprocessing import TrainingDataPreprocessing


app = Flask(__name__, template_folder = "templates") 


@app.route("/", methods = ["GET", "POST"]) 
def training():
    if request.method == 'POST':

        # Getting the file from the client, using a REST API.
        file_obj = request.files['file']
        filename = file_obj.filename
        FileHandler.save_file(file_obj, filename, "training")

        # handles the file operations like creation and deletion of the directory.
        file_operation_obj = FileOperation("training")

        # creation of good data and bad data folder.
        file_operation_obj.directory_creation()

        # validating the raw data from the client.
        training_data_validator = TrainingDataValidator()
        training_data_validator.validate_training_data(filename)

        # preprocessing of the validated client data.
        training_data_preprocessor = TrainingDataPreprocessing(filename)

        # feature selection -> clustering
        training_data_preprocessor.feature_selection()
        training_data_preprocessor.clustering()

        # creating a csvfile out of preprocessed data
        training_data_preprocessor.csv_from_preprocessed_data()

        # adding the data to the db.
        training_data_preprocessor.db_operations()

        # Deleting the files from the trainingdata directory
        file_operation_obj.deletion_of_good_files()

        #moving the bad files to the archive folder
        file_operation_obj.moving_bad_files_to_archive()


    
    return render_template('index.html')

@app.route('/testing', methods = ["POST", "GET"])
def testing():

    if request.method == 'POST':

        # Getting the file from the client, using a REST API.
        file_obj = request.files['file']
        filename = file_obj.filename
        FileHandler.save_file(file_obj, filename, "testing")

        # validating the raw data from the client.
        testing_data_validator = TestingDataValidator()
        testing_data_validator.validate_testing_data(filename)

        file_operation_obj = FileOperation("testing")

        # creation of good data and bad data folder.
        file_operation_obj.directory_creation()
    

       # file_operation_obj.deletion_of_good_files()

        #moving the bad files to the archive folder
        file_operation_obj.moving_bad_files_to_archive()
    
    return render_template('index.html')


if __name__ == "__main__":
    app.run(port = 15000, debug = True)

# Always maintain this Folder name starts with Captial and filename should be in smaller.