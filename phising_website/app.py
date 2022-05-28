from flask import Flask, render_template, request
from file_handler import File_Handler
from training_data_validation import Training_data_validator 
from testing_data_validation import Testing_data_validator
from file_operation import File_Operation


app = Flask(__name__, template_folder = "templates") 


@app.route("/", methods = ["GET", "POST"]) 
def training() :
    if request.method == 'POST' :

        # Getting the file from the client, using a REST API.
        file_obj = request.files['file']
        filename = file_obj.filename
        print(File_Handler.save_file(file_obj, filename, "training"))

        # handles the file operations like creation and deletion of the directory.
        file_operation_obj = File_Operation("training")

        # creation of good data and bad data folder.
        file_operation_obj.directory_creation()

        # validating the raw data from the client.
        training_data_validator = Training_data_validator()
        training_data_validator.validate_training_data(filename)
    

        file_operation_obj.deletion_of_good_files()

        #moving the bad files to the archive folder
        file_operation_obj.moving_bad_files_to_archive()
    
    return render_template('index.html')

@app.route('/testing', methods = ["POST", "GET"])
def testing() :
    if request.method == 'POST' :

        # Getting the file from the client, using a REST API.
        file_obj = request.files['file']
        filename = file_obj.filename
        File_Handler.save_file(file_obj, filename, "testing")

        # validating the raw data from the client.
        testing_data_validator = Testing_data_validator()
        testing_data_validator.validate_training_data(filename)

        # This should happen only after we successfully store the dataset in the database.

        #deletion of good data file
       # file_operation_obj.deletion_of_good_files()

        #moving the bad files to the archive folder
        #file_operation_obj.moving_bad_files_to_archive()
    
    return render_template('index.html')


if __name__ == "__main__" :
    app.run(port = 15000, debug = True)

# Always maintain this Folder name starts with Captial and filename should be in smaller.