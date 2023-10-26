# Display and Conversion of .udf files 

## Use .exe application
- Go to the directory ```flask_app\dist\__init__.exe```
- Run the application and go to the server wherein the website is hosted. This link can be viewed in the command line 
- The uploaded .bin/.udf file as well as the downloaded .parquet files can be found in the following path : ```flask_app\dist\uploads```

## Install and Run 
- Go to the project directory 
- Open/Create a virtual environment 
- Run the command ``` pip install -e . ```
- All the packages necessary for the site to run will be installed 
- Make sure you are in the top level of the file directory and run the command ``` flask --app flask_app run --debug ```
- You can find a link to the server where the site is hosted in the command line