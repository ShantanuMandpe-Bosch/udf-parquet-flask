from distutils.log import debug
from fileinput import filename
import pandas as pd
from flask import *
import os
import sys
from werkzeug.utils import secure_filename
from pathlib import Path

from UDFDecoder import UDFDecoder 

## try adding a python path to the pyinstaller .spec file 

#UPLOAD_FOLDER = os.path.join('flask_app','staticFiles', 'uploads')
UPLOAD_FOLDER = os.path.join('uploads')

# Define allowed files
ALLOWED_EXTENSIONS = {'csv','bin','parquet','udf'}

app = Flask(__name__)

# Configure upload file path flask
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

app.secret_key = 'This is your secret key to utilize session in Flask'

nameFile = ""

@app.route('/', methods=['GET', 'POST'])
def uploadFile():
	if request.method == 'POST':
	# upload file flask
		f = request.files.get('file')

		# Extracting uploaded file name
		data_filename = secure_filename(f.filename)
		nameFile = f.filename

		f.save(os.path.join(app.config['UPLOAD_FOLDER'],
							data_filename))

		session['uploaded_data_file_path'] = os.path.join(app.config['UPLOAD_FOLDER'],
					data_filename)

		return render_template('index2.html')
	return render_template("index.html")


@app.route('/show_data')
def showData():
	# Uploaded File Path
	data_file_path = session.get('uploaded_data_file_path', None)
	# read csv
	
	udf_decoder = UDFDecoder.UDFDecoder()
	udf_decoder.read_bin_file(data_file_path)
	udf_decoder.write_parquet_file(False)

	parquetFilePath =Path(data_file_path) 
	parquetExtensionPath = parquetFilePath.with_suffix('.parquet')

	uploaded_df = pd.read_parquet(parquetExtensionPath)

	# Converting to html Table
	uploaded_df_html = uploaded_df.to_html()
	return render_template('show_udf_data.html', data_var=uploaded_df_html, path_var = parquetExtensionPath)


if __name__ == '__main__':
	app.run(debug=True)