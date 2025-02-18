from flask import Flask, request, jsonify
from datetime import datetime
import uuid
import os
import logging
from utils.s3_utils import upload_file
from utils.indexing_utils import append_to_map, save_html_file
from utils.tokenizer import tokenize_html_file

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # Limit upload size to 50MB

@app.errorhandler(Exception)
def handle_global_error(error):
    """
    Handle uncaught exceptions globally.
    """
    logger.error(f"Unhandled error: {str(error)}", exc_info=True)
    return jsonify({"error": "An unexpected error occurred.", "details": str(error)}), 500

@app.route('/upload', methods=['POST'])
def upload_file_endpoint():
    try:
        # Validate required fields
        if 'file' not in request.files or 'timestamp' not in request.form or 'url' not in request.form:
            return jsonify({"error": "'file', 'timestamp', 'url', and 'directory' fields are required"}), 400

        file = request.files['file']
        timestamp = request.form['timestamp']
        url = request.form['url']

        # Verify and transform the timestamp
        try:
            date_obj = datetime.fromtimestamp(float(timestamp))
            month_year = date_obj.strftime('%m-%Y')  
        except (ValueError, OSError) as e:
            logger.warning(f"Invalid timestamp provided: {timestamp}. Error: {str(e)}")
            return jsonify({"error": f"Invalid timestamp: {str(e)}"}), 400

        # Generate a unique filename
        html_file_name = str(uuid.uuid4()) + '.html'
        s3_html_path = f"html_files/{month_year}"
        index_directory = f"index_files/{month_year}"
        local_tokenized_directory = f"tokenized_files/{month_year}"
        local_html_path = f"html_files/{month_year}"

        save_html_file(file, local_html_path, html_file_name)

        # Upload the file to S3
        try:
            upload_file(s3_path=s3_html_path, file_object=file, file_name=html_file_name)
        except Exception as e:
            logger.error(f"Error uploading file to S3: {str(e)}", exc_info=True)
            return jsonify({"error": "Failed to upload file to S3.", "details": str(e)}), 500

        # Append metadata to the map
        try:
            append_to_map(index_directory, html_file_name, url, timestamp)
        except Exception as e:
            logger.error(f"Error appending to map: {str(e)}", exc_info=True)
            return jsonify({"error": "Failed to update metadata.", "details": str(e)}), 500
        
        # Tokenize HTML file
        try:
            tokenize_html_file(os.path.join(local_html_path, html_file_name), local_tokenized_directory)
        except Exception as e:
            logger.error(f"Error tokenizing HTML file: {str(e)}", exc_info=True)
            return jsonify({"error": "Failed to tokenize HTML file.", "details": str(e)}), 500

        return jsonify({"message": f"File uploaded successfully."}), 201
    except Exception as e:
        logger.error(f"Error in file upload endpoint: {str(e)}", exc_info=True)
        return jsonify({"error": "Failed to process upload.", "details": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)

