from flask import Flask, request, jsonify
from datetime import datetime
import uuid
import os
import logging
from utils.s3_utils import list_objects_in_bucket, create_directory, upload_file
from utils.indexing_utils import append_to_map

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.errorhandler(Exception)
def handle_global_error(error):
    """
    Handle uncaught exceptions globally.
    """
    logger.error(f"Unhandled error: {str(error)}", exc_info=True)
    return jsonify({"error": "An unexpected error occurred.", "details": str(error)}), 500

@app.route('/list', methods=['GET'])
def list_objects():
    try:
        objects = list_objects_in_bucket()
        return jsonify({"objects": objects}), 200
    except Exception as e:
        logger.error(f"Error listing objects: {str(e)}", exc_info=True)
        return jsonify({"error": "Failed to list objects.", "details": str(e)}), 500

@app.route('/create-directory', methods=['POST'])
def create_directory_endpoint():
    try:
        data = request.json
        directory_name = data.get('directory_name')
        if not directory_name:
            return jsonify({"error": "directory_name is required"}), 400

        bucket_name = os.getenv('AWS_BUCKET_NAME')
        if not bucket_name:
            logger.error("AWS_BUCKET_NAME environment variable is not set.")
            return jsonify({"error": "Server misconfiguration: AWS_BUCKET_NAME is missing."}), 500

        create_directory(bucket_name=bucket_name, directory_name=directory_name)
        return jsonify({"message": f"Directory '{directory_name}' created successfully."}), 201
    except Exception as e:
        logger.error(f"Error creating directory: {str(e)}", exc_info=True)
        return jsonify({"error": "Failed to create directory.", "details": str(e)}), 500

@app.route('/upload', methods=['POST'])
def upload_file_endpoint():
    try:
        # Validate required fields
        if 'file' not in request.files or 'timestamp' not in request.form or 'url' not in request.form:
            return jsonify({"error": "'file', 'timestamp', and 'url' fields are required"}), 400

        file = request.files['file']
        timestamp = request.form['timestamp']
        url = request.form['url']

        # Verify and transform the timestamp to month-year format
        try:
            date_obj = datetime.fromtimestamp(float(timestamp))
            month_year = date_obj.strftime('%m-%Y')  # Format as MM-YYYY
        except (ValueError, OSError) as e:
            logger.warning(f"Invalid timestamp provided: {timestamp}. Error: {str(e)}")
            return jsonify({"error": f"Invalid timestamp: {str(e)}"}), 400

        # Generate a unique filename
        file_name = str(uuid.uuid4())

        # Define the S3 path using the month-year
        s3_path = month_year
        indexing_path = month_year

        # Upload the file to S3
        try:
            upload_file(s3_path=s3_path, file_object=file, file_name=file_name)
        except Exception as e:
            logger.error(f"Error uploading file to S3: {str(e)}", exc_info=True)
            return jsonify({"error": "Failed to upload file to S3.", "details": str(e)}), 500

        # Append metadata to the map
        try:
            append_to_map(indexing_path, file_name, url)
        except Exception as e:
            logger.error(f"Error appending to map: {str(e)}", exc_info=True)
            return jsonify({"error": "Failed to update metadata.", "details": str(e)}), 500

        return jsonify({"message": f"File uploaded successfully to '{s3_path}/{file_name}'."}), 201
    except Exception as e:
        logger.error(f"Error in file upload endpoint: {str(e)}", exc_info=True)
        return jsonify({"error": "Failed to process upload.", "details": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)