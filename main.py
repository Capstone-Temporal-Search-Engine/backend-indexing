from flask import Flask, request, jsonify
from datetime import datetime
import uuid
import os
import logging
from utils.s3_utils import upload_file
from utils.indexing_utils import append_to_map, save_html_file, duplicate_file_object
from utils.tokenizer import tokenize_html_file
from utils.helper import *
from utils.retrieve_utils import *

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


        # Upload the file to S3
        try:
            save_html_file(duplicate_file_object(file), local_html_path, html_file_name)
        except Exception as e:
            logger.error(f"Error saving file to disk: {str(e)}", exc_info=True)
            return jsonify({"error": "Failed to save file to disk.", "details": str(e)}), 500


        # Upload the file to S3
        try:
            upload_file(s3_path=s3_html_path, file_object=duplicate_file_object(file), file_name=html_file_name)
        except Exception as e:
            logger.error(f"Error uploading file to S3: {str(e)}", exc_info=True)
            return jsonify({"error": "Failed to upload file to S3.", "details": str(e)}), 500

        # Tokenize HTML file
        try:
            tokenize_html_file(os.path.join(local_html_path, html_file_name), local_tokenized_directory)
        except Exception as e:
            logger.error(f"Error tokenizing HTML file: {str(e)}", exc_info=True)
            return jsonify({"error": "Failed to tokenize HTML file.", "details": str(e)}), 500
        
        # Append metadata to the map
        try:
            append_to_map(index_directory, html_file_name, url, timestamp)
        except Exception as e:
            logger.error(f"Error appending to map: {str(e)}", exc_info=True)
            return jsonify({"error": "Failed to update metadata.", "details": str(e)}), 500

        return jsonify({"message": f"File uploaded successfully."}), 201
    except Exception as e:
        logger.error(f"Error in file upload endpoint: {str(e)}", exc_info=True)
        return jsonify({"error": "Failed to process upload.", "details": str(e)}), 500

@app.route('/retrieve', methods=['POST'])
def retrieve():
    # Get form data
    start_time = request.form.get('start_time')
    end_time = request.form.get('end_time')
    query_term = request.form.get('query_term')
    
    # Validate input
    if not start_time or not end_time or not query_term:
        return jsonify({'error': 'Missing required parameters'}), 400
    
    try:
        start_time = int(start_time)
        end_time = int(end_time)
    except ValueError:
        return jsonify({'error': 'Invalid timestamp format'}), 400
    
    if start_time >= end_time:
        return jsonify({'error': 'start_time must be less than end_time'}), 400
    
    results = {
        'start_time': start_time,
        'end_time': end_time,
        'query_term': query_term
    }

    base_s3_url = "https://tp-search-s3-bucket.s3.us-east-2.amazonaws.com/html_files"
    months = get_months_between(start_time, end_time)
    tokens = query_term.split()
    index_files_base_path = os.path.abspath('index_files')
    acc = []
    for month in months:
        dict_file_path =  f'{index_files_base_path}/{month}/dict.txt'
        post_file_path =  f'{index_files_base_path}/{month}/post.txt'
        map_file_path =  f'{index_files_base_path}/{month}/map_s3_name.txt'
        print(month)
        for token in tokens:
            result_term, num_docs, posting_start_idx = retrieve_dict_record(dict_file_path, 65, token)
            if result_term == '-1': continue
            postings = retrieve_postings_record(post_file_path, 20, posting_start_idx, num_docs)
            for posting in postings:
                map_record = retrieve_map_record(map_file_path, 64, posting[1])
                acc.append([posting[0], f'{base_s3_url}/{month}/{map_record[0]}'])

    results["results"] = acc
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)

