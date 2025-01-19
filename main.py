from flask import Flask, request, jsonify
from datetime import datetime
import uuid
from utils.s3_utils import list_objects_in_bucket, create_directory, upload_file
from utils.indexing_utils import append_to_map

app = Flask(__name__)

@app.route('/list', methods=['GET'])
def list_objects():
    try:
        objects = list_objects_in_bucket()
        return jsonify({"objects": objects}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/create-directory', methods=['POST'])
def create_directory_endpoint():
    try:
        data = request.json
        directory_name = data.get('directory_name')
        if not directory_name:
            return jsonify({"error": "directory_name is required"}), 400

        create_directory(bucket_name=os.getenv('AWS_BUCKET_NAME'), directory_name=directory_name)
        return jsonify({"message": f"Directory '{directory_name}' created successfully."}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/upload', methods=['POST'])
def upload_file_endpoint():
    try:
        if 'file' not in request.files or 'timestamp' not in request.form or 'url' not in request.form:
            return jsonify({"error": "'file', 'timestamp' and 'url' fields are required"}), 400

        file = request.files['file']
        timestamp = request.form['timestamp']
        url = request.form['url']

        # Verify and transform the timestamp to month-year format
        try:
            date_obj = datetime.fromtimestamp(float(timestamp))
            month_year = date_obj.strftime('%m-%Y')  # Format as MM-YYYY
        except (ValueError, OSError) as e:
            return jsonify({"error": f"Invalid timestamp: {str(e)}"}), 400

        # Generate a unique filename
        file_name = str(uuid.uuid4())

        # Define the S3 path using the month-year
        s3_path = month_year
        indexing_path = month_year

        # Upload the file directly to S3
        upload_file(
            s3_path=s3_path,
            file_object=file,
            file_name=file_name
        )

        try:
            append_to_map(
                indexing_path, 
                file_name, 
                url
            )
            return jsonify({'success': True}), 200
        except Exception as e:
            return jsonify({'error': str(e)}), 500

        return jsonify({"message": f"File uploaded successfully to '{s3_path}/{file_name}'."}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
