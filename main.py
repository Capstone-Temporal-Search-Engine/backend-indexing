from flask import Flask, request, jsonify
from flask_cors import CORS
from collections import defaultdict
from datetime import datetime, timedelta
import psycopg2
import uuid
import os
import logging
from flask_bcrypt import Bcrypt
import jwt
from functools import wraps
from utils.s3_utils import upload_file, upload_html_files
from utils.indexing_utils import append_to_map, save_html_file, duplicate_file_object
from utils.tokenizer import tokenize_html_file
from utils.helper import *
from utils.retrieve_utils import *
from dotenv import load_dotenv
from utils.dynamo_db_utils import *
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes and origins
bcrypt = Bcrypt(app)


# PostgreSQL Database Connection
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")
JWT_SECRET = os.getenv("JWT_SECRET")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME")
ADMIN_HASHED_PASSWORD = os.getenv("ADMIN_HASHED_PASSWORD")

ALLOWED_EXTENSIONS = {"pdf", "jpg", "png"}  # Allowed file types
MAX_FILE_SIZE_MB = 10  # Max file size in MB
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024  # Convert MB to byte
mock_user = {
    'username': 'alex',
    'hashed_password': bcrypt.generate_password_hash("123Password").decode('utf-8')  # for demo
}

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

def allowed_file(filename):
    """Check if the file has a valid extension."""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None

        # Get token from Authorization header
        if 'Authorization' in request.headers:
            bearer = request.headers.get('Authorization')
            if bearer.startswith("Bearer "):
                token = bearer.split(" ")[1]

        if not token:
            return jsonify({"error": "Token is missing"}), 401

        try:
            decoded = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
            if decoded.get("role") != "admin":
                return jsonify({"error": "Admin role required"}), 403
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token has expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"error": "Invalid token"}), 401

        return f(*args, **kwargs)
    return decorated

@app.errorhandler(Exception)
def handle_global_error(error):
    """
    Handle uncaught exceptions globally.
    """
    logger.error(f"Unhandled error: {str(error)}", exc_info=True)
    return jsonify({"error": "An unexpected error occurred.", "details": str(error)}), 500


@app.route('/update-request-status', methods=['PUT'])
@admin_required
def update_request():
    """
    API endpoint to update the approval_status and optionally admin_notes of a request using form-data.
    """
    try:
        # Get form-data values
        request_id = request.form.get("request_id")
        new_status = request.form.get("approval_status")
        admin_notes = request.form.get("admin_notes")

        # Validate input data
        if not request_id or not new_status:
            return jsonify({"error": "Both request_id and approval_status are required"}), 400

        # Validate approval_status ENUM values
        valid_statuses = {"pending", "approved", "rejected"}
        if new_status not in valid_statuses:
            return jsonify({"error": f"Invalid approval_status. Allowed values: {valid_statuses}"}), 400

        # Connect to database
        conn = get_db_connection()
        cur = conn.cursor()

        # Check if request_id exists
        cur.execute("SELECT * FROM requests WHERE request_id = %s;", (request_id,))
        existing_request = cur.fetchone()

        if not existing_request:
            cur.close()
            conn.close()
            return jsonify({"error": "Request not found"}), 404

        # Update query
        if admin_notes:
            cur.execute("""
                UPDATE requests 
                SET approval_status = %s,
                    admin_notes = %s
                WHERE request_id = %s
                RETURNING approval_status, admin_notes;
            """, (new_status, admin_notes, request_id))
        else:
            cur.execute("""
                UPDATE requests 
                SET approval_status = %s
                WHERE request_id = %s
                RETURNING approval_status;
            """, (new_status, request_id))

        result = cur.fetchone()
        conn.commit()

        # Close connection
        cur.close()
        conn.close()

        response = {
            "message": "Request updated successfully!",
            "request_id": request_id,
            "new_approval_status": result[0]
        }

        if admin_notes:
            response["admin_notes"] = result[1]

        return jsonify(response), 200

    except psycopg2.Error as e:
        return jsonify({"error": f"Database error: {str(e)}"}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

@app.route('/login', methods=['POST'])
def login():
    username = request.form.get("username")
    password = request.form.get("password")

    # Validate input
    if not username or not password:
        return jsonify({'error': 'Username and password required'}), 400

    # Mock authentication (replace this with DB lookup)
    if username != ADMIN_USERNAME:
        return jsonify({'error': 'Invalid username or password'}), 401

    if not bcrypt.check_password_hash(ADMIN_HASHED_PASSWORD, password):
        return jsonify({'error': 'Invalid username or password'}), 401
    
    token = jwt.encode({
        'username': username,
        'role': 'admin'
    }, JWT_SECRET, algorithm='HS256')

    # Decode token if returned as bytes
    token =  f"Bearer {token if isinstance(token, str) else token.decode('utf-8')}"

    return jsonify({'token': token}), 200


@app.route('/requests-with-documents', methods=['GET'])
def fetch_requests_with_documents():
    """
    API endpoint to fetch all requests joined with documents using NATURAL JOIN.
    Ensures proper column matching and returns structured JSON.
    """
    try:
        # Connect to the database
        conn = get_db_connection()
        cur = conn.cursor()
        base_s3_url = "https://tp-search-s3-bucket.s3.us-east-2.amazonaws.com"

        # Execute NATURAL JOIN query
        query = """
            SELECT * FROM requests NATURAL JOIN documents;
        """
        cur.execute(query)

        # Fetch all results
        results = cur.fetchall()

        # Get column names dynamically
        column_names = [desc[0] for desc in cur.description]

        # Close database connection
        cur.close()
        conn.close()

        # Dynamically map row data to column names
        requests_dict = defaultdict(lambda: {
            "request_id": None,
            "content_type": None,
            "priority": None,
            "content_url": None,
            "description": None,
            "email": None,
            "approval_status": None,
            "created_at": None,
            "documents": []  # Store documents here
        })

        for row in results:
            row_dict = dict(zip(column_names, row))  # Map row values to column names

            request_id = row_dict["request_id"]

            if not requests_dict[request_id]["request_id"]:  # Initialize request data
                requests_dict[request_id].update({
                    "request_id": row_dict["request_id"],
                    "content_type": row_dict["content_type"],
                    "priority": row_dict["priority"],
                    "content_url": row_dict["content_url"],
                    "description": row_dict["description"],
                    "email": row_dict["email"],
                    "approval_status": row_dict["approval_status"],
                    "created_at": row_dict["created_at"]
                })

            # Append document details under the corresponding request
            requests_dict[request_id]["documents"].append({
                "document_id": row_dict["document_id"],
                "document_title": row_dict["document_title"],
                "document_url": base_s3_url + '/' + row_dict["document_url"],
                "document_type": row_dict["document_type"]
            })

        # Convert grouped data to list format
        requests_list = list(requests_dict.values())

        return jsonify(requests_list), 200

    except psycopg2.Error as e:
        return jsonify({"error": f"Database error: {str(e)}"}), 500

@app.route('/create-request', methods=['POST'])
def create_request():
    """
    Handles form-data submissions to create a request entry in the database.
    Ensures files are uploaded successfully before inserting the request.
    """
    try:
        # Required fields
        content_type = request.form.get('content_type')
        priority = request.form.get('priority', 'medium')  # Default to 'medium'
        content_url = request.form.get('content_url')
        email = request.form.get('email')
        description = request.form.get('description', '')
        documents = request.files.getlist('documents')

        # Validate required fields
        if not content_type or not content_url or not email or len(documents) < 1:
            return jsonify({"error": "Missing required fields: content_type, content_url, email, and at least one document."}), 400

        # Validate ENUM values
        valid_content_types = {'website', 'media', 'document', 'social media post', 'other'}
        valid_priorities = {'low', 'medium', 'high'}

        if content_type not in valid_content_types:
            return jsonify({"error": f"Invalid content_type. Allowed: {valid_content_types}"}), 400
        if priority not in valid_priorities:
            return jsonify({"error": f"Invalid priority. Allowed: {valid_priorities}"}), 400
        
        for doc in documents:
            if doc.filename:  # Ensure filename is not empty
                # Validate file type
                if not allowed_file(doc.filename):
                    return jsonify({"error": f"Invalid file type: {doc.filename}"}), 400
                
                # Validate file size
                doc.seek(0, os.SEEK_END)  # Move to end of file to get size
                file_size = doc.tell()
                doc.seek(0)  # Reset file pointer to start

                if file_size > MAX_FILE_SIZE_BYTES:
                    return jsonify({"error": f"File {doc.filename} exceeds the 10MB size limit."}), 400
                
        uploaded_files = []
        for doc in documents:
            if doc.filename:  # Ensure filename is not empty
                # Generate unique filename
                document_id = str(uuid.uuid4())
                file_extension = doc.filename.rsplit(".", 1)[1].lower()
                filename = f"{document_id}.{file_extension}"
                s3_base_path = "documents"

                # Attempt to upload file
                document_url = upload_file(s3_base_path, doc, filename)
                if not document_url:
                    return jsonify({"error": f"File upload failed for {doc.filename}"}), 500  # Stop execution if upload fails

                # Extract title (filename without extension)
                document_title = os.path.splitext(doc.filename)[0] or "undefined"

                # Store file details for later insertion
                uploaded_files.append({
                    "document_id": document_id,
                    "document_title": document_title,
                    "document_url": document_url,
                    "document_type": doc.filename.rsplit(".", 1)[1].lower()
                })

        # Ensure at least one file was successfully uploaded
        if not uploaded_files:
            return jsonify({"error": "No files were successfully uploaded."}), 500

        # Connect to database
        conn = get_db_connection()
        cur = conn.cursor()

        # Insert into requests table (approval_status defaults to 'pending')
        cur.execute("""
            INSERT INTO requests (content_type, priority, content_url, description, email)
            VALUES (%s, %s, %s, %s, %s) RETURNING request_id;
        """, (content_type, priority, content_url, description, email))

        request_id = cur.fetchone()[0]  # Retrieve the UUID of the newly inserted record

        # Insert documents after successful request creation
        for file in uploaded_files:
            cur.execute("""
                INSERT INTO documents (document_id, request_id, document_title, document_url, document_type)
                VALUES (%s, %s, %s, %s, %s);
            """, (file["document_id"], request_id, file["document_title"], file["document_url"], file["document_type"]))

        conn.commit()

        # Close connection
        cur.close()
        conn.close()

        # Return success response
        return jsonify({
            "message": "Request created successfully!",
            "request_id": str(request_id),
            "content_type": content_type,
            "priority": priority,
            "content_url": content_url,
            "description": description,
            "email": email,
            "approval_status": "pending",  # Always defaults to pending
            "uploaded_documents": uploaded_files
        }), 201

    except psycopg2.Error as e:
        return jsonify({"error": f"Database error: {str(e)}"}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500


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

        # Generate a unique 
        file_id = str(uuid.uuid4())
        html_file_name =  file_id + '.html'
        s3_html_base_path = f"html_files/{month_year}"
        index_directory = f"index_files/{month_year}"
        local_tokenized_base_directory = f"tokenized_files/{month_year}"
        local_html_path = f"html_files/{month_year}"
        s3_full_path = f"{s3_html_base_path}/{html_file_name}"

        # save file locally 
        try:
            save_html_file(duplicate_file_object(file), local_html_path, html_file_name)
        except Exception as e:
            logger.error(f"Error saving file to disk: {str(e)}", exc_info=True)
            return jsonify({"error": "Failed to save file to disk.", "details": str(e)}), 500


        # Upload the file to S3
        try:
            upload_html_files(s3_path=s3_html_base_path, file_object=duplicate_file_object(file), file_name=html_file_name)
        except Exception as e:
            logger.error(f"Error uploading file to S3: {str(e)}", exc_info=True)
            return jsonify({"error": "Failed to upload file to S3.", "details": str(e)}), 500

        # Tokenize HTML file
        try:
            tokenize_html_file(duplicate_file_object(file), file_id, local_tokenized_base_directory)
        except Exception as e:
            logger.error(f"Error tokenizing HTML file: {str(e)}", exc_info=True)
            return jsonify({"error": "Failed to tokenize HTML file.", "details": str(e)}), 500
            # Append metadata to the map
        try:
            append_to_map(index_directory, file_id)
        except Exception as e:
            logger.error(f"Error appending to map: {str(e)}", exc_info=True)
            return jsonify({"error": "Failed to update metadata.", "details": str(e)}), 500
        
         # Store metadata in dynamo_db
        try:
            title, description = extract_title_description_from_html(duplicate_file_object(file))
            print(title, description)
            add_metadata_to_dynamo_db(file_id, title, description, timestamp, url, s3_full_path)
        except Exception as e:
            return jsonify({"error": "Failed to store metadata.", "details": str(e)}), 500


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

    months = get_months_between(start_time, end_time)
    tokens = query_term.split()
    index_files_base_path = os.path.abspath('index_files')
    acc = {}
    for month in months:
        dict_file_path =  f'{index_files_base_path}/{month}/dict.txt'
        post_file_path =  f'{index_files_base_path}/{month}/post.txt'
        map_file_path =  f'{index_files_base_path}/{month}/map.txt'
        for token in tokens:
            result_term, num_docs, posting_start_idx = retrieve_dict_record(dict_file_path, 65, token)
            if result_term == '-1': continue
            postings = retrieve_postings_record(post_file_path, 20, posting_start_idx, num_docs)
            for posting in postings:
                map_record = retrieve_map_record(map_file_path, 37, posting[1])
                file_id = map_record
                acc[file_id] = acc.get(file_id, 0) + int(posting[0])

    for file_id, tf_idf in acc.items():
        metadata = retrieve_metadata_from_dynamo_db(file_id)
        acc[file_id] = metadata
        acc[file_id]["tf_idf"] = tf_idf
    
    results["data"] = acc

    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)

