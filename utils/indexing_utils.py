import os
from datetime import datetime
from utils.s3_utils import retrieve_object
import logging
import io
from werkzeug.datastructures import FileStorage
from collections import defaultdict
from decimal import Decimal
from CustomHashTable import CustomHashTable 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FILE_SIZE_LENGTH = 10
SCALE_FACTOR=10000

def append_to_map(directory, file_name, url, timestamp):
    """
    Appends file information to map files in a specified directory.

    Args:
        directory (str): The full directory where index files are stored.
        file_name (str): Name of the file.
        url (str): URL of the file.
        timestamp (int): Timestamp when the file was added.
    """
    # Define file paths
    map_s3_name_path = os.path.join(directory, "map_s3_name.txt")
    map_url_path = os.path.join(directory, "map_url_path.txt")

    # Ensure the directory exists
    os.makedirs(directory, exist_ok=True)

    # Calculate the file size of map_url_path (right-aligned, padded with zeros to 10 bytes)
    try:
        file_size = os.path.getsize(map_url_path)
    except FileNotFoundError:
        file_size = 0
    file_size_str = str(file_size).zfill(FILE_SIZE_LENGTH)  # Pad with zeros if less than 10 bytes

    # Append to map_s3_name.txt
    with open(map_s3_name_path, "a") as s3_file:
        s3_file.write(f"{file_name} {timestamp} {file_size_str}\n")

    # Append to map_url_path.txt
    with open(map_url_path, "a") as url_file:
        url_file.write(f"{url}\n")

def retrieve_html_file_in_map(directory):
    """
    Retrieves and processes HTML files listed in map_s3_name.txt.

    Args:
        directory (str): The full directory where index files are stored.
    """
    map_s3_name_path = os.path.join(directory, "map_s3_name.txt")

    if not os.path.exists(map_s3_name_path):
        logger.error(f"File not found: {map_s3_name_path}")
        return

    with open(map_s3_name_path, 'r') as file:
        for line in file:
            # Split the line into UUID and timestamp
            parts = line.strip().split()
            if len(parts) < 2:
                logger.warning(f"Invalid line format: {line}")
                continue

            uuid, timestamp = parts[0], parts[1]

            try:
                # Convert the timestamp to the month-year format
                month_year = datetime.fromtimestamp(int(timestamp)).strftime('%m-%Y')
                s3_path = f"{month_year}/{uuid}"
                
                # Define a local save path
                local_save_path = os.path.join(directory, f"{uuid}.html")
                
                # Retrieve the file from S3
                retrieve_object(s3_path, local_save_path)
            except Exception as e:
                logger.error(f"Error processing line '{line}': {str(e)}")

def retrieve_map_file(directory):
    """
    Retrieves map files from S3 and saves them locally.

    Args:
        directory (str): The full directory where index files are stored.
    """
    try:
        if not directory:
            raise ValueError("'directory' is a required field")

        # Ensure local directory exists
        os.makedirs(directory, exist_ok=True)

        # Define file paths
        map_s3_name_path = os.path.join(directory, "map_s3_name.txt")
        map_url_path = os.path.join(directory, "map_url_path.txt")

        # Retrieve files from S3
        try:
            retrieve_object(os.path.join(directory, "map_s3_name.txt"), map_s3_name_path)
        except Exception as e:
            logger.error(f"Failed to retrieve 'map_s3_name.txt' from S3: {str(e)}")
            return

        try:
            retrieve_object(os.path.join(directory, "map_url_path.txt"), map_url_path)
        except Exception as e:
            logger.error(f"Failed to retrieve 'map_url_path.txt' from S3: {str(e)}")
            return

    except Exception as e:
        logger.error(f"Unhandled error in 'retrieve_map_file': {str(e)}", exc_info=True)


def save_html_file(file_obj, directory, filename):
    os.makedirs(directory, exist_ok=True)
    file_obj.save(os.path.join(directory, filename))


def duplicate_file_object(file_obj):
    # Read the entire content from the original file object
    file_obj.seek(0)
    content = file_obj.read()
    
    # Create a new BytesIO stream with the content
    new_stream = io.BytesIO(content)
    new_stream.seek(0)
    
    # Wrap the new stream in a FileStorage so that it supports .save()
    duplicate = FileStorage(
        stream=new_stream,
        filename=file_obj.filename,
        content_type=file_obj.content_type,
        content_length=len(content)
    )
    return duplicate


def parse_map_record(record):
    uuid = record[:36]
    timestamp = record[42: 52]
    map_location_offset = record[53: ]
    return (uuid, timestamp, map_location_offset)

def generate_dict_record(token, num_docs, posting_offset):
    print("here")

def generate_index(tokenized_files_base_path, index_files_base_path):
    map_file_path = f'{index_files_base_path}/map_s3_name.txt'
    dict_file_path = f'{index_files_base_path}/dict.txt'
    post_file_path = f'{index_files_base_path}/post.txt'
    global_hash_table = defaultdict(list)
    map_file_index = -1
    local_hash_table = {}

    with open(map_file_path, "r") as file:
        while True:
            record = file.readline()  # Read a single record manually
            if not record: break 

            map_file_index += 1
            local_hash_table.clear()
            document_word_count = 0
            uuid, timestamp, map_location_offset = parse_map_record(record)
            tokenized_file_path = f'{tokenized_files_base_path}/{uuid}_tokenized.txt'

            with open(tokenized_file_path, "r") as tokenized_file:
                for token in tokenized_file:
                    token = token.strip()
                    if not token:
                        continue
                    local_hash_table[token] = local_hash_table.get(token, 0) + 1
                    document_word_count += 1

            for key, value in local_hash_table.items():
                scaled_tf = round((value / document_word_count) * SCALE_FACTOR)
                global_hash_table[key].append((scaled_tf, map_file_index))

    ht = CustomHashTable(dict_size=len(global_hash_table))
    for key, postings in global_hash_table.items():
        ht.insert(key, postings)

    ht.write_to_dict_file(dict_file_path)
    ht.write_to_post_file(post_file_path)
