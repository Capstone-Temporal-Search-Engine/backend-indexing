import os
import math
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

def append_to_map(directory, file_id):
    """
    Appends file information to map files in a specified directory.

    Args:
        directory (str): The full directory where index files are stored.
        file_id (str): Name of the file.
    """
    # Define file paths
    map_file_path = os.path.join(directory, "map.txt")

    # Ensure the directory exists
    os.makedirs(directory, exist_ok=True)

    # Append to map_s3_name.txt
    with open(map_file_path, "a") as s3_file:
        s3_file.write(f"{file_id}\n")

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
    file_id = record[:36]
    return file_id

def generate_index(tokenized_files_base_path, index_files_base_path):
    map_file_path = f'{index_files_base_path}/map.txt'
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
            file_id = parse_map_record(record)
            tokenized_file_path = f'{tokenized_files_base_path}/{file_id}_tokenized.txt'

            with open(tokenized_file_path, "r") as tokenized_file:
                for token in tokenized_file:
                    token = token.strip()
                    if not token:
                        continue
                    local_hash_table[token] = local_hash_table.get(token, 0) + 1
                    document_word_count += 1

            for key, value in local_hash_table.items():
                tf = value / document_word_count
                global_hash_table[key].append((tf, map_file_index))

    ht = CustomHashTable(dict_size=len(global_hash_table))

    for key, postings in global_hash_table.items():
        idf = math.log((map_file_index + 1) / (1 + len(postings)))  # Avoid zero division
        for i, (tf, doc_id) in enumerate(postings):
            tf_idf = tf * idf
            postings[i] = (round(tf_idf * SCALE_FACTOR), doc_id)  # Apply scaling after IDF
        ht.insert(key, postings)

    ht.write_to_dict_file(dict_file_path)
    ht.write_to_post_file(post_file_path)
