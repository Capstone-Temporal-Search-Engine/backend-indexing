import os
from concurrent.futures import ThreadPoolExecutor
from utils.s3_utils import *

# Retrieve S3 keys for index files
index_files_s3_keys = retrieve_index_files_s3_keys('index_files')
banned_prefix_s3_keys = "banned/banned_prefix.txt"

current_file_directory = os.path.dirname(os.path.abspath(__file__))
base_directory = os.path.join(current_file_directory)

# Define a function for downloading
def download_file(s3_key):
    local_path = os.path.join(base_directory, s3_key)
    download_from_s3(s3_key, local_path)

# Use ThreadPoolExecutor for concurrent downloads
with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust workers as needed
    executor.map(download_file, index_files_s3_keys)

download_from_s3(banned_prefix_s3_keys, os.path.join(base_directory, "banned/banned_prefix.txt"))

print("All downloads completed!")