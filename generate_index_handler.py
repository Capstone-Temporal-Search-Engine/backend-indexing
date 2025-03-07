from utils.s3_utils import list_objects_in_bucket, create_directory, upload_file, retrieve_object
from utils.indexing_utils import generate_index
import sys
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

current_file_directory = os.path.dirname(os.path.abspath(__file__))
base_directory = os.path.join(current_file_directory, "./index_files")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generate_index_handler.py <month_year>")
        sys.exit(1)

    month_year_input = sys.argv[1]

