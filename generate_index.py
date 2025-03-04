from utils.indexing_utils import generate_index
import os


tokenized_files_base_path = os.path.abspath('tokenized_files/02-2027')
index_files_base_path = os.path.abspath('index_files/02-2027')

generate_index(tokenized_files_base_path, index_files_base_path)

