from utils.indexing_utils import generate_index
from utils.s3_utils import upload_file
import os

months = input("Enter in the format MM-YYYY: ")

tokenized_files_base_path = os.path.abspath(f'tokenized_files/{months}')
index_files_base_path = os.path.abspath(f'index_files/{months}')
generate_index(tokenized_files_base_path, index_files_base_path)

# Open the file in read mode ('r')
dict_file = open(f'{index_files_base_path}/dict.txt', 'rb')
post_file = open(f'{index_files_base_path}/post.txt', 'rb')
map_s3_name = open(f'{index_files_base_path}/map_s3_name.txt', 'rb')
map_url_path = open(f'{index_files_base_path}/map_url_path.txt', 'rb')

# (s3_path, file_object, file_name)
upload_file(f'index_files/{months}', dict_file, "dict.txt")
upload_file(f'index_files/{months}', post_file, "post.txt")
upload_file(f'index_files/{months}', map_s3_name, "map_s3_name.txt")
upload_file(f'index_files/{months}', map_url_path, "map_url_path.txt")



# Close files after upload
dict_file.close()
post_file.close()