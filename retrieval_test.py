import os
from utils.retrieve_utils import *
tokenized_files_base_path = os.path.abspath('tokenized_files/03-2027')
index_files_base_path = os.path.abspath('index_files/03-2027')

dict_file_path =  f'{index_files_base_path}/dict.txt'
post_file_path =  f'{index_files_base_path}/post.txt'
map_file_path =  f'{index_files_base_path}/map_s3_name.txt'

term, num_docs, posting_start_idx = retrieve_dict_record(dict_file_path, 65, 'ate')

postings = retrieve_postings_record(post_file_path, 20, posting_start_idx, num_docs)
print(term, num_docs, posting_start_idx)
for posting in postings:
    record = retrieve_map_record(map_file_path, 64, posting[1])
    print(posting[0], record[0])
# add back in weird message to test github  