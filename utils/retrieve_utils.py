import mmh3
import os

def generate_index(key, hash_table_size):
    return mmh3.hash(key) % hash_table_size

def parse_dict_record(record):
    """Parse a dictionary record, ensuring proper decoding."""
    fields = record.decode("utf-8").strip().split()
    if len(fields) < 3:
        return None  # Handle empty or malformed records
    term = fields[0]
    num_docs = int(fields[1])
    posting_start_index = int(fields[2])
    return (term, num_docs, posting_start_index)

def retrieve_dict_record(file_path, record_size, term):
    """Retrieve a record from a fixed-size dictionary file using linear probing."""
    if not os.path.exists(file_path): return ("-1", -1, -1)
    file_size = os.path.getsize(file_path)  # Get file size
    num_dict_record = file_size // record_size  # Compute number of records

    index = generate_index(term, num_dict_record)  # Initial hash index
    parsed_record = None

    with open(file_path, "rb") as f:  # Open file in binary mode
        offset = index * record_size  # Calculate byte offset
        f.seek(offset)  # Move to the specific record
        
        while True:
            record = f.read(record_size)  # Read fixed-size record
            if not record: break

            parsed_record = parse_dict_record(record)

            if parsed_record[0] == '-1': break

            stored_term, num_docs, posting_start_index = parsed_record
            if stored_term == term:  # If we found the correct term, return it
                return (stored_term, num_docs, posting_start_index)
            
            # Linear probing: move to the next index (circular wrap-around)
            index = (index + 1) % num_dict_record  
    return parsed_record


def parse_posting_record(record):
    """Parse a posting record, ensuring proper decoding."""
    fields = record.decode("utf-8").strip().split()
    if len(fields) < 2:
        return None  # Handle empty or malformed records
    scaled_tf = fields[0]
    map_file_index = int(fields[1])
    return (scaled_tf, map_file_index)

def retrieve_postings_record(file_path, record_size, start_index, num_records):
    posting_records = []
    with open(file_path, "rb") as f:  # Open file in binary mode
        offset = start_index * record_size  # Calculate byte offset
        f.seek(offset)  # Move to the specific record
        
        for _ in range(num_records):
            record = f.read(record_size)  # Read fixed-size record
            parsed_record = parse_posting_record(record)
            posting_records.append(parsed_record)
    return posting_records


def parse_map_record(record):
    """Parse a posting record, ensuring proper decoding."""
    fields = record.decode("utf-8").strip().split()
    if len(fields) < 3:
        return None  # Handle empty or malformed records
    html_file_name = fields[0]
    timestamp = int(fields[1])
    map_url_path_offset = int(fields[1])
    return (html_file_name, timestamp, map_url_path_offset)

def retrieve_map_record(file_path, record_size, index):
    with open(file_path, "rb") as f:  # Open file in binary mode
        offset = index * record_size  # Calculate byte offset
        f.seek(offset)  # Move to the specific record
        record = f.read(record_size)  # Read fixed-size record
        parsed_record = parse_map_record(record)
        return parsed_record

    

