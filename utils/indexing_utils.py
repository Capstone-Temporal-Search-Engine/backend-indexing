import os

# Define the base directory relative to the current script
current_file_directory = os.path.dirname(os.path.abspath(__file__))
base_directory = os.path.join(current_file_directory, "../index_files")
FILE_SIZE_LENGTH = 10

def append_to_map(indexing_path, file_name, url):
    # Define the directory and file paths
    directory = os.path.join(base_directory, indexing_path)
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
        s3_file.write(f"{file_name} {file_size_str}\n")  # Add a newline for better readability

    # Append to map_url_path.txt
    with open(map_url_path, "a") as url_file:
        url_file.write(f"{url}\n")  # Add a newline for better readability