from dotenv import load_dotenv
import os
import boto3
import logging
import botocore.exceptions


# Load the environment variables from the .env file
load_dotenv()

# Access the environment variables
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')
AWS_REGION = os.getenv('AWS_REGION')

# Initialize the S3 resource
s3 = boto3.resource(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Function to list objects in the bucket
def list_objects_in_bucket():
    my_bucket = s3.Bucket(AWS_BUCKET_NAME)
    return [obj.key for obj in my_bucket.objects.all()]

def get_object():
    my_bucket = s3.Bucket(AWS_BUCKET_NAME)
    return [obj.key for obj in my_bucket.objects.all()]

# Function to create a directory
def create_directory(directory_name):
    s3.Object(AWS_BUCKET_NAME, directory_name).put()

# Function to upload a file
def upload_file(s3_path, file_object, file_name):
    s3_key = f"{s3_path.rstrip('/')}/{file_name}"
    try:
        s3.Bucket(AWS_BUCKET_NAME).put_object(Key=s3_key, Body=file_object)
        logging.info(f"Successfully uploaded {file_name} to {s3_key}")
        return s3_key
    except botocore.exceptions.ClientError as error:
        logging.error(f"ClientError while uploading {file_name} to S3: {error}")
    except Exception as error:
        logging.error(f"An error occurred while uploading {file_name}: {error}")
    return False

def upload_html_files(s3_path, file_object, file_name):
    """Uploads an HTML file with the correct metadata."""
    s3_key = f"{s3_path.rstrip('/')}/{file_name}"
    try:
        s3.Bucket(AWS_BUCKET_NAME).put_object(
            Key=s3_key,
            Body=file_object,
            ContentType="text/html"  # Ensure the correct Content-Type for HTML files
        )
        logging.info(f"Successfully uploaded {file_name} with Content-Type 'text/html' to {s3_key}")
        return True
    except botocore.exceptions.ClientError as error:
        logging.error(f"ClientError while uploading {file_name} to S3: {error}")
    except Exception as error:
        logging.error(f"An error occurred while uploading {file_name}: {error}")
    return False

def retrieve_object(s3_path, download_path=None):
    try:
        # Get the object from S3
        obj = s3.Object(AWS_BUCKET_NAME, s3_path)
        response = obj.get()

        if download_path:
            # Save the object to the local file system
            with open(download_path, 'wb') as file:
                file.write(response['Body'].read())
            return download_path
        else:
            # Return the object content
            return response['Body'].read()
    except Exception as e:
        print(f"Error retrieving object from S3: {str(e)}")
        return None
    
# Function to get all folder names in the directory /index_files
def retrieve_index_files_s3_keys(prefix='index_files'):
    try:
        my_bucket = s3.Bucket(AWS_BUCKET_NAME)
        files = []

        for bucket in s3.buckets.all():
            for obj in bucket.objects.filter(Prefix=prefix):
                files.append(obj.key)

        return files

    except Exception as e:
        print(f"Error retrieving folder names: {str(e)}")
        return None
    
def download_from_s3(s3_key, local_file_path):
    """
    Downloads a file from an S3 bucket.

    :param bucket_name: str, The name of the S3 bucket.
    :param s3_key: str, The key (path) of the file in the S3 bucket.
    :param local_file_path: str, The local file path to save the downloaded file.
    """
    try:
        local_dir = os.path.dirname(local_file_path)
        if local_dir and not os.path.exists(local_dir):
            os.makedirs(local_dir, exist_ok=True)

        # Initialize an S3 client
        s3_client = boto3.client('s3')

        # Download the file from S3
        s3_client.download_file(AWS_BUCKET_NAME, s3_key, local_file_path)
        
        print(f"File downloaded successfully to {local_file_path}")

    except Exception as e:
        print(f"Error downloading file: {e}")

# Funct