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