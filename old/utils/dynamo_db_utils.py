import os
import boto3
import uuid
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS Credentials
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME", "metadata")

# Ensure credentials exist
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    raise ValueError("❌ AWS credentials are missing. Check your .env file.")

# Initialize DynamoDB Client with credentials
dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)


def add_metadata_to_dynamo_db(file_id:str, title: str, description: str, timestamps: str, url: str, s3_url: str):
    """
    Inserts metadata into the DynamoDB `metadata` table.

    Parameters:
        title (str): The title of the metadata entry.
        description (str): The description of the entry.
        timestamps (int): The Unix timestamp.
        url (str): The associated URL.

    Returns:
        dict: The response from DynamoDB.
    """
    table = dynamodb.Table('metadata')

    # ✅ Insert Data into DynamoDB
    try:
        response = table.put_item(
            Item={
                "file_id": file_id,
                "title": title,
                "description": description,
                "timestamps": timestamps,
                "url": url,
                "s3_url": s3_url
            }
        )
        print("✅ Item added successfully!")
        print(f"Inserted: {title} ({url}) with ID {file_id}")
        return response
    except Exception as e:
        print(f"❌ Error adding item: {e}")
        return {"error": str(e)}
    

def retrieve_metadata_from_dynamo_db(file_id: str):
    """
    Retrieves metadata from the DynamoDB `metadata` table using the primary key (`file_id`).

    Parameters:
        file_id (str): The unique file identifier.

    Returns:
        dict: The metadata entry if found, or an error message.
    """
    table = dynamodb.Table('metadata')

    try:
        response = table.get_item(Key={"file_id": file_id})
        item = response.get("Item")

        if item:
            print("✅ Metadata retrieved successfully!")
            return item
        else:
            print("❌ No metadata found for the given file_id.")
            return {"error": "Metadata not found."}

    except Exception as e:
        print(f"❌ Error retrieving metadata: {e}")
        return {"error": str(e)}

