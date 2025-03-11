import json
import spacy
import os
from spacy.lang.en.stop_words import STOP_WORDS
from bs4 import BeautifulSoup
import base64

# Load spaCy model
nlp = spacy.load("en_core_web_sm")

def lambda_handler(event, context):
    """
    AWS Lambda function that:
    1. Accepts an uploaded HTML file (multipart/form-data).
    2. Extracts and tokenizes the text (removing stopwords & punctuation).
    3. Returns a downloadable text file.

    Expected API Gateway Event (Base64-encoded HTML file in body):
    {
        "headers": { "Content-Type": "multipart/form-data" },
        "body": "BASE64_ENCODED_HTML_CONTENT",
        "isBase64Encoded": true
    }
    """
    
    try:
        # Ensure body is base64 encoded
        if not event.get("isBase64Encoded", False):
            return {"statusCode": 400, "body": "Invalid request. Expected base64-encoded file."}

        # Decode the HTML file
        html_content = base64.b64decode(event["body"]).decode("utf-8")

        # Parse HTML using BeautifulSoup
        soup = BeautifulSoup(html_content, "html.parser")

        # Remove unwanted tags (script, style, nav, button, footer, header)
        for tag in soup(["script", "style", "nav", "button", "footer", "header"]):
            tag.extract()

        # Extract cleaned text
        text = soup.get_text(separator=" ")

        # Process with spaCy
        doc = nlp(text)

        # Extract words and numbers (excluding punctuation and stopwords)
        filtered_tokens = [
            token.text.lower()
            for token in doc
            if (token.is_alpha or token.like_num) and token.text.lower() not in STOP_WORDS
        ]

        # Convert tokens to a newline-separated string
        tokenized_text = "\n".join(filtered_tokens)

        # Encode the response as a downloadable text file
        encoded_text = base64.b64encode(tokenized_text.encode("utf-8")).decode("utf-8")

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/octet-stream",
                "Content-Disposition": "attachment; filename=tokenized_output.txt"
            },
            "body": encoded_text,
            "isBase64Encoded": True
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
