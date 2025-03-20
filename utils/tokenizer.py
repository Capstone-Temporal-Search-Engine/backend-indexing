import spacy
import os
from spacy.lang.en.stop_words import STOP_WORDS
from bs4 import BeautifulSoup

# Load spaCy English model
nlp = spacy.load("en_core_web_sm")

def tokenize_html_file(file_obj, file_id, output_dir):
    """
    Tokenizes an HTML file from a file object by extracting text, removing stopwords & punctuation, 
    and saving the result to a specified output directory.

    Args:
        file_obj (file-like object): Opened file object containing HTML content.
        output_dir (str): Directory to save the output tokenized text file.

    Returns:
        str: The path of the output tokenized file.
    """
    
    # Validate output directory existence (create if not exists)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Get file name from file object (if available)
    output_file = os.path.join(output_dir, f"{file_id}_tokenized.txt")

    # Read HTML content from file object
    html_content = file_obj.read()

    # Use BeautifulSoup to parse HTML
    soup = BeautifulSoup(html_content, "html.parser")

    # Remove unwanted tags (script, style, nav, button, footer, header)
    for tag in soup(["script", "style", "nav", "button", "footer", "header"]):
        tag.extract()

    # Extract cleaned text
    text = soup.get_text(separator=" ")  # Keeps spaces between elements for readability

    # Process with spaCy
    doc = nlp(text)

    # Extract words and numbers (excluding punctuation and stopwords)
    filtered_tokens = [
        token.text.lower() for token in doc 
        if (token.is_alpha or token.like_num) and token.text.lower() not in STOP_WORDS
    ]

    # Write tokens to file, each on a new line
    with open(output_file, "w", encoding="utf-8") as file:
        file.write("\n".join(filtered_tokens))

    print(f"Tokenized text saved to {output_file}")
    return output_file
