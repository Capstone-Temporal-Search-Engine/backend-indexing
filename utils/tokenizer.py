import spacy
import os
from spacy.lang.en.stop_words import STOP_WORDS
from bs4 import BeautifulSoup

# Load spaCy English model
nlp = spacy.load("en_core_web_sm")

def tokenize_html_file(input_file_dir, output_dir):
    """
    Tokenizes an HTML file by extracting text, removing stopwords & punctuation, 
    and saving the result to a specified output directory.

    Args:
        input_file (str): Path to the HTML file.
        output_dir (str): Directory to save the output tokenized text file.

    Returns:
        str: The path of the output tokenized file.
    """
    # Validate file existence
    if not os.path.exists(input_file_dir):
        print(f"Error: File '{input_file_dir}' not found.")
        return None

    # Validate output directory existence (create if not exists)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Generate output file name
    base_name = os.path.splitext(os.path.basename(input_file_dir))[0]  # Remove .html extension
    output_file = os.path.join(output_dir, f"{base_name}_tokenized.txt")  # Save in output_dir

    # Read HTML file
    with open(input_file_dir, "r", encoding="utf-8") as file:
        html_content = file.read()

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

# Example usage:
# output_path = tokenize_html_file("catalog.uark.edu_.html", "output_tokens")
# print("Tokenized file stored at:", output_path)