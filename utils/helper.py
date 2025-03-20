from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import logging
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_months_between(start_timestamp, end_timestamp):
    start_date = datetime.utcfromtimestamp(start_timestamp)
    end_date = datetime.utcfromtimestamp(end_timestamp)

    months = []
    current_date = start_date.replace(day=1)  # Start from the first day of the month

    while current_date <= end_date:
        months.append(f'{current_date.month:02d}-{current_date.year}')  # Ensures two-digit month format
        # Move to the next month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)

    return months

def extract_title_description_from_html(file_object):
    try:
        # Read the file object as text
        file_object.seek(0)  # Ensure we're reading from the start
        soup = BeautifulSoup(file_object.read(), 'html.parser')

        title = soup.title.string.strip() if soup.title else "No Title"
        description_meta = soup.find("meta", attrs={"name": "description"})
        description = description_meta["content"].strip() if description_meta else "No Description"
        print("title is", title)
        print("description is - ", description)

        return title, description
    except Exception as e:
        logger.error(f"Error extracting title and description: {str(e)}", exc_info=True)
        return "No Title", "No Description"
