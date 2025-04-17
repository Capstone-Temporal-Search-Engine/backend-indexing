import os
import psycopg2
from dotenv import load_dotenv
import logging
from utils.s3_utils import upload_file

def export_approved_urls_to_file():
    load_dotenv()

    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER", "")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", 5432)


    output_file = "banned_prefix.txt"

    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()

        # Fetch all approved content URLs
        cur.execute("SELECT content_url FROM requests WHERE approval_status = 'approved';")
        urls = cur.fetchall()

        # Write to file
        with open(output_file, "w") as f:
            for url in urls:
                f.write(f"{url[0]}\n")

        logging.info(f"✅ {len(urls)} URLs written to {output_file}")

        # Upload file to S3
        with open(output_file, "rb") as f:
            upload_file("banned", f, output_file)

        print(f"✅ {len(urls)} URLs written to banned_prefix.txt")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    export_approved_urls_to_file()