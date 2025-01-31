from scrapy.crawler import CrawlerProcess
from mini_search_engine.spiders.website_spider import WebsiteSpider
import os
import re
import datetime
from sqlalchemy import create_engine, text, Column, String, Text, MetaData, Table
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from collections import defaultdict
from scrapy.utils.log import configure_logging

def db_test(conn, output_file):
    try:
        res = conn.execute(text("SELECT now()")).fetchall()
        output_file.write(f"{str(res)}\n")
    except Exception as e:
        output_file.write(f"Error executing query: {e}\n")

def recreate_table(engine, output_file):
    try:
        metadata = MetaData()
        crawled_data = Table('crawled_data', metadata,
            Column('url', String, primary_key=True),
            Column('title', String),
            Column('content', Text),
        )
        metadata.create_all(engine)
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM crawled_data"))
    except Exception as e:
        output_file.write(f"Error creating table or clearing data: {e}\n")

def insert_crawled_data(engine, output_file, url, title, content):
    try:
        # insert data to db
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO crawled_data (url, title, content)
                    VALUES (:url, :title, :content)
                    ON CONFLICT (url) DO UPDATE SET
                    title = EXCLUDED.title,
                    content = EXCLUDED.content
                """),
                {"url": url, "title": title, "content": content}
            )
            conn.commit()
            output_file.write(f"Successfully inserted/updated data for URL: {url}\n")
    except Exception as e:
        output_file.write(f"Error inserting/updating data for URL: {url} - {e}\n")
        raise

if __name__ == "__main__":
    project_root = os.path.dirname(os.path.dirname(__file__))
    website_list_path = os.path.join(project_root, 'website_list_full.txt')

    with open(website_list_path) as f:
        urls = [
            line.strip() if line.strip().startswith(('http://', 'https://')) 
            else 'https://' + line.strip() 
            for line in f.readlines()
        ]

    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'log_{timestamp}.txt'
    output_file = open(f'output_{timestamp}.txt', 'w')

    settings = {
        'DEPTH_PRIORITY': 1,
        'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
        'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue',
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.RFPDupeFilter',  # Use the default duplicate filter
        'LOG_LEVEL': 'DEBUG',  # Set the log level to DEBUG to capture all logs
        'LOG_FILE': log_file,  # Write logs to a file
    }

    process = CrawlerProcess(settings=settings)

    # Log here in the output file for the url
    output_file.write(f'In main, Crawling {len(urls)} URLs.\n')

    load_dotenv(os.path.join(project_root, 'env/.env'))
    db_user = os.getenv('SQL_user')
    db_password = os.getenv('SQL_password')
    db_connection = os.getenv('SQL_connection')

    # Create database engine
    engine = create_engine(f'cockroachdb://{db_user}:{db_password}{db_connection}')
    Session = sessionmaker(bind=engine)

    # Test connection and recreate table
    with engine.connect() as conn:
        db_test(conn, output_file)
        recreate_table(engine, output_file)

    for url in urls:
        start_urls = [url]
        allowed_domains = []
        allowed_paths = []

        for url in start_urls:
            domain = url.split('//')[-1].split('/')[0]
            allowed_domains.append(domain)
            
            path = '/'.join(url.split('//')[-1].split('/')[1:])
            if path:
                allowed_paths.append(re.escape(path))

        # Log here in the output file for the url
        output_file.write(f'In main, current URL: {url}\n')

        output_file.write(f'Queuing process: {allowed_domains[0]}\n')
        process.crawl(WebsiteSpider, start_urls=start_urls, allowed_domains=allowed_domains, allowed_paths=allowed_paths, output_file=output_file, engine=engine, insert_crawled_data=insert_crawled_data)

    output_file.write('Starting the crawling process...\n')
    process.start()  # Start the crawling process for all URLs
    output_file.write('Crawling process finished.\n')

    output_file.close()