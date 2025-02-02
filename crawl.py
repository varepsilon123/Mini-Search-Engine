from scrapy.crawler import CrawlerProcess
from mini_search_engine.spiders.website_spider import WebsiteSpider
import os
import re
import datetime
from sqlalchemy import text
import logging

def db_test(conn, output_file):
    try:
        res = conn.execute(text("SELECT now()")).fetchall()
        output_file.write(f"{str(res)}\n")
    except Exception as e:
        output_file.write(f"Error executing query: {e}\n")

def insert_crawled_data(engine, output_file, url, title, content):
    try:
        # upsert data to db
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

def run_crawler(engine):
    project_root = os.path.dirname(__file__)
    website_list_path = os.path.join(project_root, 'website_list_test.txt')

    with open(website_list_path) as f:
        urls = [
            line.strip() if line.strip().startswith(('http://', 'https://')) 
            else 'https://' + line.strip() 
            for line in f.readlines()
        ]

    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    output_folder = f'output_{timestamp}'
    os.makedirs(output_folder, exist_ok=True)
    
    log_file_info = os.path.join(output_folder, f'log_info_{timestamp}.txt')
    log_file_warning = os.path.join(output_folder, f'log_warning_{timestamp}.txt')
    log_file_error = os.path.join(output_folder, f'log_error_{timestamp}.txt')
    log_file_critical = os.path.join(output_folder, f'log_critical_{timestamp}.txt')
    output_file = open(os.path.join(output_folder, f'output_{timestamp}.txt'), 'w')

    settings = {
        'DEPTH_PRIORITY': 1,
        'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
        'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue',
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.RFPDupeFilter',  # Use the default duplicate filter
        'LOG_LEVEL': 'DEBUG',  # Set the global log level to DEBUG to capture all logs
        'TELNETCONSOLE_ENABLED': False,  # Disable the Telnet console extension
        'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',  # Update to the recommended value
    }

    # Configure specific loggers
    file_handler_info = logging.FileHandler(log_file_info, mode='w')
    file_handler_info.setLevel(logging.INFO)
    file_handler_warning = logging.FileHandler(log_file_warning, mode='w')
    file_handler_warning.setLevel(logging.WARNING)
    file_handler_error = logging.FileHandler(log_file_error, mode='w')
    file_handler_error.setLevel(logging.ERROR)
    file_handler_critical = logging.FileHandler(log_file_critical, mode='w')
    file_handler_critical.setLevel(logging.CRITICAL)

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        handlers=[
            file_handler_info,
            file_handler_warning,
            file_handler_error,
            file_handler_critical,
            logging.StreamHandler()
        ]
    )

    process = CrawlerProcess(settings=settings)

    # Log here in the output file for the url
    output_file.write(f'In main, Crawling {len(urls)} URLs.\n')

    # Test connection
    with engine.connect() as conn:
        db_test(conn, output_file)

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

        output_file.write(f'Queuing process: {allowed_domains[0]}\n')
        process.crawl(WebsiteSpider, start_urls=start_urls, allowed_domains=allowed_domains, allowed_paths=allowed_paths, output_file=output_file, engine=engine, insert_crawled_data=insert_crawled_data)

    output_file.write('Starting the crawling process...\n')
    process.start()  # Start the crawling process for all URLs
    output_file.write('Crawling process finished.\n')

    output_file.close()
