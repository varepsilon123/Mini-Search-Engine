from scrapy.crawler import CrawlerProcess
from mini_search_engine.spiders.website_spider import WebsiteSpider
import os
import re
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

def db_test(conn):
    try:
        res = conn.execute(text("SELECT now()")).fetchall()
        print(f"{str(res)}")
    except Exception as e:
        print(f"Error executing query: {e}")

def create_table_if_not_exists(engine, table_name):
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT to_regclass('{table_name}')")).scalar()
        if not result:
            conn.execute(text(f"""
                CREATE TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    url TEXT UNIQUE NOT NULL,
                    title TEXT,
                    content TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            print(f"Table {table_name} created.")
        else:
            print(f"Table {table_name} already exists.")

def insert_crawled_data(engine, url, title, content):
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        # upsert data to db
        session.execute(
            text("""
                INSERT INTO crawled_data (url, title, content)
                VALUES (:url, :title, :content)
                ON CONFLICT (url) DO UPDATE SET
                title = EXCLUDED.title,
                content = EXCLUDED.content
            """),
            {"url": url, "title": title, "content": content}
        )
        session.commit()
        print(f"Successfully inserted/updated data for URL: {url}")
    except Exception as e:
        print(f"Error inserting/updating data for URL: {url} - {e}")
        session.rollback()
        raise
    finally:
        session.close()

def insert_failed_log(engine, url, issue, reason=None):
    Session = sessionmaker(bind=engine)
    session = Session()
    if not reason:
        reason = 'Unknown'
    try:
        session.execute(
            text("""
                INSERT INTO failed_logs (url, issue, reason)
                VALUES (:url, :issue, :reason)
            """),
            {"url": url, "issue": issue, "reason": reason}
        )
        session.commit()
        print(f"Successfully inserted failed log for URL: {url}")
    except Exception as e:
        print(f"Error inserting failed log for URL: {url} - {e}")
        session.rollback()
        raise
    finally:
        session.close()

def run_crawler(engine):
    project_root = os.path.dirname(__file__)
    website_list_path = os.path.join(project_root, 'website_list_full.txt')

    with open(website_list_path) as f:
        urls = [
            line.strip() if line.strip().startswith(('http://', 'https://')) 
            else 'https://' + line.strip() 
            for line in f.readlines()
        ]

    settings = {
        'DEPTH_PRIORITY': 0,
        'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
        'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue',
        'TELNETCONSOLE_ENABLED': False,  # Disable the Telnet console extension
        'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',  # Update to the recommended value
        'CONCURRENT_REQUESTS': 16,  # default: 16
        'DOWNLOAD_DELAY': 1,  # default: 0
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,  # default: 8
        'CONCURRENT_REQUESTS_PER_IP': 0,  # default: 0
        'AUTOTHROTTLE_ENABLED': True,  # Enable AutoThrottle extension
        'AUTOTHROTTLE_START_DELAY': 5,  # default: 5
        'AUTOTHROTTLE_MAX_DELAY': 60,  # default: 60
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 1,  # default: 1.0
        'AUTOTHROTTLE_DEBUG': False,  # default: False
        'DEFAULT_REQUEST_HEADERS': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        },
        'COOKIES_ENABLED': True,
    }

    process = CrawlerProcess(settings=settings)

    # Log here in the output file for the url
    print(f'In main, Crawling {len(urls)} URLs.')

    # Test connection
    with engine.connect() as conn:
        db_test(conn)

    # Truncate table if exists
    create_table_if_not_exists(engine, 'crawled_data')

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

        print(f'Queuing process: {allowed_domains[0]}')
        process.crawl(WebsiteSpider, start_urls=start_urls, allowed_domains=allowed_domains, allowed_paths=allowed_paths, engine=engine, insert_crawled_data=insert_crawled_data, insert_failed_log=insert_failed_log, max_pages_per_domain=10000)

    print('Starting the crawling process...')
    process.start()  # Start the crawling process for all URLs
    print('Crawling process finished.')