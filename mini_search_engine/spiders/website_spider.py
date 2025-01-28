import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import os
import re
import datetime
from sqlalchemy import create_engine, text, Column, String, Text, MetaData, Table
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from collections import defaultdict

class WebsiteSpider(CrawlSpider):
    name = "website_spider"

    # Read the list of websites from website_list.txt
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    website_list_path = os.path.join(project_root, 'mini_search_engine/website_list_full.txt')

    with open(website_list_path) as f:
        start_urls = [
            line.strip() if line.strip().startswith(('http://', 'https://')) 
            else 'https://' + line.strip() 
            for line in f.readlines()
        ]

    allowed_domains = []
    allowed_paths = []

    for url in start_urls:
        domain = url.split('//')[-1].split('/')[0]
        allowed_domains.append(domain)
        
        path = '/'.join(url.split('//')[-1].split('/')[1:])
        if path:
            allowed_paths.append(re.escape(path))

    rules = (
        Rule(LinkExtractor(allow=allowed_paths, allow_domains=allowed_domains), callback='parse_item', follow=True),
    )

    def __init__(self, *args, **kwargs):
        super(WebsiteSpider, self).__init__(*args, **kwargs)
        self.project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_file = open(f'output_{timestamp}.txt', 'w')

        load_dotenv(os.path.join(self.project_root, 'env/.env'))
        db_user = os.getenv('SQL_user')
        db_password = os.getenv('SQL_password')
        db_connection = os.getenv('SQL_connection')

        # Create database engine
        self.engine = create_engine(f'cockroachdb://{db_user}:{db_password}{db_connection}')
        self.Session = sessionmaker(bind=self.engine)

        # Ensure the table exists and clear its data if it does
        metadata = MetaData()
        self.crawled_data = Table('crawled_data', metadata,
            Column('url', String, primary_key=True),
            Column('title', String),
            Column('content', Text),
        )
        metadata.create_all(self.engine)

        with self.engine.connect() as conn:
            self.db_test(conn)
            self.recreate_table(conn)

        # Initialize page count per domain
        self.page_count_per_domain = defaultdict(int)
        self.max_pages_per_domain = 10000  # Set your desired limit per domain

    def db_test(self, conn):
        try:
            res = conn.execute(text("SELECT now()")).fetchall()
            self.output_file.write(f"{str(res)}\n")
        except Exception as e:
            self.output_file.write(f"Error executing query: {e}\n")

    def recreate_table(self, conn):
        try:
            metadata = MetaData()
            self.crawled_data = Table('crawled_data', metadata,
                Column('url', String, primary_key=True),
                Column('title', String),
                Column('content', Text),
            )
            metadata.create_all(self.engine)
            conn.execute(text("DELETE FROM crawled_data"))
        except Exception as e:
            self.output_file.write(f"Error creating table or clearing data: {e}\n")

    def insert_crawled_data(self, conn, url, title, content):
        try:
            conn.execute(
                text("INSERT INTO crawled_data (url, title, content) VALUES (:url, :title, :content)"),
                {"url": url, "title": title, "content": content}
            )
            conn.commit()
            self.output_file.write(f"Successfully inserted data for URL: {url}\n")
        except Exception as e:
            self.output_file.write(f"Error inserting data for URL: {url} - {e}\n")

    def parse_item(self, response):
        self.output_file.write(f'Crawling page: {response.url}\n')
        # Define the data to be extracted from each page
        url = response.url
        title = response.xpath('//title/text()').get()
        content = ' '.join(response.xpath('//body//text()').getall())

        # Insert data into the database
        with self.engine.connect() as conn:
            self.insert_crawled_data(conn, url, title, content)

        # Increment page count for the domain
        domain = response.url.split('//')[-1].split('/')[0]
        self.page_count_per_domain[domain] += 1

        # Check if the page count limit for the domain has been reached
        if self.page_count_per_domain[domain] >= self.max_pages_per_domain:
            self.crawler.engine.close_spider(self, f"Reached page count limit for domain: {domain}")

        yield {
            'url': url,
            'title': title,
            'content': content,
        }

    def closed(self, reason):
        self.output_file.close()
