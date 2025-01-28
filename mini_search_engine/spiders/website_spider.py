import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import os
import re
import datetime
from sqlalchemy import create_engine, text, Column, String, Text, MetaData, Table
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

class WebsiteSpider(CrawlSpider):
    name = "website_spider"

    # Read the list of websites from website_list.txt
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    website_list_path = os.path.join(project_root, 'mini_search_engine/website_list_test.txt')

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

    custom_settings = {
        'CLOSESPIDER_PAGECOUNT': 10000,
    }

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
            conn.execute(text("DELETE FROM crawled_data"))

    def parse_item(self, response):
        self.output_file.write(f'Crawling page: {response.url}\n')
        # Define the data to be extracted from each page
        url = response.url
        title = response.xpath('//title/text()').get()
        content = ' '.join(response.xpath('//body//text()').getall())

        # Insert data into the database
        with self.engine.connect() as conn:
            try:
                res = conn.execute(text("SELECT now()")).fetchall()
                self.output_file.write(f"{str(res)}\n")
            except Exception as e:
                self.output_file.write(f"Error executing query: {e}\n")
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

            try:
                conn.execute(
                    text("INSERT INTO crawled_data (url, title, content) VALUES (:url, :title, :content)"),
                    {"url": url, "title": title, "content": content}
                )
                conn.commit()
                self.output_file.write(f"Successfully inserted data for URL: {url}\n")
            except Exception as e:
                self.output_file.write(f"Error inserting data for URL: {url} - {e}\n")

        yield {
            'url': url,
            'title': title,
            'content': content,
        }

    def closed(self, reason):
        self.output_file.close()
