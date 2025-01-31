import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import os
from collections import defaultdict

class WebsiteSpider(CrawlSpider):
    name = "website_spider"

    def __init__(self, start_urls=None, allowed_domains=None, allowed_paths=None, output_file=None, engine=None, insert_crawled_data=None, *args, **kwargs):
        super(WebsiteSpider, self).__init__(*args, **kwargs)  # Initialize the parent class
        self.start_urls = start_urls
        self.allowed_domains = allowed_domains
        self.allowed_paths = allowed_paths
        self.output_file = output_file
        self.engine = engine
        self.insert_crawled_data = insert_crawled_data
        self.crawled_count = 0

        # Log allowed path and domain
        self.output_file.write(f"Allowed domain: {self.allowed_domains}\n")
        self.output_file.write(f"Allowed path: {self.allowed_paths}\n")

        # Initialize page count per domain
        self.page_count_per_domain = defaultdict(int)
        self.max_pages_per_domain = 10000  # Set your desired limit per domain

        # Set a single rule for the given URL
        self.rules = [
            Rule(LinkExtractor(allow=self.allowed_paths, allow_domains=self.allowed_domains), callback='parse_item', follow=True)
        ]
        self._compile_rules()  # Compile the rules

    def start_requests(self):
        for url in self.start_urls:
            self.output_file.write(f'Starting request for URL: {url}\n')
            yield scrapy.Request(url=url, callback=self.parse_item, dont_filter=True)

    def parse_item(self, response):
        self.output_file.write(f'Crawl count: {self.crawled_count}, Crawling page: {response.url}\n')
        self.crawled_count += 1
        # Define the data to be extracted from each page
        url = response.url
        title = response.xpath('//title/text()').get()
        content = ' '.join(response.xpath('//body//text()').getall())

        # Insert data into the database
        try:
            self.insert_crawled_data(self.engine, self.output_file, url, title, content)
        except Exception as e:
            self.output_file.write(f"Error inserting/updating data for URL: {url} - {e}\n")

        # Increment page count for the domain
        domain = response.url.split('//')[-1].split('/')[0]
        self.page_count_per_domain[domain] += 1

        # Check if the page count limit for the domain has been reached
        if self.page_count_per_domain[domain] > self.max_pages_per_domain:
            self.output_file.write(f"Skipping further pages for domain: {domain} as page count limit reached\n")
        else:
            # Continue crawling other pages
            self.output_file.write(f"Extracting links from: {response.url}\n")
            for link in LinkExtractor(allow=self.allowed_paths, allow_domains=self.allowed_domains).extract_links(response):
                yield scrapy.Request(link.url, callback=self.parse_item)

        yield {
            'url': url,
            'title': title,
            'content': content,
        }

    def closed(self, reason):
        self.output_file.write(f"Spider closed: {reason}, URL: {self.allowed_domains[0]},  Crawled: {self.crawled_count}\n")
