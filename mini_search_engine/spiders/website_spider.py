import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy import signals
from collections import defaultdict
import datetime

class WebsiteSpider(CrawlSpider):
    name = "website_spider"

    def __init__(self, start_urls=None, allowed_domains=None, allowed_paths=None, engine=None, insert_crawled_data=None, *args, **kwargs):
        super(WebsiteSpider, self).__init__(*args, **kwargs)  # Initialize the parent class
        self.start_urls = start_urls
        self.allowed_domains = allowed_domains
        self.allowed_paths = allowed_paths
        self.engine = engine
        self.insert_crawled_data = insert_crawled_data
        self.crawled_count = 0

        # Initialize page count per domain
        self.page_count_per_domain = defaultdict(int)
        self.max_pages_per_domain = 10000  # Set your desired limit per domain

        # Set a single rule for the given URL
        self.rules = [
            Rule(LinkExtractor(allow=self.allowed_paths, allow_domains=self.allowed_domains), callback='parse_item', follow=True)
        ]
        self._compile_rules()  # Compile the rules

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(WebsiteSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_error, signal=signals.spider_error)
        crawler.signals.connect(spider.request_dropped, signal=signals.request_dropped)
        return spider

    def start_requests(self):
        for url in self.start_urls:
            print(f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}: Starting request for URL: {url}")
            yield scrapy.Request(url=url, callback=self.parse_item, errback=self.errback_httpbin, dont_filter=True)

    def parse_item(self, response):
        self.crawled_count += 1
        # Define the data to be extracted from each page
        url = response.url
        title = response.xpath('//title/text()').get()
        content = ' '.join(response.xpath('//body//text()').getall())

        # Insert data into the database
        self.insert_crawled_data(self.engine, url, title, content)

        # Increment page count for the domain
        domain = response.url.split('//')[-1].split('/')[0]
        self.page_count_per_domain[domain] += 1

        # Check if the page count limit for the domain has been reached
        if self.page_count_per_domain[domain] > self.max_pages_per_domain:
            print(f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}: Skipping further pages for domain: {domain} as page count limit reached")
        else:
            # Continue crawling other pages
            print(f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}: Extracting links from: {response.url}")
            links = LinkExtractor(allow=self.allowed_paths, allow_domains=self.allowed_domains).extract_links(response)
            if not links:
                print(f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}: No followable links found on: {response.url}")
            for link in links:
                yield scrapy.Request(link.url, callback=self.parse_item, errback=self.errback_httpbin)

        yield {
            'url': url,
            'title': title,
            'content': content,
        }

    def errback_httpbin(self, failure):
        print(f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}: Network error on {failure.request.url}: {failure.value}")

    def spider_error(self, failure, response, spider):
        print(f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}: Spider error on {response.url}: {failure.value}")

    def request_dropped(self, request, spider):
        print(f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}: Request dropped: {request.url}")

    def closed(self, reason):
        print(f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}: Spider closed: {reason}, URL: {self.allowed_domains[0]}, Crawled: {self.crawled_count}")
        if 'robots.txt' in reason:
            print(f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}: Crawler stopped due to robots.txt restrictions for URL: {self.allowed_domains[0]}")
