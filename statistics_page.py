import os
import datetime
import tantivy

def get_indexed_pages_per_domain():
    project_root = os.path.dirname(__file__)
    website_list_path = os.path.join(project_root, 'website_list_full.txt')

    with open(website_list_path) as f:
        urls = [
            line.strip()
            for line in f.readlines()
        ]

    # Open the index
    index_path = os.path.join(project_root, 'tantivy_index')
    index = tantivy.Index.open(index_path)
    searcher = index.searcher()

    results = []
    for url in urls:
        print(f"Checking URL: {url}")
        query = index.parse_query(url, ['url'])
        result = searcher.search(query, 11000)
        page_count = len(result.hits)

        results.append({'url': url, 'page_count': page_count})
        print(f"URL: {url}, Indexed Pages: {page_count}")

    return results

def get_total_crawl_time():
    project_root = os.path.dirname(__file__)
    website_list_path = os.path.join(project_root, 'website_list_full.txt')

    with open(website_list_path) as f:
        urls = [
            line.strip()
            for line in f.readlines()
        ]

    # Open the index
    index_path = os.path.join(project_root, 'tantivy_index')
    index = tantivy.Index.open(index_path)
    searcher = index.searcher()

    crawl_times = []
    for url in urls:
        print(f"Calculating crawl time for URL: {url}")
        query = index.parse_query(url, ['url'])
        result = searcher.search(query, 11000)

        if result.hits:
            created_at_times = [hit.document.get_first("created_at") for hit in result.hits]
            created_at_times = [datetime.datetime.strptime(time, '%Y-%m-%dT%H:%M:%S') for time in created_at_times]
            total_crawl_time = max(created_at_times) - min(created_at_times)
            print(f"URL: {url}, Total Crawl Time: {total_crawl_time}")
            crawl_times.append({'url': url, 'total_crawl_time': total_crawl_time})
            print(f"URL: {url}, Total Crawl Time: {total_crawl_time}")

    return crawl_times

# average page size
def get_average_page_size():
    project_root = os.path.dirname(__file__)
    index_path = os.path.join(project_root, 'tantivy_index')
    index = tantivy.Index.open(index_path)
    searcher = index.searcher()

    total_size = 0
    total_docs = searcher.doc_count()

    for doc_id in range(total_docs):
        doc = searcher.doc(doc_id)
        total_size += len(str(doc))

    average_size = total_size / total_docs if total_docs > 0 else 0
    return average_size