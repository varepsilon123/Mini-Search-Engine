import os
import tantivy
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

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
    
    print("Ran get_indexed_pages_per_domain")

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
            created_at_times = [
                searcher.doc(doc_address).get_first("created_at")
                for score, doc_address in result.hits
            ]
            # print(f"Created at times for {url}: {created_at_times}")  # Print the created_at values
            created_at_times = [time for time in created_at_times]
            total_crawl_time = max(created_at_times) - min(created_at_times)
            crawl_times.append({'url': url, 'total_crawl_time': str(total_crawl_time)})
            print(f"URL: {url}, Total Crawl Time: {total_crawl_time}")
    

    print("Ran get_total_crawl_time")

    return crawl_times

# average page size
def get_average_page_size():
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
        # print(f"Checking URL: {url}")
        query = index.parse_query(url, ['url'])
        result = searcher.search(query, 11000)

        total_size = 0
        total_docs = 0

        for score, doc_address in result.hits:
            doc = searcher.doc(doc_address)
            content = doc.get_first("content")
            total_size += len(content.encode('utf-8'))  # Calculate size in bytes
            total_docs += 1

        average_size = total_size / total_docs if total_docs > 0 else 0

        # Redundant unit conversion, but it's okay for now
        if average_size >= 1_000_000:
            average_size_display = f"{average_size / 1_000_000:.2f} MB"
        elif average_size >= 1_000:
            average_size_display = f"{average_size / 1_000:.2f} KB"
        else:
            average_size_display = f"{average_size:.2f} bytes"
        
        if total_size >= 1_000_000:
            total_size_display = f"{total_size / 1_000_000:.2f} MB"
        elif total_size >= 1_000:
            total_size_display = f"{total_size / 1_000:.2f} KB"
        else:
            total_size_display = f"{total_size:.2f} bytes"
        
        results.append({
            'url': url, 
            'total_docs': total_docs,
            'average_size': average_size_display,
            'total_size': total_size_display
        })
        
        # print(f"URL: {url}, Total size: {total_size_display}, Total docs: {total_docs}, Average size: {average_size_display}")

    print("Ran get_average_page_size")
    return results

def get_failed_logs():
    project_root = os.path.dirname(__file__)
    load_dotenv(os.path.join(project_root, 'env/.env'))
    db_user = os.getenv('SQL_user')
    db_password = os.getenv('SQL_password')
    db_connection = os.getenv('SQL_connection')

    # Create database engine
    engine = create_engine(f'cockroachdb://{db_user}:{db_password}{db_connection}')

    with engine.connect() as conn:
        result = conn.execute(text("SELECT issue, COUNT(*) as `number` FROM failed_logs GROUP BY issue")).fetchall()

    failed_logs = {}
    for row in result:
        failed_logs.append({
            'issue': row[0],
            'number': row[1]
        })

    return failed_logs

if __name__ == "__main__":
    get_failed_logs()