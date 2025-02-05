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
        result = searcher.search(query, 20000)
        page_count = len(result.hits)

        results.append({'url': url, 'page_count': page_count})
        print(f"URL: {url}, Indexed Pages: {page_count}")

    return results

if __name__ == "__main__":
    get_indexed_pages_per_domain()