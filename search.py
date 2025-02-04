import tantivy
import os
import time
from tantivy import Query, Occur

class Searcher:
    def __init__(self):
        self.project_root = os.path.dirname(__file__)
        self.index_path = os.path.join(self.project_root, 'tantivy_index')
        self.index = tantivy.Index.open(self.index_path)
        self.searcher = self.index.searcher()
        self.cached_queries = {}

    def search(self, query_str, top_k=10):
        start_time = time.time()
        print(f"Searching for: {query_str} at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")
        
        if query_str in self.cached_queries:
            complex_query = self.cached_queries[query_str]
        else:
            complex_query = Query.boolean_query(
                [
                    (
                        Occur.Must,
                        Query.disjunction_max_query(
                            [
                                Query.boost_query(
                                    self.index.parse_query(query_str, ["title"]), 
                                    3.0
                                ),
                                Query.boost_query(
                                    self.index.parse_query(query_str, ["content"]), 
                                    2.0
                                ),
                                Query.boost_query(
                                    self.index.parse_query(query_str, ["url"]), 
                                    0.5 
                                ),
                            ],
                            0.4,
                        ),
                    )
                ]
            )
            self.cached_queries[query_str] = complex_query
        
        top_docs = self.searcher.search(complex_query, top_k).hits
        end_time = time.time()
        elapsed_time = end_time - start_time

        results = []
        if top_docs:
            for score, doc_address in top_docs:
                doc = self.searcher.doc(doc_address)
                results.append({
                    "score": score,
                    "url": doc.get_first("url"),
                    "title": doc.get_first("title"),
                    "content": doc.get_first("content")
                })
            print(f"Found {len(results)} results in {elapsed_time:.2f} seconds.")
        else:
            print("No results found.")
        return {
            "elapsed_time": elapsed_time,
            "results": results
        }

    # for standalone testing purposes
    def run_search(self):
        query_str = input("Enter your search query: ")
        results = self.search(query_str)
        for result in results:
            print(f"Score: {result['score']}")
            print(f"URL: {result['url']}")
            print(f"Title: {result['title']}")
            print("="*20)

# if __name__ == "__main__":
#     searcherClass = Searcher()
#     searcherClass.run_search()