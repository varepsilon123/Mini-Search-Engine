import tantivy
import os
import time
from tantivy import Index, Query, Occur, SchemaBuilder, SnippetGenerator

class Searcher:
    def __init__(self):
        self.project_root = os.path.dirname(__file__)
        self.index_path = os.path.join(self.project_root, 'tantivy_index')
        self.index = Index.open(self.index_path)
        self.searcher = self.index.searcher()
        self.cached_queries = {}

        # Declaring our schema.
        self.schema_builder = SchemaBuilder()
        self.schema_builder.add_text_field("title", stored=True)
        self.schema_builder.add_text_field("content", stored=True)
        self.schema_builder.add_text_field("url", stored=True)
        self.schema = self.schema_builder.build()


    def search(self, query_str, top_k=10):
        start_time = time.time()
        print(f"Searching for: {query_str} at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")
        
        if query_str in self.cached_queries:
            complex_query = self.cached_queries[query_str]
        else:
            terms = query_str.split()
            must_queries = [
                Query.disjunction_max_query(
                    [
                        Query.boost_query(
                            self.index.parse_query(term, ["title"]), 
                            1.5
                        ),
                        Query.boost_query(
                            self.index.parse_query(term, ["content"]), 
                            2.0
                        ),
                        Query.boost_query(
                            self.index.parse_query(term, ["url"]), 
                            0.5 
                        ),
                    ],
                    0.4,
                ) for term in terms
            ]
            complex_query = Query.boolean_query(
                [(Occur.Must, must_query) for must_query in must_queries]
            )
            self.cached_queries[query_str] = complex_query
        
        top_docs = self.searcher.search(complex_query, top_k).hits
        end_time = time.time()
        elapsed_time = (end_time - start_time) * 1000  # convert to milliseconds

        results = []
        if top_docs:
            snippet_generator = SnippetGenerator.create(
                self.searcher, complex_query, self.schema, "content"
            )
            for score, doc_address in top_docs:
                doc = self.searcher.doc(doc_address)
                snippet = snippet_generator.snippet_from_doc(doc)
                results.append({
                    "score": score,
                    "url": doc.get_first("url"),
                    "title": doc.get_first("title"),
                    "snippet": snippet.to_html()
                })
            print(f"Found {len(results)} results in {elapsed_time:.2f} milliseconds.")
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