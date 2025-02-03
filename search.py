import tantivy
import os

class Searcher:
    def __init__(self):
        self.project_root = os.path.dirname(__file__)
        self.index_path = os.path.join(self.project_root, 'tantivy_index')
        self.index = tantivy.Index.open(self.index_path)
        # self.index.reload()  # Ensure the index points to the last commit
        self.searcher = self.index.searcher()

    def search(self, query_str, top_k=10):
        print(f"Searching for: {query_str}")
        query = self.index.parse_query(query_str, ["title", "content", "url"])
        # top_docs = self.searcher.search(query, top_k).hits
        print(self.searcher.search(query, 3).hits)
        # (best_score, best_doc_address) = self.searcher.search(query, 3).hits
        best_doc_address_arr = self.searcher.search(query, 3).hits
        print(f"best_doc_address: {best_doc_address_arr}")
        best_doc = self.searcher.doc(best_doc_address_arr[0])
        results = []
        # if top_docs:
        #     for score, doc_address in top_docs:
        #         doc = self.searcher.doc(doc_address)
        #         results.append({
        #             "score": score,
        #             "url": doc.get("url"),
        #             "title": doc.get("title"),
        #             "content": doc.get("content")
        #         })
        # else:
        #     print("No results found.")
        return results

    def run_search(self):
        query_str = input("Enter your search query: ")
        results = self.search(query_str)
        for result in results:
            print(f"Score: {result['score']}")
            print(f"URL: {result['url']}")
            print(f"Title: {result['title']}")
            print(f"Content: {result['content']}")
            print("="*20)

# if __name__ == "__main__":
#     searcherClass = Searcher()
#     searcherClass.run_search()
