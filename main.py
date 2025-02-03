from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import sys
from crawl import run_crawler
from index import Indexer
from search import Searcher

if __name__ == "__main__":
    project_root = os.path.dirname(os.path.dirname(__file__))
    load_dotenv(os.path.join(project_root, 'env/.env'))
    db_user = os.getenv('SQL_user')
    db_password = os.getenv('SQL_password')
    db_connection = os.getenv('SQL_connection')

    print(f"Connecting to database: {db_connection}")
    
    # Create database engine
    engine = create_engine(f'cockroachdb://{db_user}:{db_password}{db_connection}')

    if len(sys.argv) > 1:
        if sys.argv[1] == "crawl":
            run_crawler(engine)
        elif sys.argv[1] == "index":
            indexer = Indexer()
            indexer.run_index(engine)
        elif sys.argv[1] == "search":
            searcher = Searcher()
            searcher.run_search()