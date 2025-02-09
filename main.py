from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import sys
from crawl import run_crawler
from index import Indexer
from search import Searcher

if __name__ == "__main__":
    load_dotenv('env/.env')
    db_user = os.getenv('SQL_user')
    db_password = os.getenv('SQL_password')
    db_connection = os.getenv('SQL_connection')

    # Create database engine
    engine = create_engine(f'cockroachdb://{db_user}:{db_password}{db_connection}')

    if len(sys.argv) > 1:
        if sys.argv[1] == "crawl":
            # Run the crawler
            run_crawler(engine)
        elif sys.argv[1] == "index":
            # Run the indexer
            indexer = Indexer()
            indexer.run_index(engine)
        elif sys.argv[1] == "search":
            # Run the searcher, for testing purposes
            searcher = Searcher()
            searcher.run_search()