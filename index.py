import tantivy
import os
import datetime
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor

class Indexer:
    def __init__(self):
        print("Indexer initialized")
        self.project_root = os.path.dirname(__file__)
        self.log_writer('Indexing process started')

    def log_writer(self, message):
        log_str = datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + f": {message}\n"
        print(log_str)

    def fetch_batch(self, engine, offset, limit):
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT url, title, content FROM crawled_data LIMIT {limit} OFFSET {offset}"))
            return result.fetchall()

    def process_batch(self, index_writer, batch):
        for row in batch:
            url = row[0] if row[0] is not None else ""
            title = row[1] if row[1] is not None else ""
            content = row[2] if row[2] is not None else ""
            index_writer.add_document(tantivy.Document(
                url=url,
                title=title,
                content=content
            ))

    def run_index(self, engine):

        # Define the schema
        schema_builder = tantivy.SchemaBuilder()
        schema_builder.add_text_field("title", stored=True)
        schema_builder.add_text_field("content", stored=True)
        schema_builder.add_text_field("url", stored=True)
        schema = schema_builder.build()

        # Create an index
        index_path = os.path.join(self.project_root, 'tantivy_index')
        if not os.path.exists(index_path):
            os.makedirs(index_path)
        index = tantivy.Index(schema, path=index_path)

        # Create an index writer
        index_writer = index.writer()

        # Process data in batches
        batch_size = 1000
        batch_num = 0
        with ThreadPoolExecutor() as executor:
            futures = []
            offset = 0
            batch = self.fetch_batch(engine, offset, batch_size)
            while batch:
                self.log_writer(f"Batch number: {batch_num}")
                batch_num += 1
                futures.append(executor.submit(self.process_batch, index_writer, batch))
                offset += batch_size
                batch = self.fetch_batch(engine, offset, batch_size)

            # Wait for all futures to complete
            for future in futures:
                future.result()

        # Commit the changes to the index
        index_writer.commit()
        self.log_writer("Indexing process completed.")