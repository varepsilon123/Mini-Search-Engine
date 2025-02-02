import tantivy
import os
import datetime
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor

class Indexer:
    def __init__(self):
        self.project_root = os.path.dirname(__file__)
        self.init_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_folder = f'output_{self.init_timestamp}'
        os.makedirs(self.output_folder, exist_ok=True)
        self.output_file = open(os.path.join(self.output_folder, f'output_{self.init_timestamp}.txt'), 'w')
        self.log_writer('Indexing process started')

    def log_writer(self, message):
        self.output_file.write(datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))
        self.output_file.write(f": {message}\n")

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
        # Load environment variables
        project_root = os.path.dirname(os.path.dirname(__file__))

        # Define the schema
        schema_builder = tantivy.SchemaBuilder()
        schema_builder.add_text_field("title", stored=True)
        schema_builder.add_text_field("content", stored=True)
        schema_builder.add_text_field("url", stored=True)
        schema = schema_builder.build()

        # Create an index
        index_path = os.path.join(project_root, 'tantivy_index')
        if not os.path.exists(index_path):
            os.makedirs(index_path)
        index = tantivy.Index(schema, path=index_path)

        # Create an index writer
        index_writer = index.writer()

        # Fetch total number of rows
        with engine.connect() as conn:
            total_rows = conn.execute(text("SELECT COUNT(*) FROM crawled_data")).scalar()
            self.log_writer(f"Total rows to be indexed: {total_rows}")

        # Process data in batches
        batch_size = 100
        with ThreadPoolExecutor() as executor:
            futures = []
            for offset in range(0, total_rows, batch_size):
                batch = self.fetch_batch(engine, offset, batch_size)
                futures.append(executor.submit(self.process_batch, index_writer, batch))

            # Wait for all futures to complete
            for future in futures:
                future.result()

        # Commit the changes to the index
        index_writer.commit()
        self.log_writer("Indexing process completed.")
        self.output_file.close()