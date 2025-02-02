import tantivy
import os
import datetime
from sqlalchemy import text

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

        # Fetch data from the database
        with engine.connect() as conn:
            result = conn.execute(text("SELECT url, title, content FROM crawled_data"))
            for row in result:
                index_writer.add_document(tantivy.Document(
                    url=row['url'],
                    title=row['title'],
                    content=row['content']
                ))

        # Commit the changes to the index
        index_writer.commit()
        self.log_writer("Indexing process completed.")
        self.output_file.close()