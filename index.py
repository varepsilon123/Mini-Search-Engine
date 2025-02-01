import tantivy
import os
from sqlalchemy import text

def run_index(engine):
    print('indexing process started')
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
    print("Indexing process completed.")