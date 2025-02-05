from flask import Flask, request, jsonify
from flask_cors import CORS
from search import Searcher
from statistics_page import get_indexed_pages_per_domain
import re

app = Flask(__name__)
CORS(app)  # Enable CORS

def escape_special_characters(query):
    # Escape special characters without removing them
    return re.sub(r'([(){}[\]])', r'\\\1', query)

@app.route('/search', methods=['POST'])
def search():
    data = request.get_json()
    query_str = data.get('query', '')
    
    if not query_str:
        return jsonify({"error": "Query string is required"}), 400
    
    query_str = escape_special_characters(query_str)
    searcher = Searcher()
    results = searcher.search(query_str)
    
    return jsonify(results)

@app.route('/indexed_pages', methods=['GET'])
def indexed_pages():
    results = get_indexed_pages_per_domain()
    return jsonify(results)

if __name__ == "__main__":
    app.run(debug=True)
