from flask import Flask, request, jsonify
from search import Searcher
import re

app = Flask(__name__)

def escape_special_characters(query):
    return re.escape(query)

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

if __name__ == "__main__":
    app.run(debug=True)
