document.getElementById('fetchIndexedPages').addEventListener('click', () => {
  fetchIndexedPages();
});

document.getElementById('fetchTotalCrawlTime').addEventListener('click', () => {
  fetchTotalCrawlTime();
});

document.getElementById('fetchAveragePageSize').addEventListener('click', () => {
  fetchAveragePageSize();
});

// const APIurl = 'http://0.0.0.0:8000';
const APIurl = 'http://fungthedev.fun/api';

function fetchFromAPI(endpoint) {
  return fetch(`${APIurl}/${endpoint}`)
    .then((response) => response.json());
}

function fetchIndexedPages() {
  const resultsDiv = document.getElementById('results');
  resultsDiv.innerHTML = 'Loading...';
  const start_time = new Date().getTime();
  fetchFromAPI('indexed_pages')
    .then((data) => {
      console.log('Response data:', data);  // Debugging statement
      if (!Array.isArray(data)) {
        throw new Error('Expected an array but got ' + typeof data);
      }

      const elapsed_time = new Date().getTime() - start_time;
      resultsDiv.innerHTML = '';

      // Add elapsed time display
      const elapsedTimeDiv = document.createElement('div');
      elapsedTimeDiv.style.margin = '10px 0';
      elapsedTimeDiv.textContent = `Elapsed time: ${elapsed_time}ms`;
      resultsDiv.appendChild(elapsedTimeDiv);

      const table = document.createElement('table');
      const thead = document.createElement('thead');
      const tbody = document.createElement('tbody');

      thead.innerHTML = `<tr><th>URL (Alphabetical Order)</th><th>Indexed Pages</th></tr>`;

      data.forEach((item) => {
        const row = document.createElement('tr');
        row.innerHTML = `<td>${item.url}</td><td>${item.page_count}</td>`;
        tbody.appendChild(row);
      });

      table.appendChild(thead);
      table.appendChild(tbody);
      resultsDiv.appendChild(table);
    })
    .catch((error) => {
      resultsDiv.innerHTML = `Error: ${error.message}`;
      console.error('Error:', error);  // Debugging statement
    });
}

function fetchTotalCrawlTime() {
  const resultsDiv = document.getElementById('results');
  resultsDiv.innerHTML = 'Loading...';
  const start_time = new Date().getTime();
  fetchFromAPI('total_crawl_time')
    .then((data) => {
      console.log('Response data:', data);  // Debugging statement
      if (!Array.isArray(data)) {
        throw new Error('Expected an array but got ' + typeof data);
      }

      const elapsed_time = new Date().getTime() - start_time;
      resultsDiv.innerHTML = '';

      // Add elapsed time display
      const elapsedTimeDiv = document.createElement('div');
      elapsedTimeDiv.style.margin = '10px 0';
      elapsedTimeDiv.textContent = `Elapsed time: ${elapsed_time}ms`;
      resultsDiv.appendChild(elapsedTimeDiv);

      const table = document.createElement('table');
      const thead = document.createElement('thead');
      const tbody = document.createElement('tbody');

      thead.innerHTML = `<tr><th>URL</th><th>Total Crawl Time</th></tr>`;

      data.forEach((item) => {
        const row = document.createElement('tr');
        row.innerHTML = `<td>${item.url}</td><td>${item.total_crawl_time}</td>`;
        tbody.appendChild(row);
      });

      table.appendChild(thead);
      table.appendChild(tbody);
      resultsDiv.appendChild(table);
    })
    .catch((error) => {
      resultsDiv.innerHTML = `Error: ${error.message}`;
      console.error('Error:', error);  // Debugging statement
    });
}

function fetchAveragePageSize() {
  const resultsDiv = document.getElementById('results');
  resultsDiv.innerHTML = 'Loading...';
  const start_time = new Date().getTime();
  fetchFromAPI('average_page_size')
    .then((data) => {
      console.log('Response data:', data);  // Debugging statement
      if (!Array.isArray(data)) {
        throw new Error('Expected an array but got ' + typeof data);
      }

      const elapsed_time = new Date().getTime() - start_time;
      resultsDiv.innerHTML = '';

      // Add elapsed time display
      const elapsedTimeDiv = document.createElement('div');
      elapsedTimeDiv.style.margin = '10px 0';
      elapsedTimeDiv.textContent = `Elapsed time: ${elapsed_time}ms`;
      resultsDiv.appendChild(elapsedTimeDiv);

      const table = document.createElement('table');
      const thead = document.createElement('thead');
      const tbody = document.createElement('tbody');

      thead.innerHTML = `<tr><th>URL</th><th>Total Docs</th><th>Average Size</th><th>Total Size</th></tr>`;

      data.forEach((item) => {
        const row = document.createElement('tr');
        row.innerHTML = `<td>${item.url}</td><td>${item.total_docs}</td><td>${item.average_size}</td><td>${item.total_size}</td>`;
        tbody.appendChild(row);
      });

      table.appendChild(thead);
      table.appendChild(tbody);
      resultsDiv.appendChild(table);
    })
    .catch((error) => {
      resultsDiv.innerHTML = `Error: ${error.message}`;
      console.error('Error:', error);  // Debugging statement
    });
}
