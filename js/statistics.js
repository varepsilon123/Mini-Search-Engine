document.getElementById('fetchIndexedPages').addEventListener('click', () => {
  fetchData('indexed_pages', 'URL (Alphabetical Order)', 'Indexed Pages', 'page_count');
});

document.getElementById('fetchTotalCrawlTime').addEventListener('click', () => {
  fetchData('total_crawl_time', 'URL', 'Total Crawl Time', 'total_crawl_time');
});

document.getElementById('fetchAveragePageSize').addEventListener('click', () => {
  fetchData('average_page_size', 'Metric', 'Value', 'average_page_size', true);
});

function fetchData(endpoint, col1Header, col2Header, col2Key, isSingleValue = false) {
  const resultsDiv = document.getElementById('results');
  resultsDiv.innerHTML = 'Loading...';
  const start_time = new Date().getTime();
//   fetch(`http://0.0.0.0:8000/${endpoint}`)
  fetch(`http://fungthedev.fun/api/${endpoint}`)
    .then((response) => response.json())
    .then((data) => {
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

      thead.innerHTML = `
        <tr>
          <th>${col1Header}</th>
          <th>${col2Header}</th>
        </tr>
      `;

      if (isSingleValue) {
        const row = document.createElement('tr');
        row.innerHTML = `
          <td>${col2Header}</td>
          <td>${data[col2Key]}</td>
        `;
        tbody.appendChild(row);
      } else {
        data.forEach((item) => {
          const row = document.createElement('tr');
          row.innerHTML = `
            <td>${item.url}</td>
            <td>${item[col2Key]}</td>
          `;
          tbody.appendChild(row);
        });
      }

      table.appendChild(thead);
      table.appendChild(tbody);
      resultsDiv.appendChild(table);
    })
    .catch((error) => {
      resultsDiv.innerHTML = `Error: ${error.message}`;
    });
}
