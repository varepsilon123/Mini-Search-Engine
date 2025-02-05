document.getElementById('fetchData').addEventListener('click', () => {
  const resultsDiv = document.getElementById('results');
  resultsDiv.innerHTML = 'Loading...';
  const start_time = new Date().getTime();
  // fetch('http://0.0.0.0:8000/indexed_pages')
  fetch('http://fungthedev.fun/api/indexed_pages')
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
          <th>URL (Alphabetical Order)</th>
          <th>Indexed Pages</th>
        </tr>
      `;

      data.forEach((item) => {
        const row = document.createElement('tr');
        row.innerHTML = `
          <td>${item.url}</td>
          <td>${item.page_count}</td>
        `;
        tbody.appendChild(row);
      });

      table.appendChild(thead);
      table.appendChild(tbody);
      resultsDiv.appendChild(table);
    })
    .catch((error) => {
      resultsDiv.innerHTML = `Error: ${error.message}`;
    });
});
