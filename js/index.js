const form = document.getElementById('searchForm');
const resultDiv = document.getElementById('result');

form.addEventListener('submit', (event) => {
  event.preventDefault();

  const query = document.getElementById('query').value;

  // fetch('http://0.0.0.0:8000/search', {
  fetch('http://fungthedev.fun/api/search', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query }),
  })
    .then((response) => response.json())
    .then((data) => {
      if (data.results && data.results.length > 0) {
        resultDiv.innerHTML = `
          <div class="elaspse-time">
            <p>Elapsed Time: ${data.elapsed_time} seconds</p>
          </div>
        ` + data.results.map(result => `
          <div class="result-item">
            <h3>${result.title}</h3>
            <p>${result.snippet}</p>
            <a href="${result.url}" target="_blank">${result.url}</a>
            <p>Score: ${result.score}</p>
          </div>
        `).join('');
      } else {
        resultDiv.innerHTML = '<p>No results found.</p>';
      }
    })
    .catch((error) => {
      resultDiv.innerHTML = `Error: ${error.message}`;
    });
});
