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
        // Remove CSS classes from body and h1
        document.body.classList.remove('transition-all', 'duration-1000', 'ease-in-out', 'mt-72', 'text-center', 'animate-fadeIn');
        const h1 = document.querySelector('h1');
        const button = document.querySelector('button');
        const input = document.querySelector('input');
        h1.classList.remove('transition-all', 'duration-1000', 'ease-in-out', 'scale-150', 'mt-72', 'text-center', 'animate-fadeIn');
        button.classList.remove('pre-search-margin');
        input.classList.remove('pre-search-margin');

        resultDiv.innerHTML = `
          <div class="elaspse-time">
            <p>Elapsed Time (Index Search Time): ${data.elapsed_time} ms</p>
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
