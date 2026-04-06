/**
 * Node View — interactive tree sidebar and content panel
 */

document.addEventListener('DOMContentLoaded', () => {
  // Search functionality
  const searchInput = document.getElementById('search-input');
  const searchResults = document.getElementById('search-results');

  if (searchInput) {
    let debounceTimer;
    searchInput.addEventListener('input', () => {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(async () => {
        const query = searchInput.value.trim();
        if (query.length < 2) {
          searchResults.classList.add('hidden');
          return;
        }
        const results = await TaxonomyAPI.search(query, null, 8);
        if (results.length > 0) {
          searchResults.innerHTML = results.map(r => `
            <a href="/system/${r.system_id}/${r.code}" class="search-result-item">
              <span class="code">${r.code}</span>
              <span class="title">${r.title}</span>
              <span class="system">${r.system_id}</span>
            </a>
          `).join('');
          searchResults.classList.remove('hidden');
        } else {
          searchResults.innerHTML = '<div class="search-no-results">No results</div>';
          searchResults.classList.remove('hidden');
        }
      }, 300);
    });

    // Close search results on click outside
    document.addEventListener('click', (e) => {
      if (!searchInput.contains(e.target) && !searchResults.contains(e.target)) {
        searchResults.classList.add('hidden');
      }
    });
  }
});
