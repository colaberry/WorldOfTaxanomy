/**
 * WorldOfTaxanomy API Client
 * Thin wrapper around fetch() for the REST API.
 */

const API_BASE = '/api/v1';

const api = {
  async getSystems() {
    const resp = await fetch(`${API_BASE}/systems`);
    return resp.json();
  },

  async getSystem(systemId) {
    const resp = await fetch(`${API_BASE}/systems/${systemId}`);
    if (!resp.ok) throw new Error(`System '${systemId}' not found`);
    return resp.json();
  },

  async getNode(systemId, code) {
    const resp = await fetch(`${API_BASE}/systems/${systemId}/nodes/${code}`);
    if (!resp.ok) throw new Error(`Node '${code}' not found`);
    return resp.json();
  },

  async getChildren(systemId, parentCode) {
    const resp = await fetch(`${API_BASE}/systems/${systemId}/nodes/${parentCode}/children`);
    return resp.json();
  },

  async getAncestors(systemId, code) {
    const resp = await fetch(`${API_BASE}/systems/${systemId}/nodes/${code}/ancestors`);
    return resp.json();
  },

  async getEquivalences(systemId, code) {
    const resp = await fetch(`${API_BASE}/systems/${systemId}/nodes/${code}/equivalences`);
    return resp.json();
  },

  async search(query, systemId = null, limit = 20) {
    const params = new URLSearchParams({ q: query, limit });
    if (systemId) params.set('system', systemId);
    const resp = await fetch(`${API_BASE}/search?${params}`);
    return resp.json();
  },

  async getStats() {
    const resp = await fetch(`${API_BASE}/equivalences/stats`);
    return resp.json();
  },
};

// Export for use in other scripts
window.TaxonomyAPI = api;
