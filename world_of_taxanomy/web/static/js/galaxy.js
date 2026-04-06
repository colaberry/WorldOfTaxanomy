/**
 * Galaxy View — d3-force simulation of classification systems
 * Risk 1: The Living Map / Federation Galaxy
 */

document.addEventListener('DOMContentLoaded', async () => {
  const container = document.getElementById('galaxy-viz');
  if (!container) return;

  const width = container.clientWidth;
  const height = container.clientHeight || 500;

  // Fetch system data
  const systems = await TaxonomyAPI.getSystems();
  const stats = await TaxonomyAPI.getStats();

  // Update stats display
  const totalNodes = systems.reduce((sum, s) => sum + s.node_count, 0);
  const totalEdges = stats.reduce((sum, s) => sum + s.edge_count, 0);
  document.getElementById('stat-systems').textContent = systems.length;
  document.getElementById('stat-nodes').textContent = totalNodes.toLocaleString();
  document.getElementById('stat-edges').textContent = totalEdges.toLocaleString();

  // Build nodes and links for force simulation
  const nodes = systems.map(s => ({
    id: s.id,
    name: s.name,
    fullName: s.full_name,
    region: s.region,
    nodeCount: s.node_count,
    radius: Math.max(30, Math.sqrt(s.node_count) * 1.5),
    color: s.tint_color || '#3B82F6',
  }));

  // Build edges from crosswalk stats (one direction only)
  const links = [];
  const seen = new Set();
  stats.forEach(s => {
    const key = [s.source_system, s.target_system].sort().join('|');
    if (!seen.has(key)) {
      seen.add(key);
      links.push({
        source: s.source_system,
        target: s.target_system,
        weight: s.edge_count,
      });
    }
  });

  // Create SVG
  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width} ${height}`)
    .attr('class', 'galaxy-svg');

  // Defs for glow filter
  const defs = svg.append('defs');
  const filter = defs.append('filter').attr('id', 'glow');
  filter.append('feGaussianBlur').attr('stdDeviation', '4').attr('result', 'coloredBlur');
  const merge = filter.append('feMerge');
  merge.append('feMergeNode').attr('in', 'coloredBlur');
  merge.append('feMergeNode').attr('in', 'SourceGraphic');

  // Force simulation
  const simulation = d3.forceSimulation(nodes)
    .force('link', d3.forceLink(links).id(d => d.id).distance(180))
    .force('charge', d3.forceManyBody().strength(-400))
    .force('center', d3.forceCenter(width / 2, height / 2))
    .force('collision', d3.forceCollide().radius(d => d.radius + 20));

  // Draw edges
  const link = svg.append('g')
    .selectAll('line')
    .data(links)
    .join('line')
    .attr('stroke', '#3B82F6')
    .attr('stroke-opacity', 0.15)
    .attr('stroke-width', d => Math.max(1, Math.log(d.weight) * 0.5));

  // Draw nodes
  const node = svg.append('g')
    .selectAll('g')
    .data(nodes)
    .join('g')
    .attr('cursor', 'pointer')
    .on('click', (event, d) => {
      window.location.href = `/system/${d.id}`;
    })
    .call(d3.drag()
      .on('start', (event, d) => {
        if (!event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x; d.fy = d.y;
      })
      .on('drag', (event, d) => { d.fx = event.x; d.fy = event.y; })
      .on('end', (event, d) => {
        if (!event.active) simulation.alphaTarget(0);
        d.fx = null; d.fy = null;
      })
    );

  // Node circles
  node.append('circle')
    .attr('r', d => d.radius)
    .attr('fill', d => d.color)
    .attr('fill-opacity', 0.15)
    .attr('stroke', d => d.color)
    .attr('stroke-width', 1.5)
    .attr('filter', 'url(#glow)');

  // Node labels
  node.append('text')
    .attr('text-anchor', 'middle')
    .attr('dy', -8)
    .attr('fill', '#E8E6E1')
    .attr('font-family', "'Instrument Serif', serif")
    .attr('font-size', '14px')
    .text(d => d.name);

  // Node count
  node.append('text')
    .attr('text-anchor', 'middle')
    .attr('dy', 12)
    .attr('fill', '#7A7872')
    .attr('font-family', "'Geist Mono', monospace")
    .attr('font-size', '11px')
    .text(d => `${d.nodeCount.toLocaleString()} codes`);

  // Hover effects
  node.on('mouseover', function(event, d) {
    d3.select(this).select('circle')
      .transition().duration(150)
      .attr('fill-opacity', 0.3)
      .attr('stroke-width', 2.5);
    link.attr('stroke-opacity', l =>
      (l.source.id === d.id || l.target.id === d.id) ? 0.5 : 0.08
    );
  }).on('mouseout', function() {
    d3.select(this).select('circle')
      .transition().duration(150)
      .attr('fill-opacity', 0.15)
      .attr('stroke-width', 1.5);
    link.attr('stroke-opacity', 0.15);
  });

  // Tick
  simulation.on('tick', () => {
    link
      .attr('x1', d => d.source.x)
      .attr('y1', d => d.source.y)
      .attr('x2', d => d.target.x)
      .attr('y2', d => d.target.y);
    node.attr('transform', d => `translate(${d.x},${d.y})`);
  });
});
