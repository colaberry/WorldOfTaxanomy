/**
 * Sector View — d3 treemap of sectors within a system
 * Sectors colored by hue, clickable to drill into node view.
 */

document.addEventListener('DOMContentLoaded', async () => {
  const container = document.getElementById('treemap-viz');
  if (!container) return;

  const systemId = container.dataset.systemId;
  if (!systemId) return;

  const width = container.clientWidth;
  const height = container.clientHeight || 500;

  // Sector color map
  const sectorColors = {
    '11': '#4ADE80', '21': '#F59E0B', '22': '#06B6D4', '23': '#EF4444',
    '31-33': '#8B5CF6', '42': '#EC4899', '44-45': '#F97316', '48-49': '#14B8A6',
    '51': '#3B82F6', '52': '#6366F1', '53': '#A78BFA', '54': '#10B981',
    '55': '#64748B', '56': '#78716C', '61': '#2563EB', '62': '#0D9488',
    '71': '#E11D48', '72': '#D97706', '81': '#9CA3AF', '92': '#1E40AF',
    // ISIC sections
    'A': '#4ADE80', 'B': '#F59E0B', 'C': '#8B5CF6', 'D': '#06B6D4',
    'E': '#14B8A6', 'F': '#EF4444', 'G': '#F97316', 'H': '#14B8A6',
    'I': '#D97706', 'J': '#3B82F6', 'K': '#6366F1', 'L': '#A78BFA',
    'M': '#10B981', 'N': '#78716C', 'O': '#1E40AF', 'P': '#2563EB',
    'Q': '#0D9488', 'R': '#E11D48', 'S': '#9CA3AF', 'T': '#64748B',
    'U': '#7A7872',
  };

  // Fetch roots (sectors)
  const system = await TaxonomyAPI.getSystem(systemId);
  const roots = system.roots;

  if (!roots || roots.length === 0) return;

  // Build treemap data
  const treemapData = {
    name: system.name,
    children: roots.map(r => ({
      name: r.title,
      code: r.code,
      value: Math.max(1, r.seq_order || 1),
      color: sectorColors[r.code] || '#3B82F6',
    })),
  };

  const root = d3.hierarchy(treemapData)
    .sum(d => d.value)
    .sort((a, b) => b.value - a.value);

  d3.treemap()
    .size([width, height])
    .paddingInner(3)
    .paddingOuter(6)
    .round(true)(root);

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width} ${height}`);

  const cell = svg.selectAll('g')
    .data(root.leaves())
    .join('g')
    .attr('transform', d => `translate(${d.x0},${d.y0})`)
    .attr('cursor', 'pointer')
    .on('click', (event, d) => {
      window.location.href = `/system/${systemId}/${d.data.code}`;
    });

  // Rectangles
  cell.append('rect')
    .attr('width', d => d.x1 - d.x0)
    .attr('height', d => d.y1 - d.y0)
    .attr('fill', d => d.data.color)
    .attr('fill-opacity', 0.2)
    .attr('stroke', d => d.data.color)
    .attr('stroke-opacity', 0.4)
    .attr('stroke-width', 1)
    .attr('rx', 4)
    .on('mouseover', function() {
      d3.select(this).attr('fill-opacity', 0.35).attr('stroke-opacity', 0.7);
    })
    .on('mouseout', function() {
      d3.select(this).attr('fill-opacity', 0.2).attr('stroke-opacity', 0.4);
    });

  // Code labels
  cell.append('text')
    .attr('x', 8)
    .attr('y', 20)
    .attr('fill', '#A8A69E')
    .attr('font-family', "'Geist Mono', monospace")
    .attr('font-size', '11px')
    .text(d => d.data.code)
    .attr('opacity', d => (d.x1 - d.x0) > 50 ? 1 : 0);

  // Title labels
  cell.append('text')
    .attr('x', 8)
    .attr('y', 36)
    .attr('fill', '#E8E6E1')
    .attr('font-family', "'Instrument Serif', serif")
    .attr('font-size', '13px')
    .text(d => {
      const maxChars = Math.floor((d.x1 - d.x0) / 7);
      return d.data.name.length > maxChars
        ? d.data.name.slice(0, maxChars - 1) + '…'
        : d.data.name;
    })
    .attr('opacity', d => (d.x1 - d.x0) > 80 ? 1 : 0);
});
