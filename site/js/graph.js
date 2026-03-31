// --- Bipartite Graph Rendering ---

const MAX_NODES = 25;

function svgEl(tag, attrs) {
    const el = document.createElementNS('http://www.w3.org/2000/svg', tag);
    for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
    return el;
}

export function renderBipartite(container, data, { onNodeClick }) {
    container.innerHTML = '';

    const fedSorted = Object.entries(data.fedCounts).sort((a, b) => b[1] - a[1]);
    const plainSorted = Object.entries(data.plainCounts).sort((a, b) => b[1] - a[1]);
    const topFed = fedSorted.slice(0, MAX_NODES).map(e => e[0]);
    const topPlain = plainSorted.slice(0, MAX_NODES).map(e => e[0]);
    const visibleLinks = data.links.filter(l => topFed.includes(l.fed) && topPlain.includes(l.plain));

    // Layout
    const nodeHeight = 32;
    const nodeGap = 6;
    const leftColWidth = 320;
    const rightColWidth = 280;
    const svgWidth = container.clientWidth || 900;
    const margin = { top: 20, bottom: 20 };
    const rightX = svgWidth - rightColWidth;
    const svgHeight = Math.max(topFed.length, topPlain.length) * (nodeHeight + nodeGap) + margin.top + margin.bottom;

    const svg = svgEl('svg', { width: '100%', viewBox: `0 0 ${svgWidth} ${svgHeight}`, class: 'bipartite-svg' });

    const yPos = (i) => margin.top + i * (nodeHeight + nodeGap);

    const fedIndex = Object.fromEntries(topFed.map((f, i) => [f, i]));
    const plainIndex = Object.fromEntries(topPlain.map((p, i) => [p, i]));

    // Links
    const linksGroup = svgEl('g', { class: 'bipartite-links' });
    for (const link of visibleLinks) {
        const y1 = yPos(fedIndex[link.fed]) + nodeHeight / 2;
        const y2 = yPos(plainIndex[link.plain]) + nodeHeight / 2;
        const x1 = leftColWidth, x2 = rightX;
        const cx1 = x1 + (x2 - x1) * 0.4, cx2 = x1 + (x2 - x1) * 0.6;

        const path = svgEl('path', {
            d: `M${x1},${y1} C${cx1},${y1} ${cx2},${y2} ${x2},${y2}`,
            stroke: 'var(--color-highlight)',
            'stroke-width': Math.max(1, Math.min(link.count * 1.5, 8)),
            fill: 'none',
            opacity: '0.15',
            'data-fed': link.fed,
            'data-plain': link.plain,
            class: 'bipartite-link',
        });
        linksGroup.appendChild(path);
    }
    svg.appendChild(linksGroup);

    // Helper: create a node group (fed or plain side)
    function createNode({ label, count, x, y, width, side, cssClass, fillRect, strokeRect, fillText, fillCount }) {
        const g = svgEl('g', { class: `bipartite-node ${cssClass}`, [`data-${side}`]: label });
        g.style.cursor = 'pointer';

        g.appendChild(svgEl('rect', {
            x, y, width, height: nodeHeight, rx: 4,
            fill: fillRect, stroke: strokeRect || 'none', 'stroke-width': strokeRect ? 1 : 0,
        }));

        const truncated = label.length > 38 ? label.slice(0, 36) + '...' : label;
        const text = svgEl('text', {
            x: x + 10, y: y + nodeHeight / 2 + 1,
            'dominant-baseline': 'middle', 'font-size': 12,
            fill: fillText, 'font-family': 'var(--font-heading)', 'font-weight': 500,
        });
        text.textContent = truncated;
        g.appendChild(text);

        const countEl = svgEl('text', {
            x: x + width - 8, y: y + nodeHeight / 2 + 1,
            'dominant-baseline': 'middle', 'text-anchor': 'end', 'font-size': 11,
            fill: fillCount, 'font-family': 'var(--font-mono)',
        });
        countEl.textContent = count;
        g.appendChild(countEl);

        g.addEventListener('mouseenter', () => highlightNode(svg, label, side));
        g.addEventListener('mouseleave', () => clearHighlight(svg));
        g.addEventListener('click', () => onNodeClick(label, side === 'fed' ? 'federal' : 'plain'));

        return g;
    }

    // Federal nodes (left)
    for (let i = 0; i < topFed.length; i++) {
        svg.appendChild(createNode({
            label: topFed[i], count: data.fedCounts[topFed[i]],
            x: 0, y: yPos(i), width: leftColWidth - 8, side: 'fed', cssClass: 'fed-node',
            fillRect: 'var(--color-surface)', strokeRect: 'var(--color-border)',
            fillText: 'var(--color-text)', fillCount: 'var(--color-muted)',
        }));
    }

    // Plain language nodes (right)
    for (let i = 0; i < topPlain.length; i++) {
        svg.appendChild(createNode({
            label: topPlain[i], count: data.plainCounts[topPlain[i]],
            x: rightX + 8, y: yPos(i), width: rightColWidth - 8, side: 'plain', cssClass: 'plain-node',
            fillRect: 'var(--color-primary)', strokeRect: null,
            fillText: 'white', fillCount: 'rgba(255,255,255,0.7)',
        }));
    }

    // Column headers
    const hdrAttrs = { 'font-size': 11, fill: 'var(--color-muted)', 'font-family': 'var(--font-heading)', 'font-weight': 600 };
    const leftHdr = svgEl('text', { x: 10, y: 12, ...hdrAttrs });
    leftHdr.textContent = 'FEDERAL TITLE';
    svg.appendChild(leftHdr);

    const rightHdr = svgEl('text', { x: rightX + 18, y: 12, ...hdrAttrs });
    rightHdr.textContent = 'PLAIN LANGUAGE TITLE';
    svg.appendChild(rightHdr);

    container.appendChild(svg);
}

function highlightNode(svg, name, side) {
    svg.querySelectorAll('.bipartite-link').forEach(p => p.setAttribute('opacity', '0.04'));
    svg.querySelectorAll('.bipartite-node rect').forEach(r => r.setAttribute('opacity', '0.3'));
    svg.querySelectorAll('.bipartite-node text').forEach(t => t.setAttribute('opacity', '0.3'));

    const attr = `data-${side}`;
    const otherSide = side === 'fed' ? 'plain' : 'fed';
    const connected = new Set();

    svg.querySelectorAll(`.bipartite-link[${attr}="${CSS.escape(name)}"]`).forEach(p => {
        p.setAttribute('opacity', '0.6');
        connected.add(p.getAttribute(`data-${otherSide}`));
    });

    // Restore this node + connected nodes
    const thisNode = svg.querySelector(`.${side}-node[data-${side}="${CSS.escape(name)}"]`);
    if (thisNode) thisNode.querySelectorAll('rect, text').forEach(el => el.setAttribute('opacity', '1'));

    for (const other of connected) {
        const node = svg.querySelector(`.${otherSide}-node[data-${otherSide}="${CSS.escape(other)}"]`);
        if (node) node.querySelectorAll('rect, text').forEach(el => el.setAttribute('opacity', '1'));
    }
}

function clearHighlight(svg) {
    svg.querySelectorAll('.bipartite-link').forEach(p => p.setAttribute('opacity', '0.15'));
    svg.querySelectorAll('.bipartite-node rect').forEach(r => r.setAttribute('opacity', '1'));
    svg.querySelectorAll('.bipartite-node text').forEach(t => t.setAttribute('opacity', '1'));
}
