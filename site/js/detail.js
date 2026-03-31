// --- Detail View ---

function escapeHtml(str) {
    if (!str) return '';
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

export function showDetail(data, title, viewType, { onBack }) {
    let postings, subtitle;

    if (viewType === 'federal') {
        const norm = title.toUpperCase().trim();
        postings = data.postings.filter(p => p.position_title.toUpperCase().trim() === norm);
        const plainTitles = [...new Set(postings.map(p => p.plain_title))];
        subtitle = `${postings.length} postings titled "${title}" were classified into ${plainTitles.length} plain language title${plainTitles.length !== 1 ? 's' : ''}`;
    } else {
        postings = data.postings.filter(p => p.plain_title === title);
        const fedTitles = [...new Set(postings.map(p => p.position_title))];
        subtitle = data.titleDescMap[title] || '';
        if (fedTitles.length > 0) {
            subtitle += (subtitle ? ' ' : '') + `Drawn from ${fedTitles.length} federal title variant${fedTitles.length !== 1 ? 's' : ''}.`;
        }
    }

    document.getElementById('bipartiteSection').style.display = 'none';
    document.getElementById('detail').style.display = '';
    document.getElementById('detailTitle').textContent = title;
    document.getElementById('detailSubtitle').textContent = subtitle;
    document.getElementById('backBtn').onclick = onBack;

    const container = document.getElementById('detailPostings');
    container.innerHTML = '';

    for (const p of postings) {
        const card = document.createElement('div');
        card.className = 'posting-card';

        const agency = p.agency || p.department || '';
        const grade = p.grade || '';
        const badgeText = viewType === 'federal' ? p.plain_title : p.position_title;

        const duties = p.duties || p.duties_preview || '';
        let dutiesHtml = '';
        if (duties) {
            dutiesHtml = `
                <details class="posting-duties">
                    <summary>View duties</summary>
                    <p style="margin-top:0.5rem">${escapeHtml(duties)}</p>
                </details>
            `;
        }

        const linkHtml = p.url ? `<a href="${escapeHtml(p.url)}" target="_blank" class="posting-link">View on USAJobs</a>` : '';

        card.innerHTML = `
            <div class="posting-header">
                <div>
                    <div class="posting-fed-title">${escapeHtml(viewType === 'federal' ? p.position_title : p.plain_title)}</div>
                    <div class="posting-meta">${escapeHtml(agency)}${grade ? ' &middot; ' + escapeHtml(grade) : ''}${linkHtml ? ' &middot; ' + linkHtml : ''}</div>
                </div>
                <div class="posting-plain-title">${escapeHtml(badgeText)}</div>
            </div>
            <div class="posting-reasoning">${escapeHtml(p.reasoning)}</div>
            ${dutiesHtml}
        `;
        container.appendChild(card);
    }

    window.scrollTo({ top: document.getElementById('detail').offsetTop - 60, behavior: 'smooth' });
}

export function hideDetail() {
    document.getElementById('detail').style.display = 'none';
    document.getElementById('bipartiteSection').style.display = '';
}
