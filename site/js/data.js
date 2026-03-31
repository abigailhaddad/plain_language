// --- Series config & data loading ---

const DATA_PATH = 'data';

let seriesList = [];
let seriesConfigMap = {};
let seriesDataCache = {};

export async function loadSeriesConfig() {
    const manifest = await fetch(`${DATA_PATH}/series.json`).then(r => r.json());

    // Only include series whose data files exist
    const checked = await Promise.all(manifest.map(async (s) => {
        const res = await fetch(`${DATA_PATH}/${s.code}_site.json`, { method: 'HEAD' });
        return res.ok ? s : null;
    }));
    seriesList = checked.filter(Boolean);

    seriesConfigMap = {};
    for (const s of seriesList) {
        seriesConfigMap[s.code] = s;
    }
    return seriesList;
}

export function getSeriesList() { return seriesList; }
export function getSeriesConfig(code) { return seriesConfigMap[code]; }
export function isValidSeries(code) { return !!seriesConfigMap[code]; }
export function defaultSeriesCode() { return seriesList[0]?.code; }

export async function loadSeriesData(code) {
    if (seriesDataCache[code]) return seriesDataCache[code];

    const [postings, coverage, titlesFile] = await Promise.all([
        fetch(`${DATA_PATH}/${code}_site.json`).then(r => r.json()),
        fetch(`${DATA_PATH}/${code}_coverage.json`).then(r => r.json()),
        fetch(`${DATA_PATH}/${code}_titles_validated.json`).then(r => r.json()),
    ]);

    const titleDescMap = {};
    for (const t of titlesFile.titles) {
        titleDescMap[t.title] = t.description;
    }

    const filtered = postings.filter(p => p.plain_title && p.plain_title !== 'none' && p.plain_title !== 'error');

    // Normalize federal titles (case-insensitive grouping)
    const fedNormMap = {};
    const fedCaseCounts = {};
    for (const p of filtered) {
        const norm = p.position_title.toUpperCase().trim();
        if (!fedCaseCounts[norm]) fedCaseCounts[norm] = {};
        fedCaseCounts[norm][p.position_title] = (fedCaseCounts[norm][p.position_title] || 0) + 1;
    }
    for (const [norm, cases] of Object.entries(fedCaseCounts)) {
        const best = Object.entries(cases).sort((a, b) => b[1] - a[1])[0][0];
        fedNormMap[norm] = best;
    }

    // Build links: [{ fed, plain, count }]
    const linkMap = {};
    for (const p of filtered) {
        const fed = fedNormMap[p.position_title.toUpperCase().trim()];
        const plain = p.plain_title;
        const key = `${fed}|||${plain}`;
        linkMap[key] = (linkMap[key] || 0) + 1;
    }

    const links = Object.entries(linkMap).map(([key, count]) => {
        const [fed, plain] = key.split('|||');
        return { fed, plain, count };
    });

    const fedCounts = {};
    const plainCounts = {};
    for (const l of links) {
        fedCounts[l.fed] = (fedCounts[l.fed] || 0) + l.count;
        plainCounts[l.plain] = (plainCounts[l.plain] || 0) + l.count;
    }

    seriesDataCache[code] = { postings: filtered, coverage, titleDescMap, links, fedCounts, plainCounts, fedNormMap };
    return seriesDataCache[code];
}
