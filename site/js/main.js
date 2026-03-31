import { loadSeriesConfig, getSeriesList, getSeriesConfig, isValidSeries, defaultSeriesCode, loadSeriesData } from './data.js';
import { decodeHashState, pushHashState, onHashChange } from './router.js';
import { renderBipartite } from './graph.js';
import { showDetail, hideDetail } from './detail.js';

let currentSeries = null;

// --- Tabs ---

function renderSeriesTabs() {
    const container = document.getElementById('seriesTabs');
    container.innerHTML = '';
    for (const s of getSeriesList()) {
        const tab = document.createElement('button');
        tab.className = `series-tab${s.code === currentSeries ? ' active' : ''}`;
        tab.textContent = `${s.code} - ${s.name}`;
        tab.onclick = () => switchSeries(s.code);
        container.appendChild(tab);
    }
}

// --- Stats ---

function renderStats(data) {
    const config = getSeriesConfig(currentSeries);
    const numPlain = Object.keys(data.plainCounts).length;
    const numFed = Object.keys(data.fedCounts).length;
    const total = data.postings.length;

    document.getElementById('seriesIntro').innerHTML =
        `The <strong>${config.code} - ${config.name}</strong> series titles everyone ` +
        `"${config.fedTitle}." We analyzed ${total.toLocaleString()} postings and found ` +
        `${numPlain} distinct roles hiding behind ${numFed} federal title variants.`;

    document.getElementById('statsGrid').innerHTML = `
        <div class="stat-card">
            <div class="stat-value">${total.toLocaleString()}</div>
            <div class="stat-label">Postings Analyzed</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">${numFed}</div>
            <div class="stat-label">Federal Title Variants</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">${numPlain}</div>
            <div class="stat-label">Plain Language Titles</div>
        </div>
    `;
}

// --- Series switching ---

async function switchSeries(code, skipHash) {
    currentSeries = code;
    renderSeriesTabs();
    hideDetail();

    if (!skipHash) pushHashState({ series: code });

    const data = await loadSeriesData(code);
    renderStats(data);
    renderBipartite(document.getElementById('bipartiteContainer'), data, {
        onNodeClick: (title, viewType) => handleShowDetail(title, viewType),
    });
}

// --- Detail wiring ---

async function handleShowDetail(title, viewType, skipHash) {
    const data = await loadSeriesData(currentSeries);
    if (!skipHash) pushHashState({ series: currentSeries, title, view: viewType });
    showDetail(data, title, viewType, {
        onBack: () => handleHideDetail(),
    });
}

function handleHideDetail(skipHash) {
    hideDetail();
    if (!skipHash) pushHashState({ series: currentSeries });
}

// --- Routing ---

async function navigateToHash() {
    const state = decodeHashState();
    const series = isValidSeries(state.series) ? state.series : defaultSeriesCode();

    if (series !== currentSeries) {
        await switchSeries(series, true);
    }

    if (state.title && state.view) {
        await handleShowDetail(state.title, state.view, true);
    } else {
        handleHideDetail(true);
    }
}

// --- Init ---

async function init() {
    await loadSeriesConfig();
    renderSeriesTabs();
    await navigateToHash();
    onHashChange(navigateToHash);
}

init();
