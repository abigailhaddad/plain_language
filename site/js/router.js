// --- URL Hash State ---

let updatingHash = false;

export function encodeHashState(state) {
    const params = new URLSearchParams();
    if (state.series) params.set('series', state.series);
    if (state.title) params.set('title', state.title);
    if (state.view) params.set('view', state.view);
    return params.toString();
}

export function decodeHashState() {
    const hash = location.hash.replace(/^#/, '');
    if (!hash) return {};
    const params = new URLSearchParams(hash);
    return {
        series: params.get('series') || null,
        title: params.get('title') || null,
        view: params.get('view') || null,
    };
}

export function pushHashState(state) {
    updatingHash = true;
    const encoded = encodeHashState(state);
    history.pushState(null, '', encoded ? '#' + encoded : location.pathname);
    updatingHash = false;
}

export function onHashChange(callback) {
    const handler = () => { if (!updatingHash) callback(); };
    window.addEventListener('popstate', handler);
    window.addEventListener('hashchange', handler);
}
