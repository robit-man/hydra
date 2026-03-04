import { CFG, saveCFG } from './config.js';
import { setBadge, log, qs, b64ToBytes, SIGNOFF_RE, td } from './utils.js';
import { makeTransportButtonUpdater } from './transport.js';
import { Net } from './net.js';
import { Router } from './router.js';
import { NodeStore } from './nodeStore.js';
import { createSentenceMux, makeNdjsonPump, stripEOT } from './sentence.js';
import { setupQrScanner, openQrScanner, closeQrScanner, registerQrResultHandler } from './qrScanner.js';
import { createLLM } from './llm.js';
import { createTTS } from './tts.js';
import { createASR } from './asr.js';
import { createNknDM } from './nknDm.js';
import { createMCP } from './mcp.js';
import { createMediaNode } from './mediaNode.js';
import { createOrientationNode } from './orientation.js';
import { createLocationNode } from './location.js';
import { createFileTransfer } from './fileTransfer.js';
import { createWorkspaceSync } from './workspaceSync.js';
import { createGraph } from './graph.js';
import { createFlowsLibrary } from './flows.js';
import { createMeshtastic } from './meshtastic.js';
import { createWebSerial } from './webSerial.js';
import { createVision } from './vision.js';
import { createPointcloud } from './pointcloud.js';
import { primeLocalNetworkRequest } from './localNetwork.js';
import { createPeerDiscovery } from './peerDiscovery.js';
import { createPayments } from './payments.js';
import { createWebScraper } from './webScraper.js';
import { createNoClipBridge } from './noclipBridge.js';
import { initSmartObjectInvite } from './smartObjectInvite.js';
import { createNoClipBridgeSync } from './noclipBridgeSync.js';
import { createRouterDiscovery } from './routerDiscovery.js';
import { normalizeResolvedMap } from './endpointResolver.js';

const updateTransportButton = makeTransportButtonUpdater({ CFG, Net });
Net.setTransportUpdater(updateTransportButton);
primeLocalNetworkRequest();
let MarketplaceSummary = null;
let MarketplaceDirectory = null;
let MarketplaceConfigEditor = null;
let SharedPeerDiscovery = null;

const graphAccess = {
  getNode: () => null
};

let applyRelayState = () => {};
const setRelayState = (...args) => applyRelayState(...args);

const LLM = createLLM({
  getNode: (id) => graphAccess.getNode(id),
  NodeStore,
  Router,
  Net,
  CFG,
  createSentenceMux,
  makeNdjsonPump,
  stripEOT,
  log,
  setRelayState
});

const TTS = createTTS({
  getNode: (id) => graphAccess.getNode(id),
  NodeStore,
  Net,
  CFG,
  log,
  b64ToBytes,
  setRelayState,
  Router
});

const ASR = createASR({
  getNode: (id) => graphAccess.getNode(id),
  Router,
  NodeStore,
  Net,
  CFG,
  SIGNOFF_RE,
  td,
  setBadge,
  log,
  setRelayState
});

const NknDM = createNknDM({
  getNode: (id) => graphAccess.getNode(id),
  NodeStore,
  Net,
  CFG,
  Router,
  log,
  setRelayState
});

const MCP = createMCP({
  getNode: (id) => graphAccess.getNode(id),
  NodeStore,
  Router,
  Net,
  CFG,
  log,
  setBadge,
  setRelayState
});

const Media = createMediaNode({
  getNode: (id) => graphAccess.getNode(id),
  Router,
  NodeStore,
  Net,
  setBadge,
  log,
  setRelayState
});

const Orientation = createOrientationNode({
  getNode: (id) => graphAccess.getNode(id),
  Router,
  NodeStore,
  setBadge,
  log
});

const LocationNode = createLocationNode({
  getNode: (id) => graphAccess.getNode(id),
  Router,
  NodeStore,
  setBadge,
  log
});

const FileTransfer = createFileTransfer({
  getNode: (id) => graphAccess.getNode(id),
  NodeStore,
  Router,
  log,
  setBadge
});

const Payments = createPayments({
  Router,
  NodeStore,
  CFG,
  setBadge,
  log
});

const WebScraper = createWebScraper({
  getNode: (id) => graphAccess.getNode(id),
  NodeStore,
  Router,
  Net,
  CFG,
  setBadge,
  log
});

const Meshtastic = createMeshtastic({
  getNode: (id) => graphAccess.getNode(id),
  NodeStore,
  Router,
  log,
  setBadge
});

const WebSerial = createWebSerial({
  getNode: (id) => graphAccess.getNode(id),
  NodeStore,
  Router,
  log,
  setBadge
});

const Vision = createVision({
  getNode: (id) => graphAccess.getNode(id),
  Router,
  NodeStore,
  FaceViewer: null,
  setBadge,
  log
});

const Pointcloud = createPointcloud({
  getNode: (id) => graphAccess.getNode(id),
  Router,
  NodeStore,
  CFG,
  setBadge,
  log
});

const NoClipBridge = createNoClipBridge({
  NodeStore,
  Router,
  Net,
  CFG,
  setBadge,
  log
});

if (CFG?.routerMarketplaceCatalog && typeof CFG.routerMarketplaceCatalog === 'object') {
  try {
    NoClipBridge?.onRouterCatalog?.(CFG.routerMarketplaceCatalog, {
      broadcast: false,
      silent: true
    });
  } catch (err) {
    log(`[router.catalog] failed to seed cached catalog: ${err?.message || err}`);
  }
}

window.addEventListener('hydra-provider-publish-catalog', async (event) => {
  try {
    const detail = event?.detail && typeof event.detail === 'object' ? event.detail : {};
    const publishScope = typeof detail.publishScope === 'string' ? detail.publishScope : '';
    const resultEl = qs('#marketplacePublishResult');
    const setResult = (text, ok = true) => {
      if (!resultEl) return;
      resultEl.textContent = String(text || 'publish idle');
      resultEl.style.color = ok ? 'var(--ok)' : 'var(--err)';
    };
    const result = await NoClipBridge?.publishMarketplaceCatalog?.({
      targetPub: typeof detail.targetPub === 'string' ? detail.targetPub : '',
      includeStatus: detail.includeStatus !== false,
      force: detail.force === true,
      silent: detail.silent === true,
      publishScope
    });
    const scopeLabel = String(result?.publishScope || publishScope || 'targeted');
    const targeted = result?.targeted && typeof result.targeted === 'object' ? result.targeted : {};
    const broadcast = result?.broadcast && typeof result.broadcast === 'object' ? result.broadcast : {};
    const targetedAttempted = Number(targeted.attempted || 0);
    const targetedSent = Number(targeted.sent || 0);
    const broadcastAttempted = Number(broadcast.attempted || 0);
    const broadcastSent = Number(broadcast.sent || 0);
    const summaryBits = [];
    if (targetedAttempted > 0 || scopeLabel === 'targeted' || scopeLabel === 'both') {
      summaryBits.push(`targeted ${targetedSent}/${targetedAttempted}`);
    }
    if (broadcastAttempted > 0 || scopeLabel === 'broadcast' || scopeLabel === 'both') {
      summaryBits.push(`broadcast ${broadcastSent}/${broadcastAttempted}`);
    }
    const summaryText = summaryBits.length ? summaryBits.join(' • ') : `${result?.sent || 0}/${result?.attempted || 0}`;
    if (result?.ok) {
      setBadge(`Marketplace catalog published (${summaryText})`);
      setResult(`${scopeLabel} ok • ${summaryText}`, true);
    } else {
      setBadge('Marketplace catalog publish skipped', false);
      const reason = String(result?.reason || result?.error || 'skipped').trim() || 'skipped';
      setResult(`${scopeLabel} skipped • ${reason}`, false);
    }
  } catch (err) {
    setBadge(`Marketplace catalog publish failed: ${err?.message || err}`, false);
    const resultEl = qs('#marketplacePublishResult');
    if (resultEl) {
      resultEl.textContent = `publish error • ${err?.message || err}`;
      resultEl.style.color = 'var(--err)';
    }
  }
});

const applyRouterResolvedPayload = (reply) => {
  const resolved = normalizeResolvedMap(reply);
  const catalogRaw = reply?.catalog?.raw || reply?.catalog || reply?.rawReply?.catalog || null;
  const catalogGeneratedTs = Number(
    reply?.catalog?.generatedAtMs ||
    reply?.catalog?.generated_at_ms ||
    catalogRaw?.generated_at_ms ||
    catalogRaw?.generatedAtMs ||
    0
  );
  const catalogSource = String(
    reply?.catalog?.source ||
    catalogRaw?.discovery_source ||
    catalogRaw?.discoverySource ||
    'router-resolve'
  ).trim();
  const catalogSourcePriority = Number(reply?.catalog?.sourcePriority || 0);
  if ((!resolved || typeof resolved !== 'object') && !catalogRaw) return;
  const incomingTs = Number(reply?.timestampMs || reply?.rawReply?.timestamp_ms || catalogGeneratedTs || 0);
  const currentTs = Number(CFG.routerLastResolvedAt || 0);
  const routerApiBase = String(
    reply?.network?.local ||
    reply?.reply?.network?.local ||
    reply?.rawReply?.network?.local ||
    CFG.routerControlPlaneApiBase ||
    ''
  ).trim();
  if (routerApiBase) {
    CFG.routerControlPlaneApiBase = routerApiBase.replace(/\/+$/, '');
  }
  if (incomingTs > 0 && currentTs > 0 && incomingTs < currentTs) {
    log(`[router.resolve] stale endpoint payload ignored ts=${incomingTs} current=${currentTs}`);
    return;
  }
  const autoApplyEnabled = CFG?.featureFlags?.resolverAutoApply !== false;
  if (autoApplyEnabled) {
    CFG.routerResolvedEndpoints = resolved;
  } else {
    log('[router.resolve] auto-apply disabled by featureFlags.resolverAutoApply');
  }
  CFG.routerLastResolveResult = reply?.rawReply || reply;
  if (Number.isFinite(incomingTs) && incomingTs > 0) {
    CFG.routerLastResolvedAt = incomingTs;
  }
  if (catalogRaw && typeof catalogRaw === 'object') {
    CFG.routerMarketplaceCatalog = catalogRaw;
    if (Number.isFinite(catalogGeneratedTs) && catalogGeneratedTs > 0) {
      CFG.routerLastCatalogAt = catalogGeneratedTs;
    }
    if (catalogSource) CFG.routerLastCatalogSource = catalogSource;
    if (Number.isFinite(catalogSourcePriority)) {
      CFG.routerLastCatalogSourcePriority = Math.max(0, Math.floor(catalogSourcePriority));
    }
    try {
      NoClipBridge?.onRouterCatalog?.(catalogRaw, { includeStatus: true });
    } catch (err) {
      log(`[router.catalog] bridge sync hook failed: ${err?.message || err}`);
    }
    try {
      MarketplaceSummary?.onCatalog?.(catalogRaw, {
        updatedAtMs: Number(catalogGeneratedTs || incomingTs || Date.now()) || Date.now()
      });
    } catch (_) {
      // ignore marketplace panel update failures
    }
    try {
      window.dispatchEvent(new CustomEvent('hydra-router-catalog', { detail: catalogRaw }));
    } catch (_) {
      // ignore DOM event dispatch errors
    }
  }
  saveCFG();
};

const MARKET_STATE = Object.freeze({
  ONLINE: 'online',
  DEGRADED: 'degraded',
  OFFLINE: 'offline'
});

const MARKET_STATE_LABEL = Object.freeze({
  online: 'online',
  degraded: 'degraded',
  offline: 'offline'
});

const normalizeMarketState = ({ status = '', healthy = null } = {}) => {
  const key = String(status || '').trim().toLowerCase();
  if (healthy === true || key === 'online' || key === 'ok' || key === 'healthy') {
    return MARKET_STATE.ONLINE;
  }
  if (
    key === 'degraded' ||
    key === 'warn' ||
    key === 'warning' ||
    key === 'limited' ||
    key === 'partial'
  ) {
    return MARKET_STATE.DEGRADED;
  }
  if (!key || key === 'offline' || key === 'error' || key === 'unhealthy' || key === 'down') {
    return MARKET_STATE.OFFLINE;
  }
  return healthy === false ? MARKET_STATE.OFFLINE : MARKET_STATE.DEGRADED;
};

const formatRelativeMs = (value) => {
  const delta = Number(value || 0);
  if (!Number.isFinite(delta) || delta <= 0) return 'unknown';
  const sec = Math.floor(delta / 1000);
  if (sec < 60) return `${sec}s`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h`;
  return `${Math.floor(hr / 24)}d`;
};

const createMarketplaceSummaryPanel = () => {
  const ui = {
    root: qs('#marketplaceSummaryCard'),
    freshness: qs('#marketplaceFreshness'),
    provider: qs('#marketplaceProviderLabel'),
    healthBadge: qs('#marketplaceHealthBadge'),
    transportBadge: qs('#marketplaceTransportBadge'),
    creditPreview: qs('#marketplaceCreditPreview'),
    quotePreview: qs('#marketplaceQuotePreview'),
    directoryPreview: qs('#marketplaceDirectoryPreview'),
    services: qs('#marketplaceServiceBadges')
  };
  if (!ui.root) {
    return {
      onCatalog: () => {},
      onStatus: () => {},
      onCreditPreview: () => {},
      onQuotePreview: () => {},
      onDirectoryPreview: () => {},
      start: () => {},
      stop: () => {}
    };
  }

  const state = {
    catalog: CFG?.routerMarketplaceCatalog && typeof CFG.routerMarketplaceCatalog === 'object'
      ? CFG.routerMarketplaceCatalog
      : null,
    statusByService: {},
    lastUpdatedMs: Number(CFG?.routerLastCatalogAt || 0),
    creditText: 'Credits: --',
    quoteText: 'Estimate: --',
    directoryText: 'Directory: --'
  };

  const pickTransport = (services = []) => {
    const counts = new Map();
    services.forEach((service) => {
      const key = String(service.selected_transport || service.selectedTransport || service.transport || '').trim().toLowerCase();
      if (!key) return;
      counts.set(key, (counts.get(key) || 0) + 1);
    });
    let selected = '';
    let maxCount = -1;
    counts.forEach((count, key) => {
      if (count > maxCount) {
        maxCount = count;
        selected = key;
      }
    });
    return selected || '--';
  };

  const coerceServices = (catalogLike) => {
    if (!catalogLike || typeof catalogLike !== 'object') return [];
    const list = Array.isArray(catalogLike.services) ? catalogLike.services : [];
    return list
      .map((entry) => {
        if (!entry || typeof entry !== 'object') return null;
        const serviceId = String(entry.service_id || entry.serviceId || entry.service || '').trim();
        if (!serviceId) return null;
        const statusOverride = state.statusByService[serviceId];
        return {
          serviceId,
          status: String(statusOverride?.status || entry.status || '').trim().toLowerCase(),
          healthy: statusOverride?.healthy === true || entry.healthy === true,
          visibility: String(
            statusOverride?.visibility ||
            entry.visibility ||
            'public'
          ).trim().toLowerCase() || 'public',
          selectedTransport: String(
            statusOverride?.selectedTransport ||
            entry.selected_transport ||
            entry.selectedTransport ||
            entry.transport ||
            ''
          ).trim().toLowerCase()
        };
      })
      .filter(Boolean);
  };

  const render = () => {
    const catalog = state.catalog && typeof state.catalog === 'object' ? state.catalog : {};
    const provider = catalog.provider && typeof catalog.provider === 'object' ? catalog.provider : {};
    const summary = catalog.summary && typeof catalog.summary === 'object' ? catalog.summary : {};
    const services = coerceServices(catalog);

    const serviceCount = services.length || Number(summary.serviceCount || 0) || 0;
    const healthyCount = services.filter((service) => normalizeMarketState(service) === MARKET_STATE.ONLINE).length;
    let stateLabel = MARKET_STATE.OFFLINE;
    if (serviceCount > 0 && healthyCount === serviceCount) stateLabel = MARKET_STATE.ONLINE;
    else if (healthyCount > 0) stateLabel = MARKET_STATE.DEGRADED;

    const providerLabel = String(
      provider.provider_label ||
      provider.providerLabel ||
      provider.provider_id ||
      provider.providerId ||
      'No provider catalog'
    ).trim();
    ui.provider.textContent = providerLabel || 'No provider catalog';
    ui.provider.title = providerLabel || 'No provider catalog';

    ui.healthBadge.textContent = MARKET_STATE_LABEL[stateLabel] || 'offline';
    ui.healthBadge.classList.remove('state-online', 'state-degraded', 'state-offline');
    ui.healthBadge.classList.add(`state-${stateLabel}`);

    const transport = pickTransport(services);
    ui.transportBadge.textContent = `transport: ${transport}`;

    ui.creditPreview.textContent = state.creditText;
    ui.quotePreview.textContent = state.quoteText;
    if (ui.directoryPreview) ui.directoryPreview.textContent = state.directoryText;

    const now = Date.now();
    const ageMs = state.lastUpdatedMs > 0 ? Math.max(0, now - state.lastUpdatedMs) : Number.POSITIVE_INFINITY;
    const fresh = Number.isFinite(ageMs) && ageMs <= 45_000;
    const freshness = fresh ? `fresh ${formatRelativeMs(ageMs)}` : (Number.isFinite(ageMs) ? `stale ${formatRelativeMs(ageMs)}` : 'stale');
    ui.freshness.textContent = freshness;
    ui.freshness.classList.toggle('is-fresh', fresh);
    ui.freshness.classList.toggle('is-stale', !fresh);

    ui.services.innerHTML = '';
    if (!services.length) {
      const empty = document.createElement('span');
      empty.className = 'market-service-chip state-offline';
      empty.textContent = 'No published services';
      ui.services.appendChild(empty);
      return;
    }
    services.slice(0, 8).forEach((service) => {
      const serviceState = normalizeMarketState(service);
      const chip = document.createElement('span');
      chip.className = `market-service-chip state-${serviceState}`;
      const visibility = service.visibility === 'friends' ? 'friends' : 'public';
      const transportText = service.selectedTransport || '--';
      chip.innerHTML = `<span class="service-id">${service.serviceId}</span><span class="service-transport">${transportText}</span><span class="service-visibility">${visibility}</span>`;
      chip.title = `${service.serviceId} • ${serviceState} • ${transportText} • ${visibility}`;
      ui.services.appendChild(chip);
    });
  };

  const onCatalog = (catalogRaw, meta = {}) => {
    if (!catalogRaw || typeof catalogRaw !== 'object') return;
    state.catalog = catalogRaw;
    state.lastUpdatedMs = Number(
      meta.updatedAtMs ||
      catalogRaw.generated_at_ms ||
      catalogRaw.generatedAtMs ||
      Date.now()
    ) || Date.now();
    render();
  };

  const onStatus = (payload = {}) => {
    const list = Array.isArray(payload.services) ? payload.services : [];
    const next = {};
    list.forEach((entry) => {
      if (!entry || typeof entry !== 'object') return;
      const serviceId = String(entry.service_id || entry.serviceId || entry.service || '').trim();
      if (!serviceId) return;
      next[serviceId] = {
        status: String(entry.status || '').trim().toLowerCase(),
        healthy: entry.healthy === true,
        visibility: String(entry.visibility || '').trim().toLowerCase() || 'public',
        selectedTransport: String(entry.selected_transport || entry.selectedTransport || entry.transport || '').trim().toLowerCase()
      };
    });
    state.statusByService = {
      ...state.statusByService,
      ...next
    };
    state.lastUpdatedMs = Number(payload.updatedAtMs || payload.ts || Date.now()) || Date.now();
    render();
  };

  const onCreditPreview = (detail = {}) => {
    const credits = String(detail.text || detail.label || '').trim();
    if (credits) state.creditText = credits;
    state.lastUpdatedMs = Number(detail.updatedAtMs || Date.now()) || Date.now();
    render();
  };

  const onQuotePreview = (detail = {}) => {
    const quote = String(detail.text || detail.label || '').trim();
    if (quote) state.quoteText = quote;
    state.lastUpdatedMs = Number(detail.updatedAtMs || Date.now()) || Date.now();
    render();
  };

  const onDirectoryPreview = (detail = {}) => {
    const ok = detail?.ok !== false;
    if (!ok) {
      const reason = String(detail.error || detail.reason || 'unavailable').trim() || 'unavailable';
      state.directoryText = `Directory: ${reason}`;
      render();
      return;
    }
    const total = Number(
      detail.total ??
      detail.totalCount ??
      detail.diagnostics?.totalCount ??
      detail.diagnostics?.total_count ??
      0
    );
    const fresh = Number(
      detail.freshCount ??
      detail.diagnostics?.freshCount ??
      detail.diagnostics?.fresh_count ??
      0
    );
    const stale = Number(
      detail.staleCount ??
      detail.diagnostics?.staleCount ??
      detail.diagnostics?.stale_count ??
      0
    );
    const providerLabel = String(
      detail.primaryProviderLabel ||
      detail.providerLabel ||
      detail.provider?.providerLabel ||
      ''
    ).trim();
    const totalText = Number.isFinite(total) ? Math.max(0, Math.floor(total)) : 0;
    const freshText = Number.isFinite(fresh) ? Math.max(0, Math.floor(fresh)) : 0;
    const staleText = Number.isFinite(stale) ? Math.max(0, Math.floor(stale)) : 0;
    let text = `Directory: ${totalText} provider${totalText === 1 ? '' : 's'} • fresh ${freshText} • stale ${staleText}`;
    const importedPeers = Number(
      detail.importSummary?.imported ??
      detail.importedPeers ??
      -1
    );
    if (Number.isFinite(importedPeers) && importedPeers >= 0) {
      text += ` • peers ${Math.max(0, Math.floor(importedPeers))}`;
    }
    if (providerLabel) text += ` • ${providerLabel}`;
    state.directoryText = text;
    render();
  };

  let freshnessTimer = null;
  const start = () => {
    render();
    if (freshnessTimer) return;
    freshnessTimer = setInterval(() => render(), 15_000);
  };
  const stop = () => {
    if (!freshnessTimer) return;
    clearInterval(freshnessTimer);
    freshnessTimer = null;
  };

  return {
    onCatalog,
    onStatus,
    onCreditPreview,
    onQuotePreview,
    onDirectoryPreview,
    start,
    stop
  };
};

const normalizeMarketplaceApiBase = (value, fallback = 'http://127.0.0.1:3001') => {
  const raw = String(value || '').trim() || fallback;
  try {
    const parsed = new URL(raw);
    parsed.search = '';
    parsed.hash = '';
    return parsed.toString().replace(/\/+$/, '');
  } catch (_) {
    return String(fallback || 'http://127.0.0.1:3001').trim().replace(/\/+$/, '');
  }
};

const createMarketplaceDirectoryClient = ({ CFG, saveCFG, setBadge, log, onDirectory }) => {
  const state = {
    inFlight: false,
    timer: null
  };

  const refreshMs = () => {
    const raw = Number(CFG?.noclipMarketplaceDirectoryRefreshMs || 60000);
    if (!Number.isFinite(raw)) return 60000;
    return Math.max(15000, Math.min(300000, Math.floor(raw)));
  };

  const buildUrl = () => {
    const base = normalizeMarketplaceApiBase(CFG?.noclipMarketplaceApiBase || 'http://127.0.0.1:3001');
    if (CFG.noclipMarketplaceApiBase !== base) {
      CFG.noclipMarketplaceApiBase = base;
      saveCFG();
    }
    const url = new URL(`${base}/api/interop/marketplace/catalog/providers`);
    url.searchParams.set('sourceNetwork', 'hydra');
    url.searchParams.set('activeOnly', 'true');
    url.searchParams.set('limit', '100');
    return url.toString();
  };

  const notify = (payload) => {
    const detail = payload && typeof payload === 'object' ? payload : {};
    onDirectory?.(detail);
    try {
      window.dispatchEvent(new CustomEvent('hydra-market-directory', { detail }));
    } catch (_) {
      // ignore DOM dispatch failures
    }
  };

  const fail = (message, opts = {}) => {
    const text = String(message || 'directory_unavailable').trim() || 'directory_unavailable';
    CFG.noclipMarketplaceDirectoryLastStatus = 'error';
    CFG.noclipMarketplaceDirectoryLastAt = Date.now();
    saveCFG();
    notify({
      ok: false,
      error: text,
      source: 'http-directory',
      sourcePriority: 20,
      updatedAtMs: Date.now()
    });
    if (opts.manual) setBadge(`Marketplace directory refresh failed: ${text}`, false);
    log?.(`[market.directory] ${text}`);
    return { ok: false, error: text };
  };

  const refresh = async ({ manual = false } = {}) => {
    if (state.inFlight) return { ok: false, reason: 'in_flight' };
    state.inFlight = true;
    const controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
    const timeout = setTimeout(() => {
      try {
        controller?.abort();
      } catch (_) {
        // ignore abort errors
      }
    }, 12000);
    try {
      const response = await fetch(buildUrl(), {
        method: 'GET',
        cache: 'no-store',
        headers: { Accept: 'application/json' },
        signal: controller?.signal
      });
      if (!response.ok) {
        return fail(`http_${response.status}`, { manual });
      }
      const payload = await response.json().catch(() => null);
      if (!payload || typeof payload !== 'object') {
        return fail('invalid_directory_response', { manual });
      }
      const status = String(payload.status || '').trim().toLowerCase();
      if (status && status !== 'ok') {
        return fail(String(payload.error || 'directory_status_not_ok'), { manual });
      }
      const catalogs = Array.isArray(payload.catalogs) ? payload.catalogs : [];
      const diagnostics = payload.diagnostics && typeof payload.diagnostics === 'object'
        ? payload.diagnostics
        : {};
      const total = Number(payload.total ?? diagnostics.totalCount ?? diagnostics.total_count ?? catalogs.length);
      const freshCount = Number(diagnostics.freshCount ?? diagnostics.fresh_count ?? 0);
      const staleCount = Number(diagnostics.staleCount ?? diagnostics.stale_count ?? 0);
      const primaryProviderLabel = String(
        catalogs[0]?.providerLabel ||
        catalogs[0]?.provider_label ||
        ''
      ).trim();
      const detail = {
        ok: true,
        source: 'http-directory',
        sourcePriority: 20,
        total: Number.isFinite(total) ? Math.max(0, Math.floor(total)) : 0,
        freshCount: Number.isFinite(freshCount) ? Math.max(0, Math.floor(freshCount)) : 0,
        staleCount: Number.isFinite(staleCount) ? Math.max(0, Math.floor(staleCount)) : 0,
        catalogs,
        diagnostics,
        primaryProviderLabel,
        updatedAtMs: Date.now()
      };
      CFG.noclipMarketplaceDirectoryLastStatus = 'ok';
      CFG.noclipMarketplaceDirectoryLastAt = detail.updatedAtMs;
      saveCFG();
      notify(detail);
      if (manual) {
        const importedPeers = Number(detail.importSummary?.imported ?? -1);
        const peerText = Number.isFinite(importedPeers) && importedPeers >= 0
          ? ` • peers ${Math.max(0, Math.floor(importedPeers))}`
          : '';
        setBadge(
          `Marketplace directory refreshed (${detail.total} provider${detail.total === 1 ? '' : 's'}${peerText})`
        );
      }
      return detail;
    } catch (err) {
      const aborted = err && typeof err === 'object' && err.name === 'AbortError';
      return fail(aborted ? 'timeout' : (err?.message || String(err)), { manual });
    } finally {
      clearTimeout(timeout);
      state.inFlight = false;
    }
  };

  const schedule = () => {
    if (state.timer) {
      clearTimeout(state.timer);
      state.timer = null;
    }
    if (CFG?.noclipMarketplaceDirectoryAutoRefresh === false) return;
    state.timer = setTimeout(async () => {
      await refresh({ manual: false });
      schedule();
    }, refreshMs());
  };

  const start = () => {
    if (CFG?.noclipMarketplaceDirectoryAutoRefresh === false) {
      notify({
        ok: true,
        disabled: true,
        source: 'http-directory',
        updatedAtMs: Date.now(),
        catalogs: []
      });
      return;
    }
    refresh({ manual: false });
    schedule();
  };

  const stop = () => {
    if (!state.timer) return;
    clearTimeout(state.timer);
    state.timer = null;
  };

  return {
    refresh,
    start,
    stop
  };
};

const createMarketplaceConfigEditor = ({ CFG, saveCFG, setBadge, log, onCatalog }) => {
  const ui = {
    root: qs('#marketplaceConfigPanel'),
    status: qs('#marketConfigStatus'),
    loadBtn: qs('#marketConfigLoadBtn'),
    saveBtn: qs('#marketConfigSaveBtn'),
    exportBtn: qs('#marketConfigExportBtn'),
    importInput: qs('#marketConfigImportInput'),
    providerId: qs('#marketProviderIdInput'),
    providerLabel: qs('#marketProviderLabelInput'),
    providerNetwork: qs('#marketProviderNetworkInput'),
    providerContact: qs('#marketProviderContactInput'),
    defaultCurrency: qs('#marketDefaultCurrencyInput'),
    defaultUnit: qs('#marketDefaultUnitInput'),
    defaultPrice: qs('#marketDefaultPriceInput'),
    includeUnhealthy: qs('#marketIncludeUnhealthyInput'),
    serviceList: qs('#marketServiceConfigList')
  };

  if (!ui.root) {
    return {
      start: () => {},
      stop: () => {},
      refresh: async () => ({ ok: false, reason: 'ui_unavailable' })
    };
  }

  const state = {
    etag: '',
    config: {
      provider: {},
      services: {}
    },
    inFlight: false,
    started: false
  };

  const VISIBILITY_OPTIONS = ['public', 'friends', 'private'];
  const TRANSPORT_OPTIONS = ['auto', 'cloudflare', 'nats', 'nkn', 'local', 'upnp'];

  const setStatus = (text, tone = 'muted') => {
    if (!ui.status) return;
    ui.status.textContent = String(text || 'config idle');
    ui.status.dataset.tone = String(tone || 'muted');
  };

  const normalizeApiBase = (value) => {
    const raw = String(value || '').trim();
    if (!raw) return '';
    try {
      const parsed = new URL(raw, window.location.href);
      parsed.search = '';
      parsed.hash = '';
      return parsed.toString().replace(/\/+$/, '');
    } catch (_) {
      return '';
    }
  };

  const resolveRouterApiBase = () => {
    const candidates = [
      CFG?.routerControlPlaneApiBase,
      CFG?.routerLastResolveResult?.network?.local,
      CFG?.routerLastResolveResult?.reply?.network?.local,
      CFG?.routerLastResolveResult?.snapshot?.network?.local,
      window?.location?.origin && window.location.origin.includes(':9071') ? window.location.origin : '',
      'http://127.0.0.1:9071'
    ];
    for (const candidate of candidates) {
      const normalized = normalizeApiBase(candidate);
      if (normalized) return normalized;
    }
    return 'http://127.0.0.1:9071';
  };

  const apiUrl = (path) => {
    const base = resolveRouterApiBase();
    return `${base}${path.startsWith('/') ? path : `/${path}`}`;
  };

  const clearInvalidMarkers = () => {
    ui.root.querySelectorAll('.is-invalid').forEach((el) => el.classList.remove('is-invalid'));
  };

  const markInvalid = (el) => {
    if (!el) return;
    el.classList.add('is-invalid');
  };

  const escapeAttr = (value) => String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

  const serviceRowTemplate = (serviceId, entry = {}) => {
    const pricing = entry.pricing && typeof entry.pricing === 'object' ? entry.pricing : {};
    const visibility = VISIBILITY_OPTIONS.includes(String(entry.visibility || '').trim().toLowerCase())
      ? String(entry.visibility || '').trim().toLowerCase()
      : 'public';
    const transport = TRANSPORT_OPTIONS.includes(String(entry.transport_preference || '').trim().toLowerCase())
      ? String(entry.transport_preference || '').trim().toLowerCase()
      : 'auto';
    const category = String(entry.category || serviceId).trim().toLowerCase() || serviceId;
    const tags = Array.isArray(entry.tags) ? entry.tags.join(',') : '';
    const unit = String(pricing.unit || 'request').trim().toLowerCase() || 'request';
    const currency = String(pricing.currency || 'USDC').trim().toUpperCase() || 'USDC';
    const basePrice = Number(pricing.base_price ?? 0);
    const quotePublic = pricing.quote_public !== false;
    const capacity = Number(entry.capacity_hint ?? 1);
    const safeServiceId = escapeAttr(serviceId);
    const safeRepository = escapeAttr(String(entry.repository || 'hydra').trim() || 'hydra');
    const safeCategory = escapeAttr(category);
    const safeTags = escapeAttr(tags);
    const safeUnit = escapeAttr(unit);
    const safeCurrency = escapeAttr(currency);
    return `<div class="market-service-config-row" data-service-id="${safeServiceId}" data-repository="${safeRepository}" data-min-units="${Math.max(1, Math.floor(Number(pricing.min_units || 1)))}">
      <span class="market-service-config-id" title="${safeServiceId}">${safeServiceId}</span>
      <label><input class="svc-enabled" type="checkbox" ${entry.enabled !== false ? 'checked' : ''}>on</label>
      <select class="svc-visibility">${VISIBILITY_OPTIONS.map((value) => `<option value="${value}" ${value === visibility ? 'selected' : ''}>${value}</option>`).join('')}</select>
      <select class="svc-transport">${TRANSPORT_OPTIONS.map((value) => `<option value="${value}" ${value === transport ? 'selected' : ''}>${value}</option>`).join('')}</select>
      <input class="svc-capacity" type="number" min="0" max="100000" step="1" value="${Number.isFinite(capacity) ? Math.max(0, Math.floor(capacity)) : 1}" title="capacity hint">
      <input class="svc-price" type="number" min="0" step="0.000001" value="${Number.isFinite(basePrice) ? Math.max(0, basePrice) : 0}" title="base price">
      <input class="svc-unit" type="text" maxlength="32" value="${safeUnit}" title="pricing unit">
      <input class="svc-currency" type="text" maxlength="12" value="${safeCurrency}" title="pricing currency">
      <label><input class="svc-quote-public" type="checkbox" ${quotePublic ? 'checked' : ''}>quote</label>
      <input class="svc-category" type="text" maxlength="48" value="${safeCategory}" title="service category">
      <input class="svc-tags" type="text" maxlength="180" value="${safeTags}" title="tags comma-delimited">
    </div>`;
  };

  const renderServices = (servicesMap = {}) => {
    if (!ui.serviceList) return;
    const keys = Object.keys(servicesMap).sort();
    if (!keys.length) {
      ui.serviceList.innerHTML = '<div class="market-service-config-id">No services</div>';
      return;
    }
    ui.serviceList.innerHTML = keys
      .map((serviceId) => serviceRowTemplate(serviceId, servicesMap[serviceId] && typeof servicesMap[serviceId] === 'object' ? servicesMap[serviceId] : {}))
      .join('');
  };

  const applyConfig = (config = {}, { etag = '', statusText = 'config loaded' } = {}) => {
    const provider = config.provider && typeof config.provider === 'object' ? config.provider : {};
    const services = config.services && typeof config.services === 'object' ? config.services : {};
    state.config = {
      provider: provider,
      services: services
    };
    state.etag = String(etag || '').trim();
    if (ui.providerId) ui.providerId.value = String(provider.provider_id || '').trim();
    if (ui.providerLabel) ui.providerLabel.value = String(provider.provider_label || '').trim();
    if (ui.providerNetwork) ui.providerNetwork.value = String(provider.provider_network || '').trim();
    if (ui.providerContact) ui.providerContact.value = String(provider.provider_contact || '').trim();
    if (ui.defaultCurrency) ui.defaultCurrency.value = String(provider.default_currency || 'USDC').trim();
    if (ui.defaultUnit) ui.defaultUnit.value = String(provider.default_unit || 'request').trim();
    if (ui.defaultPrice) {
      const price = Number(provider.default_price_per_unit ?? 0);
      ui.defaultPrice.value = Number.isFinite(price) ? String(Math.max(0, price)) : '0';
    }
    if (ui.includeUnhealthy) ui.includeUnhealthy.checked = provider.include_unhealthy !== false;
    renderServices(services);
    setStatus(statusText, 'ok');
  };

  const normalizeTags = (raw) => String(raw || '')
    .split(',')
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean)
    .slice(0, 16);

  const collectPayload = () => {
    clearInvalidMarkers();
    const errors = [];

    const providerId = String(ui.providerId?.value || '').trim().toLowerCase();
    const providerLabel = String(ui.providerLabel?.value || '').trim();
    const providerNetwork = String(ui.providerNetwork?.value || '').trim().toLowerCase();
    const providerContact = String(ui.providerContact?.value || '').trim();
    const defaultCurrency = String(ui.defaultCurrency?.value || '').trim().toUpperCase();
    const defaultUnit = String(ui.defaultUnit?.value || '').trim().toLowerCase();
    const defaultPrice = Number(ui.defaultPrice?.value || 0);

    if (!/^[a-z0-9][a-z0-9_.:-]{1,63}$/.test(providerId)) {
      errors.push('Provider ID must match [a-z0-9][a-z0-9_.:-]{1,63}.');
      markInvalid(ui.providerId);
    }
    if (providerLabel.length < 1 || providerLabel.length > 120) {
      errors.push('Provider label must be 1-120 characters.');
      markInvalid(ui.providerLabel);
    }
    if (!/^[a-z0-9][a-z0-9_-]{1,31}$/.test(providerNetwork)) {
      errors.push('Provider network must match [a-z0-9][a-z0-9_-]{1,31}.');
      markInvalid(ui.providerNetwork);
    }
    if (providerContact.length > 160) {
      errors.push('Provider contact must be <= 160 characters.');
      markInvalid(ui.providerContact);
    }
    if (!/^[A-Z0-9_-]{2,12}$/.test(defaultCurrency)) {
      errors.push('Default currency must match [A-Z0-9_-]{2,12}.');
      markInvalid(ui.defaultCurrency);
    }
    if (!/^[a-z0-9_:-]{1,32}$/.test(defaultUnit)) {
      errors.push('Default unit must match [a-z0-9_:-]{1,32}.');
      markInvalid(ui.defaultUnit);
    }
    if (!Number.isFinite(defaultPrice) || defaultPrice < 0 || defaultPrice > 1_000_000) {
      errors.push('Default base price must be between 0 and 1000000.');
      markInvalid(ui.defaultPrice);
    }

    const provider = {
      provider_id: providerId,
      provider_label: providerLabel,
      provider_network: providerNetwork,
      provider_contact: providerContact,
      default_currency: defaultCurrency,
      default_unit: defaultUnit,
      default_price_per_unit: Number.isFinite(defaultPrice) ? Number(defaultPrice.toFixed(8)) : 0,
      include_unhealthy: ui.includeUnhealthy?.checked !== false
    };

    const services = {};
    const rows = ui.serviceList ? Array.from(ui.serviceList.querySelectorAll('.market-service-config-row')) : [];
    rows.forEach((row) => {
      const serviceId = String(row.getAttribute('data-service-id') || '').trim();
      if (!serviceId) return;
      const base = state.config?.services?.[serviceId] && typeof state.config.services[serviceId] === 'object'
        ? state.config.services[serviceId]
        : {};
      const repository = String(row.getAttribute('data-repository') || base.repository || 'hydra').trim() || 'hydra';
      const minUnits = Math.max(1, Math.floor(Number(row.getAttribute('data-min-units') || base?.pricing?.min_units || 1)));
      const enabledEl = row.querySelector('.svc-enabled');
      const visibilityEl = row.querySelector('.svc-visibility');
      const transportEl = row.querySelector('.svc-transport');
      const capacityEl = row.querySelector('.svc-capacity');
      const priceEl = row.querySelector('.svc-price');
      const unitEl = row.querySelector('.svc-unit');
      const currencyEl = row.querySelector('.svc-currency');
      const quoteEl = row.querySelector('.svc-quote-public');
      const categoryEl = row.querySelector('.svc-category');
      const tagsEl = row.querySelector('.svc-tags');

      const visibility = String(visibilityEl?.value || 'public').trim().toLowerCase();
      const transportPreference = String(transportEl?.value || 'auto').trim().toLowerCase();
      const category = String(categoryEl?.value || serviceId).trim().toLowerCase();
      const tags = normalizeTags(tagsEl?.value || '');
      const capacity = Math.floor(Number(capacityEl?.value || 0));
      const basePrice = Number(priceEl?.value || 0);
      const unit = String(unitEl?.value || defaultUnit).trim().toLowerCase();
      const currency = String(currencyEl?.value || defaultCurrency).trim().toUpperCase();

      if (!VISIBILITY_OPTIONS.includes(visibility)) {
        errors.push(`${serviceId}: visibility must be public/friends/private.`);
        markInvalid(visibilityEl);
      }
      if (!TRANSPORT_OPTIONS.includes(transportPreference)) {
        errors.push(`${serviceId}: transport preference invalid.`);
        markInvalid(transportEl);
      }
      if (!Number.isFinite(capacity) || capacity < 0 || capacity > 100000) {
        errors.push(`${serviceId}: capacity must be integer between 0 and 100000.`);
        markInvalid(capacityEl);
      }
      if (!Number.isFinite(basePrice) || basePrice < 0 || basePrice > 1_000_000) {
        errors.push(`${serviceId}: base price must be between 0 and 1000000.`);
        markInvalid(priceEl);
      }
      if (!/^[a-z0-9_:-]{1,32}$/.test(unit)) {
        errors.push(`${serviceId}: pricing unit invalid.`);
        markInvalid(unitEl);
      }
      if (!/^[A-Z0-9_-]{2,12}$/.test(currency)) {
        errors.push(`${serviceId}: pricing currency invalid.`);
        markInvalid(currencyEl);
      }
      if (!/^[a-z0-9_:-]{1,48}$/.test(category)) {
        errors.push(`${serviceId}: category invalid.`);
        markInvalid(categoryEl);
      }
      if (!tags.length) {
        errors.push(`${serviceId}: at least one tag required.`);
        markInvalid(tagsEl);
      }

      services[serviceId] = {
        enabled: enabledEl?.checked === true,
        visibility,
        repository,
        category,
        capacity_hint: Number.isFinite(capacity) ? capacity : 0,
        transport_preference: transportPreference,
        tags,
        pricing: {
          currency,
          unit,
          base_price: Number.isFinite(basePrice) ? Number(basePrice.toFixed(8)) : 0,
          min_units: minUnits,
          quote_public: quoteEl?.checked === true
        }
      };
    });

    return {
      ok: errors.length === 0,
      errors,
      payload: {
        provider,
        services
      }
    };
  };

  const loadConfig = async ({ silent = false } = {}) => {
    if (state.inFlight) return { ok: false, reason: 'in_flight' };
    state.inFlight = true;
    setStatus('loading...', 'warn');
    try {
      const response = await fetch(apiUrl('/marketplace/config'), {
        method: 'GET',
        cache: 'no-store',
        headers: { Accept: 'application/json' }
      });
      const payload = await response.json().catch(() => null);
      if (!response.ok || !payload || payload.status !== 'success') {
        const reason = String(payload?.error || `http_${response.status}` || 'load_failed').trim();
        setStatus(`load failed • ${reason}`, 'error');
        if (!silent) setBadge(`Marketplace config load failed: ${reason}`, false);
        return { ok: false, error: reason };
      }
      const etag = String(payload.etag || response.headers.get('ETag') || '').trim().replace(/"/g, '');
      applyConfig(payload.config, {
        etag,
        statusText: `loaded • ${Object.keys(payload?.config?.services || {}).length} services`
      });
      CFG.routerControlPlaneApiBase = resolveRouterApiBase();
      saveCFG();
      if (!silent) setBadge('Marketplace config loaded');
      return { ok: true, payload };
    } catch (err) {
      const reason = String(err?.message || err || 'load_failed').trim();
      setStatus(`load failed • ${reason}`, 'error');
      if (!silent) setBadge(`Marketplace config load failed: ${reason}`, false);
      return { ok: false, error: reason };
    } finally {
      state.inFlight = false;
    }
  };

  const saveConfig = async () => {
    if (state.inFlight) return { ok: false, reason: 'in_flight' };
    const collected = collectPayload();
    if (!collected.ok) {
      const message = collected.errors[0] || 'validation failed';
      setStatus(`validation failed • ${message}`, 'error');
      setBadge(`Marketplace config validation failed: ${message}`, false);
      return { ok: false, error: message, errors: collected.errors };
    }
    state.inFlight = true;
    setStatus('saving...', 'warn');
    try {
      const response = await fetch(apiUrl('/marketplace/config'), {
        method: 'PUT',
        cache: 'no-store',
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/json',
          ...(state.etag ? { 'If-Match': state.etag } : {})
        },
        body: JSON.stringify({
          ...collected.payload,
          persist: true
        })
      });
      const payload = await response.json().catch(() => null);
      if (response.status === 409) {
        const reason = String(payload?.message || payload?.error || 'config_conflict').trim();
        setStatus(`conflict • ${reason}`, 'warn');
        setBadge(`Marketplace config conflict: ${reason}`, false);
        return { ok: false, error: reason, conflict: true };
      }
      if (!response.ok || !payload || payload.status !== 'success') {
        const reason = String(payload?.message || payload?.error || `http_${response.status}` || 'save_failed').trim();
        const firstError = Array.isArray(payload?.errors) && payload.errors.length ? String(payload.errors[0]) : reason;
        setStatus(`save failed • ${firstError}`, 'error');
        setBadge(`Marketplace config save failed: ${firstError}`, false);
        return { ok: false, error: firstError };
      }
      applyConfig(payload.config, {
        etag: String(payload.etag || response.headers.get('ETag') || '').trim().replace(/"/g, ''),
        statusText: 'saved'
      });
      if (payload.catalog && typeof payload.catalog === 'object') {
        onCatalog?.(payload.catalog);
      }
      setBadge('Marketplace config saved');
      try {
        window.dispatchEvent(new CustomEvent('hydra-provider-publish-catalog', {
          detail: {
            publishScope: 'both',
            includeStatus: true,
            force: true,
            silent: true
          }
        }));
      } catch (_) {
        // ignore publish dispatch failures
      }
      return { ok: true, payload };
    } catch (err) {
      const reason = String(err?.message || err || 'save_failed').trim();
      setStatus(`save failed • ${reason}`, 'error');
      setBadge(`Marketplace config save failed: ${reason}`, false);
      return { ok: false, error: reason };
    } finally {
      state.inFlight = false;
    }
  };

  const exportConfig = () => {
    const collected = collectPayload();
    const payload = collected.ok ? collected.payload : (state.config || { provider: {}, services: {} });
    try {
      const stamp = new Date().toISOString().replace(/[:.]/g, '-');
      const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `hydra-marketplace-config-${stamp}.json`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
      setStatus('exported', 'ok');
    } catch (err) {
      setStatus('export failed', 'error');
      setBadge(`Marketplace config export failed: ${err?.message || err}`, false);
    }
  };

  const importConfigFromFile = async (file) => {
    if (!file) return;
    try {
      const text = await file.text();
      const parsed = JSON.parse(text);
      const config = parsed && typeof parsed === 'object' && parsed.config && typeof parsed.config === 'object'
        ? parsed.config
        : parsed;
      if (!config || typeof config !== 'object' || !config.provider || !config.services) {
        throw new Error('Expected JSON with provider and services sections');
      }
      applyConfig(config, {
        etag: state.etag,
        statusText: 'imported draft'
      });
      setBadge('Marketplace config imported (draft loaded, click Save to apply)');
      setStatus('imported draft', 'warn');
    } catch (err) {
      setStatus('import failed', 'error');
      setBadge(`Marketplace config import failed: ${err?.message || err}`, false);
    } finally {
      if (ui.importInput) ui.importInput.value = '';
    }
  };

  const start = () => {
    if (state.started) return;
    state.started = true;
    ui.loadBtn?.addEventListener('click', async (event) => {
      event.preventDefault();
      if (ui.loadBtn.disabled) return;
      ui.loadBtn.disabled = true;
      try {
        await loadConfig();
      } finally {
        ui.loadBtn.disabled = false;
      }
    });
    ui.saveBtn?.addEventListener('click', async (event) => {
      event.preventDefault();
      if (ui.saveBtn.disabled) return;
      ui.saveBtn.disabled = true;
      try {
        await saveConfig();
      } finally {
        ui.saveBtn.disabled = false;
      }
    });
    ui.exportBtn?.addEventListener('click', (event) => {
      event.preventDefault();
      exportConfig();
    });
    ui.importInput?.addEventListener('change', async () => {
      const file = ui.importInput?.files?.[0];
      await importConfigFromFile(file);
    });
    loadConfig({ silent: true });
  };

  const stop = () => {
    state.started = false;
  };

  return {
    start,
    stop,
    refresh: loadConfig
  };
};

const RouterDiscovery = createRouterDiscovery({
  Net,
  CFG,
  saveCFG,
  setBadge,
  log,
  onResolved: applyRouterResolvedPayload
});

MarketplaceSummary = createMarketplaceSummaryPanel();
MarketplaceDirectory = createMarketplaceDirectoryClient({
  CFG,
  saveCFG,
  setBadge,
  log,
  onDirectory: (detail) => {
    if (detail?.ok !== false && Array.isArray(detail?.catalogs) && SharedPeerDiscovery?.ingestMarketplaceDirectory) {
      try {
        detail.importSummary = SharedPeerDiscovery.ingestMarketplaceDirectory(detail.catalogs, {
          ping: false,
          source: 'http-directory'
        });
      } catch (err) {
        log(`[market.directory] peer ingest failed: ${err?.message || err}`);
      }
    }
    MarketplaceSummary?.onDirectoryPreview?.(detail);
  }
});
MarketplaceConfigEditor = createMarketplaceConfigEditor({
  CFG,
  saveCFG,
  setBadge,
  log,
  onCatalog: (catalog) => {
    if (!catalog || typeof catalog !== 'object') return;
    const updatedAtMs = Number(catalog.generated_at_ms || catalog.generatedAtMs || Date.now()) || Date.now();
    CFG.routerMarketplaceCatalog = catalog;
    CFG.routerLastCatalogAt = updatedAtMs;
    saveCFG();
    MarketplaceSummary?.onCatalog?.(catalog, { updatedAtMs });
    try {
      NoClipBridge?.onRouterCatalog?.(catalog, {
        includeStatus: true,
        silent: true
      });
    } catch (err) {
      log(`[market.config] bridge catalog sync failed: ${err?.message || err}`);
    }
  }
});
if (CFG?.routerMarketplaceCatalog && typeof CFG.routerMarketplaceCatalog === 'object') {
  MarketplaceSummary.onCatalog(CFG.routerMarketplaceCatalog, {
    updatedAtMs: Number(CFG.routerLastCatalogAt || Date.now()) || Date.now()
  });
}
if (String(CFG?.noclipMarketplaceDirectoryLastStatus || '').toLowerCase() === 'error') {
  MarketplaceSummary.onDirectoryPreview({
    ok: false,
    error: 'unavailable',
    updatedAtMs: Number(CFG?.noclipMarketplaceDirectoryLastAt || Date.now()) || Date.now()
  });
}
window.addEventListener('hydra-market-catalog', (event) => {
  const detail = event?.detail && typeof event.detail === 'object' ? event.detail : {};
  if (!detail.catalog || typeof detail.catalog !== 'object') return;
  MarketplaceSummary.onCatalog(detail.catalog, {
    updatedAtMs: Number(detail.ts || detail.updatedAtMs || Date.now()) || Date.now()
  });
});
window.addEventListener('hydra-market-status', (event) => {
  const detail = event?.detail && typeof event.detail === 'object' ? event.detail : {};
  MarketplaceSummary.onStatus(detail);
});
window.addEventListener('hydra-market-credit-preview', (event) => {
  const detail = event?.detail && typeof event.detail === 'object' ? event.detail : {};
  MarketplaceSummary.onCreditPreview(detail);
});
window.addEventListener('hydra-market-quote-preview', (event) => {
  const detail = event?.detail && typeof event.detail === 'object' ? event.detail : {};
  MarketplaceSummary.onQuotePreview(detail);
});

let NoClipBridgeSyncImpl = null;
const NoClipBridgeSyncProxy = {
  getHydraIdentity: (...args) => NoClipBridgeSyncImpl?.getHydraIdentity?.(...args) || {},
  listPendingRequests: (...args) => NoClipBridgeSyncImpl?.listPendingRequests?.(...args) || [],
  subscribe: (listener) => {
    if (!NoClipBridgeSyncImpl?.subscribe) return () => {};
    return NoClipBridgeSyncImpl.subscribe(listener);
  },
  approveSyncRequest: (...args) => {
    if (!NoClipBridgeSyncImpl?.approveSyncRequest) {
      return Promise.reject(new Error('NoClipBridgeSync not ready'));
    }
    return NoClipBridgeSyncImpl.approveSyncRequest(...args);
  },
  rejectSyncRequest: (...args) => {
    if (!NoClipBridgeSyncImpl?.rejectSyncRequest) {
      return Promise.reject(new Error('NoClipBridgeSync not ready'));
    }
    return NoClipBridgeSyncImpl.rejectSyncRequest(...args);
  },
  updateSessionStatus: (...args) => NoClipBridgeSyncImpl?.updateSessionStatus?.(...args)
};

const Graph = createGraph({
  Router,
  NodeStore,
  LLM,
  TTS,
  ASR,
  NknDM,
  Media,
  Orientation,
  Location: LocationNode,
  FileTransfer,
  Payments,
  MCP,
  Meshtastic,
  WebSerial,
  WebScraper,
  Vision,
  Pointcloud,
  NoClip: NoClipBridge,
  NoClipBridgeSync: NoClipBridgeSyncProxy,
  Net,
  CFG,
  saveCFG,
  openQrScanner,
  closeQrScanner,
  registerQrResultHandler,
  updateTransportButton
});

graphAccess.getNode = (id) => Graph.getNode(id);
applyRelayState = (...args) => Graph.setRelayState(...args);

const NOCLIP_PUB_RE = /^[0-9a-f]{64}$/i;
const YES_VALUES = new Set(['1', 'true', 'yes', 'y', 'on']);
const INVITE_SYNC_MAX_ATTEMPTS = 6;
const INVITE_SYNC_BASE_DELAY_MS = 2500;
const INVITE_SYNC_MAX_DELAY_MS = 30000;
const INVITE_SYNC_MAX_AGE_MS = 10 * 60 * 1000;

const InviteSyncState = {
  timer: null,
  inFlight: false
};

const cleanTargetField = (value, maxLen = 160) => {
  const raw = String(value || '').trim();
  if (!raw) return '';
  return raw.slice(0, Math.max(1, Math.floor(Number(maxLen) || 160)));
};

const parseBooleanish = (value, fallback = false) => {
  if (value === true || value === false) return value;
  const key = String(value || '').trim().toLowerCase();
  if (!key) return !!fallback;
  if (YES_VALUES.has(key)) return true;
  if (key === '0' || key === 'false' || key === 'no' || key === 'n' || key === 'off') return false;
  return !!fallback;
};

const parseNoClipPub = (value) => {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return '';
  const normalized = raw
    .replace(/^nkn:\/\//, '')
    .replace(/^noclip\./, '')
    .replace(/^hydra\./, '');
  return NOCLIP_PUB_RE.test(normalized) ? normalized : '';
};

const normalizeBridgeTargetContext = (source = {}) => {
  const input = source && typeof source === 'object' ? source : {};
  const sessionId = cleanTargetField(input.sessionId || input.session || input.session_id, 128);
  const objectUuid = cleanTargetField(
    input.objectUuid ||
    input.object_uuid ||
    input.objectId ||
    input.object_id ||
    input.object ||
    input.itemId ||
    input.item_id,
    160
  );
  const overlayId = cleanTargetField(input.overlayId || input.overlay || input.overlay_id, 128);
  const itemId = cleanTargetField(input.itemId || input.item || input.item_id || objectUuid, 160);
  const layerId = cleanTargetField(input.layerId || input.layer || input.layer_id, 128);
  const hasTarget = !!(sessionId || objectUuid || overlayId || itemId || layerId);
  return { sessionId, objectUuid, overlayId, itemId, layerId, hasTarget };
};

const listNoClipBridgeNodeIds = () => {
  if (typeof document === 'undefined') return [];
  const out = [];
  const seen = new Set();
  const nodeEls = Array.from(document.querySelectorAll('.node[data-id]'));
  nodeEls.forEach((el) => {
    const id = String(el?.dataset?.id || '').trim();
    if (!id || seen.has(id)) return;
    const rec = NodeStore.load(id);
    if (!rec || rec.type !== 'NoClipBridge') return;
    if (!Graph.getNode(id)) return;
    out.push(id);
    seen.add(id);
  });
  return out;
};

const pickNoClipBridgeNodeId = (ids = []) => {
  const nodeIds = Array.isArray(ids) ? ids.filter(Boolean) : [];
  if (!nodeIds.length) return '';
  const selected = new Set(
    Array.from(document.querySelectorAll('.node.selected[data-id]'))
      .map((el) => String(el?.dataset?.id || '').trim())
      .filter(Boolean)
  );
  for (const id of nodeIds) {
    if (selected.has(id)) return id;
  }
  for (const id of nodeIds) {
    const cfg = NodeStore.load(id)?.config || {};
    if (!String(cfg.targetPub || '').trim()) return id;
  }
  return nodeIds[0] || '';
};

const ensureNoClipBridgeNode = (preferredNodeId = '') => {
  const preferred = String(preferredNodeId || '').trim();
  if (preferred) {
    const rec = NodeStore.load(preferred);
    if (rec?.type === 'NoClipBridge' && Graph.getNode(preferred)) {
      return { nodeId: preferred, created: false };
    }
  }
  const existing = listNoClipBridgeNodeIds();
  const picked = pickNoClipBridgeNodeId(existing);
  if (picked) return { nodeId: picked, created: false };
  const created = Graph.addNode('NoClipBridge', 760, 140, { select: true });
  if (!created?.id) {
    throw new Error('unable_to_create_noclip_bridge_node');
  }
  return { nodeId: created.id, created: true };
};

const normalizeInviteErrorMessage = (errorLike) => {
  if (errorLike == null) return '';
  if (typeof errorLike === 'string') return errorLike.trim();
  if (errorLike && typeof errorLike === 'object' && 'message' in errorLike) {
    return String(errorLike.message || '').trim();
  }
  return String(errorLike).trim();
};

const isUrlInviteSource = (value) => {
  const key = String(value || '').trim().toLowerCase();
  return key === 'url-invite' || key === 'url_invite' || key === 'urlinvite' || key.startsWith('url-invite');
};

const clearInviteSyncTimer = () => {
  if (!InviteSyncState.timer) return;
  clearTimeout(InviteSyncState.timer);
  InviteSyncState.timer = null;
};

const clearPendingNoClipInvite = () => {
  clearInviteSyncTimer();
  InviteSyncState.inFlight = false;
  try {
    delete window.pendingNoClipInvite;
  } catch (_) {
    window.pendingNoClipInvite = null;
  }
};

const getPendingNoClipInvite = () => {
  const value = window.pendingNoClipInvite;
  if (!value || typeof value !== 'object') return null;
  return value;
};

const updatePendingNoClipInvite = (patch = {}) => {
  const current = getPendingNoClipInvite();
  if (!current) return null;
  const next = { ...current, ...(patch && typeof patch === 'object' ? patch : {}) };
  window.pendingNoClipInvite = next;
  return next;
};

const getInviteSyncAttemptCount = (invite) => {
  const raw = Number(invite?.syncAttempts ?? invite?.attempts ?? 0);
  if (!Number.isFinite(raw)) return 0;
  return Math.max(0, Math.floor(raw));
};

const getInviteSyncAgeMs = (invite) => {
  const base = Number(invite?.createdAt ?? invite?.timestamp ?? Date.now());
  if (!Number.isFinite(base)) return 0;
  return Math.max(0, Date.now() - base);
};

const isInviteSyncExpired = (invite) => getInviteSyncAgeMs(invite) > INVITE_SYNC_MAX_AGE_MS;

const isInviteSyncTerminalError = (errorLike) => {
  const message = normalizeInviteErrorMessage(errorLike).toLowerCase();
  if (!message) return false;
  return (
    message.includes('invalid_noclip_target_pub') ||
    message.includes('unable_to_create_noclip_bridge_node') ||
    message.includes('noclip_sync_unavailable') ||
    message.includes('no target pubkey configured') ||
    message.includes('noclip peer not selected')
  );
};

const getInviteSyncBackoffDelayMs = (completedAttempts = 0) => {
  const attempts = Math.max(0, Math.floor(Number(completedAttempts) || 0));
  const factor = Math.max(0, attempts - 1);
  const delay = INVITE_SYNC_BASE_DELAY_MS * (2 ** factor);
  return Math.max(INVITE_SYNC_BASE_DELAY_MS, Math.min(INVITE_SYNC_MAX_DELAY_MS, Math.floor(delay)));
};

const schedulePendingInviteSyncRetry = (reason = 'sync_failed') => {
  const invite = getPendingNoClipInvite();
  if (!invite) return false;
  if (!parseBooleanish(invite.autoSync, false)) {
    clearPendingNoClipInvite();
    return false;
  }
  if (isInviteSyncExpired(invite)) {
    const targetPub = parseNoClipPub(invite.targetPub || invite.noclipPub || '');
    const short = targetPub ? `noclip.${targetPub.slice(0, 8)}` : 'NoClip invite';
    setBadge(`${short} auto-sync invite expired before completion`, false);
    clearPendingNoClipInvite();
    return false;
  }
  const attempts = getInviteSyncAttemptCount(invite);
  if (attempts >= INVITE_SYNC_MAX_ATTEMPTS) {
    const targetPub = parseNoClipPub(invite.targetPub || invite.noclipPub || '');
    const short = targetPub ? `noclip.${targetPub.slice(0, 8)}` : 'NoClip invite';
    const failure = normalizeInviteErrorMessage(invite.lastError || reason);
    setBadge(`${short} auto-sync failed after ${attempts} attempts${failure ? `: ${failure}` : ''}`, false);
    clearPendingNoClipInvite();
    return false;
  }
  clearInviteSyncTimer();
  const delayMs = getInviteSyncBackoffDelayMs(attempts);
  updatePendingNoClipInvite({
    retryReason: cleanTargetField(reason, 120) || 'sync_failed',
    nextRetryAt: Date.now() + delayMs
  });
  InviteSyncState.timer = setTimeout(() => {
    processPendingInviteSyncRetry('timer').catch(() => {});
  }, delayMs);
  return true;
};

const processPendingInviteSyncRetry = async (trigger = 'manual') => {
  if (InviteSyncState.inFlight) return { ok: false, reason: 'in_flight' };
  const invite = getPendingNoClipInvite();
  if (!invite) return { ok: false, reason: 'missing_invite' };
  if (!parseBooleanish(invite.autoSync, false)) {
    clearPendingNoClipInvite();
    return { ok: false, reason: 'auto_sync_disabled' };
  }
  if (isInviteSyncExpired(invite)) {
    const targetPub = parseNoClipPub(invite.targetPub || invite.noclipPub || '');
    const short = targetPub ? `noclip.${targetPub.slice(0, 8)}` : 'NoClip invite';
    setBadge(`${short} auto-sync invite expired before completion`, false);
    clearPendingNoClipInvite();
    return { ok: false, reason: 'expired' };
  }
  const attempts = getInviteSyncAttemptCount(invite);
  if (attempts >= INVITE_SYNC_MAX_ATTEMPTS) {
    const failure = normalizeInviteErrorMessage(invite.lastError || 'sync_failed');
    const targetPub = parseNoClipPub(invite.targetPub || invite.noclipPub || '');
    const short = targetPub ? `noclip.${targetPub.slice(0, 8)}` : 'NoClip invite';
    setBadge(`${short} auto-sync failed after ${attempts} attempts${failure ? `: ${failure}` : ''}`, false);
    clearPendingNoClipInvite();
    return { ok: false, reason: 'attempts_exhausted', error: failure };
  }

  clearInviteSyncTimer();
  const nextAttempt = attempts + 1;
  InviteSyncState.inFlight = true;
  updatePendingNoClipInvite({
    syncAttempts: nextAttempt,
    lastAttemptAt: Date.now(),
    lastTrigger: cleanTargetField(trigger, 40) || 'manual'
  });

  try {
    const result = await applyNoClipBridgeTarget({
      ...invite,
      source: 'url-invite-retry',
      autoSync: true,
      silent: true
    });
    if (result?.autoSync && result?.syncOk === false) {
      const syncError = normalizeInviteErrorMessage(result.syncError || 'sync_failed') || 'sync_failed';
      updatePendingNoClipInvite({
        lastError: syncError,
        lastFailureAt: Date.now(),
        lastStatus: 'sync_failed'
      });
      if (nextAttempt >= INVITE_SYNC_MAX_ATTEMPTS || isInviteSyncTerminalError(syncError)) {
        const targetPub = parseNoClipPub(invite.targetPub || invite.noclipPub || '');
        const short = targetPub ? `noclip.${targetPub.slice(0, 8)}` : 'NoClip invite';
        setBadge(`${short} auto-sync failed after ${nextAttempt} attempts: ${syncError}`, false);
        clearPendingNoClipInvite();
        return { ok: false, reason: 'sync_failed_terminal', error: syncError };
      }
      schedulePendingInviteSyncRetry(syncError);
      return { ok: false, reason: 'sync_failed_retrying', error: syncError };
    }
    const targetPub = result?.targetPub || parseNoClipPub(invite.targetPub || invite.noclipPub || '');
    const short = targetPub ? `noclip.${targetPub.slice(0, 8)}` : 'NoClip invite';
    setBadge(`${short} auto-sync ready`);
    clearPendingNoClipInvite();
    return { ok: true, nodeId: result?.nodeId || '', targetPub: targetPub || '' };
  } catch (err) {
    const message = normalizeInviteErrorMessage(err) || 'sync_failed';
    updatePendingNoClipInvite({
      lastError: message,
      lastFailureAt: Date.now(),
      lastStatus: 'apply_failed'
    });
    if (nextAttempt >= INVITE_SYNC_MAX_ATTEMPTS || isInviteSyncTerminalError(message)) {
      const targetPub = parseNoClipPub(invite.targetPub || invite.noclipPub || '');
      const short = targetPub ? `noclip.${targetPub.slice(0, 8)}` : 'NoClip invite';
      setBadge(`${short} auto-sync failed after ${nextAttempt} attempts: ${message}`, false);
      clearPendingNoClipInvite();
      return { ok: false, reason: 'apply_failed_terminal', error: message };
    }
    schedulePendingInviteSyncRetry(message);
    return { ok: false, reason: 'apply_failed_retrying', error: message };
  } finally {
    InviteSyncState.inFlight = false;
  }
};

const applyNoClipBridgeTarget = async (detail = {}) => {
  const targetPub = parseNoClipPub(detail?.targetPub || detail?.target || '');
  if (!targetPub) {
    throw new Error('invalid_noclip_target_pub');
  }
  const preferredNodeId = cleanTargetField(detail?.bridgeNodeId || detail?.nodeId || detail?.node, 96);
  const targetCtx = normalizeBridgeTargetContext(detail);
  const autoSync = parseBooleanish(detail?.autoSync, false);
  const silent = parseBooleanish(detail?.silent, false);
  const targetLabelRaw = String(detail?.displayName || '').trim();
  const targetLabel = targetLabelRaw || `noclip.${targetPub.slice(0, 8)}...`;
  const { nodeId, created } = ensureNoClipBridgeNode(preferredNodeId);

  NoClipBridge?.setTargetPeer?.(nodeId, targetPub);
  if (targetCtx.hasTarget) {
    NodeStore.update(nodeId, {
      type: 'NoClipBridge',
      ...(targetCtx.sessionId ? { sessionId: targetCtx.sessionId } : {}),
      ...(targetCtx.objectUuid ? { objectUuid: targetCtx.objectUuid } : {}),
      ...(targetCtx.overlayId ? { overlayId: targetCtx.overlayId } : {}),
      ...(targetCtx.itemId ? { itemId: targetCtx.itemId } : {}),
      ...(targetCtx.layerId ? { layerId: targetCtx.layerId } : {}),
      interopTarget: {
        sessionId: targetCtx.sessionId || '',
        objectUuid: targetCtx.objectUuid || '',
        overlayId: targetCtx.overlayId || '',
        itemId: targetCtx.itemId || '',
        layerId: targetCtx.layerId || ''
      }
    });
  }
  NoClipBridge?.refresh?.(nodeId);
  NoClipBridge?.refreshPeerDropdown?.(nodeId, { silent: true });
  setTimeout(() => {
    NoClipBridge?.refreshPeerDropdown?.(nodeId, { silent: true });
  }, 300);

  if (!silent) {
    const nodeEl = Graph.getNode(nodeId)?.el;
    if (nodeEl?.scrollIntoView) {
      try {
        nodeEl.scrollIntoView({ block: 'center', behavior: 'smooth' });
      } catch (_) {
        nodeEl.scrollIntoView({ block: 'center' });
      }
    }
  }

  if (!silent) {
    const statusPrefix = created ? 'Created' : 'Updated';
    const contextLabel = targetCtx.objectUuid
      ? ` • object ${targetCtx.objectUuid.slice(0, 12)}`
      : (targetCtx.sessionId ? ` • session ${targetCtx.sessionId.slice(0, 12)}` : '');
    setBadge(`${statusPrefix} NoClip Bridge ${nodeId} for ${targetLabel}${contextLabel}`);
    NoClipBridge?.logToNode?.(nodeId, `✓ Target set to noclip.${targetPub.slice(0, 8)}…`, 'success');
    if (targetCtx.hasTarget) {
      NoClipBridge?.logToNode?.(
        nodeId,
        `✓ Interop target set (${targetCtx.objectUuid || targetCtx.sessionId || targetCtx.itemId || 'context'})`,
        'info'
      );
    }
  }

  let syncOk = !autoSync;
  let syncError = '';
  if (autoSync) {
    try {
      const requestSync = NoClipBridge?.requestSync;
      if (typeof requestSync !== 'function') {
        throw new Error('noclip_sync_unavailable');
      }
      await requestSync(nodeId, targetPub, { silent });
      syncOk = true;
    } catch (err) {
      syncOk = false;
      syncError = normalizeInviteErrorMessage(err) || 'sync_failed';
      if (!silent) {
        setBadge(`Bridge sync request failed: ${syncError}`, false);
      }
    }
  }
  return {
    ok: true,
    nodeId,
    targetPub,
    autoSync,
    syncOk,
    syncError,
    created
  };
};

window.addEventListener('hydra-noclip-bridge-target', (event) => {
  const detail = event?.detail && typeof event.detail === 'object' ? event.detail : {};
  const source = cleanTargetField(detail?.source, 64);
  const sourceKey = source.toLowerCase();
  const autoSync = parseBooleanish(detail?.autoSync, false);
  const isUrlInvite = isUrlInviteSource(source);
  const isRetrySource = sourceKey.startsWith('url-invite-retry');
  if (isUrlInvite && !autoSync) clearPendingNoClipInvite();

  if (isUrlInvite && autoSync) {
    const current = getPendingNoClipInvite();
    const normalizedTarget = parseNoClipPub(detail.targetPub || detail.target || '');
    const currentTarget = parseNoClipPub(current?.targetPub || current?.noclipPub || '');
    const shouldResetPending = !isRetrySource || !current || currentTarget !== normalizedTarget;
    if (shouldResetPending) {
      clearPendingNoClipInvite();
      window.pendingNoClipInvite = {
        ...detail,
        noclipPub: normalizedTarget,
        source: source || 'url-invite',
        autoSync: true,
        syncAttempts: 0,
        createdAt: Date.now(),
        timestamp: Date.now(),
        lastError: ''
      };
    }
  }

  applyNoClipBridgeTarget(detail).then((result) => {
    if (!isUrlInvite || !autoSync) return;
    if (result?.autoSync && result?.syncOk === false) {
      const errorText = normalizeInviteErrorMessage(result.syncError || 'sync_failed') || 'sync_failed';
      const currentAttempts = Math.max(1, getInviteSyncAttemptCount(getPendingNoClipInvite()));
      updatePendingNoClipInvite({
        syncAttempts: currentAttempts,
        lastError: errorText,
        lastFailureAt: Date.now(),
        lastStatus: 'sync_failed_initial'
      });
      schedulePendingInviteSyncRetry(errorText);
      return;
    }
    clearPendingNoClipInvite();
  }).catch((err) => {
    const message = normalizeInviteErrorMessage(err) || 'bridge_target_failed';
    setBadge(`NoClip bridge target failed: ${message}`, false);
    if (!isUrlInvite || !autoSync) return;
    if (isInviteSyncTerminalError(message)) {
      clearPendingNoClipInvite();
      return;
    }
    const currentAttempts = Math.max(1, getInviteSyncAttemptCount(getPendingNoClipInvite()));
    updatePendingNoClipInvite({
      syncAttempts: currentAttempts,
      lastError: message,
      lastFailureAt: Date.now(),
      lastStatus: 'apply_failed_initial'
    });
    schedulePendingInviteSyncRetry(message);
  });
});

const Flows = createFlowsLibrary({ Graph, log });
Graph.setFlowSaveHandler(() => Flows.openCreate());

const WorkspaceSync = createWorkspaceSync({
  Graph,
  Net,
  CFG,
  saveCFG,
  setBadge,
  log,
  updateTransportButton
});

const PeerDiscovery = createPeerDiscovery({
  Net,
  CFG,
  WorkspaceSync,
  setBadge,
  log,
  NoClip: NoClipBridge
});
SharedPeerDiscovery = PeerDiscovery;

NoClipBridge?.attachPeerDiscovery?.(PeerDiscovery);

// Initialize NoClip Bridge Sync - note: Graph is defined earlier in the file
const NoClipBridgeSync = createNoClipBridgeSync({
  Graph,
  PeerDiscovery,
  Net,
  CFG,
  setBadge,
  log,
  NoClip: NoClipBridge
});
NoClipBridgeSyncImpl = NoClipBridgeSync;

// Link bridge modules for session coordination
NoClipBridge?.registerSyncAdapter?.(NoClipBridgeSync);
NoClipBridgeSync?.registerBridgeAdapter?.(NoClipBridge);

// Register DM handler with PeerDiscovery for NoClip Bridge Sync messages
// This routes all sync-related DMs to the NoClipBridgeSync module
PeerDiscovery.registerDmHandler((event) => {
  const { from, msg } = event || {};
  if (!msg || !msg.type) return;

  // Route all noclip-bridge-sync-* messages to NoClipBridgeSync
  if (msg.type.startsWith('noclip-bridge-sync-')) {
    NoClipBridgeSync.handleDiscoveryDm(event);
  }
});

const graphDropEls = {
  modal: qs('#graphDropModal'),
  backdrop: qs('#graphDropBackdrop'),
  close: qs('#graphDropClose'),
  load: qs('#graphDropLoad'),
  save: qs('#graphDropSave'),
  no: qs('#graphDropNo'),
  message: qs('#graphDropMessage')
};

const graphDropState = { snapshot: null, name: '', source: '' };
let graphDragDepth = 0;

async function runHealthCheck() {
  const defaultRelay = (Net?.nkn?.addr || '').trim();
  const relay = (prompt('Relay NKN address to check', defaultRelay) || defaultRelay || '').trim();
  if (!relay) {
    setBadge('Health check cancelled', false);
    return;
  }
  setBadge(`Checking relay ${relay}…`);
  try {
    const resp = await Net.relayHealth(relay, 15000);
    const iso = resp?.port_isolation ? 'on' : 'off';
    log(`[relay.health] node=${resp?.node || '-'} addr=${resp?.addr || relay} isolation=${iso}`);
    const services = Array.isArray(resp?.services) ? resp.services : [];
    services.forEach((svc) => {
      const name = svc?.name || 'service';
      const port = svc?.port ? `:${svc.port}` : '';
      const endpoint = (svc?.endpoint || '').replace(/^https?:\/\//, '');
      const assigned = svc?.assigned_node ? ` assigned=${svc.assigned_node}` : '';
      log(`  ${name}${port ? ' @ ' + port : ''} → ${endpoint || 'unknown'}${assigned}`);
    });
    if (!services.length) log('  (no services reported)');
    setBadge('Health check complete');
  } catch (err) {
    setBadge(`Health check failed: ${err?.message || err}`, false);
  }
}

const ROUTER_QR_KEYS = [
  'router',
  'router_target',
  'router_nkn',
  'router_nkn_address',
  'routerTargetNknAddress',
  'relay',
  'target',
  'address',
  'nkn'
];

const tryParseRouterTarget = (value) => {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const normalized = RouterDiscovery.normalizeRouterAddress(raw);
  const validation = RouterDiscovery.validateRouterAddress(normalized);
  return validation.ok ? validation.target : '';
};

const parseRouterTargetFromQr = (rawText) => {
  const raw = String(rawText || '').trim();
  if (!raw) return '';

  const direct = tryParseRouterTarget(raw);
  if (direct) return direct;

  if (raw.startsWith('{') || raw.startsWith('[')) {
    try {
      const parsed = JSON.parse(raw);
      const obj = parsed && typeof parsed === 'object' ? parsed : null;
      if (obj) {
        for (const key of ROUTER_QR_KEYS) {
          const candidate = tryParseRouterTarget(obj[key]);
          if (candidate) return candidate;
        }
      }
    } catch (_) {
      // ignore parse errors
    }
  }

  try {
    const url = new URL(raw);
    for (const key of ROUTER_QR_KEYS) {
      const candidate = tryParseRouterTarget(url.searchParams.get(key) || '');
      if (candidate) return candidate;
    }
    if (String(url.protocol || '').toLowerCase() === 'nkn:') {
      const nknCandidate = [url.host, url.pathname.replace(/^\/+/, '')].filter(Boolean).join('');
      const candidate = tryParseRouterTarget(nknCandidate);
      if (candidate) return candidate;
    }
  } catch (_) {
    // ignore URL parse errors
  }

  if (raw.includes('=')) {
    try {
      const params = new URLSearchParams(raw);
      for (const key of ROUTER_QR_KEYS) {
        const candidate = tryParseRouterTarget(params.get(key) || '');
        if (candidate) return candidate;
      }
    } catch (_) {
      // ignore query parse errors
    }
  }

  return '';
};

function bindUI() {
  const toggle = qs('#transportToggle');
  if (toggle) {
    toggle.classList.add('transport-locked');
    toggle.addEventListener('click', (e) => {
      e.preventDefault();
      setBadge('Hybrid transport: HTTP for local endpoints, NKN for relays');
    });
  }
  const healthBtn = qs('#healthCheckBtn');
  if (healthBtn) {
    healthBtn.addEventListener('click', (e) => {
      e.preventDefault();
      runHealthCheck();
    });
  }
  const routerInput = qs('#routerTargetInput');
  const routerResolveBtn = qs('#routerResolveBtn');
  const routerReconnectBtn = qs('#routerReconnectBtn');
  const routerScanBtn = qs('#routerScanBtn');
  const routerAutoBtn = qs('#routerAutoResolveBtn');
  const directoryRefreshBtn = qs('#marketplaceDirectoryRefreshBtn');
  const publishBtn = qs('#marketplacePublishBtn');
  const publishScopeSelect = qs('#marketplacePublishScope');
  const publishIncludeStatus = qs('#marketplacePublishIncludeStatus');
  const publishResultEl = qs('#marketplacePublishResult');
  const routerStatus = qs('#routerResolveStatus');
  const routerMessage = qs('#routerResolveMessage');

  const formatResolveError = (message) => {
    const text = String(message || '').trim();
    if (!text) return 'Router resolve failed';
    if (/timeout/i.test(text)) return 'Resolve timed out waiting for router response';
    if (/missing router target/i.test(text)) return 'Enter router NKN address first';
    if (/not ready/i.test(text)) return 'NKN transport is not ready yet';
    return text;
  };

  const summarizeResolvedWarnings = (resolved) => {
    const map = resolved && typeof resolved === 'object' ? resolved : {};
    const stale = [];
    const errors = [];
    for (const [service, entry] of Object.entries(map)) {
      const raw = entry?.raw && typeof entry.raw === 'object' ? entry.raw : (entry || {});
      const tunnelError = String(raw.tunnel_error || raw?.tunnel?.error || raw?.fallback?.cloudflare?.error || '').trim();
      const staleTunnel = String(raw.stale_tunnel_url || raw?.tunnel?.stale_tunnel_url || raw?.fallback?.cloudflare?.stale_tunnel_url || '').trim();
      if (tunnelError) {
        errors.push(`${service}: ${tunnelError}`);
        continue;
      }
      if (staleTunnel) stale.push(service);
    }
    if (errors.length) {
      const first = errors.slice(0, 2).join(' | ');
      const extra = errors.length > 2 ? ` (+${errors.length - 2} more)` : '';
      return `Cloudflared warning: ${first}${extra}`;
    }
    if (stale.length) {
      const labels = stale.slice(0, 3).join(', ');
      const extra = stale.length > 3 ? ` (+${stale.length - 3} more)` : '';
      return `Stale cloudflared URL: ${labels}${extra}`;
    }
    return '';
  };

  const renderRouterState = (state) => {
    if (!routerStatus && !routerMessage) return;
    const status = String(state?.status || RouterDiscovery.getStatus() || 'idle');
    const resolvedMap = state?.resolved && typeof state.resolved === 'object'
      ? state.resolved
      : (CFG.routerResolvedEndpoints && typeof CFG.routerResolvedEndpoints === 'object' ? CFG.routerResolvedEndpoints : {});
    const serviceCount = Object.keys(resolvedMap).length;
    const warning = summarizeResolvedWarnings(resolvedMap);
    let displayStatus = status;
    let tone = 'muted';
    let message = 'Router discovery idle';

    if (state?.stale) {
      displayStatus = 'warn';
      tone = 'warn';
      message = 'Ignored stale router resolve response';
    } else if (status === 'resolving') {
      tone = 'warn';
      message = 'Resolving router endpoints...';
    } else if (status === 'ok') {
      if (warning) {
        displayStatus = 'warn';
        tone = 'warn';
        message = warning;
      } else {
        tone = 'ok';
        message = serviceCount
          ? `Resolved ${serviceCount} service endpoint${serviceCount === 1 ? '' : 's'}`
          : 'Resolve complete (no services reported)';
      }
    } else if (status === 'error') {
      tone = 'error';
      message = formatResolveError(state?.lastError || CFG.routerLastResolveError || '');
    } else if (CFG.routerLastResolveError) {
      tone = 'warn';
      message = `Last error: ${formatResolveError(CFG.routerLastResolveError)}`;
    }

    if (routerStatus) {
      routerStatus.textContent = displayStatus;
      routerStatus.dataset.status = displayStatus;
    }
    if (routerMessage) {
      routerMessage.textContent = message;
      routerMessage.dataset.tone = tone;
      routerMessage.title = message;
    }
    if (routerInput && state && typeof state.target === 'string' && routerInput.value !== state.target) {
      routerInput.value = state.target;
    }
    if (routerAutoBtn) {
      routerAutoBtn.classList.toggle('active', !!CFG.routerAutoResolve);
      routerAutoBtn.textContent = CFG.routerAutoResolve ? 'Auto:On' : 'Auto';
    }
  };

  if (routerInput) {
    routerInput.value = String(CFG.routerTargetNknAddress || '');
    routerInput.addEventListener('input', () => {
      RouterDiscovery.setTarget(routerInput.value || '');
    });
  }
  if (routerResolveBtn) {
    routerResolveBtn.addEventListener('click', async (e) => {
      e.preventDefault();
      const target = RouterDiscovery.setTarget(routerInput?.value || CFG.routerTargetNknAddress || '');
      if (!target) {
        setBadge('Enter router NKN address first', false);
        return;
      }
      try {
        await RouterDiscovery.resolveNow(target, { timeoutMs: 25000 });
      } catch (_) {
        // already surfaced in RouterDiscovery
      }
    });
  }
  if (routerReconnectBtn) {
    routerReconnectBtn.addEventListener('click', async (e) => {
      e.preventDefault();
      if (routerReconnectBtn.disabled) return;
      const target = RouterDiscovery.setTarget(routerInput?.value || CFG.routerTargetNknAddress || '');
      routerReconnectBtn.disabled = true;
      setBadge('Reconnecting NKN transport...');
      try {
        await Net.reconnectNkn({ timeout: 25000 });
        setBadge('NKN transport reconnected');
        if (target) {
          await RouterDiscovery.resolveNow(target, { timeoutMs: 25000 });
        } else {
          renderRouterState({ status: RouterDiscovery.getStatus(), target: '' });
        }
      } catch (err) {
        setBadge(`Reconnect failed: ${err?.message || err}`, false);
      } finally {
        routerReconnectBtn.disabled = false;
      }
    });
  }
  if (routerScanBtn) {
    routerScanBtn.addEventListener('click', async (e) => {
      e.preventDefault();
      await openQrScanner(routerInput, (text) => {
        const parsed = parseRouterTargetFromQr(text);
        if (!parsed) {
          setBadge('QR did not contain a valid router NKN address', false);
          return;
        }
        const target = RouterDiscovery.setTarget(parsed);
        if (routerInput) routerInput.value = target;
      }, { populateTarget: false });
    });
  }
  if (routerAutoBtn) {
    routerAutoBtn.addEventListener('click', (e) => {
      e.preventDefault();
      CFG.routerAutoResolve = !CFG.routerAutoResolve;
      saveCFG();
      if (CFG.routerAutoResolve) RouterDiscovery.startAuto();
      else RouterDiscovery.stopAuto();
      renderRouterState({ status: RouterDiscovery.getStatus(), target: CFG.routerTargetNknAddress || '' });
    });
  }
  if (directoryRefreshBtn) {
    directoryRefreshBtn.addEventListener('click', async (e) => {
      e.preventDefault();
      if (directoryRefreshBtn.disabled) return;
      directoryRefreshBtn.disabled = true;
      try {
        await MarketplaceDirectory?.refresh?.({ manual: true });
      } finally {
        directoryRefreshBtn.disabled = false;
      }
    });
  }
  if (publishBtn) {
    publishBtn.addEventListener('click', async (e) => {
      e.preventDefault();
      if (publishBtn.disabled) return;
      const publishScope = String(publishScopeSelect?.value || 'both').trim().toLowerCase();
      const includeStatus = publishIncludeStatus?.checked !== false;
      publishBtn.disabled = true;
      if (publishResultEl) {
        publishResultEl.textContent = 'publishing...';
        publishResultEl.style.color = 'var(--warn)';
      }
      try {
        window.dispatchEvent(new CustomEvent('hydra-provider-publish-catalog', {
          detail: {
            publishScope,
            includeStatus,
            silent: false
          }
        }));
      } finally {
        setTimeout(() => {
          publishBtn.disabled = false;
        }, 250);
      }
    });
  }
  RouterDiscovery.subscribe(renderRouterState);
  renderRouterState({ status: CFG.routerLastResolveStatus || 'idle', target: CFG.routerTargetNknAddress || '' });
  if (CFG.routerAutoResolve) RouterDiscovery.startAuto();
  MarketplaceSummary.start();
  MarketplaceDirectory?.start?.();
  MarketplaceConfigEditor?.start?.();

  if (CFG.transport !== 'nkn') {
    CFG.transport = 'nkn';
    saveCFG();
  }
  Net.ensureNkn();
  updateTransportButton();
}

const cloneGraphSnapshot = (snapshot) => {
  if (!snapshot || typeof snapshot !== 'object') return null;
  const base = {
    nodes: Array.isArray(snapshot.nodes) ? snapshot.nodes : [],
    links: Array.isArray(snapshot.links) ? snapshot.links : [],
    nodeConfigs: snapshot.nodeConfigs && typeof snapshot.nodeConfigs === 'object' ? snapshot.nodeConfigs : {}
  };
  if (snapshot.viewport && typeof snapshot.viewport === 'object') base.viewport = snapshot.viewport;
  if (snapshot.transport) base.transport = snapshot.transport;
  if (snapshot.meta && typeof snapshot.meta === 'object') base.meta = snapshot.meta;
  if (snapshot.nodeStates && typeof snapshot.nodeStates === 'object') base.nodeStates = snapshot.nodeStates;
  try {
    return JSON.parse(JSON.stringify(base));
  } catch (err) {
    return base;
  }
};

function normalizeDroppedGraph(parsed) {
  if (!parsed || typeof parsed !== 'object') return null;
  if (Array.isArray(parsed.nodes)) {
    return {
      snapshot: cloneGraphSnapshot(parsed),
      name: typeof parsed.name === 'string' ? parsed.name : ''
    };
  }
  if (parsed.data && typeof parsed.data === 'object' && Array.isArray(parsed.data.nodes)) {
    const inner = cloneGraphSnapshot(parsed.data);
    return {
      snapshot: inner,
      name: typeof parsed.name === 'string' && parsed.name.trim()
        ? parsed.name
        : typeof parsed.data.name === 'string' ? parsed.data.name : ''
    };
  }
  return null;
}

function resetGraphDropState() {
  graphDropState.snapshot = null;
  graphDropState.name = '';
  graphDropState.source = '';
}

function closeGraphDropModal() {
  if (graphDropEls.modal) {
    graphDropEls.modal.classList.add('hidden');
    graphDropEls.modal.setAttribute('aria-hidden', 'true');
  }
  resetGraphDropState();
}

function openGraphDropModal(label) {
  if (!graphDropEls.modal) return;
  graphDropEls.message.textContent = label
    ? `Graph detected in "${label}". Load into editor?`
    : 'Graph detected, load into editor?';
  graphDropEls.modal.classList.remove('hidden');
  graphDropEls.modal.setAttribute('aria-hidden', 'false');
}

function clearGraphDropHighlight() {
  graphDragDepth = 0;
  document.body.classList.remove('graph-drop-hover');
}

function handleGraphFileDrop(fileList) {
  const files = Array.from(fileList || []);
  if (!files.length) {
    setBadge('Drop contained no files', false);
    return;
  }
  const jsonFile = files.find((file) => {
    if (!file) return false;
    const name = String(file.name || '').toLowerCase();
    return name.endsWith('.json') || file.type === 'application/json' || file.type === 'text/json';
  });
  if (!jsonFile) {
    setBadge('No compatible JSON graph found in drop', false);
    return;
  }
  jsonFile
    .text()
    .then((text) => {
      let parsed;
      try {
        parsed = JSON.parse(text);
      } catch (err) {
        setBadge('Dropped file is not valid JSON', false);
        return;
      }
      const normalized = normalizeDroppedGraph(parsed);
      if (!normalized?.snapshot) {
        setBadge('Dropped file is not a compatible graph', false);
        return;
      }
      graphDropState.snapshot = normalized.snapshot;
      graphDropState.name = typeof normalized.name === 'string' ? normalized.name.trim() : '';
      graphDropState.source = jsonFile.name || graphDropState.name;
      openGraphDropModal(graphDropState.source || graphDropState.name);
    })
    .catch((err) => setBadge(`Unable to read file: ${err?.message || err}`, false));
}

function bindGraphDropModal() {
  graphDropEls.close?.addEventListener('click', closeGraphDropModal);
  graphDropEls.backdrop?.addEventListener('click', closeGraphDropModal);
  graphDropEls.no?.addEventListener('click', (e) => {
    e.preventDefault();
    closeGraphDropModal();
  });
  graphDropEls.load?.addEventListener('click', (e) => {
    e.preventDefault();
    if (!graphDropState.snapshot) {
      closeGraphDropModal();
      return;
    }
    const loaded = Graph.importWorkspace(graphDropState.snapshot, { badgeText: 'Graph loaded from file drop' });
    if (!loaded) {
      setBadge('Unable to load dropped graph', false);
    }
    closeGraphDropModal();
  });
  graphDropEls.save?.addEventListener('click', (e) => {
    e.preventDefault();
    if (!graphDropState.snapshot) {
      closeGraphDropModal();
      return;
    }
    const name = graphDropState.name || graphDropState.source || 'Dropped Flow';
    const flow = Flows.saveSnapshot(name, graphDropState.snapshot);
    if (flow) setBadge(`Saved as "${flow.name}"`);
    else setBadge('Unable to save flow', false);
    closeGraphDropModal();
  });
}

function bindGlobalDrop() {
  bindGraphDropModal();
  const hasFiles = (event) => {
    const dt = event?.dataTransfer;
    if (!dt) return false;
    if (dt.items) {
      for (const item of dt.items) {
        if (item.kind === 'file') return true;
      }
    }
    if (dt.types) {
      return Array.from(dt.types).includes('Files');
    }
    return false;
  };

  window.addEventListener('dragenter', (e) => {
    if (!hasFiles(e)) return;
    graphDragDepth += 1;
    document.body.classList.add('graph-drop-hover');
  });

  window.addEventListener('dragleave', (e) => {
    graphDragDepth = Math.max(0, graphDragDepth - 1);
    if (graphDragDepth === 0) clearGraphDropHighlight();
  });

  const handleDragOver = (e) => {
    if (!hasFiles(e)) return;
    e.preventDefault();
    e.stopPropagation();
    if (e.dataTransfer) e.dataTransfer.dropEffect = 'copy';
  };

  const handleDrop = (e) => {
    if (!hasFiles(e)) return;
    e.preventDefault();
    e.stopPropagation();
    clearGraphDropHighlight();
    handleGraphFileDrop(e.dataTransfer?.files);
  };

  window.addEventListener('dragover', handleDragOver);
  window.addEventListener('drop', handleDrop);
  document.addEventListener('dragover', handleDragOver, true);
  document.addEventListener('drop', handleDrop, true);
}

function init() {
  setupQrScanner();
  bindUI();
  Graph.init();
  Router.render();
  if (CFG.transport === 'nkn') Net.ensureNkn();
  WorkspaceSync.init();
  PeerDiscovery.init();

  // Initialize NoClip Bridge Sync system
  NoClipBridgeSync.init();

  bindGlobalDrop();

  // Initialize Smart Object Invite system
  initSmartObjectInvite({ NodeStore, Net, setBadge });

  // Handle URL parameters for NoClip Smart Object invites
  handleInviteUrlParams();

  setBadge('Ready');
}

/**
 * Handle URL parameters for NoClip Smart Object invites
 * Supports: ?noclip=noclip.<hex>&object=<uuid>
 */
function handleInviteUrlParams() {
  try {
    const url = new URL(window.location.href);

    const firstValue = (keys = []) => {
      for (const key of keys) {
        const value = String(url.searchParams.get(key) || '').trim();
        if (value) return value;
      }
      return '';
    };

    const noclipPub = parseNoClipPub(firstValue(['noclip', 'peer', 'target']));
    if (!noclipPub) return;
    const autoSync = parseBooleanish(firstValue(['autosync', 'autoSync', 'sync']), false);

    const inviteDetail = {
      targetPub: noclipPub,
      targetAddr: `noclip.${noclipPub}`,
      displayName: 'URL invite',
      source: 'url-invite',
      autoSync,
      bridgeNodeId: firstValue(['bridgeNodeId', 'bridge_node_id', 'nodeId', 'node']),
      sessionId: firstValue(['sessionId', 'session_id', 'session']),
      objectUuid: firstValue(['objectUuid', 'object_uuid', 'objectId', 'object_id', 'object']),
      overlayId: firstValue(['overlayId', 'overlay_id', 'overlay']),
      itemId: firstValue(['itemId', 'item_id', 'item']),
      layerId: firstValue(['layerId', 'layer_id', 'layer'])
    };
    if (!inviteDetail.objectUuid && inviteDetail.itemId) {
      inviteDetail.objectUuid = inviteDetail.itemId;
    }

    clearPendingNoClipInvite();
    if (autoSync) {
      const now = Date.now();
      window.pendingNoClipInvite = {
        ...inviteDetail,
        noclipPub,
        syncAttempts: 0,
        createdAt: now,
        timestamp: now,
        lastError: ''
      };
    }

    try {
      window.dispatchEvent(new CustomEvent('hydra-noclip-bridge-target', { detail: inviteDetail }));
      const suffix = inviteDetail.objectUuid ? ` • object ${inviteDetail.objectUuid.slice(0, 12)}` : '';
      if (autoSync) {
        setBadge(`NoClip invite loaded: noclip.${noclipPub.slice(0, 8)}${suffix} • auto-sync`);
      } else {
        setBadge(`NoClip invite applied: noclip.${noclipPub.slice(0, 8)}${suffix}`);
      }
    } catch (err) {
      setBadge(`NoClip invite detected for noclip.${noclipPub.slice(0, 8)}`, true);
    }

    [
      'noclip',
      'peer',
      'target',
      'autosync',
      'autoSync',
      'sync',
      'bridgeNodeId',
      'bridge_node_id',
      'nodeId',
      'node',
      'sessionId',
      'session_id',
      'session',
      'objectUuid',
      'object_uuid',
      'objectId',
      'object_id',
      'object',
      'overlayId',
      'overlay_id',
      'overlay',
      'itemId',
      'item_id',
      'item',
      'layerId',
      'layer_id',
      'layer',
      'kind'
    ].forEach((key) => url.searchParams.delete(key));
    const newUrl = url.pathname + (url.searchParams.toString() ? `?${url.searchParams.toString()}` : '') + url.hash;
    try {
      window.history.replaceState({}, document.title, newUrl);
    } catch (_) {
      // ignore history failures
    }

  } catch (err) {
    console.error('[Hydra] Failed to parse invite URL parameters:', err);
  }
}

document.addEventListener('DOMContentLoaded', init);
