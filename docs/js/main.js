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
    const result = await NoClipBridge?.publishMarketplaceCatalog?.({
      targetPub: typeof detail.targetPub === 'string' ? detail.targetPub : '',
      includeStatus: detail.includeStatus !== false,
      force: detail.force === true,
      silent: detail.silent === true
    });
    if (result?.ok) {
      setBadge(`Marketplace catalog published (${result.sent}/${result.attempted})`);
    } else {
      setBadge('Marketplace catalog publish skipped', false);
    }
  } catch (err) {
    setBadge(`Marketplace catalog publish failed: ${err?.message || err}`, false);
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
  if ((!resolved || typeof resolved !== 'object') && !catalogRaw) return;
  const incomingTs = Number(reply?.timestampMs || reply?.rawReply?.timestamp_ms || catalogGeneratedTs || 0);
  const currentTs = Number(CFG.routerLastResolvedAt || 0);
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
          source: 'marketplace.directory'
        });
      } catch (err) {
        log(`[market.directory] peer ingest failed: ${err?.message || err}`);
      }
    }
    MarketplaceSummary?.onDirectoryPreview?.(detail);
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

const applyNoClipBridgeTarget = async (detail = {}) => {
  const targetPub = parseNoClipPub(detail?.targetPub || detail?.target || '');
  if (!targetPub) {
    throw new Error('invalid_noclip_target_pub');
  }
  const preferredNodeId = cleanTargetField(detail?.bridgeNodeId || detail?.nodeId || detail?.node, 96);
  const targetCtx = normalizeBridgeTargetContext(detail);
  const autoSync = parseBooleanish(detail?.autoSync, false);
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

  const nodeEl = Graph.getNode(nodeId)?.el;
  if (nodeEl?.scrollIntoView) {
    try {
      nodeEl.scrollIntoView({ block: 'center', behavior: 'smooth' });
    } catch (_) {
      nodeEl.scrollIntoView({ block: 'center' });
    }
  }

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

  if (autoSync) {
    try {
      await NoClipBridge?.requestSync?.(nodeId, targetPub);
    } catch (err) {
      setBadge(`Bridge sync request failed: ${err?.message || err}`, false);
    }
  }
};

window.addEventListener('hydra-noclip-bridge-target', (event) => {
  const detail = event?.detail && typeof event.detail === 'object' ? event.detail : {};
  applyNoClipBridgeTarget(detail).catch((err) => {
    setBadge(`NoClip bridge target failed: ${err?.message || err}`, false);
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
  RouterDiscovery.subscribe(renderRouterState);
  renderRouterState({ status: CFG.routerLastResolveStatus || 'idle', target: CFG.routerTargetNknAddress || '' });
  if (CFG.routerAutoResolve) RouterDiscovery.startAuto();
  MarketplaceSummary.start();
  MarketplaceDirectory?.start?.();

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

    const inviteDetail = {
      targetPub: noclipPub,
      targetAddr: `noclip.${noclipPub}`,
      displayName: 'URL invite',
      source: 'url-invite',
      autoSync: parseBooleanish(firstValue(['autosync', 'autoSync', 'sync']), false),
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

    window.pendingNoClipInvite = {
      ...inviteDetail,
      noclipPub,
      timestamp: Date.now()
    };

    try {
      window.dispatchEvent(new CustomEvent('hydra-noclip-bridge-target', { detail: inviteDetail }));
      const suffix = inviteDetail.objectUuid ? ` • object ${inviteDetail.objectUuid.slice(0, 12)}` : '';
      setBadge(`NoClip invite applied: noclip.${noclipPub.slice(0, 8)}${suffix}`);
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
