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

const applyRouterResolvedPayload = (reply) => {
  const resolved = normalizeResolvedMap(reply);
  if (!resolved || typeof resolved !== 'object') return;
  const incomingTs = Number(reply?.timestampMs || reply?.rawReply?.timestamp_ms || 0);
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
  saveCFG();
};

const RouterDiscovery = createRouterDiscovery({
  Net,
  CFG,
  saveCFG,
  setBadge,
  log,
  onResolved: applyRouterResolvedPayload
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
  RouterDiscovery.subscribe(renderRouterState);
  renderRouterState({ status: CFG.routerLastResolveStatus || 'idle', target: CFG.routerTargetNknAddress || '' });
  if (CFG.routerAutoResolve) RouterDiscovery.startAuto();

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

    // Check for noclip parameter (from NoClip Smart Object)
    const noclipParam = url.searchParams.get('noclip');
    const objectParam = url.searchParams.get('object');

    if (noclipParam) {
      // Parse NoClip address: noclip.<hex> or just <hex>
      const parts = noclipParam.split('.');
      const noclipPub = parts.length === 2 ? parts[1] : noclipParam;

      console.log('[Hydra] NoClip invite detected:', { noclipPub, objectParam });

      // Show notification
      setTimeout(() => {
        const message = objectParam
          ? `Ready to connect to NoClip Smart Object!\n\nObject ID: ${objectParam}\nNoClip Peer: ${noclipPub}\n\nCreate or select a NoClipBridge node to establish the connection.`
          : `Ready to connect to NoClip peer: ${noclipPub}\n\nCreate a NoClipBridge node to establish the connection.`;

        setBadge('NoClip invite detected', true);
        alert(message);
      }, 1000);

      // Store invite info for later use
      window.pendingNoClipInvite = {
        noclipPub,
        objectUuid: objectParam,
        timestamp: Date.now()
      };

      // Clean up URL
      url.searchParams.delete('noclip');
      url.searchParams.delete('object');
      const newUrl = url.pathname + (url.searchParams.toString() ? `?${url.searchParams.toString()}` : '') + url.hash;

      try {
        window.history.replaceState({}, document.title, newUrl);
      } catch (err) {
        // ignore history failures
      }
    }

  } catch (err) {
    console.error('[Hydra] Failed to parse invite URL parameters:', err);
  }
}

document.addEventListener('DOMContentLoaded', init);
