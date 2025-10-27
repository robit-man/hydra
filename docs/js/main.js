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
import { primeLocalNetworkRequest } from './localNetwork.js';
import { createPeerDiscovery } from './peerDiscovery.js';
import { createPayments } from './payments.js';
import { createWebScraper } from './webScraper.js';
import { createNoClipBridge } from './noclipBridge.js';
import { initSmartObjectInvite } from './smartObjectInvite.js';
import { createNoClipBridgeSync } from './noclipBridgeSync.js';

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

const NoClipBridge = createNoClipBridge({
  NodeStore,
  Router,
  Net,
  CFG,
  setBadge,
  log
});

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
  NoClip: NoClipBridge,
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
  log
});

// Initialize NoClip Bridge Sync - note: Graph is defined earlier in the file
const NoClipBridgeSync = createNoClipBridgeSync({
  Graph,
  PeerDiscovery,
  Net,
  CFG,
  setBadge,
  log
});

// Register DM handler with PeerDiscovery
PeerDiscovery.registerDmHandler((event) => {
  const { from, msg } = event || {};
  if (!msg || !msg.type) return;

  if (msg.type === 'noclip-bridge-sync-request') {
    NoClipBridgeSync.handleSyncRequest(from, msg);
  } else if (msg.type === 'noclip-bridge-sync-accepted') {
    // NoClip acknowledging our approval (optional)
    console.log('[NoClipBridgeSync] Sync acknowledged by NoClip:', from);
  } else if (msg.type === 'noclip-bridge-sync-rejected') {
    // NoClip rejected our approval (optional)
    console.log('[NoClipBridgeSync] Sync rejected by NoClip:', from);
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

function bindUI() {
  const toggle = qs('#transportToggle');
  if (toggle) {
    toggle.classList.add('transport-locked');
    toggle.addEventListener('click', (e) => {
      e.preventDefault();
      setBadge('Hybrid transport: HTTP for local endpoints, NKN for relays');
    });
  }
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
