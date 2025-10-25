import { CFG } from './config.js';

const componentIndex = new Map();
const addressIndex = new Map();
const nodeAddressIndex = new Map();
const subscriberMap = new Map();
const pendingEnvelopes = [];

const COMPONENT_TYPE = 'MediaStream';
const MEDIA_EVENT = 'nkndm.media';
const LEGACY_MEDIA_EVENT = 'mediastream.control';
const HEARTBEAT_INTERVAL_MS = 10_000;
const HEARTBEAT_MAX_MISSED = 3;
const HANDSHAKE_RETRY_MS = 3_000;
const MAX_PENDING_ATTEMPTS = 6;

const HANDSHAKE_DEFAULT = {
  status: 'idle',
  direction: 'idle',
  id: '',
  ts: 0,
  commitActive: false,
  remoteId: '',
  graphId: '',
  peer: ''
};

const PEER_META_DEFAULT = {
  status: 'offline',
  lastPing: 0,
  lastPong: 0,
  missed: 0,
  awaitingPingId: '',
  latency: null,
  active: false,
  remoteActive: false,
  viewing: false,
  remoteViewing: false,
  auto: false,
  componentId: '',
  graphId: '',
  handshake: { ...HANDSHAKE_DEFAULT }
};

const mediaLog = (label, details) => {
  try {
    if (typeof console === 'undefined') return;
    if (details !== undefined) console.log(`[media] ${label}`, details);
    else console.log(`[media] ${label}`);
  } catch (err) {
    // ignore console failures so UI keeps working
  }
};

const scheduleNotify = (id) => {
  const subs = subscriberMap.get(id);
  if (!subs || !subs.size) return;
  for (const fn of Array.from(subs)) {
    try {
      fn();
    } catch (err) {
      // ignore subscriber errors
    }
  }
};

const subscribeToNode = (id, fn) => {
  if (typeof fn !== 'function') return () => {};
  if (!subscriberMap.has(id)) subscriberMap.set(id, new Set());
  const set = subscriberMap.get(id);
  set.add(fn);
  return () => {
    const group = subscriberMap.get(id);
    if (!group) return;
    group.delete(fn);
    if (!group.size) subscriberMap.delete(id);
  };
};

const stripGraphPrefix = (addr) => String(addr || '').replace(/^graph\./i, '').trim();
const ensureGraphPrefix = (addr) => {
  const raw = stripGraphPrefix(addr);
  if (!raw) return '';
  const lower = raw.toLowerCase();
  // Use hydra. prefix for Hydra peers (changed from graph.)
  return lower.startsWith('hydra.') ? lower : `hydra.${lower}`;
};

const formatAddress = (addr) => {
  if (!addr) return '';
  const text = String(addr);
  if (text.length <= 20) return text;
  return `${text.slice(0, 10)}…${text.slice(-6)}`;
};

const isHandshakeAccepted = (meta) => (meta?.handshake?.status === 'accepted');
const isHandshakePendingIncoming = (meta) => (meta?.handshake?.status === 'pending' && meta?.handshake?.direction === 'incoming');

const queueEnvelope = (envelope, attempts = 0) => {
  pendingEnvelopes.push({ envelope, attempts });
  if (pendingEnvelopes.length > 100) pendingEnvelopes.shift();
};

function flushPending() {
  if (!pendingEnvelopes.length) return;
  const remaining = [];
  while (pendingEnvelopes.length) {
    const entry = pendingEnvelopes.shift();
    const recipients = resolveRecipients(entry.envelope);
    if (!recipients.size) {
      if (entry.attempts + 1 < MAX_PENDING_ATTEMPTS) {
        remaining.push({ envelope: entry.envelope, attempts: entry.attempts + 1 });
      }
      continue;
    }
    recipients.forEach((nodeId) => {
      try {
        handleIncoming(nodeId, entry.envelope);
      } catch (err) {
        // ignore per-node errors
      }
    });
  }
  pendingEnvelopes.push(...remaining);
}

const arrayBufferToBase64 = (buffer) => {
  const bytes = new Uint8Array(buffer);
  let binary = '';
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  return btoa(binary);
};

const base64ToBlob = (b64, mime) => {
  const binary = atob(b64);
  const len = binary.length;
  const bytes = new Uint8Array(len);
  for (let i = 0; i < len; i++) bytes[i] = binary.charCodeAt(i);
  return new Blob([bytes], { type: mime || 'application/octet-stream' });
};

const base64ToArrayBuffer = (b64) => {
  const binary = atob(b64);
  const len = binary.length;
  const bytes = new Uint8Array(len);
  for (let i = 0; i < len; i++) bytes[i] = binary.charCodeAt(i);
  return bytes.buffer;
};

function createMediaNode({ getNode, Router, NodeStore, Net, setBadge, log, setRelayState = () => {} }) {
  const state = new Map();
  let listenerAttached = false;

  function ensureState(id) {
    if (!state.has(id)) {
      state.set(id, {
        running: false,
        stream: null,
        frameTimer: null,
        recorder: null,
        recorderHandler: null,
        canvas: null,
        lastSendTs: 0,
        lastRemoteFrom: null,
        audioCtx: null,
        audioQueueTime: 0,
        frameSeq: 0,
        audioSeq: 0,
        remotePeers: new Map(),
        heartbeats: new Map(),
        handshakeTimers: new Map()
      });
      ensureComponentId(id);
      refreshAddressIndex(id);
      flushPending();
    }
    return state.get(id);
  }

  function nodeElements(id) {
    const node = getNode(id);
    if (!node || !node.el) return {};
    return {
      container: node.el,
      localVideo: node.el.querySelector('[data-media-local]'),
      localWrap: node.el.querySelector('[data-media-local-wrap]'),
      localHint: node.el.querySelector('[data-media-local-hint]'),
      flashBtn: node.el.querySelector('[data-media-flash]'),
      targetInput: node.el.querySelector('[data-media-target-input]'),
      targetAdd: node.el.querySelector('[data-media-target-add]'),
      targetChips: node.el.querySelector('[data-media-target-chips]'),
      remoteGrid: node.el.querySelector('[data-media-remote-grid]'),
      remoteEmpty: node.el.querySelector('[data-media-remote-empty]'),
      remoteInfo: node.el.querySelector('[data-media-remote-info]'),
      status: node.el.querySelector('[data-media-status]'),
      audioOut: node.el.querySelector('[data-media-audio]')
    };
  }

  function config(id) {
    return NodeStore.ensure(id, 'MediaStream').config || {};
  }

  function unregisterAddresses(id) {
    const prev = nodeAddressIndex.get(id);
    if (!prev) return;
    for (const addr of prev) {
      const set = addressIndex.get(addr);
      if (!set) continue;
      set.delete(id);
      if (!set.size) addressIndex.delete(addr);
    }
    nodeAddressIndex.delete(id);
  }

  function refreshAddressIndex(id) {
    const cfg = config(id);
    if (!cfg) return;
    const collected = new Set();
    const add = (value) => {
      const normalized = ensureGraphPrefix(value);
      if (normalized) collected.add(normalized);
    };
    const targets = Array.isArray(cfg.targets) ? cfg.targets : [];
    const activeTargets = Array.isArray(cfg.activeTargets) ? cfg.activeTargets : [];
    targets.forEach(add);
    activeTargets.forEach(add);
    if (cfg.address) add(cfg.address);
    if (cfg.handshake?.peer) add(cfg.handshake.peer);
    if (cfg.peer?.address) add(cfg.peer.address);
    if (cfg.pendingAddress) add(cfg.pendingAddress);
    if (cfg.lastRemoteFrom) add(cfg.lastRemoteFrom);
    const meta = cfg.peerMeta || {};
    for (const key of Object.keys(meta)) add(key);
    const prev = nodeAddressIndex.get(id) || new Set();
    for (const addr of prev) {
      if (!collected.has(addr)) {
        const set = addressIndex.get(addr);
        if (!set) continue;
        set.delete(id);
        if (!set.size) addressIndex.delete(addr);
      }
    }
    for (const addr of collected) {
      if (!addressIndex.has(addr)) addressIndex.set(addr, new Set());
      addressIndex.get(addr).add(id);
    }
    nodeAddressIndex.set(id, collected);
  }

  function qualityFromCompression(compression) {
    const value = Number(compression);
    if (!Number.isFinite(value)) return 0.5;
    const clamped = Math.max(0, Math.min(100, value));
    return Math.max(0.05, (100 - clamped) / 100);
  }

  function emitStatus(id, payload) {
    try {
      Router.sendFrom(id, 'status', { nodeId: id, ...payload });
      mediaLog('status', { nodeId: id, ...payload });
    } catch (err) {
      // ignore router errors
    }
  }

  function getTargets(id) {
    const cfg = config(id);
    const list = Array.isArray(cfg.targets) ? cfg.targets : [];
    const normalized = [];
    const seen = new Set();
    for (const raw of list) {
      const addr = ensureGraphPrefix(raw);
      if (!addr || seen.has(addr)) continue;
      seen.add(addr);
      normalized.push(addr);
    }
    if (normalized.length !== list.length) {
      NodeStore.update(id, { type: 'MediaStream', targets: normalized });
    }
    return normalized;
  }

  function getActiveTargets(id) {
    const cfg = config(id);
    const list = Array.isArray(cfg.activeTargets) ? cfg.activeTargets : [];
    return list.map(ensureGraphPrefix).filter(Boolean);
  }

  function updateRelaySummary(id) {
    if (typeof setRelayState !== 'function') return;
    const targets = getTargets(id);
    const meta = getPeerMeta(id);
    let accepted = 0;
    let pendingCount = 0;
    let viewing = 0;
    for (const addr of targets) {
      const info = meta[addr] || {};
      const status = info.handshake?.status || 'idle';
      if (status === 'accepted') {
        accepted += 1;
        if (info.viewing || info.active) viewing += 1;
      } else if (status === 'pending') {
        pendingCount += 1;
      }
    }
    let state = 'warn';
    let message = 'No targets';
    if (targets.length) {
      if (accepted) {
        state = 'ok';
        message = `${accepted} peer${accepted === 1 ? '' : 's'} connected`;
        if (pendingCount) message += ` • ${pendingCount} pending`;
        if (viewing && viewing !== accepted) message += ` • ${viewing} viewing`;
      } else if (pendingCount) {
        message = `${pendingCount} handshake${pendingCount === 1 ? '' : 's'} pending`;
      } else {
        message = 'Awaiting handshake';
      }
    }
    setRelayState(id, { state, message });
  }

  function persistTargets(id, values) {
    const normalized = [];
    const seen = new Set();
    for (const entry of values) {
      const addr = ensureGraphPrefix(entry);
      if (!addr || seen.has(addr)) continue;
      seen.add(addr);
      normalized.push(addr);
    }
    const cfg = config(id);
    const active = new Set(getActiveTargets(id));
    const nextActive = normalized.filter((addr) => active.has(addr));
    const nextMeta = { ...(cfg.peerMeta || {}) };
    for (const key of Object.keys(nextMeta)) {
      if (!normalized.includes(key)) delete nextMeta[key];
    }
    NodeStore.update(id, {
      type: 'MediaStream',
      targets: normalized,
      activeTargets: nextActive,
      peerMeta: nextMeta
    });
    refreshAddressIndex(id);
    scheduleNotify(id);
    updateRelaySummary(id);
    flushPending();
    return normalized;
  }

  function setActiveTargets(id, addresses) {
    const targets = new Set(getTargets(id));
    const nextActive = Array.from(new Set(addresses.map(ensureGraphPrefix).filter((addr) => targets.has(addr))));
    NodeStore.update(id, { type: 'MediaStream', activeTargets: nextActive });
    refreshAddressIndex(id);
    scheduleNotify(id);
    updateRelaySummary(id);
    flushPending();
    return nextActive;
  }

  function getPeerMeta(id) {
    return config(id).peerMeta || {};
  }

  function updatePeerMeta(id, address, patch = {}, { remove = false } = {}) {
    const normalized = ensureGraphPrefix(address);
    if (!normalized) return null;
    const cfg = config(id);
    const peerMeta = { ...(cfg.peerMeta || {}) };
    if (remove) {
      if (peerMeta[normalized]) delete peerMeta[normalized];
      NodeStore.update(id, { type: 'MediaStream', peerMeta });
      refreshAddressIndex(id);
      scheduleNotify(id);
      updateRelaySummary(id);
      return null;
    }
    const current = peerMeta[normalized] ? { ...peerMeta[normalized] } : { ...PEER_META_DEFAULT };
    const next = {
      ...current,
      ...patch,
      handshake: {
        ...HANDSHAKE_DEFAULT,
        ...(current.handshake || {}),
        ...(patch.handshake || {})
      }
    };
    peerMeta[normalized] = next;
    NodeStore.update(id, { type: 'MediaStream', peerMeta });
    refreshAddressIndex(id);
    scheduleNotify(id);
    updateRelaySummary(id);
    return next;
  }

  function ensurePeerMeta(id, address, patch = {}) {
    const normalized = ensureGraphPrefix(address);
    if (!normalized) return null;
    const meta = getPeerMeta(id)[normalized];
    if (!meta) return updatePeerMeta(id, normalized, patch);
    if (patch && Object.keys(patch).length) return updatePeerMeta(id, normalized, patch);
    return meta;
  }

  function ensureComponentId(id) {
    const cfg = config(id);
    let { componentId } = cfg;
    if (!componentId) {
      const graphId = CFG.graphId || 'graph';
      componentId = `${graphId}:media:${id}`;
      NodeStore.update(id, { type: 'MediaStream', componentId });
    }
    if (componentId) componentIndex.set(componentId, id);
    return componentId;
  }

  function unregisterComponent(id) {
    const cfg = config(id);
    const { componentId } = cfg;
    if (componentId && componentIndex.get(componentId) === id) {
      componentIndex.delete(componentId);
    }
  }

  function ensureClient() {
    if (!Net) return null;
    Net.ensureNkn();
    return Net.nkn?.client || null;
  }

  async function waitForReady(timeoutMs = 15000) {
    if (!Net) return true;
    if (Net.nkn?.ready && Net.nkn?.addr) return true;
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      if (Net.nkn?.ready && Net.nkn?.addr) return true;
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    return !!(Net.nkn?.ready && Net.nkn?.addr);
  }

  async function sendControl(id, address, payload, { skipReadyCheck = false } = {}) {
    const normalized = ensureGraphPrefix(address);
    if (!normalized) return false;
    const componentId = ensureComponentId(id);
    const meta = ensurePeerMeta(id, normalized) || {};
    refreshAddressIndex(id);
    const graphId = typeof CFG.graphId === 'string' ? CFG.graphId.trim() : '';
    const rawFrom = Net?.nkn?.addr || '';
    const normalizedFrom = ensureGraphPrefix(rawFrom);
    const fromAddress = normalizedFrom || rawFrom;
    const targetComponentId = String(meta.componentId || meta.handshake?.remoteId || '').trim();
    const targetGraphId = String(meta.graphId || meta.handshake?.graphId || '').trim();
    const basePayload = { ...(payload || {}) };
    basePayload.kind = basePayload.kind || 'control';
    basePayload.channel = basePayload.channel || 'media';
    const impliedStage = basePayload.stage || basePayload.action || '';
    if (!basePayload.route) basePayload.route = impliedStage ? `media.${impliedStage}` : 'media.control';
    if (!basePayload.event) basePayload.event = basePayload.route;
    if (!basePayload.type) basePayload.type = MEDIA_EVENT;
    basePayload.componentType = COMPONENT_TYPE;
    basePayload.targetComponentType = COMPONENT_TYPE;
    if (componentId) basePayload.componentId = componentId;
    if (graphId) basePayload.graphId = graphId;
    if (fromAddress) {
      basePayload.from = fromAddress;
      basePayload.src = fromAddress;
      basePayload.peer = fromAddress;
    } else if (!basePayload.from && rawFrom) {
      const normalizedFromFallback = ensureGraphPrefix(rawFrom);
      basePayload.from = rawFrom;
      basePayload.src = rawFrom;
      basePayload.peer = normalizedFromFallback || rawFrom;
    }
    basePayload.targetAddress = normalized;
    if (targetComponentId) {
      basePayload.targetComponentId = targetComponentId;
      basePayload.targetId = targetComponentId;
    } else {
      delete basePayload.targetComponentId;
      delete basePayload.targetId;
    }
    if (targetGraphId) basePayload.targetGraphId = targetGraphId;
    else delete basePayload.targetGraphId;
    basePayload.ts = basePayload.ts || Date.now();
    const isHandshake = Boolean(basePayload.stage) || basePayload.action === 'handshake' || basePayload.type === 'nkndm.handshake';
    let outboundMessage = null;
    if (isHandshake) {
      const stage = (basePayload.stage || basePayload.action || 'request').toString().toLowerCase();
      const handshakeAction = ['request', 'accept', 'decline', 'sync'].includes(stage) ? stage : 'request';
      const handshakeId = basePayload.handshakeId || createHandshakeId();
      outboundMessage = {
        type: 'nkndm.handshake',
        action: handshakeAction,
        stage,
        handshakeId,
        viewing: basePayload.viewing === true,
        active: basePayload.active === true,
        componentType: COMPONENT_TYPE,
        targetComponentType: COMPONENT_TYPE,
        channel: 'media',
        componentId: componentId || '',
        targetComponentId: targetComponentId || '',
        targetId: targetComponentId || '',
        graphId: graphId || '',
        targetGraphId: targetGraphId || '',
        from: fromAddress || basePayload.from || '',
        src: fromAddress || basePayload.src || '',
        peer: basePayload.peer || fromAddress || normalized,
        to: normalized,
        address: normalized,
        targetAddress: normalized,
        ts: basePayload.ts,
        route: basePayload.route,
        event: basePayload.event,
        messageType: basePayload.event,
        payload: {
          type: 'nkndm.handshake',
          action: handshakeAction,
          stage,
          handshakeId,
          viewing: basePayload.viewing === true,
          active: basePayload.active === true,
          componentType: COMPONENT_TYPE,
          targetComponentType: COMPONENT_TYPE,
          componentId: componentId || '',
          targetComponentId: targetComponentId || '',
          graphId: graphId || '',
          targetGraphId: targetGraphId || '',
          from: fromAddress || basePayload.from || '',
          peer: basePayload.peer || fromAddress || normalized,
          to: normalized,
          address: normalized,
          targetAddress: normalized,
          ts: basePayload.ts,
          route: basePayload.route,
          event: basePayload.event,
          channel: basePayload.channel
        }
      };
    } else {
      const header = {
        type: MEDIA_EVENT,
        route: basePayload.route,
        event: basePayload.event,
        messageType: basePayload.event,
        componentType: COMPONENT_TYPE,
        targetComponentType: COMPONENT_TYPE,
        channel: 'media',
        peer: fromAddress || normalized,
        address: fromAddress || normalized,
        targetAddress: normalized,
        ts: Date.now(),
        payload: basePayload
      };
      if (componentId) header.componentId = componentId;
      if (graphId) header.graphId = graphId;
      if (fromAddress) {
        header.from = fromAddress;
        header.src = fromAddress;
      } else if (basePayload.from) {
        header.from = basePayload.from;
        header.src = basePayload.from;
      }
      if (targetComponentId) {
        header.targetComponentId = targetComponentId;
        header.targetId = targetComponentId;
      }
      if (targetGraphId) header.targetGraphId = targetGraphId;
      outboundMessage = header;
    }
    if (!skipReadyCheck) {
      const ready = await waitForReady();
      if (!ready) return false;
    }
    const client = ensureClient();
    if (!client) {
      mediaLog('control send skipped (no NKN client)', { nodeId: id, address: normalized, route: outboundMessage?.route || basePayload.route });
      return false;
    }
    ensureListener();
    try {
      await client.send(normalized, JSON.stringify(outboundMessage), { noReply: true, maxHoldingSeconds: 60 });
      mediaLog('control send success', { nodeId: id, address: normalized, route: outboundMessage?.route || basePayload.route, type: outboundMessage?.type || basePayload.type });
      return true;
    } catch (err) {
      log(`[media] control send error: ${err?.message || err}`);
      mediaLog('control send error', { nodeId: id, address: normalized, route: outboundMessage?.route || basePayload.route, error: err?.message || String(err) });
      return false;
    }
  }

  function createHandshakeId() {
    return `hs.${Date.now()}.${Math.random().toString(16).slice(2, 8)}`;
  }

  async function sendHandshake(id, address, stage, options = {}) {
    const handshakeId = options.handshakeId || createHandshakeId();
    mediaLog(`handshake ${stage} → ${formatAddress(address)}`, { nodeId: id, address: ensureGraphPrefix(address), handshakeId: handshakeId, viewing: options.viewing === true, active: options.active === true });
    const normalizedStage = String(stage || '').toLowerCase();
    const action = ['request', 'accept', 'decline', 'sync'].includes(normalizedStage) ? normalizedStage : 'request';
    return sendControl(id, address, {
      action,
      stage: normalizedStage || action,
      type: 'nkndm.handshake',
      handshakeId,
      viewing: options.viewing === true,
      active: options.active === true
    });
  }

  function scheduleHandshakeRetry(id, address, handshakeId) {
    const st = ensureState(id);
    const normalized = ensureGraphPrefix(address);
    if (!normalized || st.handshakeTimers.has(normalized)) return;
    const timer = setInterval(() => {
      const meta = ensurePeerMeta(id, normalized);
      if (!meta) {
        clearInterval(timer);
        st.handshakeTimers.delete(normalized);
        return;
      }
      if (meta.handshake?.status !== 'pending' || meta.handshake?.direction !== 'outgoing') {
        clearInterval(timer);
        st.handshakeTimers.delete(normalized);
        return;
      }
      mediaLog('retrying handshake request', { nodeId: id, address: normalized, handshakeId: meta.handshake?.id });
      sendHandshake(id, normalized, 'request', {
        handshakeId: meta.handshake?.id || handshakeId,
        viewing: meta.viewing,
        active: meta.active
      });
    }, HANDSHAKE_RETRY_MS);
    st.handshakeTimers.set(normalized, timer);
  }

  function stopHandshakeRetry(id, address) {
    const st = ensureState(id);
    const normalized = ensureGraphPrefix(address);
    const timer = st.handshakeTimers.get(normalized);
    if (timer) {
      clearInterval(timer);
      st.handshakeTimers.delete(normalized);
    }
  }

  async function sendPing(id, address) {
    const now = Date.now();
    const normalized = ensureGraphPrefix(address);
    if (!normalized || normalized.startsWith('hydra.local.')) return;
    const meta = ensurePeerMeta(id, normalized) || {};
    const pingId = `ping.${now}.${Math.random().toString(16).slice(2, 8)}`;
    const nextMissed = meta.awaitingPingId ? (meta.missed || 0) + 1 : meta.missed || 0;
    updatePeerMeta(id, normalized, {
      awaitingPingId: pingId,
      lastPing: now,
      missed: nextMissed,
      status: nextMissed >= HEARTBEAT_MAX_MISSED ? 'offline' : meta.status
    });
    const ok = await sendControl(id, normalized, { action: 'ping', pingId, ts: now }, { skipReadyCheck: true });
    if (!ok) {
      mediaLog('heartbeat ping failed', { nodeId: id, address: normalized, pingId });
      updatePeerMeta(id, normalized, { status: 'offline' });
      emitStatus(id, { type: 'heartbeat', status: 'offline', peer: normalized });
    }
  }

  function startHeartbeat(id, address) {
    const st = ensureState(id);
    const normalized = ensureGraphPrefix(address);
    if (!normalized || normalized.startsWith('hydra.local.') || st.heartbeats.has(normalized)) return;
    const meta = ensurePeerMeta(id, normalized);
    if (!isHandshakeAccepted(meta)) return;
    mediaLog('heartbeat started', { nodeId: id, address: normalized });
    const timer = setInterval(() => sendPing(id, normalized), HEARTBEAT_INTERVAL_MS);
    st.heartbeats.set(normalized, timer);
    sendPing(id, normalized);
  }

  function stopHeartbeat(id, address) {
    const st = ensureState(id);
    const normalized = ensureGraphPrefix(address);
    const timer = st.heartbeats.get(normalized);
    if (timer) {
      clearInterval(timer);
      st.heartbeats.delete(normalized);
    }
    mediaLog('heartbeat stopped', { nodeId: id, address: normalized });
  }

  function resolveRecipients(envelope) {
    const recipients = new Set();
    if (!envelope || typeof envelope !== 'object') return recipients;
    const payload = envelope.payload && typeof envelope.payload === 'object' ? envelope.payload : envelope;
    const targetGraph = String(payload.targetGraphId || envelope.targetGraphId || '').trim();
    if (targetGraph && CFG.graphId && targetGraph !== CFG.graphId) return recipients;
    const targetComponent = String(payload.targetComponentId || payload.targetId || payload.componentId || '').trim();
    if (targetComponent && componentIndex.has(targetComponent)) {
      recipients.add(componentIndex.get(targetComponent));
      return recipients;
    }
    const targetAddress = ensureGraphPrefix(payload.targetAddress || payload.address || payload.peer || payload.to || '');
    if (targetAddress && addressIndex.has(targetAddress)) {
      for (const nodeId of addressIndex.get(targetAddress)) recipients.add(nodeId);
      if (recipients.size) return recipients;
    }
    const fromAddress = ensureGraphPrefix(payload.from || envelope.from || envelope.src || envelope.peer || '');
    if (fromAddress && addressIndex.has(fromAddress)) {
      for (const nodeId of addressIndex.get(fromAddress)) recipients.add(nodeId);
      if (recipients.size) return recipients;
    }
    const originComponent = String(payload.componentId || envelope.componentId || '').trim();
    if (originComponent && componentIndex.has(originComponent)) {
      recipients.add(componentIndex.get(originComponent));
      if (recipients.size) return recipients;
    }
    const typeHint = String(payload.targetComponentType || payload.componentType || envelope.componentType || '').trim();
    const normalizedFrom = fromAddress;
    state.forEach((_, nodeId) => {
      if (recipients.has(nodeId)) return;
      const node = getNode(nodeId);
      if (!node) return;
      if (typeHint && node.type !== typeHint && node.type !== COMPONENT_TYPE) return;
      const cfg = config(nodeId);
      const meta = cfg.peerMeta || {};
      if (normalizedFrom && meta[normalizedFrom]) {
        recipients.add(nodeId);
        return;
      }
      const handshake = cfg.handshake || {};
      const handshakePeer = ensureGraphPrefix(handshake.peer || '');
      if (normalizedFrom && handshakePeer && handshakePeer === normalizedFrom) {
        recipients.add(nodeId);
        return;
      }
      if ((handshake.status || 'idle') !== 'accepted') {
        recipients.add(nodeId);
      }
    });
    return recipients;
  }

  function ensureListener() {
    if (listenerAttached) return;
    const client = ensureClient();
    if (!client) return;
    client.on('message', (src, payload) => {
      try {
        let text = '';
        if (typeof payload === 'string') text = payload;
        else if (payload && typeof payload === 'object') {
          if (typeof payload.payload === 'string') text = payload.payload;
          else if (payload.payload && payload.payload.toString) text = payload.payload.toString();
          else if (payload.toString) text = payload.toString();
        } else if (payload && payload.toString) {
          text = payload.toString();
        }
        if (!text) return;
        const parsed = JSON.parse(text);
        if (!parsed || typeof parsed !== 'object') return;
        const parsedType = typeof parsed.type === 'string' ? parsed.type : '';
        if (parsedType !== MEDIA_EVENT && parsedType !== LEGACY_MEDIA_EVENT) {
          const parsedChannel = typeof parsed.channel === 'string' ? parsed.channel.toLowerCase() : '';
          if (!(parsedType === 'nkndm.handshake' && parsedChannel === 'media')) return;
        }
        const recipients = resolveRecipients(parsed);
        if (!recipients.size) {
          queueEnvelope(parsed);
          return;
        }
        recipients.forEach((nodeId) => {
          try {
            handleIncoming(nodeId, parsed);
          } catch (err) {
            // ignore per-node errors
          }
        });
      } catch (err) {
        // ignore malformed packets
      }
    });
    listenerAttached = true;
  }

  function ensureTargetPresence(id, address, { auto = false } = {}) {
    const normalized = ensureGraphPrefix(address);
    if (!normalized) return;
    const targets = getTargets(id);
    if (!targets.includes(normalized)) {
      persistTargets(id, [...targets, normalized]);
      updatePeerMeta(id, normalized, { auto });
      setBadge(`Media peer discovered: ${formatAddress(normalized)}`);
      mediaLog('target discovered', { nodeId: id, address: normalized, auto });
    } else if (auto) {
      updatePeerMeta(id, normalized, { auto });
    }
    refreshAddressIndex(id);
    flushPending();
    updateTargetChips(id);
    syncRemoteTiles(id);
    refreshStatusSummary(id);
  }

  async function requestHandshake(id, address, { auto = false, wantActive = false } = {}) {
    const normalized = ensureGraphPrefix(address);
    if (!normalized) {
      setBadge('Enter a valid NKN address', false);
      return;
    }
    ensureComponentId(id);
    const meta = ensurePeerMeta(id, normalized) || {};
    if (isHandshakeAccepted(meta)) {
      startHeartbeat(id, normalized);
      await sendHandshake(id, normalized, 'sync', { handshakeId: meta.handshake?.id || createHandshakeId(), viewing: meta.viewing });
      return;
    }
    mediaLog('initiating handshake request', { nodeId: id, address: normalized, wantActive, auto });
    const handshakeId = meta.handshake?.id || createHandshakeId();
    updatePeerMeta(id, normalized, {
      status: 'idle',
      missed: 0,
      auto,
      handshake: {
        status: 'pending',
        direction: 'outgoing',
        id: handshakeId,
        ts: Date.now(),
        commitActive: wantActive,
        peer: normalized
      }
    });
    const ok = await sendHandshake(id, normalized, 'request', { handshakeId, viewing: false, active: wantActive });
    if (ok) {
      mediaLog('handshake request sent', { nodeId: id, address: normalized, handshakeId });
      scheduleHandshakeRetry(id, normalized, handshakeId);
      emitStatus(id, { type: 'handshake', status: 'pending', direction: 'outgoing', peer: normalized });
      setBadge(`Awaiting handshake acceptance from ${formatAddress(normalized)}`);
    } else {
      mediaLog('handshake request failed to send', { nodeId: id, address: normalized, handshakeId });
      setBadge(`Unable to reach ${formatAddress(normalized)} for handshake`, false);
    }
    scheduleNotify(id);
    updateTargetChips(id);
    refreshStatusSummary(id);
  }

  async function acceptHandshake(id, address) {
    const normalized = ensureGraphPrefix(address);
    if (!normalized) return;
    const meta = ensurePeerMeta(id, normalized) || {};
    const handshake = meta.handshake || HANDSHAKE_DEFAULT;
    if (handshake.status !== 'pending' || handshake.direction !== 'incoming') {
      setBadge(`No pending offer from ${formatAddress(normalized)}`, false);
      return;
    }
    mediaLog('accepting handshake', { nodeId: id, address: normalized, handshakeId: handshake.id });
    const handshakeId = handshake.id || createHandshakeId();
    updatePeerMeta(id, normalized, {
      status: 'idle',
      missed: 0,
      viewing: true,
      handshake: {
        status: 'accepted',
        direction: 'incoming',
        id: handshakeId,
        ts: Date.now(),
        commitActive: false,
        remoteId: handshake.remoteId,
        graphId: handshake.graphId,
        peer: normalized
      }
    });
    stopHandshakeRetry(id, normalized);
    await sendHandshake(id, normalized, 'accept', { handshakeId, viewing: true });
    await sendControl(id, normalized, { action: 'view', viewing: true });
    startHeartbeat(id, normalized);
    emitStatus(id, { type: 'handshake', status: 'accepted', direction: 'incoming', peer: normalized });
    setBadge(`Accepted media stream from ${formatAddress(normalized)}`);
    mediaLog('handshake accepted', { nodeId: id, address: normalized, handshakeId });
    updateTargetChips(id);
    syncRemoteTiles(id);
    refreshStatusSummary(id);
    scheduleNotify(id);
    closeInviteModal(normalized);
  }

  async function declineHandshake(id, address) {
    const normalized = ensureGraphPrefix(address);
    if (!normalized) return;
    const meta = ensurePeerMeta(id, normalized) || {};
    const handshake = meta.handshake || HANDSHAKE_DEFAULT;
    if (handshake.status !== 'pending' || handshake.direction !== 'incoming') {
      setBadge(`No pending offer from ${formatAddress(normalized)}`, false);
      return;
    }
    mediaLog('declining handshake', { nodeId: id, address: normalized, handshakeId: handshake.id });
    const handshakeId = handshake.id || createHandshakeId();
    updatePeerMeta(id, normalized, {
      status: 'offline',
      viewing: false,
      remoteActive: false,
      handshake: {
        status: 'declined',
        direction: 'idle',
        id: handshakeId,
        ts: Date.now(),
        commitActive: false,
        remoteId: handshake.remoteId,
        graphId: handshake.graphId,
        peer: normalized
      }
    });
    stopHeartbeat(id, normalized);
    stopHandshakeRetry(id, normalized);
    await sendHandshake(id, normalized, 'decline', { handshakeId });
    await sendControl(id, normalized, { action: 'view', viewing: false });
    emitStatus(id, { type: 'handshake', status: 'declined', direction: 'incoming', peer: normalized });
    setBadge(`Declined media stream from ${formatAddress(normalized)}`, false);
    mediaLog('handshake declined', { nodeId: id, address: normalized, handshakeId });
    updateTargetChips(id);
    syncRemoteTiles(id);
    refreshStatusSummary(id);
    scheduleNotify(id);
    closeInviteModal(normalized);
  }

  function addTarget(id, raw, { auto = false, handshake = true, wantActive = false } = {}) {
    const normalized = ensureGraphPrefix(raw);
    if (!normalized) return { added: false, address: '' };
    const targets = getTargets(id);
    if (targets.includes(normalized)) {
      if (handshake) requestHandshake(id, normalized, { auto, wantActive });
      return { added: false, address: normalized };
    }
    persistTargets(id, [...targets, normalized]);
    if (handshake) requestHandshake(id, normalized, { auto, wantActive });
    else ensurePeerMeta(id, normalized, { auto });
    updateTargetChips(id);
    refreshStatusSummary(id);
    syncRemoteTiles(id);
    scheduleNotify(id);
    return { added: true, address: normalized };
  }

  async function removeTarget(id, address) {
    const normalized = ensureGraphPrefix(address);
    if (!normalized) return;
    mediaLog('removing target', { nodeId: id, address: normalized });
    const meta = ensurePeerMeta(id, normalized);
    const handshakeId = meta?.handshake?.id || createHandshakeId();
    await sendHandshake(id, normalized, 'decline', { handshakeId });
    await sendControl(id, normalized, { action: 'view', viewing: false });
    const remaining = getTargets(id).filter((addr) => addr !== normalized);
    persistTargets(id, remaining);
    updatePeerMeta(id, normalized, {}, { remove: true });
    stopHeartbeat(id, normalized);
    stopHandshakeRetry(id, normalized);
    emitStatus(id, { type: 'handshake', status: 'removed', peer: normalized });
    updateTargetChips(id);
    syncRemoteTiles(id);
    refreshStatusSummary(id);
    scheduleNotify(id);
    closeInviteModal(normalized);
  }

  function setTargetActiveState(id, address, active) {
    const normalized = ensureGraphPrefix(address);
    if (!normalized) return;
    mediaLog(active ? 'activating target' : 'deactivating target', { nodeId: id, address: normalized });
    const activeSet = new Set(getActiveTargets(id));
    const isActive = activeSet.has(normalized);
    if (active && !isActive) activeSet.add(normalized);
    else if (!active && isActive) activeSet.delete(normalized);
    setActiveTargets(id, Array.from(activeSet));
    const meta = ensurePeerMeta(id, normalized) || {};
    updatePeerMeta(id, normalized, { active, viewing: active && isHandshakeAccepted(meta) });
    if (active) {
      if (!isHandshakeAccepted(meta)) {
        requestHandshake(id, normalized, { wantActive: true });
      } else {
        sendControl(id, normalized, { action: 'announce', active: true });
        sendControl(id, normalized, { action: 'view', viewing: true });
      }
    } else {
      sendControl(id, normalized, { action: 'view', viewing: false });
      sendControl(id, normalized, { action: 'announce', active: false });
    }
    updateTargetChips(id);
    refreshStatusSummary(id);
    scheduleNotify(id);
  }

  let inviteModal = null;
  let inviteBackdrop = null;
  let inviteClose = null;
  let inviteAccept = null;
  let inviteDecline = null;
  let inviteText = null;
  let currentInvite = null;
  let inviteKeybound = false;

  function ensureInviteModal() {
    if (inviteModal) return;
    inviteModal = document.getElementById('mediaInviteModal');
    if (!inviteModal) return;
    inviteBackdrop = document.getElementById('mediaInviteBackdrop');
    inviteClose = document.getElementById('mediaInviteClose');
    inviteAccept = document.getElementById('mediaInviteAccept');
    inviteDecline = document.getElementById('mediaInviteDecline');
    inviteText = inviteModal.querySelector('[data-media-invite-text]');

    const bind = (el, handler) => {
      if (!el || el._mediaBound) return;
      el._mediaBound = true;
      el.addEventListener('click', handler);
    };

    const close = (ev) => {
      if (ev) ev.preventDefault();
      closeInviteModal();
    };

    bind(inviteBackdrop, close);
    bind(inviteClose, close);
    bind(inviteDecline, async (ev) => {
      ev.preventDefault();
      const invite = currentInvite;
      closeInviteModal();
      if (invite) await declineHandshake(invite.nodeId, invite.address);
    });
    bind(inviteAccept, async (ev) => {
      ev.preventDefault();
      const invite = currentInvite;
      closeInviteModal();
      if (invite) await acceptHandshake(invite.nodeId, invite.address);
    });

    if (!inviteKeybound) {
      inviteKeybound = true;
      window.addEventListener('keydown', (ev) => {
        if (ev.key === 'Escape') closeInviteModal();
      });
    }
  }

  function closeInviteModal(matchAddress) {
    if (!currentInvite) return;
    if (matchAddress && ensureGraphPrefix(matchAddress) !== currentInvite.address) return;
    if (inviteModal) {
      inviteModal.classList.add('hidden');
      inviteModal.setAttribute('aria-hidden', 'true');
    }
    mediaLog('invite modal closed', { nodeId: currentInvite?.nodeId, address: currentInvite?.address, matchAddress });
    currentInvite = null;
  }

  function refreshStatusSummary(id) {
    const st = ensureState(id);
    const active = getActiveTargets(id);
    const base = active.length ? `${active.length} target${active.length === 1 ? '' : 's'}` : 'no targets';
    const text = st.running ? `Streaming • ${base}` : `Ready • ${base}`;
    updateStatus(id, text);
  }

  function applyChipState(chip, meta = {}, isActive = false) {
    if (!chip) return;
    chip.classList.remove('status-offline', 'status-idle', 'status-active', 'pending-handshake', 'has-offer', 'is-selected');
    const status = (meta.handshake?.status || 'idle').toLowerCase();
    const statusClass = status === 'accepted' ? 'status-active' : status === 'pending' ? 'status-idle' : 'status-offline';
    chip.classList.add(statusClass);
    if (isActive) chip.classList.add('is-selected');
    const pendingIncoming = isHandshakePendingIncoming(meta);
    const hasOffer = pendingIncoming || (!!meta.remoteActive && !isActive);
    chip.classList.toggle('pending-handshake', pendingIncoming);
    chip.classList.toggle('has-offer', hasOffer);
    const actions = chip.querySelector('.media-target-chip-actions');
    if (actions) actions.classList.toggle('hidden', !pendingIncoming);
    const label = chip.querySelector('.media-target-chip-label');
    if (label) label.title = chip.dataset.address || '';
  }

  function updateTargetChips(id) {
    const { targetChips } = nodeElements(id);
    if (!targetChips) return;
    const targets = getTargets(id);
    const activeSet = new Set(getActiveTargets(id));
    const metaMap = getPeerMeta(id);
    targetChips.innerHTML = '';
    targets.forEach((addr) => {
      const chip = document.createElement('div');
      chip.className = 'media-target-chip status-offline';
      chip.dataset.address = addr;
      chip.innerHTML = `
        <span class="status-dot"></span>
        <span class="media-target-chip-label">${formatAddress(addr)}</span>
        <div class="media-target-chip-actions hidden">
          <button type="button" class="ghost media-target-chip-accept" title="Accept incoming media">✔</button>
          <button type="button" class="ghost media-target-chip-decline" title="Decline incoming media">✖</button>
        </div>
        <button type="button" class="media-target-chip-remove" title="Remove target">🗑</button>
      `;
      targetChips.appendChild(chip);
      applyChipState(chip, metaMap[addr], activeSet.has(addr));
    });

    targetChips.querySelectorAll('.media-target-chip').forEach((chip) => {
      const address = chip.dataset.address;
      chip.addEventListener('click', (ev) => {
        if (ev.target.closest('.media-target-chip-actions') || ev.target.classList.contains('media-target-chip-remove')) return;
        ev.preventDefault();
        toggleTargetActive(id, address);
      });
      const acceptBtn = chip.querySelector('.media-target-chip-accept');
      if (acceptBtn && !acceptBtn._mediaBound) {
        acceptBtn._mediaBound = true;
        acceptBtn.addEventListener('click', async (ev) => {
          ev.preventDefault();
          await acceptHandshake(id, address);
        });
      }
      const declineBtn = chip.querySelector('.media-target-chip-decline');
      if (declineBtn && !declineBtn._mediaBound) {
        declineBtn._mediaBound = true;
        declineBtn.addEventListener('click', async (ev) => {
          ev.preventDefault();
          await declineHandshake(id, address);
        });
      }
      const removeBtn = chip.querySelector('.media-target-chip-remove');
      if (removeBtn && !removeBtn._mediaBound) {
        removeBtn._mediaBound = true;
        removeBtn.addEventListener('click', async (ev) => {
          ev.preventDefault();
          ev.stopPropagation();
          await removeTarget(id, address);
        });
      }
    });
  }

  function bindTargetUi(id) {
    const { targetInput, targetAdd } = nodeElements(id);
    if (targetInput && !targetInput._mediaBound) {
      targetInput._mediaBound = true;
      targetInput.addEventListener('keydown', (ev) => {
        if (ev.key === 'Enter') {
          ev.preventDefault();
          commitTarget(id, targetInput.value);
          targetInput.value = '';
        }
      });
    }
    if (targetAdd && !targetAdd._mediaBound) {
      targetAdd._mediaBound = true;
      targetAdd.addEventListener('click', (ev) => {
        ev.preventDefault();
        if (!targetInput) return;
        commitTarget(id, targetInput.value);
        targetInput.value = '';
      });
    }
  }

  function commitTarget(id, raw) {
    if (!raw) return;
    const { added, address } = addTarget(id, raw, { handshake: true });
    if (added) setBadge(`Added media peer ${formatAddress(address)}`);
    else setBadge('Target already added');
  }

  function toggleTargetActive(id, address) {
    const normalized = ensureGraphPrefix(address);
    if (!normalized) return;
    const active = new Set(getActiveTargets(id));
    setTargetActiveState(id, normalized, !active.has(normalized));
  }

  function ensureRemotePeer(id, address, { force = false } = {}) {
    const st = ensureState(id);
    const normalized = ensureGraphPrefix(address);
    if (!normalized) return null;
    let entry = st.remotePeers.get(normalized);
    const { remoteGrid, remoteEmpty } = nodeElements(id);
    if (!remoteGrid) return null;
    if (!entry && !force) return null;
    if (!entry) {
      const tile = document.createElement('div');
      tile.className = 'media-remote-tile';
      tile.dataset.peer = normalized;

      const header = document.createElement('div');
      header.className = 'media-remote-header';
      const label = document.createElement('span');
      label.className = 'media-remote-label';
      label.textContent = formatAddress(normalized);
      const stamp = document.createElement('span');
      stamp.className = 'media-remote-ts';
      stamp.textContent = '—';
      header.append(label, stamp);

      const img = document.createElement('img');
      img.className = 'media-remote-frame';
      img.dataset.empty = 'true';
      img.alt = `Remote stream from ${formatAddress(normalized)}`;

      const audio = document.createElement('audio');
      audio.className = 'media-remote-audio';
      audio.controls = false;
      audio.autoplay = true;
      audio.playsInline = true;
      audio.muted = false;

      const info = document.createElement('div');
      info.className = 'tiny media-remote-info';
      info.textContent = '(waiting)';

      const controls = document.createElement('div');
      controls.className = 'media-remote-controls hidden';
      const acceptBtn = document.createElement('button');
      acceptBtn.type = 'button';
      acceptBtn.className = 'media-remote-accept ghost';
      acceptBtn.textContent = '✔';
      const declineBtn = document.createElement('button');
      declineBtn.type = 'button';
      declineBtn.className = 'media-remote-decline ghost';
      declineBtn.textContent = '✖';
      controls.append(acceptBtn, declineBtn);

      tile.append(header, img, audio, info, controls);
      remoteGrid.appendChild(tile);
      if (remoteEmpty) remoteEmpty.classList.add('hidden');

      acceptBtn.addEventListener('click', async (ev) => {
        ev.preventDefault();
        await acceptHandshake(id, normalized);
      });
      declineBtn.addEventListener('click', async (ev) => {
        ev.preventDefault();
        await declineHandshake(id, normalized);
      });

      entry = { tile, label, stamp, img, audio, info, controls, audioUrl: null };
      st.remotePeers.set(normalized, entry);
    }
    return entry;
  }

  function removeRemotePeer(id, address) {
    const st = state.get(id);
    if (!st) return;
    const normalized = ensureGraphPrefix(address);
    if (!normalized) return;
    const entry = st.remotePeers.get(normalized);
    if (!entry) return;
    if (entry.audioUrl) {
      try { URL.revokeObjectURL(entry.audioUrl); } catch (_) {}
      entry.audioUrl = null;
    }
    if (entry.tile && entry.tile.parentElement) entry.tile.parentElement.removeChild(entry.tile);
    st.remotePeers.delete(normalized);
    const { remoteGrid, remoteEmpty } = nodeElements(id);
    if (remoteGrid && remoteEmpty) remoteEmpty.classList.toggle('hidden', remoteGrid.children.length > 0);
  }

  function updateRemoteTileState(id, address) {
    const entry = ensureRemotePeer(id, address);
    if (!entry) return;
    const meta = ensurePeerMeta(id, address) || {};
    const pending = isHandshakePendingIncoming(meta);
    entry.tile.classList.toggle('pending-handshake', pending);
    if (entry.controls) entry.controls.classList.toggle('hidden', !pending);
    if (entry.info && pending) entry.info.textContent = 'Incoming media offer';
  }

  function syncRemoteTiles(id) {
    const st = ensureState(id);
    const meta = getPeerMeta(id);
    const desired = new Set();
    Object.entries(meta).forEach(([addr, info]) => {
      if (info.viewing || isHandshakePendingIncoming(info)) desired.add(addr);
    });
    for (const addr of Array.from(st.remotePeers.keys())) {
      if (!desired.has(addr)) removeRemotePeer(id, addr);
    }
    desired.forEach((addr) => {
      ensureRemotePeer(id, addr, { force: true });
      updateRemoteTileState(id, addr);
    });
  }

  function handleControlMessage(id, from, payload = {}) {
    const normalized = ensureGraphPrefix(from);
    if (!normalized) return;
    ensureTargetPresence(id, normalized, { auto: true });
    switch (payload.action) {
      case 'handshake':
        handleHandshakeControl(id, normalized, payload);
        return;
      case 'ping':
        sendControl(id, normalized, { action: 'pong', pingId: payload.pingId || '', ts: Date.now() }, { skipReadyCheck: true });
        updatePeerMeta(id, normalized, { status: 'idle', missed: 0, awaitingPingId: '' });
        mediaLog('heartbeat ping received', { nodeId: id, address: normalized, pingId: payload.pingId });
        return;
      case 'pong': {
        const now = Date.now();
        const meta = ensurePeerMeta(id, normalized) || {};
        if (meta.awaitingPingId && payload.pingId && meta.awaitingPingId !== payload.pingId) return;
        const latency = meta.lastPing ? now - meta.lastPing : null;
        updatePeerMeta(id, normalized, {
          status: 'idle',
          missed: 0,
          awaitingPingId: '',
          lastPong: now,
          latency
        });
        mediaLog('heartbeat pong received', { nodeId: id, address: normalized, latency, pingId: payload.pingId });
        return;
      }
      case 'announce':
        updatePeerMeta(id, normalized, { remoteActive: !!payload.active });
        return;
      case 'view':
        updatePeerMeta(id, normalized, { remoteViewing: !!payload.viewing });
        return;
      default:
        break;
    }
  }

  function handleHandshakeControl(id, address, payload = {}) {
    const normalized = ensureGraphPrefix(address);
    if (!normalized) return;
    const handshakeId = payload.handshakeId || createHandshakeId();
    const remoteComponentId = payload.componentId || payload.componentID || '';
    const remoteGraphId = payload.graphId || '';
    const meta = ensurePeerMeta(id, normalized) || {};
    const handshake = meta.handshake || HANDSHAKE_DEFAULT;
    const stageRaw = typeof payload.stage === 'string' ? payload.stage : payload.action;
    const stage = (stageRaw || '').toLowerCase();

    if (stage === 'request' || stage === 'handshake') {
      mediaLog('incoming handshake request', { nodeId: id, address: normalized, handshakeId, remoteComponentId, remoteGraphId });
      updatePeerMeta(id, normalized, {
        status: 'idle',
        missed: 0,
        componentId: remoteComponentId || meta.componentId,
        graphId: remoteGraphId || meta.graphId,
        handshake: {
          status: 'pending',
          direction: 'incoming',
          id: handshakeId,
          ts: Date.now(),
          remoteId: remoteComponentId || handshake.remoteId,
          graphId: remoteGraphId || handshake.graphId,
          peer: normalized,
          commitActive: payload.active === true
        }
      });
      emitStatus(id, { type: 'handshake', status: 'pending', direction: 'incoming', peer: normalized });
      updateTargetChips(id);
      syncRemoteTiles(id);
      refreshStatusSummary(id);
      scheduleNotify(id);
      openHandshakePrompt(id, normalized, handshakeId);
      return;
    }

    if (stage === 'accept') {
      mediaLog('handshake accepted by peer', { nodeId: id, address: normalized, handshakeId, remoteComponentId, remoteGraphId });
      updatePeerMeta(id, normalized, {
        status: 'idle',
        missed: 0,
        remoteViewing: payload.viewing === true,
        handshake: {
          status: 'accepted',
          direction: handshake.direction === 'incoming' ? 'incoming' : 'outgoing',
          id: handshakeId,
          ts: Date.now(),
          remoteId: remoteComponentId || handshake.remoteId,
          graphId: remoteGraphId || handshake.graphId,
          peer: normalized,
          commitActive: handshake.commitActive === true
        }
      });
      stopHandshakeRetry(id, normalized);
      startHeartbeat(id, normalized);
      emitStatus(id, { type: 'handshake', status: 'accepted', direction: 'outgoing', peer: normalized });
      updateTargetChips(id);
      syncRemoteTiles(id);
      refreshStatusSummary(id);
      scheduleNotify(id);
      closeInviteModal(normalized);
      return;
    }

    if (stage === 'decline') {
      mediaLog('handshake declined by peer', { nodeId: id, address: normalized, handshakeId });
      updatePeerMeta(id, normalized, {
        status: 'offline',
        remoteActive: false,
        remoteViewing: false,
        handshake: {
          status: 'declined',
          direction: 'idle',
          id: handshakeId,
          ts: Date.now(),
          peer: normalized,
          commitActive: false
        }
      });
      stopHeartbeat(id, normalized);
      stopHandshakeRetry(id, normalized);
      emitStatus(id, { type: 'handshake', status: 'declined', direction: 'outgoing', peer: normalized });
      updateTargetChips(id);
      syncRemoteTiles(id);
      refreshStatusSummary(id);
      scheduleNotify(id);
      closeInviteModal(normalized);
      return;
    }

    if (stage === 'sync' && isHandshakeAccepted(meta)) {
      mediaLog('handshake sync requested', { nodeId: id, address: normalized, handshakeId });
      sendHandshake(id, normalized, 'accept', { handshakeId: meta.handshake?.id || handshakeId, viewing: meta.viewing });
    }
  }

  function openHandshakePrompt(id, address, handshakeId) {
    const normalized = ensureGraphPrefix(address);
    ensureInviteModal();
    if (!inviteModal || !inviteText) {
      mediaLog('incoming handshake prompt (modal unavailable)', { nodeId: id, address: normalized, handshakeId });
      setBadge(`Incoming media offer from ${formatAddress(normalized)}`);
      return;
    }
    mediaLog('incoming handshake prompt', { nodeId: id, address: normalized, handshakeId });
    currentInvite = { nodeId: id, address: normalized, handshakeId };
    inviteText.textContent = `Incoming media stream from ${formatAddress(normalized)}. Accept?`;
    inviteModal.classList.remove('hidden');
    inviteModal.setAttribute('aria-hidden', 'false');
  }

  function updateStatus(id, text) {
    const { status } = nodeElements(id);
    if (status) status.textContent = text;
  }

  function updateButtonGlyph(id) {
    const node = getNode(id);
    if (!node || !node.el) return;
    const btn = node.el.querySelector('.mediaToggle');
    const st = ensureState(id);
    if (btn) btn.textContent = st.running ? '■' : '▶';
  }

  function stopRecorder(st) {
    if (st.recorder) {
      try {
        st.recorder.stop();
      } catch (err) {
        // ignore
      }
      st.recorder = null;
    }
    if (st.recorderHandler) {
      st.recorderHandler();
      st.recorderHandler = null;
    }
  }

  function stopCapture(id) {
    const st = ensureState(id);
    if (!st.running) return;
    if (st.frameTimer) {
      clearInterval(st.frameTimer);
      st.frameTimer = null;
    }
    stopRecorder(st);
    if (st.stream) {
      try {
        st.stream.getTracks().forEach((track) => track.stop());
      } catch (err) {
        // ignore
      }
      st.stream = null;
    }
    if (st.audioCtx) {
      try { st.audioCtx.close(); } catch (_) {}
      st.audioCtx = null;
      st.audioQueueTime = 0;
    }
    st.running = false;
    updateStatus(id, 'Stopped');
    updateButtonGlyph(id);
    NodeStore.update(id, { type: 'MediaStream', running: false });
  }

  async function startCapture(id) {
    const st = ensureState(id);
    if (st.running) return;
    const cfg = config(id);
    const includeVideo = !!cfg.includeVideo;
    const includeAudio = !!cfg.includeAudio;
    if (!includeVideo && !includeAudio) {
      setBadge('Enable audio or video before starting', false);
      return;
    }

    const constraints = {};
    if (includeVideo) {
      constraints.video = {
        width: { ideal: Number(cfg.videoWidth) || 640 },
        height: { ideal: Number(cfg.videoHeight) || 480 },
        frameRate: { ideal: Number(cfg.frameRate) || 8, max: Number(cfg.frameRate) || 8 }
      };
    } else {
      constraints.video = false;
    }
    if (includeAudio) {
      const sr = Number(cfg.audioSampleRate) || 48000;
      constraints.audio = {
        sampleRate: sr,
        channelCount: Number(cfg.audioChannels) || 1,
        noiseSuppression: true,
        echoCancellation: true
      };
    } else {
      constraints.audio = false;
    }

    let stream;
    try {
      stream = await navigator.mediaDevices.getUserMedia(constraints);
    } catch (err) {
      setBadge(`Media access denied: ${err?.message || err}`, false);
      log(`[media] getUserMedia failed: ${err?.stack || err}`);
      return;
    }

    st.stream = stream;
    st.running = true;
    NodeStore.update(id, { type: 'MediaStream', running: true });

    const { localVideo } = nodeElements(id);
    if (localVideo) {
      try {
        localVideo.srcObject = stream;
        localVideo.play().catch(() => {});
      } catch (err) {
        // ignore
      }
    }

    if (includeVideo) startVideoLoop(id, cfg);
    if (includeAudio) startAudioLoop(id, cfg);

    updateStatus(id, includeVideo ? 'Streaming video' : 'Streaming audio');
    updateButtonGlyph(id);
    setBadge('Media stream started');
  }

  function startVideoLoop(id, cfg) {
    const st = ensureState(id);
    const fps = Math.max(1, Math.min(30, Number(cfg.frameRate) || 8));
    const quality = qualityFromCompression(cfg.compression);
    const { localVideo } = nodeElements(id);
    if (!localVideo) return;
    if (!st.canvas) st.canvas = document.createElement('canvas');
    const ctx = st.canvas.getContext('2d');
    st.frameSeq = 0;

    const sendFrame = () => {
      if (!st.running || !st.stream) return;
      if (!localVideo.videoWidth || !localVideo.videoHeight) return;
      st.canvas.width = localVideo.videoWidth;
      st.canvas.height = localVideo.videoHeight;
      ctx.drawImage(localVideo, 0, 0, st.canvas.width, st.canvas.height);
      let dataUrl;
      try {
        dataUrl = st.canvas.toDataURL('image/webp', quality);
      } catch (err) {
        dataUrl = st.canvas.toDataURL();
      }
      if (!dataUrl) return;
      const idx = dataUrl.indexOf(',');
      const b64 = idx >= 0 ? dataUrl.slice(idx + 1) : dataUrl;
      const mime = dataUrl.slice(5, dataUrl.indexOf(';')) || 'image/webp';
      const ts = Date.now();
      const seq = (st.frameSeq = (st.frameSeq || 0) + 1);
      const packet = {
        type: 'nkndm.media',
        op: 'video',
        kind: 'video',
        mime,
        b64,
        image: b64,
        width: st.canvas.width,
        height: st.canvas.height,
        ts,
        seq,
        nodeId: id,
        route: 'media.video'
      };
      Router.sendFrom(id, 'packet', packet);
      Router.sendFrom(id, 'media', packet);
    };

    if (st.frameTimer) clearInterval(st.frameTimer);
    st.frameTimer = setInterval(sendFrame, Math.round(1000 / fps));
  }

  function startAudioLoop(id, cfg) {
    const st = ensureState(id);
    const stream = st.stream;
    if (!stream) return;
    st.audioSeq = 0;

    const supports = (m) => {
      try {
        return typeof MediaRecorder !== 'undefined' &&
          typeof MediaRecorder.isTypeSupported === 'function' &&
          MediaRecorder.isTypeSupported(m);
      } catch (err) {
        return false;
      }
    };

    let targetMime = cfg.audioFormat === 'pcm16' ? 'audio/pcm' : 'audio/webm;codecs=opus';
    if (!supports(targetMime)) {
      if (cfg.audioFormat === 'pcm16' && supports('audio/webm;codecs=opus')) {
        targetMime = 'audio/webm;codecs=opus';
        setBadge('PCM capture unsupported, falling back to Opus', false);
      } else {
        setBadge('Audio recorder unsupported', false);
        return;
      }
    }
    const usePcm = targetMime.includes('pcm');

    const bits = Math.max(16000, Math.min(256000, Number(cfg.audioBitsPerSecond) || 32000));
    const recorder = new MediaRecorder(stream, { mimeType: targetMime, audioBitsPerSecond: bits });
    st.recorder = recorder;

    const handleData = async (event) => {
      if (!event.data || !event.data.size) return;
      try {
        const buffer = await event.data.arrayBuffer();
        const b64 = arrayBufferToBase64(buffer);
        const ts = Date.now();
        const seq = (st.audioSeq = (st.audioSeq || 0) + 1);
        const sr = Number(cfg.audioSampleRate) || 48000;
        const channels = Number(cfg.audioChannels) || 1;
        const packet = {
          type: 'nkndm.media',
          op: 'audio',
          kind: 'audio',
          mime: event.data.type || targetMime,
          b64,
          ts,
          route: 'media.audio',
          format: usePcm ? 'pcm16' : 'opus',
          sr,
          channels,
          nodeId: id,
          seq
        };
        Router.sendFrom(id, 'packet', packet);
        Router.sendFrom(id, 'media', packet);
      } catch (err) {
        log(`[media] audio encode error: ${err?.message || err}`);
      }
    };

    recorder.addEventListener('dataavailable', handleData);
    recorder.start(Math.max(200, Math.min(1000, Math.round(1000 / ((Number(cfg.frameRate) || 8) / 2)))));
    st.recorderHandler = () => recorder.removeEventListener('dataavailable', handleData);
  }

  function handleIncoming(id, payload) {
    if (payload == null) return;
    const els = nodeElements(id);
    if (!els.container) return;

    let data = null;
    if (payload && typeof payload === 'object') {
      if (payload.parsed && typeof payload.parsed === 'object') {
        data = payload.parsed;
      } else if (payload.raw && typeof payload.raw === 'object') {
        if (payload.raw.payload && typeof payload.raw.payload === 'object') {
          data = payload.raw.payload;
        } else if (typeof payload.raw.text === 'string') {
          try { data = JSON.parse(payload.raw.text); }
          catch (_) { data = null; }
        }
      }
      if (!data && typeof payload.text === 'string') {
        try { data = JSON.parse(payload.text); }
        catch (_) { data = null; }
      }
      if (!data && payload.payload && typeof payload.payload === 'object') data = payload.payload;
      if (!data && payload.message && typeof payload.message === 'object') data = payload.message;
      if (!data && payload.kind) data = payload;
    } else if (typeof payload === 'string') {
      try { data = JSON.parse(payload); }
      catch (_) { return; }
    }

    if (data && typeof data === 'object' && data.payload && typeof data.payload === 'object') {
      data = data.payload;
    }

    if (!data || typeof data !== 'object') return;

    if (data.type === 'nkndm.media' && data.payload && typeof data.payload === 'object') {
      data = { ...data, ...data.payload };
    }
    if (!data.kind && typeof data.op === 'string') data.kind = data.op;
    if (!data.route && typeof payload?.route === 'string') data.route = payload.route;
    if (!data.from && typeof payload?.from === 'string') data.from = payload.from;
    if (!data.nodeId && typeof payload?.nodeId === 'string') data.nodeId = payload.nodeId;

    const action = String(data.action || data.op || '').toLowerCase();
    if (action === 'handshake' || action === 'request' || action === 'accept' || action === 'decline' || action === 'sync' || action === 'ping' || action === 'pong' || action === 'announce' || action === 'view') {
      handleControlMessage(id, data.from || payload.from || '', data);
      return;
    }

    const kind = (data.kind || data.type || '').toLowerCase();
    const sourceAddress = data.from || payload.from || payload.peer || '';
    if (sourceAddress) ensureTargetPresence(id, sourceAddress, { auto: true });

    if (kind === 'video' && data.b64) {
      const peer = ensureRemotePeer(id, sourceAddress || (data.nodeId || payload.nodeId || 'remote'), { force: true });
      const mime = data.mime || 'image/webp';
      if (peer && peer.img) {
        peer.img.src = `data:${mime};base64,${data.b64}`;
        peer.img.dataset.empty = 'false';
        if (peer.stamp) peer.stamp.textContent = new Date().toLocaleTimeString();
        if (peer.info) peer.info.textContent = `Video • ${mime} • ${data.width || '?'}×${data.height || '?'} • from ${formatAddress(sourceAddress)}`;
      }
      updatePeerMeta(id, sourceAddress, { remoteActive: true, viewing: true });
      NodeStore.update(id, { type: 'MediaStream', lastRemoteFrom: sourceAddress });
      refreshAddressIndex(id);
      return;
    }

    if (kind === 'audio' && data.b64) {
      const peer = ensureRemotePeer(id, sourceAddress || (data.nodeId || payload.nodeId || 'remote'), { force: true });
      const format = String(data.format || data.mime || '').toLowerCase();
      if (format.includes('pcm')) {
        const sr = Number(data.sr || payload.sr || 48000);
        const channels = Number(data.channels || payload.channels || 1);
        playPcmAudio(id, data.b64, sr, channels);
        if (peer && peer.info) peer.info.textContent = `Audio • PCM • ${sr} Hz • ch ${channels} • from ${formatAddress(sourceAddress)}`;
      } else if (peer && peer.audio) {
        try {
          if (peer.audioUrl) {
            try { URL.revokeObjectURL(peer.audioUrl); } catch (_) {}
          }
          const blob = base64ToBlob(data.b64, data.mime || 'audio/webm');
          const url = URL.createObjectURL(blob);
          peer.audio.src = url;
          peer.audioUrl = url;
          const play = peer.audio.play();
          if (play && typeof play.catch === 'function') play.catch(() => {});
          peer.audio.onended = () => {
            if (peer.audioUrl) {
              try { URL.revokeObjectURL(peer.audioUrl); } catch (_) {}
              peer.audioUrl = null;
            }
          };
          if (peer.info) {
            const note = data.route ? ` • ${data.route}` : '';
            peer.info.textContent = `Audio • ${data.mime || 'audio/webm'}${note} • from ${formatAddress(sourceAddress)}`;
          }
        } catch (err) {
          log(`[media] audio playback error: ${err?.message || err}`);
        }
      }
      updatePeerMeta(id, sourceAddress, { remoteActive: true, viewing: true });
      NodeStore.update(id, { type: 'MediaStream', lastRemoteFrom: sourceAddress });
      refreshAddressIndex(id);
    }
  }

  function playPcmAudio(nodeId, b64, sr, channels) {
    const st = ensureState(nodeId);
    const buffer = base64ToArrayBuffer(b64);
    if (!buffer) return;
    const safeChannels = Math.max(1, Math.min(2, Number(channels) || 1));
    const safeSr = Number(sr) || 48000;

    const Ctx = window.AudioContext || window.webkitAudioContext;
    if (!Ctx) return;
    if (!st.audioCtx || st.audioCtx.sampleRate !== safeSr) {
      if (st.audioCtx) {
        try { st.audioCtx.close(); } catch (_) {}
      }
      st.audioCtx = new Ctx({ sampleRate: safeSr });
      st.audioQueueTime = 0;
    }
    const ctx = st.audioCtx;
    if (!ctx) return;
    if (ctx.state === 'suspended') ctx.resume().catch(() => {});

    const int16 = new Int16Array(buffer);
    if (!int16.length) return;
    const frameCount = Math.floor(int16.length / safeChannels);
    const audioBuffer = ctx.createBuffer(safeChannels, frameCount, safeSr);
    for (let ch = 0; ch < safeChannels; ch++) {
      const channelData = audioBuffer.getChannelData(ch);
      for (let i = 0; i < frameCount; i++) {
        const sample = int16[i * safeChannels + ch];
        channelData[i] = Math.max(-1, Math.min(1, sample / 32768));
      }
    }

    const source = ctx.createBufferSource();
    source.buffer = audioBuffer;
    source.connect(ctx.destination);

    const startAt = Math.max(ctx.currentTime, st.audioQueueTime || ctx.currentTime);
    try {
      source.start(startAt);
      st.audioQueueTime = startAt + audioBuffer.duration;
    } catch (err) {
      try {
        source.start();
        st.audioQueueTime = ctx.currentTime + audioBuffer.duration;
      } catch (_) {}
    }
  }

  function init(id) {
    ensureState(id);
    ensureListener();
    ensureComponentId(id);
    refreshAddressIndex(id);
    flushPending();
    bindTargetUi(id);
    updateTargetChips(id);
    syncRemoteTiles(id);
    refreshStatusSummary(id);
    updateButtonGlyph(id);
    updateStatus(id, 'Ready');
  }

  function toggle(id) {
    const st = ensureState(id);
    if (st.running) stopCapture(id);
    else startCapture(id);
  }

  function refresh(nodeId) {
    const st = ensureState(nodeId);
    const cfg = config(nodeId);
    if (st.running) {
      // restart to apply new constraints
      stopCapture(nodeId);
      setTimeout(() => startCapture(nodeId), 250);
    }
    const { container } = nodeElements(nodeId);
    if (container) {
      container.dataset.mediaIncludeVideo = cfg.includeVideo ? 'true' : 'false';
      container.dataset.mediaIncludeAudio = cfg.includeAudio ? 'true' : 'false';
    }
    bindTargetUi(nodeId);
    updateTargetChips(nodeId);
    syncRemoteTiles(nodeId);
    refreshStatusSummary(nodeId);
    updateButtonGlyph(nodeId);
  }

  function dispose(id) {
    const st = state.get(id);
    if (st) {
      if (st.heartbeats) {
        for (const timer of st.heartbeats.values()) clearInterval(timer);
        st.heartbeats.clear();
      }
      if (st.handshakeTimers) {
        for (const timer of st.handshakeTimers.values()) clearInterval(timer);
        st.handshakeTimers.clear();
      }
      if (st.remotePeers) {
        for (const addr of Array.from(st.remotePeers.keys())) removeRemotePeer(id, addr);
        st.remotePeers.clear();
      }
    }
    stopCapture(id);
    unregisterAddresses(id);
    unregisterComponent(id);
    subscriberMap.delete(id);
    state.delete(id);
  }

  return {
    init,
    toggle,
    refresh,
    onInput: handleIncoming,
    dispose,
    isRunning: (id) => ensureState(id).running,
    getTargets,
    getActiveTargets,
    getPeerMeta,
    requestHandshake,
    acceptHandshake,
    declineHandshake,
    sendPing,
    addTarget,
    removeTarget,
    setTargetActive: setTargetActiveState,
    formatAddress,
    subscribe: (id, fn) => subscribeToNode(id, fn)
  };
}

export { createMediaNode };
