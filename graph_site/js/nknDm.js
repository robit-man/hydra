import { setBadge } from './utils.js';

function createNknDM({ getNode, NodeStore, Net, CFG, Router, log }) {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  const nodeState = new Map();
  const addressIndex = new Map();
  const componentIndex = new Map();
  let listenerAttached = false;
  let modalSetup = false;
  let inviteModal = null;
  let inviteBackdrop = null;
  let inviteClose = null;
  let inviteAccept = null;
  let inviteDecline = null;
  let inviteText = null;
  let closeInviteModal = null;
  let currentInvite = null;

  function debugLog(label, details) {
    try {
      if (typeof console === 'undefined') return;
      if (details !== undefined) console.log(label, details);
      else console.log(label);
    } catch (err) {
      // ignore console errors
    }
  }

  function getState(nodeId) {
    if (!nodeState.has(nodeId)) {
      nodeState.set(nodeId, {
        heartbeatTimer: null,
        handshakeTimer: null,
        spinnerDone: null,
        lastSeen: 0,
        connected: false,
      inbox: new Map(),
      pendingHandshake: false,
      address: '',
      retryTimer: null,
      localTimer: null,
      missedBeats: 0
    });
  }
  return nodeState.get(nodeId);
  }

  function safeJson(value) {
    if (value == null) return String(value);
    if (typeof value === 'string') return value;
    const seen = new WeakSet();
    const replacer = (_, v) => {
      if (typeof v === 'bigint') return v.toString();
      if (typeof v === 'function') return `[Function ${v.name || 'anonymous'}]`;
      if (typeof v === 'object' && v !== null) {
        if (seen.has(v)) return '[Circular]';
        try {
          seen.add(v);
        } catch (err) {
          // ignore WeakSet failures
        }
      }
      return v;
    };
    try {
      return JSON.stringify(value, replacer);
    } catch (err) {
      try {
        return JSON.stringify(value, (_, v) => {
          if (typeof v === 'bigint') return v.toString();
          if (typeof v === 'object' && v !== null) return String(v);
          return v;
        });
      } catch (err2) {
        return String(value);
      }
    }
  }

  function safeParseJson(str) {
    if (typeof str !== 'string') return null;
    const trimmed = str.trim();
    if (!trimmed) return null;
    try {
      return JSON.parse(trimmed);
    } catch (err) {
      return null;
    }
  }

  function decodeBase64ToString(b64) {
    if (typeof b64 !== 'string' || !b64.trim()) return '';
    try {
      if (typeof atob !== 'function') return '';
      const bin = atob(b64);
      const out = new Uint8Array(bin.length);
      for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
      return decoder.decode(out);
    } catch (err) {
      return '';
    }
  }

  function normalizeIncomingEvent(rawSrc, rawPayload) {
    let src = typeof rawSrc === 'string' ? rawSrc : '';
    let payload = rawPayload;
    if (rawSrc && typeof rawSrc === 'object') {
      if (typeof rawSrc.src === 'string' && !src) src = rawSrc.src;
      if (rawSrc.payload !== undefined && payload === undefined) payload = rawSrc.payload;
      if (rawSrc.message !== undefined && payload === undefined) payload = rawSrc.message;
    }
    if (payload && typeof payload === 'object') {
      if (!src && typeof payload.src === 'string') src = payload.src;
      if (payload.payload !== undefined && (typeof payload.type !== 'string' || !payload.type)) {
        const nested = payload.payload;
        if (nested && typeof nested === 'object' && typeof nested.type === 'string') {
          payload = nested;
        }
      }
    }
    return { src, payload };
  }

  const NKNDM_META_KEYS = [
    'from',
    'componentId',
    'targetId',
    'graphId',
    'targetGraphId',
    'remoteId',
    'remoteComponentId',
    'targetComponentId',
    'route',
    'targetRoute',
    'targetPort',
    'channel',
    'port',
    'id',
    'seq',
    'total',
    'b64',
    'payload_b64',
    'body_b64',
    'address',
    'peer',
    'peerAddress',
    'session',
    'sessionId',
    'targetAddress',
    'ts',
    'timestamp'
  ];

  const NKNDM_TYPE_KEYS = [
    'type',
    'event',
    'eventType',
    'msgType',
    'messageType',
    'message_type',
    'kind',
    'category',
    'topic',
    'op'
  ];

  function normalizeNkndmType(value) {
    if (typeof value !== 'string') return '';
    const raw = value.trim();
    if (!raw) return '';
    const normalizedPrefix = raw.replace(/^(nkndm[-_:]+)/i, 'nkndm.');
    const lower = normalizedPrefix.toLowerCase();
    if (lower.startsWith('nkndm.')) {
      return lower;
    }
    switch (lower) {
      case 'handshake':
      case 'nkndmhandshake':
        return 'nkndm.handshake';
      case 'heartbeat':
      case 'nkndmheartbeat':
      case 'ping':
      case 'pong':
        return 'nkndm.heartbeat';
      case 'data':
      case 'message':
      case 'payload':
      case 'dm':
      case 'nkndmdata':
        return 'nkndm.data';
      case 'debug':
      case 'nkndmdebug':
        return 'nkndm.debug';
      default:
        return '';
    }
  }

  function collectNkndmMeta(meta, source) {
    const out = meta ? { ...meta } : {};
    if (!source || typeof source !== 'object') return out;
    for (const key of NKNDM_META_KEYS) {
      if (source[key] !== undefined && source[key] !== null && out[key] === undefined) {
        out[key] = source[key];
      }
    }
    if (typeof source.address === 'string' && out.address === undefined) out.address = source.address;
    if (typeof source.peer === 'string' && out.peer === undefined) out.peer = source.peer;
    if (typeof source.component === 'string' && out.componentId === undefined) out.componentId = source.component;
    if (typeof source.remoteComponent === 'string' && out.remoteComponentId === undefined) out.remoteComponentId = source.remoteComponent;
    return out;
  }

  function resolveNkndmType(obj) {
    if (!obj || typeof obj !== 'object') return '';
    for (const key of NKNDM_TYPE_KEYS) {
      const type = normalizeNkndmType(obj[key]);
      if (type) return type;
    }
    if (typeof obj.action === 'string') {
      const byAction = normalizeNkndmType(obj.action);
      if (byAction === 'nkndm.heartbeat') return byAction;
    }
    return '';
  }

  function unwrapNkndmMessage(envelope, fallbackText) {
    const queue = [];
    const seen = new Set();

    const enqueue = (value, meta) => {
      if (!value || typeof value !== 'object') return;
      queue.push({ value, meta: meta || {} });
    };

    const tryParseString = (str, meta) => {
      if (typeof str !== 'string') return;
      const trimmed = str.trim();
      if (!trimmed) return;
      const parsed = safeParseJson(trimmed);
      if (parsed) {
        enqueue(parsed, collectNkndmMeta({ ...meta }, parsed));
        return;
      }
      const decoded = decodeBase64ToString(trimmed);
      if (decoded && decoded !== trimmed) {
        const decodedParsed = safeParseJson(decoded);
        if (decodedParsed) enqueue(decodedParsed, collectNkndmMeta({ ...meta }, decodedParsed));
      }
    };

    if (envelope && typeof envelope === 'object') {
      enqueue(envelope, collectNkndmMeta({}, envelope));
    }

    if (typeof fallbackText === 'string') {
      const parsed = safeParseJson(fallbackText);
      if (parsed && parsed !== envelope) {
        enqueue(parsed, collectNkndmMeta({}, parsed));
      } else {
        tryParseString(fallbackText, {});
      }
    }

    while (queue.length) {
      const { value, meta } = queue.shift();
      if (!value || typeof value !== 'object') continue;
      if (seen.has(value)) continue;
      seen.add(value);

      const mergedMeta = collectNkndmMeta({ ...meta }, value);
      const resolvedType = resolveNkndmType(value);
      if (resolvedType && resolvedType.startsWith('nkndm.')) {
        const message = { ...value, type: resolvedType };
        for (const key of Object.keys(mergedMeta)) {
          if (message[key] === undefined) message[key] = mergedMeta[key];
        }
        return { message, envelope: envelope || value };
      }

      if (Array.isArray(value)) {
        for (const entry of value) {
          if (!entry) continue;
          if (typeof entry === 'string') {
            tryParseString(entry, mergedMeta);
          } else if (typeof entry === 'object') {
            enqueue(entry, collectNkndmMeta({ ...mergedMeta }, entry));
          }
        }
        continue;
      }

      for (const [, child] of Object.entries(value)) {
        if (child == null) continue;
        const nextMeta = collectNkndmMeta({ ...mergedMeta }, typeof child === 'object' ? child : {});
        if (typeof child === 'string') {
          tryParseString(child, nextMeta);
        } else if (typeof child === 'object') {
          enqueue(child, nextMeta);
        }
      }
    }

    return null;
  }

  function deepHydrateJson(value, depth = 0, seen = new WeakSet()) {
    if (depth > 8) return value;
    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (!trimmed) return value;
      const parsed = safeParseJson(trimmed);
      if (parsed !== null) return deepHydrateJson(parsed, depth + 1, seen);
      const decoded = decodeBase64ToString(trimmed);
      if (decoded && decoded !== trimmed) {
        const decodedParsed = safeParseJson(decoded);
        if (decodedParsed !== null) return deepHydrateJson(decodedParsed, depth + 1, seen);
      }
      return value;
    }
    if (!value || typeof value !== 'object') return value;
    if (seen.has(value)) return value;
    seen.add(value);
    if (Array.isArray(value)) {
      return value.map((item) => deepHydrateJson(item, depth + 1, seen));
    }
    const out = {};
    for (const [key, child] of Object.entries(value)) {
      out[key] = deepHydrateJson(child, depth + 1, seen);
    }
    return out;
  }

  function deriveBestText(value, depth = 0, visited = new WeakSet()) {
    if (value == null) return '';
    if (typeof value === 'string') return value;
    if (typeof value === 'number') return String(value);
    if (typeof value === 'boolean') return value ? 'true' : 'false';
    if (depth > 8) return '';
    if (typeof value !== 'object') return String(value);
    if (visited.has(value)) return '';
    visited.add(value);
    const priorityKeys = [
      'text',
      'message',
      'content',
      'value',
      'body',
      'payload',
      'data',
      'note',
      'detail',
      'result',
      'entry',
      'summary',
      'description'
    ];
    for (const key of priorityKeys) {
      if (value[key] === undefined || value[key] === null) continue;
      const derived = deriveBestText(value[key], depth + 1, visited);
      if (derived) return derived;
    }
    if (Array.isArray(value)) {
      for (const entry of value) {
        const derived = deriveBestText(entry, depth + 1, visited);
        if (derived) return derived;
      }
      return '';
    }
    for (const child of Object.values(value)) {
      if (child === undefined || child === null) continue;
      const derived = deriveBestText(child, depth + 1, visited);
      if (derived) return derived;
    }
    return '';
  }

  function interpretIncomingPayload(payload) {
    if (payload == null) return { text: '', object: null };
    if (typeof payload === 'string') {
      return { text: payload, object: safeParseJson(payload) };
    }
    if (payload instanceof ArrayBuffer) {
      const text = decoder.decode(new Uint8Array(payload));
      return { text, object: safeParseJson(text) };
    }
    if (typeof ArrayBuffer !== 'undefined' && ArrayBuffer.isView && ArrayBuffer.isView(payload)) {
      const view = payload;
      const text = decoder.decode(new Uint8Array(view.buffer, view.byteOffset, view.byteLength));
      return { text, object: safeParseJson(text) };
    }
    if (typeof payload === 'object') {
      return { text: safeJson(payload), object: payload };
    }
    const text = String(payload);
    return { text, object: safeParseJson(text) };
  }

  function summarizeText(text, limit = 240) {
    if (!text) return '';
    const trimmed = text.trim();
    if (trimmed.length <= limit) return trimmed;
    return `${trimmed.slice(0, limit)}…`;
  }

  function extractRouteFromPayload(data) {
    if (!data || typeof data !== 'object') return '';
    const keys = ['route', 'port', 'channel', 'targetPort', 'targetRoute'];
    for (const key of keys) {
      const value = data[key];
      if (typeof value === 'string' && value.trim()) return value.trim();
    }
    return '';
  }

  function normalizeInboundPayload(data, opts = {}) {
    const { textOverride, routeOverride } = opts || {};
    const result = {
      text: '',
      parsed: null,
      route: '',
      meta: {
        source: '',
        base64: false,
        override: textOverride !== undefined && textOverride !== null,
        json: false
      }
    };

    result.route = typeof routeOverride === 'string' && routeOverride.trim()
      ? routeOverride.trim()
      : extractRouteFromPayload(data);

    const candidates = [];
    if (textOverride !== undefined && textOverride !== null) {
      candidates.push({ key: 'override', value: textOverride });
    } else if (data && typeof data === 'object') {
      const push = (key) => {
        if (data[key] !== undefined && data[key] !== null) candidates.push({ key, value: data[key] });
      };
      ['text', 'payload', 'json', 'body', 'message', 'content', 'value'].forEach(push);
    }

    let stringCandidate = null;
    let objectCandidate = null;

    for (const { key, value } of candidates) {
      if (stringCandidate && objectCandidate) break;
      if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
        if (!stringCandidate) {
          stringCandidate = { key, value: typeof value === 'string' ? value : String(value) };
        }
      } else if (value && typeof value === 'object' && !objectCandidate) {
        objectCandidate = { key, value };
      }
    }

    if (!stringCandidate && typeof textOverride === 'undefined') {
      if (typeof data?.b64 === 'string') {
        const decoded = decodeBase64ToString(data.b64);
        if (decoded) {
          stringCandidate = { key: 'b64', value: decoded };
          result.meta.base64 = true;
        }
      } else if (typeof data?.payload_b64 === 'string') {
        const decoded = decodeBase64ToString(data.payload_b64);
        if (decoded) {
          stringCandidate = { key: 'payload_b64', value: decoded };
          result.meta.base64 = true;
        }
      } else if (typeof data?.body_b64 === 'string') {
        const decoded = decodeBase64ToString(data.body_b64);
        if (decoded) {
          stringCandidate = { key: 'body_b64', value: decoded };
          result.meta.base64 = true;
        }
      }
    }

    if (stringCandidate) {
      result.text = String(stringCandidate.value);
      result.meta.source = stringCandidate.key;
    } else if (objectCandidate) {
      result.text = safeJson(objectCandidate.value);
      result.meta.source = objectCandidate.key;
      result.parsed = objectCandidate.value;
    }

    if (!result.text && objectCandidate && !result.parsed) {
      result.text = safeJson(objectCandidate.value);
      result.meta.source = objectCandidate.key;
      result.parsed = objectCandidate.value;
    }

    if (!result.parsed) {
      const parsed = safeParseJson(result.text);
      if (parsed !== null) {
        result.parsed = deepHydrateJson(parsed);
        result.meta.json = true;
      }
    } else {
      result.parsed = deepHydrateJson(result.parsed);
      result.meta.json = true;
    }

    if (!result.route && result.parsed && typeof result.parsed === 'object') {
      const primary = extractRouteFromPayload(result.parsed);
      const secondarySource = typeof result.parsed === 'object'
        ? (result.parsed.payload || result.parsed.body || result.parsed.data || null)
        : null;
      const secondary = secondarySource && typeof secondarySource === 'object'
        ? extractRouteFromPayload(secondarySource)
        : '';
      const parsedRoute = primary || secondary;
      if (parsedRoute) result.route = parsedRoute;
    }

    if (result.parsed && typeof result.parsed === 'object') {
      const derived = deriveBestText(result.parsed);
      const current = typeof result.text === 'string' ? result.text.trim() : '';
      if (derived) {
        const looksLikeObject = !current || current === '[object Object]' || /^[{\[]/.test(current);
        const shorter = current && derived.length && derived.length < current.length;
        if (!current || looksLikeObject || shorter) {
          result.text = derived;
          if (!result.meta.source) result.meta.source = 'parsed';
        }
      }
    }

    if (!result.text) result.text = '';
    return result;
  }

  function notifyStatus(nodeId, message, status = {}, isError = false) {
    if (!nodeState.has(nodeId)) return;
    logLine(nodeId, message, isError);
    emitStatus(nodeId, {
      level: isError ? 'error' : 'info',
      message,
      ...status
    });
  }

  function emitIncomingPayload(nodeId, src, original, normalized, extra = {}) {
    const payload = {
      nodeId,
      from: src,
      text: normalized.text,
      raw: original,
      parsed: normalized.parsed,
      route: normalized.route || '',
      meta: normalized.meta,
      ts: Date.now()
    };
    if (normalized.parsed) payload.payload = normalized.parsed;
    if (original && typeof original.id === 'string') payload.id = original.id;
    if (original && original.seq !== undefined && original.seq !== null) {
      const seqNum = Number(original.seq);
      if (Number.isFinite(seqNum)) payload.seq = seqNum;
    }
    if (original && original.total !== undefined && original.total !== null) {
      const totalNum = Number(original.total);
      if (Number.isFinite(totalNum)) payload.total = totalNum;
    }
    if (extra.id !== undefined) payload.id = extra.id;
    if (extra.seq !== undefined) payload.seq = extra.seq;
    if (extra.total !== undefined) payload.total = extra.total;
    if (typeof original?.componentId === 'string') payload.remoteComponentId = original.componentId;
    if (typeof original?.graphId === 'string') payload.remoteGraphId = original.graphId;
    if (typeof original?.targetId === 'string') payload.targetComponentId = original.targetId;
    if (typeof original?.targetGraphId === 'string') payload.targetGraphId = original.targetGraphId;
    Router.sendFrom(nodeId, 'incoming', payload);
  }

  function notifyNoCandidate(src, data, reason) {
    const targetId = typeof data?.targetId === 'string' ? data.targetId : '';
    const message = `Dropped ${reason} DM from ${src}${targetId ? ` targeting ${targetId}` : ''}: no matching NKN DM node`;
    log(`[nkndm] ${message}`);
    setBadge(message, false);
    const hintNodes = new Set();
    if (targetId && componentIndex.has(targetId)) {
      hintNodes.add(componentIndex.get(targetId));
    }
    for (const nodeId of nodeState.keys()) {
      const cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
      const handshakePeer = (cfg.handshake?.peer || '').trim();
      const activeAddress = (cfg.address || cfg.peer?.address || handshakePeer || '').trim();
      const allowed = Array.isArray(cfg.allowedPeers) ? cfg.allowedPeers : [];
      if (handshakePeer === src || activeAddress === src || allowed.includes(src)) {
        hintNodes.add(nodeId);
      }
    }
    if (!hintNodes.size) {
      for (const nodeId of nodeState.keys()) {
        const cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
        if ((cfg.handshake?.status || 'idle') !== 'accepted') hintNodes.add(nodeId);
      }
    }
    hintNodes.forEach((nodeId) => {
      notifyStatus(nodeId, message, { type: 'error', code: 'no-candidate', from: src, targetId }, true);
    });
  }

  function ensureComponentId(nodeId) {
    let cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
    let { componentId } = cfg;
    if (!componentId) {
      componentId = `${CFG.graphId || 'graph'}:${nodeId}`;
      cfg = NodeStore.update(nodeId, { type: 'NknDM', componentId });
    }
    if (componentId) componentIndex.set(componentId, nodeId);
    return componentId;
  }

  function registerComponent(nodeId) {
    const id = ensureComponentId(nodeId);
    if (id) componentIndex.set(id, nodeId);
    return id;
  }

  function clearStateTimers(st) {
    if (!st) return;
    if (st.heartbeatTimer) {
      clearInterval(st.heartbeatTimer);
      st.heartbeatTimer = null;
    }
    if (st.handshakeTimer) {
      clearInterval(st.handshakeTimer);
      st.handshakeTimer = null;
    }
    if (st.retryTimer) {
      clearTimeout(st.retryTimer);
      st.retryTimer = null;
    }
    if (st.localTimer) {
      clearTimeout(st.localTimer);
      st.localTimer = null;
    }
  }

  function resolveSpinner(nodeId) {
    const st = nodeState.get(nodeId);
    if (st && typeof st.spinnerDone === 'function') {
      try {
        st.spinnerDone();
      } catch (err) {
        // ignore callback errors
      }
      st.spinnerDone = null;
    }
  }

  function dispose(nodeId) {
    const st = nodeState.get(nodeId);
    if (st) {
      clearStateTimers(st);
      resolveSpinner(nodeId);
      nodeState.delete(nodeId);
    }
    for (const [addr, set] of addressIndex.entries()) {
      if (set.delete(nodeId) && set.size === 0) addressIndex.delete(addr);
    }
    for (const [compId, id] of Array.from(componentIndex.entries())) {
      if (id === nodeId) componentIndex.delete(compId);
    }
  }

  function ensureModal() {
    if (modalSetup) return;
    inviteModal = document.getElementById('nknDmInviteModal');
    if (!inviteModal) return;
    inviteBackdrop = document.getElementById('nknDmInviteBackdrop');
    inviteClose = document.getElementById('nknDmInviteClose');
    inviteAccept = document.getElementById('nknDmInviteAccept');
    inviteDecline = document.getElementById('nknDmInviteDecline');
    inviteText = inviteModal.querySelector('[data-nkndm-invite-text]');

    const close = () => {
      if (!inviteModal) return;
      inviteModal.classList.add('hidden');
      inviteModal.setAttribute('aria-hidden', 'true');
      currentInvite = null;
    };

    closeInviteModal = close;

    const respond = (accepted) => {
      if (!currentInvite) {
        close();
        return;
      }
      const { nodeId, from } = currentInvite;
      close();
      if (accepted) {
        acceptHandshake(nodeId, from);
      } else {
        declineHandshake(nodeId, from);
      }
    };

    inviteBackdrop?.addEventListener('click', close);
    inviteClose?.addEventListener('click', close);
    inviteAccept?.addEventListener('click', () => respond(true));
    inviteDecline?.addEventListener('click', () => respond(false));

    modalSetup = true;
  }

  function openInvite(nodeId, from, remoteId) {
    ensureModal();
    if (!inviteModal || !inviteText) return;
    currentInvite = { nodeId, from, remoteId };
    const suffix = remoteId ? ` (uuid ${remoteId})` : '';
    inviteText.textContent = `Peer ${from}${suffix} wants to link with node ${nodeId}. Accept?`;
    inviteModal.classList.remove('hidden');
    inviteModal.setAttribute('aria-hidden', 'false');
  }

  function emitStatus(nodeId, payload) {
    Router.sendFrom(nodeId, 'status', { nodeId, ...payload });
  }

  function logLine(nodeId, message, isError = false) {
    const node = getNode(nodeId);
    const el = node?.el?.querySelector('[data-nkndm-log]');
    if (!el) return;
    const text = `[${new Date().toLocaleTimeString()}] ${message}`;
    const existing = el.textContent ? `${el.textContent}\n${text}` : text;
    el.textContent = existing.split('\n').slice(-100).join('\n');
    if (isError) setBadge(message, false);
  }

  function setIndicator(nodeId, status) {
    const node = getNode(nodeId);
    const el = node?.el?.querySelector('[data-nkndm-indicator]');
    if (!el) return;
    el.classList.remove('online', 'pending', 'offline', 'warning', 'critical');
    if (status === 'online') {
      el.classList.add('online');
      el.title = 'Connected to peer';
    } else if (status === 'pending') {
      el.classList.add('pending');
      el.title = 'Awaiting peer response';
    } else if (status === 'warning') {
      el.classList.add('warning');
      el.title = 'Heartbeat delayed';
    } else if (status === 'critical') {
      el.classList.add('critical');
      el.title = 'Heartbeat timeout';
    } else {
      el.classList.add('offline');
      el.title = 'Offline';
    }
  }

  function togglePeerActions(nodeId, show) {
    const node = getNode(nodeId);
    const actions = node?.el?.querySelector('[data-nkndm-actions]');
    if (!actions) return;
    actions.classList.toggle('hidden', !show);
  }

  function updatePeerDisplay(nodeId, cfg) {
    const node = getNode(nodeId);
    const localEl = node?.el?.querySelector('[data-nkndm-local]');
    const peerEl = node?.el?.querySelector('[data-nkndm-peer]');
    if (localEl) localEl.textContent = Net.nkn?.addr || '(offline)';
    const peerInfo = cfg.peer || {};
    const display = peerInfo.address || cfg.address || cfg.handshake?.peer || '';
    if (peerEl) {
      const compId = peerInfo.componentId || cfg.handshake?.remoteId || '';
      const text = display ? (compId ? `${display} • ${compId}` : display) : '(none)';
      peerEl.textContent = text;
      peerEl.title = compId ? `UUID: ${compId}` : '';
    }
  }

  function updateSettingsPanel(nodeId, cfg, handshakeStatus) {
    const form = document.getElementById('settingsForm');
    if (!form || form.dataset.nodeId !== nodeId) return;
    const spinner = form.querySelector('[data-nkndm-settings-spinner]');
    const statusEl = form.querySelector('[data-nkndm-settings-status]');
    const peerEl = form.querySelector('[data-nkndm-settings-peer]');
    if (spinner) {
      if (handshakeStatus === 'pending' && (cfg.handshake?.direction || '') === 'outgoing') spinner.classList.remove('hidden');
      else spinner.classList.add('hidden');
    }
    if (statusEl) {
      statusEl.textContent = '';
      statusEl.classList.remove('ok', 'err', 'pending');
      if (handshakeStatus === 'accepted') {
        statusEl.textContent = '✔';
        statusEl.classList.add('ok');
        statusEl.title = 'Handshake accepted';
      } else if (handshakeStatus === 'declined') {
        statusEl.textContent = '✖';
        statusEl.classList.add('err');
        statusEl.title = 'Handshake declined';
      } else if (handshakeStatus === 'pending') {
        statusEl.textContent = '…';
        statusEl.classList.add('pending');
        statusEl.title = cfg.handshake?.direction === 'incoming' ? 'Awaiting your response' : 'Awaiting peer response';
      } else {
        statusEl.title = 'Handshake idle';
      }
    }
    if (peerEl) {
      const peerInfo = cfg.peer || {};
      if (peerInfo.address) {
        const compId = peerInfo.componentId ? ` • ${peerInfo.componentId}` : '';
        peerEl.textContent = `${peerInfo.address}${compId}`;
      } else if (cfg.handshake?.peer) {
        const compId = cfg.handshake?.remoteId ? ` • ${cfg.handshake.remoteId}` : '';
        peerEl.textContent = `${cfg.handshake.peer}${compId}`;
      } else {
        peerEl.textContent = '—';
      }
    }
  }

  function scheduleLocalAddress(nodeId) {
    const st = getState(nodeId);
    if (st.localTimer) {
      clearTimeout(st.localTimer);
      st.localTimer = null;
    }
    const tick = () => {
      updatePeerDisplay(nodeId, NodeStore.ensure(nodeId, 'NknDM').config || {});
      if (!Net.nkn?.addr) {
        st.localTimer = setTimeout(tick, 500);
      } else {
        st.localTimer = null;
      }
    };
    tick();
  }

  function refresh(nodeId) {
    registerComponent(nodeId);
    const cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
    const st = getState(nodeId);
    const localAddress = (cfg.address || '').trim();
    const peerAddress = (cfg.peer?.address || '').trim();
    const hintAddress = (cfg.handshake?.peer || '').trim();
    const activeAddress = localAddress || peerAddress || hintAddress;
    registerAddress(nodeId, activeAddress);
    updatePeerDisplay(nodeId, cfg);
    scheduleLocalAddress(nodeId);
    if (!activeAddress) {
      setIndicator(nodeId, 'offline');
      emitStatus(nodeId, { type: 'idle' });
      togglePeerActions(nodeId, false);
      stopHandshakeLoop(nodeId);
      resolveSpinner(nodeId);
      return;
    }
    const handshakeStatus = cfg.handshake?.status || 'idle';
    if (handshakeStatus === 'accepted') {
      const intervalMs = Math.max(5, Number(cfg.heartbeatInterval) || 15) * 1000;
      const now = Date.now();
      if (!st.connected || !st.lastSeen) {
        setIndicator(nodeId, 'pending');
      } else if (now - st.lastSeen < intervalMs) {
        setIndicator(nodeId, 'online');
      } else {
        applyHeartbeatIndicator(nodeId, intervalMs);
        emitStatus(nodeId, { type: 'pending', peer: st.address });
      }
      togglePeerActions(nodeId, false);
      stopHandshakeLoop(nodeId);
    } else if (handshakeStatus === 'pending') {
      setIndicator(nodeId, 'pending');
      emitStatus(nodeId, { type: 'pending', peer: st.address });
      const incoming = cfg.handshake?.direction === 'incoming';
      togglePeerActions(nodeId, incoming);
      if (!incoming) startHandshakeLoop(nodeId);
      st.connected = false;
      st.missedBeats = 0;
    } else {
      setIndicator(nodeId, 'offline');
      emitStatus(nodeId, { type: 'idle' });
      togglePeerActions(nodeId, false);
      stopHandshakeLoop(nodeId);
      st.connected = false;
      st.missedBeats = 0;
      resolveSpinner(nodeId);
    }
    updateSettingsPanel(nodeId, cfg, handshakeStatus);
  }

  function getClient() {
    Net.ensureNkn();
    return Net.nkn?.client || null;
  }

  function ensureListener() {
    const client = getClient();
    if (!client || listenerAttached) return;
    client.on('message', (src, payload) => {
      try {
        debugLog('[nkndm] message:event', { src, payload });
        const normalized = normalizeIncomingEvent(src, payload);
        const actualSrc = normalized.src || (typeof src === 'string' ? src : '');
        const actualPayload = normalized.payload;
        debugLog('[nkndm] message:normalized', { src: actualSrc, payload: actualPayload });
        const { text, object } = interpretIncomingPayload(actualPayload);
        const envelope = object && typeof object === 'object' ? object : safeParseJson(text);
        const unwrapped = unwrapNkndmMessage(envelope, text);
        const data = unwrapped?.message || envelope;
        if (!data || typeof data.type !== 'string') {
          logRawDm(actualSrc || src, envelope ?? text ?? '');
          return;
        }
        const type = String(data.type || '').trim();
        if (!type.startsWith('nkndm.')) {
          logRawDm(actualSrc || src, envelope ?? text ?? '', data);
          return;
        }
        handleMessage(actualSrc || src, { ...data, type });
      } catch (err) {
        debugLog('[nkndm] message:error', { err: err?.message || err, src, payload });
        logRawDm(typeof src === 'string' ? src : '', payload);
      }
    });
    listenerAttached = true;
  }

  function registerAddress(nodeId, address) {
    const st = getState(nodeId);
    if (st.address && addressIndex.has(st.address)) {
      const set = addressIndex.get(st.address);
      set.delete(nodeId);
      if (set.size === 0) addressIndex.delete(st.address);
    }
    st.address = address || '';
    if (st.address) {
      if (!addressIndex.has(st.address)) addressIndex.set(st.address, new Set());
      addressIndex.get(st.address).add(nodeId);
    }
  }

  function decoratePayload(nodeId, payload, targetId) {
    const out = { ...(payload || {}) };
    if (!out.from && Net.nkn?.addr) out.from = Net.nkn.addr;
    const componentId = registerComponent(nodeId);
    if (componentId) out.componentId = componentId;
    if (targetId) out.targetId = targetId;
    if (CFG.graphId && !out.graphId) out.graphId = CFG.graphId;
    return out;
  }

  function setPeerInfo(nodeId, info) {
    const cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
    const next = { ...(cfg.peer || {}), ...(info || {}) };
    const patch = { type: 'NknDM', peer: next };
    if (info && info.address) patch.address = info.address;
    const updatedCfg = NodeStore.update(nodeId, patch);
    const activeAddress = updatedCfg.address || next.address || '';
    registerAddress(nodeId, activeAddress);
    return { peer: next, cfg: updatedCfg };
  }

  function getTargetComponentId(cfg) {
    return (cfg.peer && cfg.peer.componentId) || (cfg.handshake && cfg.handshake.remoteId) || '';
  }

  function findNodesForTarget(src, data, reason) {
    const result = new Map();
    const targetId = typeof data?.targetId === 'string' ? data.targetId : '';
    if (targetId && componentIndex.has(targetId)) {
      const nodeId = componentIndex.get(targetId);
      result.set(nodeId, NodeStore.ensure(nodeId, 'NknDM').config || {});
    }
    for (const candidate of findNodesForAddress(src)) {
      result.set(candidate.id, candidate.cfg);
    }
    if (!result.size) {
      const targetGraph = typeof data?.targetGraphId === 'string' ? data.targetGraphId : '';
      if (!targetGraph || targetGraph === CFG.graphId) {
        for (const nodeId of nodeState.keys()) {
          const cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
          const handshake = cfg.handshake || {};
          const status = handshake.status || 'idle';
          const handshakePeer = (handshake.peer || '').trim();
          const activeAddress = (cfg.address || cfg.peer?.address || handshakePeer || '').trim();
          if (status === 'accepted' && handshakePeer && handshakePeer !== src) continue;
          const allowed = Array.isArray(cfg.allowedPeers) ? cfg.allowedPeers : [];
          const likely =
            (handshakePeer && handshakePeer === src) ||
            (!activeAddress && status !== 'accepted') ||
            allowed.includes(src) ||
            cfg.autoAccept;
          if (likely) {
            result.set(nodeId, cfg);
          }
        }
      }
    }
    const out = Array.from(result.entries()).map(([id, cfg]) => ({ id, cfg }));
    if (!out.length && reason) {
      log(`[nkndm] no candidate nodes for ${reason} from ${src}`);
    }
    return out;
  }

  function logRawDm(src, raw, data) {
    const candidates = findNodesForTarget(src, data || {}, null);
    const payloadObj = (data && typeof data === 'object') ? data : null;
    const normalizedRaw = typeof raw === 'object' && raw !== null ? deepHydrateJson(raw) : raw;
    const normalizedPayload = payloadObj ? deepHydrateJson(payloadObj) : null;
    const rawString = typeof normalizedRaw === 'string' ? normalizedRaw : safeJson(normalizedRaw);
    const payloadString = normalizedPayload ? safeJson(normalizedPayload) : '';
    const displayChunks = [];
    if (rawString && rawString !== 'undefined') displayChunks.push(rawString);
    if (payloadString && payloadString !== rawString) displayChunks.push(payloadString);
    const display = displayChunks.join('\n') || '[no content]';
    debugLog('[nkndm] raw-dm', { src, raw, data, rawString, payloadString });
    if (candidates.length) {
      candidates.forEach(({ id }) => {
        logLine(id, `Raw DM from ${src}:\n${display}`);
        Router.sendFrom(id, 'raw', {
          nodeId: id,
          from: src,
          raw: rawString,
          data: normalizedPayload,
          ts: Date.now()
        });
      });
    } else {
      log(`[nkndm] raw DM from ${src}: ${display}`);
    }
  }

  function applyHeartbeatIndicator(nodeId, intervalMs) {
    const st = getState(nodeId);
    if (!intervalMs) return;
    const now = Date.now();
    const lastSeen = st.lastSeen || 0;
    const misses = lastSeen ? Math.floor((now - lastSeen) / intervalMs) : 0;
    if (misses === st.missedBeats) return;
    st.missedBeats = misses;
    if (!st.connected) return;
    if (misses >= 5) {
      setIndicator(nodeId, 'critical');
    } else if (misses >= 1) {
      setIndicator(nodeId, 'warning');
    } else {
      setIndicator(nodeId, 'online');
    }
  }

  function attemptHandshake(nodeId) {
    const cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
    refresh(nodeId);
    const targetAddress = (cfg.address || cfg.peer?.address || cfg.handshake?.peer || '').trim();
    if (!targetAddress) {
      stopHeartbeat(nodeId);
      return;
    }
    if (CFG.transport !== 'nkn') {
      logLine(nodeId, 'Switch transport to NKN to connect', true);
      return;
    }
    ensureListener();
    const client = getClient();
    if (!client || !Net.nkn.ready || !Net.nkn.addr) {
      scheduleRetry(nodeId);
      return;
    }
    const status = cfg.handshake?.status || 'idle';
    if (status === 'accepted') {
      startHeartbeat(nodeId);
      sendHandshake(nodeId, 'sync');
      return;
    }
    if (status === 'pending') {
      if (cfg.handshake?.direction === 'outgoing') {
        sendHandshake(nodeId, 'request');
        startHandshakeLoop(nodeId);
      }
      return;
    }
    updateHandshake(nodeId, { status: 'pending', peer: targetAddress, direction: 'outgoing' });
    sendHandshake(nodeId, 'request');
    startHandshakeLoop(nodeId);
  }

  function scheduleRetry(nodeId) {
    const st = getState(nodeId);
    if (st.retryTimer) return;
    st.retryTimer = setTimeout(() => {
      st.retryTimer = null;
      attemptHandshake(nodeId);
    }, 3000);
  }

  function updateHandshake(nodeId, patch) {
    const cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
    const base = { status: 'idle', peer: '', direction: 'idle', remoteId: '', graphId: '' };
    const current = { ...base, ...(cfg.handshake || {}) };
    const next = { ...current, ...patch };
    if (next.direction === undefined) next.direction = current.direction;
    NodeStore.update(nodeId, { type: 'NknDM', handshake: next });
    refresh(nodeId);
    emitStatus(nodeId, { type: 'handshake', status: next.status, peer: next.peer });
    togglePeerActions(nodeId, next.status === 'pending' && next.direction === 'incoming');
    if (next.status === 'pending' && next.direction === 'outgoing') {
      startHandshakeLoop(nodeId);
    } else {
      stopHandshakeLoop(nodeId);
      resolveSpinner(nodeId);
    }
  }

  function sendJson(nodeId, address, payload, targetId) {
    const client = getClient();
    if (!client) {
      scheduleRetry(nodeId);
      return;
    }
    try {
      const enriched = decoratePayload(nodeId, payload, targetId);
      debugLog('[nkndm] send-json', { address, payload: enriched, targetId });
      client.send(address, JSON.stringify(enriched), { noReply: true });
    } catch (err) {
      log(`nkn dm send error: ${err?.message || err}`);
      scheduleRetry(nodeId);
    }
  }

  function sendHandshake(nodeId, action) {
    const cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
    const address = (cfg.address || cfg.peer?.address || cfg.handshake?.peer || '').trim();
    if (!address) return;
    if (!Net.nkn.addr) {
      scheduleRetry(nodeId);
      return;
    }
    const targetId = getTargetComponentId(cfg) || undefined;
    const payload = {
      type: 'nkndm.handshake',
      action,
      ts: Date.now(),
      heartbeat: Number(cfg.heartbeatInterval) || 15
    };
    if (cfg.peer?.graphId) payload.targetGraphId = cfg.peer.graphId;
    else if (cfg.handshake?.graphId) payload.targetGraphId = cfg.handshake.graphId;
    sendJson(nodeId, address, payload, targetId);
    if (action === 'request') {
      logLine(nodeId, `Handshake request sent to ${address}`);
    }
  }

  function acceptHandshake(nodeId, from) {
    const cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
    const allowedSet = new Set(Array.isArray(cfg.allowedPeers) ? cfg.allowedPeers : []);
    if (!allowedSet.has(from)) {
      allowedSet.add(from);
      NodeStore.update(nodeId, { type: 'NknDM', allowedPeers: Array.from(allowedSet) });
    }
    const peerInfo = {
      address: from,
      componentId: cfg.handshake?.remoteId || cfg.peer?.componentId || '',
      graphId: cfg.handshake?.graphId || cfg.peer?.graphId || ''
    };
    setPeerInfo(nodeId, peerInfo);
    updateHandshake(nodeId, {
      status: 'accepted',
      peer: from,
      direction: 'accepted',
      remoteId: peerInfo.componentId,
      graphId: peerInfo.graphId
    });
    logLine(nodeId, `Handshake accepted for ${from}`);
    const localAddr = Net.nkn?.addr;
    if (!localAddr) {
      scheduleRetry(nodeId);
      return;
    }
    const updatedCfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
    const targetId = updatedCfg.handshake?.remoteId || updatedCfg.peer?.componentId || undefined;
    sendJson(nodeId, from, {
      type: 'nkndm.handshake',
      action: 'accept',
      ts: Date.now()
    }, targetId);
    startHeartbeat(nodeId);
    togglePeerActions(nodeId, false);
  }

  function declineHandshake(nodeId, from) {
    const localAddr = Net.nkn?.addr;
    updateHandshake(nodeId, { status: 'declined', peer: from, direction: 'declined' });
    logLine(nodeId, `Handshake declined for ${from}`);
    if (localAddr) {
      const cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
      const targetId = cfg.handshake?.remoteId || cfg.peer?.componentId || undefined;
      sendJson(nodeId, from, {
        type: 'nkndm.handshake',
        action: 'decline',
        ts: Date.now()
      }, targetId);
    }
    stopHeartbeat(nodeId);
    togglePeerActions(nodeId, false);
  }

  function startHeartbeat(nodeId) {
    const cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
    const st = getState(nodeId);
    clearStateTimers(st);
    const intervalSec = Math.max(5, Number(cfg.heartbeatInterval) || 15);
    const intervalMs = intervalSec * 1000;
    const address = (cfg.address || cfg.peer?.address || cfg.handshake?.peer || '').trim();
    if (!address) return;
    st.connected = false;
    st.lastSeen = Date.now();
    st.missedBeats = 0;
    setIndicator(nodeId, 'pending');
    emitStatus(nodeId, { type: 'pending', peer: address });
    const targetId = getTargetComponentId(cfg) || undefined;
    st.heartbeatTimer = setInterval(() => {
      const client = getClient();
      if (!client || !Net.nkn.ready || !Net.nkn.addr) {
        setIndicator(nodeId, 'offline');
        scheduleRetry(nodeId);
        return;
      }
      const now = Date.now();
      if (st.lastSeen && now - st.lastSeen > intervalMs * 5 && st.connected) {
        st.connected = false;
        setIndicator(nodeId, 'critical');
        logLine(nodeId, 'Peer heartbeat timeout');
        emitStatus(nodeId, { type: 'timeout', peer: address });
      } else {
        applyHeartbeatIndicator(nodeId, intervalMs);
      }
      sendJson(nodeId, address, {
        type: 'nkndm.heartbeat',
        action: 'ping',
        ts: Date.now()
      }, targetId);
    }, intervalMs);
    sendJson(nodeId, address, {
      type: 'nkndm.heartbeat',
      action: 'ping',
      ts: Date.now()
    }, targetId);
  }

  function stopHeartbeat(nodeId) {
    const st = nodeState.get(nodeId);
    if (!st) return;
    clearStateTimers(st);
    st.connected = false;
    setIndicator(nodeId, 'offline');
  }

  function handleMessage(src, data) {
    if (typeof src !== 'string') return;
    debugLog('[nkndm] handleMessage:incoming', { src, data });
    if (data.type === 'nkndm.handshake') {
      handleHandshakeMessage(src, data);
      return;
    }
    if (data.type === 'nkndm.heartbeat') {
      handleHeartbeatMessage(src, data);
      return;
    }
    if (data.type === 'nkndm.data') {
      handleDataMessage(src, data);
      return;
    }
    if (data.type === 'nkndm.debug') {
      handleDebugMessage(src, data);
      return;
    }
  }

  function handleDebugMessage(src, data) {
    const candidates = findNodesForTarget(src, data, 'debug');
    if (!candidates.length) {
      log(`[nkndm] debug DM from ${src}: ${JSON.stringify(data)}`);
      return;
    }
    const note = data.message || data.note || data.payload || data.text || '';
    const suffix = note ? ` • ${note}` : '';
    const action = data.action ? ` (${data.action})` : '';
    debugLog('[nkndm] debug-message:dispatch', { src, data, candidates: candidates.map((c) => c.id) });
    candidates.forEach(({ id, cfg }) => {
      logLine(id, `Debug message from ${src}${action}${suffix}`);
      Router.sendFrom(id, 'raw', {
        nodeId: id,
        from: src,
        raw: safeJson(data),
        data,
        ts: Date.now()
      });
      if (data.action === 'ping') {
        const targetId = getTargetComponentId(cfg) || data.componentId || undefined;
        sendJson(id, src, {
          type: 'nkndm.debug',
          action: 'pong',
          ts: Date.now()
        }, targetId);
      }
    });
  }

  function findNodesForAddress(address) {
    if (!address) return [];
    const set = addressIndex.get(address);
    if (!set || !set.size) return [];
    return Array.from(set).map((id) => ({ id, cfg: NodeStore.ensure(id, 'NknDM').config || {} }));
  }

  function handleHandshakeMessage(src, data) {
    const candidates = findNodesForTarget(src, data, 'handshake');
    if (!candidates.length) return;
    debugLog('[nkndm] handshake-message:dispatch', { src, data, candidates: candidates.map((c) => c.id) });
    candidates.forEach(({ id, cfg }) => {
      const status = cfg.handshake?.status || 'idle';
      const remoteId = typeof data?.componentId === 'string' ? data.componentId : '';
      const remoteGraph = typeof data?.graphId === 'string' ? data.graphId : '';
      const logSuffix = remoteId ? ` (uuid ${remoteId})` : '';
      if (data.action === 'request') {
        if (status === 'accepted' && cfg.handshake?.peer === src) {
          sendJson(id, src, {
            type: 'nkndm.handshake',
            action: 'accept',
            ts: Date.now()
          }, remoteId || cfg.peer?.componentId || undefined);
          startHeartbeat(id);
          return;
        }
        const allowed = Array.isArray(cfg.allowedPeers) ? cfg.allowedPeers : [];
        updateHandshake(id, {
          status: 'pending',
          peer: src,
          direction: 'incoming',
          remoteId: remoteId || cfg.handshake?.remoteId || '',
          graphId: remoteGraph || cfg.handshake?.graphId || ''
        });
        if (cfg.autoAccept || allowed.includes(src)) {
          logLine(id, `Auto-accepting handshake from ${src}${logSuffix}`);
          acceptHandshake(id, src);
          return;
        }
        logLine(id, `Handshake request received from ${src}${logSuffix}`);
        openInvite(id, src, remoteId);
        setIndicator(id, 'pending');
        emitStatus(id, { type: 'handshake', status: 'pending', peer: src });
      } else if (data.action === 'accept') {
        setPeerInfo(id, { address: src, componentId: remoteId, graphId: remoteGraph });
        if (status !== 'accepted' || cfg.handshake?.peer !== src || cfg.handshake?.remoteId !== remoteId) {
          updateHandshake(id, {
            status: 'accepted',
            peer: src,
            direction: 'accepted',
            remoteId,
            graphId: remoteGraph
          });
        }
        logLine(id, `Handshake accepted by ${src}${logSuffix}`);
        startHeartbeat(id);
        emitStatus(id, { type: 'handshake', status: 'accepted', peer: src });
      } else if (data.action === 'decline') {
        logLine(id, `Handshake declined by ${src}${logSuffix}`, true);
        updateHandshake(id, {
          status: 'declined',
          peer: src,
          direction: 'declined',
          remoteId,
          graphId: remoteGraph
        });
        stopHeartbeat(id);
        emitStatus(id, { type: 'handshake', status: 'declined', peer: src });
      } else if (data.action === 'sync') {
        if (status === 'accepted' && cfg.handshake?.peer === src) {
          sendJson(id, src, {
            type: 'nkndm.handshake',
            action: 'accept',
            ts: Date.now()
          }, remoteId || cfg.peer?.componentId || undefined);
          startHeartbeat(id);
        }
      }
    });
  }

  function handleHeartbeatMessage(src, data) {
    const candidates = findNodesForTarget(src, data, 'heartbeat');
    if (!candidates.length) return;
    debugLog('[nkndm] heartbeat-message:dispatch', { src, data, candidates: candidates.map((c) => c.id) });
    candidates.forEach((candidate) => {
      const { id } = candidate;
      let cfg = candidate.cfg;
      const st = getState(id);
      const remoteId = typeof data?.componentId === 'string' ? data.componentId : '';
      const remoteGraph = typeof data?.graphId === 'string' ? data.graphId : '';
      if (remoteId && cfg.peer?.componentId !== remoteId) {
        setPeerInfo(id, { address: src, componentId: remoteId, graphId: remoteGraph });
        updateHandshake(id, { remoteId, graphId: remoteGraph });
        cfg = NodeStore.ensure(id, 'NknDM').config || {};
      }
      st.lastSeen = Date.now();
      st.missedBeats = 0;
      if (!st.connected) {
        st.connected = true;
        setIndicator(id, 'online');
        logLine(id, `Connected to ${src}`);
        emitStatus(id, { type: 'connected', peer: src });
      } else {
        const intervalMs = Math.max(5, Number(cfg.heartbeatInterval) || 15) * 1000;
        applyHeartbeatIndicator(id, intervalMs);
      }
      if (data.action === 'ping') {
        sendJson(id, src, {
          type: 'nkndm.heartbeat',
          action: 'pong',
          ts: Date.now()
        }, remoteId || cfg.peer?.componentId || undefined);
      }
    });
  }

  function ensureInboxEntry(st, id, total, from) {
    if (!st.inbox.has(id)) {
      st.inbox.set(id, { total, parts: new Array(total), from });
    }
    const entry = st.inbox.get(id);
    entry.total = total;
    entry.from = from;
    if (entry.parts.length < total) entry.parts.length = total;
    return entry;
  }

  function startHandshakeLoop(nodeId) {
    const st = getState(nodeId);
    if (st.handshakeTimer) return;
    const cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
    const intervalSec = Math.max(5, Number(cfg.heartbeatInterval) || 15);
    const address = (cfg.address || '').trim();
    if (!address) return;
    st.handshakeTimer = setInterval(() => {
      const cur = NodeStore.ensure(nodeId, 'NknDM').config || {};
      if ((cur.handshake?.status || 'idle') !== 'pending' || cur.handshake?.direction !== 'outgoing') {
        stopHandshakeLoop(nodeId);
        return;
      }
      sendHandshake(nodeId, 'request');
    }, intervalSec * 1000);
  }

  function stopHandshakeLoop(nodeId) {
    const st = nodeState.get(nodeId);
    if (!st || !st.handshakeTimer) return;
    clearInterval(st.handshakeTimer);
    st.handshakeTimer = null;
  }

  function handleDataMessage(src, data) {
    const candidates = findNodesForTarget(src, data, 'data');
    if (!candidates.length) {
      notifyNoCandidate(src, data, 'data');
      return;
    }
    debugLog('[nkndm] data-message:dispatch', { src, data, candidates: candidates.map((c) => c.id) });
    candidates.forEach((candidate) => {
      const { id } = candidate;
      let cfg = candidate.cfg;
      const st = getState(id);
      const status = cfg.handshake?.status || 'idle';
      if (status !== 'accepted') {
        notifyStatus(
          id,
          `Dropped message from ${src}: handshake status is ${status}`,
          { type: 'error', code: 'handshake-not-accepted', peer: src, status },
          true
        );
        return;
      }
      const remoteId = typeof data?.componentId === 'string' ? data.componentId : '';
      const remoteGraph = typeof data?.graphId === 'string' ? data.graphId : '';
      if (remoteId && cfg.peer?.componentId !== remoteId) {
        setPeerInfo(id, { address: src, componentId: remoteId, graphId: remoteGraph });
        updateHandshake(id, { remoteId, graphId: remoteGraph });
        cfg = NodeStore.ensure(id, 'NknDM').config || {};
      }
      const normalized = normalizeInboundPayload(data);
      if (!normalized.text) {
        notifyStatus(
          id,
          `Dropped message from ${src}: payload missing text content`,
          { type: 'error', code: 'missing-text', peer: src },
          true
        );
        return;
      }
      if (data.id && data.total) {
        const entry = ensureInboxEntry(st, data.id, data.total, src);
        const index = Math.max(0, (data.seq || 1) - 1);
        entry.parts[index] = normalized.text;
        if (!entry.route && normalized.route) entry.route = normalized.route;
        const done = entry.parts.every((part) => typeof part === 'string');
        if (done) {
          const assembled = entry.parts.join('');
          st.inbox.delete(data.id);
          const finalNormalized = normalizeInboundPayload(data, { textOverride: assembled, routeOverride: entry.route });
          const preview = summarizeText(finalNormalized.text);
          const routeNote = finalNormalized.route ? ` on route ${finalNormalized.route}` : '';
          const detail = preview ? ` → ${preview}` : '';
          logLine(id, `Received message (${entry.total} parts) from ${src}${routeNote}${detail}`);
          emitIncomingPayload(id, src, data, finalNormalized, { id: data.id, total: entry.total });
          emitStatus(id, {
            type: 'received',
            id: data.id,
            peer: src,
            total: entry.total,
            route: finalNormalized.route || undefined
          });
        }
      } else {
        const preview = summarizeText(normalized.text);
        const routeNote = normalized.route ? ` on route ${normalized.route}` : '';
        const detail = preview ? ` → ${preview}` : '';
        logLine(id, `Received message from ${src}${routeNote}${detail}`);
        emitIncomingPayload(id, src, data, normalized);
        emitStatus(id, {
          type: 'received',
          peer: src,
          total: 1,
          route: normalized.route || undefined
        });
      }
    });
  }

  function chunkText(text, maxBytes) {
    const minLimit = 64;
    let chunkLimit = Math.max(minLimit, maxBytes - 200);
    while (chunkLimit >= minLimit) {
      const chunks = [];
      let current = '';
      let currentBytes = 0;
      for (const char of text) {
        const bytes = encoder.encode(char).length;
        if (current && currentBytes + bytes > chunkLimit) {
          chunks.push(current);
          current = char;
          currentBytes = bytes;
        } else {
          current += char;
          currentBytes += bytes;
        }
      }
      if (current) chunks.push(current);
      const total = Math.max(1, chunks.length);
      const fits = chunks.every((chunk, idx) => {
        const payload = {
          type: 'nkndm.data',
          id: 'test',
          seq: idx + 1,
          total,
          text: chunk
        };
        return encoder.encode(JSON.stringify(payload)).length <= maxBytes;
      });
      if (fits) return chunks;
      chunkLimit = Math.floor(chunkLimit * 0.8);
    }
    return [text];
  }

  function sendProbe(nodeId) {
    const cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
    const address = (cfg.address || cfg.peer?.address || cfg.handshake?.peer || '').trim();
    if (!address) {
      logLine(nodeId, 'Cannot ping: peer address missing', true);
      return;
    }
    const targetId = getTargetComponentId(cfg) || undefined;
    sendJson(nodeId, address, {
      type: 'nkndm.debug',
      action: 'ping',
      ts: Date.now()
    }, targetId);
    logLine(nodeId, `Debug ping sent to ${address}`);
  }

  function sendData(nodeId, text) {
    const cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
    const address = (cfg.address || '').trim();
    if (!address) {
      logLine(nodeId, 'Peer address missing', true);
      return;
    }
    if ((cfg.handshake?.status || 'idle') !== 'accepted') {
      logLine(nodeId, 'Handshake not accepted yet', true);
      return;
    }
    const client = getClient();
    if (!client || !Net.nkn.ready || !Net.nkn.addr) {
      logLine(nodeId, 'NKN client unavailable', true);
      scheduleRetry(nodeId);
      return;
    }
    const maxBytes = Math.max(512, Number(cfg.chunkBytes) || 1800);
    const chunks = chunkText(text, maxBytes);
    const total = chunks.length;
    const batchId = `dm-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const targetId = getTargetComponentId(cfg) || undefined;
    chunks.forEach((chunk, index) => {
      sendJson(nodeId, address, {
        type: 'nkndm.data',
        id: batchId,
        seq: index + 1,
        total,
        text: chunk,
        ts: Date.now()
      }, targetId);
    });
    logLine(nodeId, `Sent ${total} part${total > 1 ? 's' : ''} to ${address}`);
    emitStatus(nodeId, { type: 'sent', id: batchId, total, peer: address });
  }

  function sendHandshakeRequest(nodeId, onDone) {
    const cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
    const address = (cfg.address || '').trim();
    if (!address) {
      logLine(nodeId, 'Peer address missing', true);
      onDone && onDone();
      return;
    }
    if (CFG.transport !== 'nkn') {
      logLine(nodeId, 'Switch transport to NKN to connect', true);
      onDone && onDone();
      return;
    }
    const client = getClient();
    if (!client || !Net.nkn.ready || !Net.nkn.addr) {
      logLine(nodeId, 'NKN client unavailable', true);
      scheduleRetry(nodeId);
      return;
    }
    const st = getState(nodeId);
    st.spinnerDone = onDone || null;
    if ((cfg.handshake?.status || 'idle') !== 'pending' || cfg.handshake?.direction !== 'outgoing' || cfg.handshake?.peer !== address) {
      updateHandshake(nodeId, {
        status: 'pending',
        peer: address,
        direction: 'outgoing',
        remoteId: cfg.peer?.componentId || cfg.handshake?.remoteId || '',
        graphId: cfg.peer?.graphId || cfg.handshake?.graphId || ''
      });
    }
    sendHandshake(nodeId, 'request');
    startHandshakeLoop(nodeId);
  }

  function onText(nodeId, payload) {
    const text = extractPayloadText(payload);
    if (!text) return;
    sendData(nodeId, text);
  }

  function extractPayloadText(payload) {
    if (payload == null) return '';
    if (typeof payload === 'string') return payload;
    if (typeof payload === 'number') return String(payload);
    if (typeof payload === 'boolean') return payload ? 'true' : 'false';
    if (typeof payload !== 'object') return String(payload);
    if (payload.text != null) return String(payload.text);
    if (payload.value != null) return String(payload.value);
    if (payload.content != null) return String(payload.content);
    try {
      return JSON.stringify(payload);
    } catch (err) {
      return String(payload);
    }
  }

  function init(nodeId) {
    ensureListener();
    let cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
    const defaultsPatch = {};
    if (!cfg.handshake) {
      defaultsPatch.handshake = { status: 'idle', peer: '', direction: 'idle', remoteId: '', graphId: '' };
    } else if (cfg.handshake && cfg.handshake.direction === undefined) {
      const status = cfg.handshake.status || 'idle';
      let dir = 'idle';
      if (status === 'pending') dir = 'incoming';
      else if (status === 'accepted') dir = 'accepted';
      else if (status === 'declined') dir = 'declined';
      defaultsPatch.handshake = { ...cfg.handshake, direction: dir };
    }
    if (cfg.handshake && cfg.handshake.remoteId === undefined) {
      defaultsPatch.handshake = { ...(defaultsPatch.handshake || cfg.handshake), remoteId: '' };
    }
    if (cfg.handshake && cfg.handshake.graphId === undefined) {
      defaultsPatch.handshake = { ...(defaultsPatch.handshake || cfg.handshake), graphId: '' };
    }
    if (!Array.isArray(cfg.allowedPeers)) defaultsPatch.allowedPeers = [];
    if (typeof cfg.autoAccept !== 'boolean') defaultsPatch.autoAccept = false;
    if (!Number.isFinite(cfg.heartbeatInterval)) defaultsPatch.heartbeatInterval = 15;
    if (!cfg.componentId) defaultsPatch.componentId = `${CFG.graphId || 'graph'}:${nodeId}`;
    if (cfg.peer === undefined) defaultsPatch.peer = null;
    if (Object.keys(defaultsPatch).length) {
      cfg = NodeStore.update(nodeId, { type: 'NknDM', ...defaultsPatch });
    }
    registerComponent(nodeId);
    registerAddress(nodeId, (cfg.address || cfg.peer?.address || cfg.handshake?.peer || '')?.trim?.() || '');
    refresh(nodeId);
    if (cfg.address || cfg.peer?.address) {
      attemptHandshake(nodeId);
    }
  }

  return {
    init,
    refresh,
    onText,
    dispose,
    sendHandshake: sendHandshakeRequest,
    sendProbe,
    approvePeer(nodeId) {
      const cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
      const address = (cfg.address || '').trim();
      if (!address) {
        setBadge('No peer to approve', false);
        return;
      }
      const allowed = Array.isArray(cfg.allowedPeers) ? cfg.allowedPeers.slice() : [];
      if (!allowed.includes(address)) allowed.push(address);
      NodeStore.update(nodeId, { type: 'NknDM', allowedPeers: allowed });
      logLine(nodeId, `Peer ${address} trusted`);
      if (closeInviteModal) closeInviteModal();
      const current = NodeStore.ensure(nodeId, 'NknDM').config || {};
      if ((current.handshake?.status || 'idle') === 'pending' && current.handshake?.peer === address) {
        acceptHandshake(nodeId, address);
      } else {
        attemptHandshake(nodeId);
      }
      togglePeerActions(nodeId, false);
    },
    revokePeer(nodeId) {
      const cfg = NodeStore.ensure(nodeId, 'NknDM').config || {};
      const address = (cfg.address || '').trim();
      const allowed = Array.isArray(cfg.allowedPeers) ? cfg.allowedPeers.filter((p) => p !== address) : [];
      if (address) {
        const status = cfg.handshake?.status || 'idle';
        if (status === 'accepted' || status === 'pending') {
          declineHandshake(nodeId, address);
        }
      }
      NodeStore.update(nodeId, {
        type: 'NknDM',
        allowedPeers: allowed,
        peer: null,
        handshake: { status: 'idle', peer: '', direction: 'idle', remoteId: '', graphId: '' }
      });
      logLine(nodeId, `Peer ${address || '(none)'} removed from trust list`);
      stopHeartbeat(nodeId);
      refresh(nodeId);
      togglePeerActions(nodeId, false);
    }
  };
}

export { createNknDM };
