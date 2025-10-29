import { createDiscovery as createNatsDiscovery } from './nats.js';

const DEFAULT_SERVERS = ['wss://demo.nats.io:8443'];
const CONNECT_TIMEOUT_MS = 12000;
const MESSAGE_ACK_TIMEOUT_MS = 10000;
const EARTH_RADIUS_M = 6371000;

const toRadians = (deg) => (deg * Math.PI) / 180;

const haversineDistance = (aLat, aLon, bLat, bLon) => {
  if (![aLat, aLon, bLat, bLon].every(Number.isFinite)) return Number.POSITIVE_INFINITY;
  const dLat = toRadians(bLat - aLat);
  const dLon = toRadians(bLon - aLon);
  const lat1 = toRadians(aLat);
  const lat2 = toRadians(bLat);
  const sinLat = Math.sin(dLat / 2);
  const sinLon = Math.sin(dLon / 2);
  const aa = sinLat * sinLat + sinLon * sinLon * Math.cos(lat1) * Math.cos(lat2);
  const c = 2 * Math.atan2(Math.sqrt(aa), Math.sqrt(1 - aa));
  return EARTH_RADIUS_M * c;
};

const normalizeGeo = (geo) => {
  if (!geo || typeof geo !== 'object') return null;
  const lat = Number(geo.lat);
  const lon = Number(geo.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  const normalized = { lat, lon };
  if (Number.isFinite(geo.radius)) normalized.radius = Number(geo.radius);
  if (typeof geo.gh === 'string') normalized.gh = geo.gh;
  if (Number.isFinite(geo.prec)) normalized.prec = Number(geo.prec);
  if (Number.isFinite(geo.eye)) normalized.eye = Number(geo.eye);
  if (Number.isFinite(geo.ground)) normalized.ground = Number(geo.ground);
  if (Number.isFinite(geo.ts)) normalized.ts = Number(geo.ts);
  return normalized;
};

const ensureArray = (value) => (Array.isArray(value) ? value : []);

function createNoClipBridge({ NodeStore, Router, Net, CFG, setBadge, log }) {
  const NODE_STATE = new Map();
  const TARGET_INDEX = new Map();
  const NOCLIP_PEERS = new Map(); // Track discovered NoClip peers
  let syncAdapter = null;
  let discovery = null;
  let discoveryInit = null;
  let overrideRoom = null;

  const nowMs = () => Date.now();

  const sanitizePubKey = (value) => {
    if (!value) return '';
    const text = String(value).trim().toLowerCase();
    return /^[0-9a-f]{64}$/.test(text) ? text : '';
  };

  const sanitizeAddr = (value) => {
    if (!value) return '';
    const text = String(value).trim();
    if (!text) return '';
    if (/^[a-z0-9_-]+\.[0-9a-f]{64}$/i.test(text)) return text;
    // Use noclip. prefix for NoClip peers (changed from web.)
    if (/^[0-9a-f]{64}$/i.test(text)) return `noclip.${text.toLowerCase()}`;
    return '';
  };

  const sanitizeRoomName = (value) => {
    const raw = String(value || 'default');
    return raw.replace(/[^a-zA-Z0-9_.-]+/g, '_');
  };

  const deriveRoom = () => {
    const host = window.location?.host || 'local';
    const path = window.location?.pathname || '';
    return sanitizeRoomName(`${host}${path.replace(/\//g, '-')}`);
  };

  const consumePeerParam = () => {
    try {
      const url = new URL(window.location.href);
      const peerParam = url.searchParams.get('peer');

      if (!peerParam) return null;

      // Format: <prefix>.<hex64> or just <hex64>
      const parts = peerParam.split('.');
      let hex = '';

      if (parts.length === 2) {
        hex = parts[1];
      } else if (parts.length === 1) {
        hex = parts[0];
      } else {
        return null;
      }

      // Validate and sanitize
      const sanitized = sanitizePubKey(hex);
      if (!sanitized) return null;

      // Remove from URL
      url.searchParams.delete('peer');
      const newUrl = url.pathname + (url.searchParams.toString() ? `?${url.searchParams.toString()}` : '') + url.hash;

      try {
        window.history.replaceState({}, document.title, newUrl);
      } catch (err) {
        // ignore history failures
      }

      return sanitized;

    } catch (err) {
      return null;
    }
  };

  const updateNoclipBadge = () => {
    try {
      const badge = document.querySelector('#noclipPeerBadge');
      if (!badge) return;

      const count = NOCLIP_PEERS.size;
      if (count > 0) {
        badge.textContent = String(count);
        badge.classList.remove('hidden');
      } else {
        badge.classList.add('hidden');
      }
    } catch (err) {
      // Silently fail if badge element doesn't exist
    }
  };

  const ensureNodeState = (nodeId) => {
    const key = String(nodeId || '').trim();
    if (!key) return null;
    if (!NODE_STATE.has(key)) {
      const record = NodeStore.ensure(key, 'NoClipBridge') || { config: {} };
      const cfg = record.config || {};
      NODE_STATE.set(key, {
        nodeId: key,
        graphId: CFG?.graphId || '',
        cfg,
        targetPub: sanitizePubKey(cfg.targetPub),
        targetAddr: sanitizeAddr(cfg.targetAddr) || null,
        remotePeers: new Map(),
        sessions: new Map(),
        sessionIndex: new Map(),
        sessionId: '',
        lastHandshakeAt: 0,
        lastState: null,
        badgeLimiter: 0,
        pendingAcks: new Map()
      });
      registerTarget(key, sanitizePubKey(cfg.targetPub));
    }
    return NODE_STATE.get(key);
  };

  const unregisterTarget = (nodeId, pub) => {
    if (!pub) return;
    const set = TARGET_INDEX.get(pub);
    if (!set) return;
    set.delete(nodeId);
    if (!set.size) TARGET_INDEX.delete(pub);
  };

  const registerTarget = (nodeId, pub) => {
    const normalized = sanitizePubKey(pub);
    // Remove previous associations first
    for (const [key, set] of TARGET_INDEX.entries()) {
      if (set.has(nodeId)) {
        set.delete(nodeId);
        if (!set.size) TARGET_INDEX.delete(key);
      }
    }
    if (!normalized) return;
    if (!TARGET_INDEX.has(normalized)) TARGET_INDEX.set(normalized, new Set());
    TARGET_INDEX.get(normalized).add(nodeId);
  };

  const upsertRemotePeer = (state, pub, updates = {}) => {
    if (!state) return null;
    const normalized = sanitizePubKey(pub);
    if (!normalized) return null;
    let entry = state.remotePeers.get(normalized);
    if (!entry) {
      entry = {
        pub: normalized,
        addr: '',
        meta: {},
        geo: null,
        pose: null,
        state: null,
        lastTs: 0
      };
    }
    if (updates.addr) {
      const addr = sanitizeAddr(updates.addr);
      if (addr) entry.addr = addr;
    }
    if (updates.meta) {
      entry.meta = { ...(entry.meta || {}), ...(updates.meta || {}) };
    }
    if (updates.geo) {
      const geo = normalizeGeo(updates.geo);
      if (geo) entry.geo = geo;
    }
    if (updates.pose) {
      entry.pose = updates.pose;
    }
    if (updates.state) {
      entry.state = { ...(entry.state || {}), ...(updates.state || {}) };
    }
    if (updates.lastTs) {
      entry.lastTs = Number(updates.lastTs) || entry.lastTs;
    }
    state.remotePeers.set(normalized, entry);
    return entry;
  };

  const peersForOutput = (state) => {
    if (!state) return [];
    return Array.from(state.remotePeers.values()).map((entry) => ({
      nknPub: entry.pub,
      addr: entry.addr || `noclip.${entry.pub}`,
      meta: entry.meta || {},
      geo: entry.geo || null,
      pose: entry.pose || null,
      state: entry.state || null,
      last: entry.lastTs || 0
    }));
  };

  const selectTargets = (state, filters = {}) => {
    const list = peersForOutput(state);
    if (!list.length) return [];
    const requireLocation = !!filters.requireLocation;
    const geohashPrefix = typeof filters.geohashPrefix === 'string' ? filters.geohashPrefix.trim().toLowerCase() : '';
    const near = filters.near && typeof filters.near === 'object' ? filters.near : null;
    const radius = Number.isFinite(near?.radiusMeters) ? Number(near.radiusMeters) : null;
    const nearLat = Number(near?.lat);
    const nearLon = Number(near?.lon);

    let filtered = list.filter((peer) => {
      const geo = peer.geo;
      if (requireLocation && !geo) return false;
      if (geohashPrefix && (!geo?.gh || !geo.gh.toLowerCase().startsWith(geohashPrefix))) return false;
      if (radius && Number.isFinite(nearLat) && Number.isFinite(nearLon)) {
        if (!geo) return false;
        const distance = haversineDistance(nearLat, nearLon, geo.lat, geo.lon);
        if (!Number.isFinite(distance) || distance > radius) return false;
      }
      if (Array.isArray(filters.tags) && filters.tags.length) {
        const tagSet = new Set(
          (Array.isArray(peer.meta?.tags) ? peer.meta.tags : [])
            .map((tag) => String(tag || '').trim().toLowerCase())
            .filter(Boolean)
        );
        const missing = filters.tags.some((tag) => !tagSet.has(String(tag || '').trim().toLowerCase()));
        if (missing) return false;
      }
      return true;
    });

    if (Number.isFinite(filters.maxPeers) && filters.maxPeers > 0) {
      filtered = filtered.slice(0, Math.max(1, Math.floor(filters.maxPeers)));
    }

    return filtered.map((peer) => peer.nknPub).filter(Boolean);
  };

  const waitForNknClient = (timeoutMs = CONNECT_TIMEOUT_MS) =>
    new Promise((resolve, reject) => {
      const start = nowMs();
      const step = () => {
        if (Net.nkn?.client && Net.nkn.ready) {
          resolve(Net.nkn.client);
          return;
        }
        if (nowMs() - start >= timeoutMs) {
          reject(new Error('NKN unavailable'));
          return;
        }
        setTimeout(step, 250);
      };
      step();
    });

  async function ensureDiscovery() {
    if (discovery) return discovery;
    if (discoveryInit) return discoveryInit;
    discoveryInit = (async () => {
      try {
        Net.ensureNkn?.();
      } catch (err) {
        log?.(`[noclip] ensureNkn failed: ${err?.message || err}`);
      }
      let client = null;
      try {
        client = await waitForNknClient();
      } catch (_) {
        // best-effort: allow discovery with synthetic id
      }
      const pub = sanitizePubKey(client?.getPublicKey?.());
      const addr = client?.addr || Net.nkn?.addr || '';
      const room = overrideRoom || sanitizeRoomName(NodeStore?.defaultsByType?.NoClipBridge?.room || 'hybrid-bridge');
      const discoveryClient = await createNatsDiscovery({
        room,
        servers: DEFAULT_SERVERS,
        me: {
          nknPub: pub || `hydra-${Math.random().toString(36).slice(2, 10)}`,
          addr: addr || '',
          meta: { ids: ['hydra', 'graph'], kind: 'hydra' }
        }
      });
      discoveryClient.on('dm', handleDiscoveryDm);
      discoveryClient.on('peer', (peer) => {
        const normalized = sanitizePubKey(peer?.nknPub);
        if (!normalized) return;

        // Track NoClip peers (identified by network: 'noclip' in metadata)
        const addr = (peer.addr || peer.nknPub || '').toLowerCase();
        const isNoClip = peer.meta?.network === 'noclip' || addr.startsWith('noclip.');
        if (isNoClip) {
          NOCLIP_PEERS.set(normalized, {
            pub: normalized,
            addr: peer.addr || `noclip.${normalized}`,
            meta: peer.meta || {},
            last: peer.last || nowMs()
          });
          updateNoclipBadge();
        }

        const watchers = TARGET_INDEX.get(normalized);
        if (!watchers || !watchers.size) return;
        const updates = {
          addr: peer.addr || '',
          meta: peer.meta || {},
          lastTs: peer.last || nowMs()
        };
        for (const nodeId of watchers) {
          const st = ensureNodeState(nodeId);
          if (!st) continue;
          upsertRemotePeer(st, normalized, updates);
          Router.sendFrom(nodeId, 'peers', {
            nodeId,
            peers: peersForOutput(st)
          });
        }
      });
      discovery = discoveryClient;
      return discoveryClient;
    })().catch((err) => {
      discoveryInit = null;
      throw err;
    });
    return discoveryInit;
  }

  function handleDiscoveryDm(evt) {
    if (!evt) return;
    const msg = evt.msg || {};
    const from = sanitizePubKey(evt.from || msg.pub || msg.from);
    if (!from) return;
    const watchers = TARGET_INDEX.get(from);
    if (!watchers || !watchers.size) return;
    const type = msg.type;
    if (type === 'hybrid-bridge-state') {
      for (const nodeId of watchers) {
        const st = ensureNodeState(nodeId);
        if (!st) continue;
        if (Array.isArray(msg.peers)) {
          msg.peers.forEach((entry) => {
            const peerPub = entry?.nknPub || entry?.pub || entry?.addr;
            if (!peerPub) return;
            upsertRemotePeer(st, peerPub, {
              addr: entry.addr || '',
              meta: entry.meta || {},
              geo: entry.geo,
              pose: entry.pose,
              state: entry.state,
              lastTs: entry.last || msg.ts || nowMs()
            });
          });
          Router.sendFrom(nodeId, 'peers', {
            nodeId,
            peer: from,
            peers: peersForOutput(st)
          });
        }
        Router.sendFrom(nodeId, 'state', {
          nodeId,
          peer: from,
          state: msg.state || null,
          pose: msg.pose || null,
          ts: msg.ts || nowMs()
        });
        if (msg.pose) {
          Router.sendFrom(nodeId, 'pose', {
            nodeId,
            peer: from,
            pose: msg.pose,
            ts: msg.ts || nowMs()
          });
        }
      }
    } else if (type === 'hybrid-friend-response' || type === 'hybrid-bridge-log') {
      for (const nodeId of watchers) {
        Router.sendFrom(nodeId, 'events', {
          nodeId,
          peer: from,
          type,
          payload: msg
        });
      }
    } else if (type === 'hybrid-bridge-resource' || type === 'hybrid-bridge-command') {
      for (const nodeId of watchers) {
        Router.sendFrom(nodeId, 'events', {
          nodeId,
          peer: from,
          type,
          payload: msg
        });
      }
    } else if (type === 'hybrid-chat') {
      for (const nodeId of watchers) {
        Router.sendFrom(nodeId, 'chat', {
          nodeId,
          peer: from,
          message: msg
        });
      }
    } else if (type === 'hybrid-bridge-handshake') {
      // Handle handshake request from NoClip peer
      for (const nodeId of watchers) {
        const st = ensureNodeState(nodeId);
        if (!st) continue;
        st.lastHandshakeAt = nowMs();

        // Send handshake event to node
        Router.sendFrom(nodeId, 'handshake', {
          nodeId,
          peer: from,
          capabilities: msg.capabilities || [],
          clientType: msg.clientType || 'unknown',
          graphId: msg.graphId || '',
          ts: msg.ts || nowMs()
        });

        // Auto-respond with acknowledgment
        sendBridgePayload(nodeId, st, {
          type: 'hybrid-bridge-handshake-ack',
          capabilities: ['graph', 'resources', 'commands', 'data-export', 'audio'],
          graphId: CFG?.graphId || nodeId,
          nodeId,
          ts: nowMs()
        });
        if (syncAdapter && typeof syncAdapter.updateSessionStatus === 'function') {
          syncAdapter.updateSessionStatus({
            noclipPub: from,
            hydraBridgeNodeId: nodeId,
            status: 'connected',
            lastHandshakeAt: st.lastHandshakeAt
          });
        } else {
          updateSessionsByPub(nodeId, from, { status: 'connected', lastHandshakeAt: st.lastHandshakeAt });
        }
        refreshSessionStatus(nodeId);
      }
    } else if (type === 'smart-object-audio') {
      // Handle audio packets from NoClip Smart Objects for ASR
      for (const nodeId of watchers) {
        Router.sendFrom(nodeId, 'audioInput', {
          nodeId,
          peer: from,
          objectId: msg.objectId,
          audioPacket: msg.audioPacket || msg,
          ts: msg.ts || nowMs()
        });
      }
    } else if (type === 'hybrid-bridge-ack') {
      const messageId = msg.inReplyTo || msg.messageId;
      const status = (msg.status || 'ok').toLowerCase();
      const detail = msg.detail || msg.error || '';
      for (const nodeId of watchers) {
        const st = ensureNodeState(nodeId);
        if (!st) continue;
        resolvePendingAck(st, messageId, { status, detail });
      }
    }
  }

  const normalizeBoolean = (value, def = true) => {
    if (typeof value === 'boolean') return value;
    if (value == null) return def;
    const text = String(value).trim().toLowerCase();
    if (!text) return def;
    if (['true', '1', 'yes', 'on'].includes(text)) return true;
    if (['false', '0', 'no', 'off'].includes(text)) return false;
    return def;
  };

  const maybeBadge = (state, message, success = true) => {
    if (!setBadge || !message) return;
    const now = nowMs();
    if (now - (state.badgeLimiter || 0) < 1500) return;
    state.badgeLimiter = now;
    setBadge(message, success);
  };

  const ensureHandshake = async (nodeId, state) => {
    if (!state || !state.targetPub) return;
    try {
      const disco = await ensureDiscovery();
      if (!disco) return;
      const meta = { ids: ['hydra', 'bridge'], nodeId };
      try {
        await disco.handshake(state.targetPub, meta, { wantAck: true });
      } catch (err) {
        log?.(`[noclip] handshake error: ${err?.message || err}`);
      }
      state.lastHandshakeAt = nowMs();
    } catch (err) {
      log?.(`[noclip] ensureHandshake failed: ${err?.message || err}`);
    }
  };

  const normalizePose = (payload) => {
    if (!payload || typeof payload !== 'object') return null;
    const out = { };
    if (Array.isArray(payload.position) && payload.position.length >= 3) {
      out.position = payload.position.slice(0, 3).map((v) => Number.isFinite(v) ? Number(v) : 0);
    }
    if (Array.isArray(payload.rotation) && payload.rotation.length) {
      out.rotation = payload.rotation.slice(0, 4).map((v) => Number.isFinite(v) ? Number(v) : 0);
    } else if (Array.isArray(payload.quaternion) && payload.quaternion.length) {
      out.rotation = payload.quaternion.slice(0, 4).map((v) => Number.isFinite(v) ? Number(v) : 0);
    } else if (typeof payload.yaw === 'number' || typeof payload.pitch === 'number' || typeof payload.roll === 'number') {
      out.euler = {
        yaw: Number(payload.yaw) || 0,
        pitch: Number(payload.pitch) || 0,
        roll: Number(payload.roll) || 0
      };
    }
    if (payload.scale != null) {
      if (Array.isArray(payload.scale)) {
        out.scale = payload.scale.slice(0, 3).map((v) => Number.isFinite(v) ? Number(v) : 1);
      } else if (Number.isFinite(payload.scale)) {
        out.scale = [Number(payload.scale), Number(payload.scale), Number(payload.scale)];
      }
    }
    if (payload.meta && typeof payload.meta === 'object') {
      out.meta = payload.meta;
    }
    if (!out.position && !out.rotation && !out.euler && !out.scale && !out.meta) {
      return null;
    }
    return out;
  };

  async function sendBridgePayload(nodeId, state, payload, { targets } = {}) {
    const targetList = ensureArray(targets)
      .map((value) => {
        if (!value) return '';
        if (typeof value === 'string') {
          const text = value.trim();
          const stripped = text.replace(/^graph\./i, '');
          return sanitizePubKey(stripped);
        }
        const candidate = value?.pub || value?.nknPub || value?.addr || '';
        const stripped = String(candidate).replace(/^graph\./i, '');
        return sanitizePubKey(stripped);
      })
      .filter(Boolean);
    if (state?.targetPub && !targetList.length) targetList.push(state.targetPub);
    if (!targetList.length) {
      maybeBadge(state || {}, 'No eligible peers selected', false);
      return false;
    }
    let disco;
    try {
      disco = await ensureDiscovery();
    } catch (err) {
      log?.(`[noclip] discovery init failed: ${err?.message || err}`);
      maybeBadge(state, 'Discovery unavailable', false);
      return false;
    }
    if (!disco) return false;
    let success = false;
    for (const targetPub of targetList) {
      try {
        await disco.dm(targetPub, { ...payload, target: targetPub });
        success = true;
      } catch (err) {
        log?.(`[noclip] send failed to ${targetPub}: ${err?.message || err}`);
      }
    }
    if (!success) maybeBadge(state, 'Bridge send failed', false);
    return success;
  }

  const createMessageId = () => `msg-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

  const registerPendingAck = (state, messageId, info) => {
    if (!state || !messageId) return;
    if (!state.pendingAcks) state.pendingAcks = new Map();
    const timer = setTimeout(() => {
      state.pendingAcks.delete(messageId);
      if (info?.nodeId) {
        logToNode(info.nodeId, `⚠️ Awaiting ack for ${info.type || 'message'} (${messageId.slice(0, 8)})`, 'warn');
      }
    }, MESSAGE_ACK_TIMEOUT_MS);
    state.pendingAcks.set(messageId, { ...info, timer });
  };

  const resolvePendingAck = (state, messageId, ack = {}) => {
    if (!state?.pendingAcks || !state.pendingAcks.size || !messageId) return;
    const entry = state.pendingAcks.get(messageId);
    if (!entry) return;
    clearTimeout(entry.timer);
    state.pendingAcks.delete(messageId);
    const status = ack.status || 'ok';
    const detail = ack.detail || ack.error || '';
    if (entry.nodeId) {
      const prefix = status === 'ok' ? '✅' : '⚠️';
      const logType = status === 'ok' ? 'success' : 'error';
      const label = entry.type || 'message';
      const logMessage = `${prefix} ${label} ${status}${detail ? ` • ${detail}` : ''}`;
      logToNode(entry.nodeId, logMessage, logType);
    }
  };

  const sendTypedBridgeMessage = async (nodeId, state, message, { expectAck = true, targets = undefined } = {}) => {
    if (!state) return { ok: false, messageId: null };
    const messageId = message.messageId || createMessageId();
    const packet = {
      ...message,
      nodeId,
      messageId,
      ts: nowMs()
    };
    const ok = await sendBridgePayload(nodeId, state, packet, { targets });
    if (ok && expectAck && messageId) {
      registerPendingAck(state, messageId, {
        nodeId,
        type: packet.type,
        sessionId: packet.sessionId,
        objectUuid: packet.objectUuid
      });
    }
    return { ok, messageId };
  };

  /**
   * Log a message to the NoClipBridge node's UI log
   */
  function logToNode(nodeId, message, type = 'info') {
    const node = NodeStore?.get?.(nodeId);
    if (!node || !node.el) return;

    const logEl = node.el.querySelector('[data-noclip-log]');
    if (!logEl) return;

    const entry = document.createElement('div');
    entry.className = `log-entry log-${type}`;

    const timestamp = new Date().toLocaleTimeString();
    entry.innerHTML = `<span style="color:#666;">[${timestamp}]</span> ${message}`;

    logEl.appendChild(entry);

    // Auto-scroll to bottom
    logEl.scrollTop = logEl.scrollHeight;

    // Limit to 50 entries
    const entries = logEl.querySelectorAll('.log-entry');
    if (entries.length > 50) {
      entries[0].remove();
    }
  }

  /**
   * Get list of discovered NoClip peers
   */
  function getDiscoveredNoClipPeers() {
    return Array.from(NOCLIP_PEERS.values());
  }

  function listSessions(nodeId) {
    const st = NODE_STATE.get(nodeId);
    if (!st?.sessions?.size) return [];
    return Array.from(st.sessions.values()).map((session) => ({ ...session }));
  }

  /**
   * Refresh the peer dropdown for a node
   */
  function refreshPeerDropdown(nodeId) {
    const node = NodeStore?.get?.(nodeId);
    if (!node || !node.el) return;

    const selectEl = node.el.querySelector('[data-noclip-peer-select]');
    if (!selectEl) return;

    const currentValue = selectEl.value;
    const peers = getDiscoveredNoClipPeers();

    // Clear and rebuild options
    selectEl.innerHTML = '<option value="">-- Select NoClip Peer --</option>';

    peers.forEach(peer => {
      const option = document.createElement('option');
      option.value = peer.nknPub;
      const label = peer.meta?.username || `NoClip ${peer.nknPub.slice(0, 8)}...`;
      option.textContent = `${label} (noclip.${peer.nknPub.slice(0, 8)})`;
      selectEl.appendChild(option);
    });

    // Restore selection if it still exists
    if (currentValue && Array.from(selectEl.options).some(opt => opt.value === currentValue)) {
      selectEl.value = currentValue;
    }

    logToNode(nodeId, `Found ${peers.length} NoClip peer(s)`, 'info');
    refreshSessionStatus(nodeId);
  }

  const refreshSessionStatus = (nodeId) => {
    const node = NodeStore?.get?.(nodeId);
    if (!node?.el) return;
    const statusEl = node.el.querySelector('[data-noclip-session-status]');
    const listEl = node.el.querySelector('[data-noclip-session-list]');
    if (!statusEl) return;
    const st = NODE_STATE.get(nodeId);
    if (!st || !st.sessions || !st.sessions.size) {
      statusEl.textContent = 'No active sessions';
      statusEl.style.color = 'var(--muted)';
      if (listEl) {
        listEl.innerHTML = '<div data-empty>Sessions will appear here after approval.</div>';
      }
      return;
    }
    const sessions = Array.from(st.sessions.values());
    const listFragments = document.createDocumentFragment();
    const summaries = sessions.map((session) => {
      const label = session.objectLabel || session.objectUuid || session.sessionId;
      const status = (session.status || 'pending').replace(/[_-]/g, ' ');
      const coords = session.position && Number.isFinite(session.position.lat) && Number.isFinite(session.position.lon)
        ? ` @ ${session.position.lat.toFixed(4)}, ${session.position.lon.toFixed(4)}`
        : '';
      return `${label}: ${status}${coords}`;
    });
    statusEl.textContent = summaries.join(' • ');
    statusEl.style.color = 'var(--accent)';
    if (listEl) {
      listEl.innerHTML = '';
      const formatDelta = (ts) => {
        if (!Number.isFinite(ts)) return '';
        const diff = Math.max(0, nowMs() - ts);
        const sec = Math.floor(diff / 1000);
        if (sec < 60) return `${sec}s ago`;
        const min = Math.floor(sec / 60);
        if (min < 60) return `${min}m ago`;
        const hr = Math.floor(min / 60);
        if (hr < 24) return `${hr}h ago`;
        const day = Math.floor(hr / 24);
        return `${day}d ago`;
      };
      sessions.forEach((session) => {
        const row = document.createElement('div');
        row.className = 'noclip-session-row';
        row.style.marginBottom = '6px';
        const status = (session.status || 'pending').replace(/[_-]/g, ' ');
        const label = session.objectLabel || session.objectUuid || session.sessionId;
        const geoParts = [];
        if (session.position && Number.isFinite(session.position.lat) && Number.isFinite(session.position.lon)) {
          geoParts.push(`${session.position.lat.toFixed(4)}, ${session.position.lon.toFixed(4)}`);
        } else if (session.geo && Number.isFinite(session.geo.lat) && Number.isFinite(session.geo.lon)) {
          geoParts.push(`${session.geo.lat.toFixed(4)}, ${session.geo.lon.toFixed(4)}`);
        }
        if (Number.isFinite(session.position?.alt)) geoParts.push(`${Number(session.position.alt).toFixed(1)}m`);
        const geoText = geoParts.length ? ` • ${geoParts.join(' • ')}` : '';
        const bridge = session.hydraBridgeNodeId ? ` • bridge ${session.hydraBridgeNodeId}` : '';
        const last = formatDelta(session.updatedAt);
        row.innerHTML = `<strong>${label}</strong> • ${status}${geoText}${bridge}${last ? ` • ${last}` : ''}`;
        listFragments.appendChild(row);
      });
      listEl.appendChild(listFragments);
    }
  };

  const storeSessionForNode = (nodeId, session) => {
    if (!session) return null;
    const st = ensureNodeState(nodeId);
    if (!st) return null;
    if (!st.sessions) st.sessions = new Map();
    if (!st.sessionIndex) st.sessionIndex = new Map();
    const normalizedPub = sanitizePubKey(session.noclipPub || session.noclipAddr || '');
    const sessionId = String(session.sessionId || `${normalizedPub || 'session'}-${nowMs().toString(36)}`);
    const record = {
      ...st.sessions.get(sessionId),
      ...session,
      sessionId,
      noclipPub: normalizedPub,
      updatedAt: nowMs()
    };
    if (session.position && typeof session.position === 'object') {
      record.position = { ...session.position };
    }
    if (session.geo && typeof session.geo === 'object') {
      record.geo = { ...session.geo };
    }
    st.sessions.set(sessionId, record);
    if (normalizedPub) {
      if (!st.sessionIndex.has(normalizedPub)) st.sessionIndex.set(normalizedPub, new Set());
      st.sessionIndex.get(normalizedPub).add(sessionId);
    }
    refreshSessionStatus(nodeId);
    return record;
  };

  const updateSessionsByPub = (nodeId, pub, updates = {}) => {
    const st = NODE_STATE.get(nodeId);
    if (!st?.sessions?.size) return [];
    const normalized = sanitizePubKey(pub);
    const list = [];
    for (const [sessionId, existing] of st.sessions.entries()) {
      if (normalized && existing.noclipPub !== normalized) continue;
      const merged = {
        ...existing,
        ...updates,
        updatedAt: nowMs()
      };
      if (updates.position && typeof updates.position === 'object') {
        merged.position = { ...(existing.position || {}), ...updates.position };
      }
      if (updates.geo && typeof updates.geo === 'object') {
        merged.geo = { ...(existing.geo || {}), ...updates.geo };
      }
      st.sessions.set(sessionId, merged);
      list.push(merged);
    }
    if (list.length) refreshSessionStatus(nodeId);
    return list;
  };

  const handleSessionUpdate = (session) => {
    if (!session) return;
    const nodeId = session.hydraBridgeNodeId || session.bridgeNodeId || session.nodeId;
    if (!nodeId) return;
    storeSessionForNode(nodeId, session);
  }

  return {
    init(nodeId) {
      const st = ensureNodeState(nodeId);
      if (!st) return;
      if (normalizeBoolean(st.cfg?.autoConnect, true)) {
        ensureHandshake(nodeId, st);
      }

      // Log init
      logToNode(nodeId, 'NoClipBridge initialized', 'info');
      refreshSessionStatus(nodeId);
    },

    refresh(nodeId) {
      const st = ensureNodeState(nodeId);
      if (!st) return;
      const record = NodeStore.ensure(nodeId, 'NoClipBridge');
      const cfg = record?.config || {};
      st.cfg = cfg;
      st.graphId = CFG?.graphId || st.graphId;
      overrideRoom = sanitizeRoomName(cfg.room || overrideRoom || 'hybrid-bridge');

      // Check for ?peer= URL parameter and use it if no targetPub configured
      let targetPub = sanitizePubKey(cfg.targetPub);
      if (!targetPub) {
        const urlPeer = consumePeerParam();
        if (urlPeer) {
          targetPub = urlPeer;
          // Update config with URL peer
          if (record && record.config) {
            record.config.targetPub = targetPub;
          }
          log?.(`[noclipBridge] Using peer from URL: ${targetPub.slice(0, 8)}...`);
        }
      }

      st.targetPub = targetPub;
      st.targetAddr = sanitizeAddr(cfg.targetAddr) || null;
      st.remotePeers.clear();
      registerTarget(nodeId, st.targetPub);
      if (normalizeBoolean(cfg.autoConnect, true) && st.targetPub) {
        ensureHandshake(nodeId, st);
      }
      refreshSessionStatus(nodeId);
    },

    async onPose(nodeId, payload) {
      const st = ensureNodeState(nodeId);
      if (!st) return;
      const pose = normalizePose(payload);
      if (!pose) return;
      const packet = {
        type: 'hybrid-bridge-update',
        nodeId,
        pose,
        ts: nowMs()
      };
      const ok = await sendBridgePayload(nodeId, st, packet);
      if (ok) maybeBadge(st, 'Sent pose update');
    },

    async onResource(nodeId, payload) {
      const st = ensureNodeState(nodeId);
      if (!st || !payload) return;
      let request = payload;
      if (typeof payload === 'string') request = { resource: { type: 'text', value: payload } };
      if (Array.isArray(payload)) request = { resource: { items: payload } };
      if (!request || typeof request !== 'object') return;

      let resource = request.resource;
      if (!resource || typeof resource !== 'object') {
        resource = { type: 'object', value: request };
      }

      if (!resource.id) {
        resource.id = `res-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      }

      if (!resource.label) {
        const graphLabel = st.graphId || CFG?.graphId || '';
        resource.label = graphLabel ? `${graphLabel}` : `node:${nodeId}`;
      }

      const filters = request.filters || resource.filters || null;
      const targetSeeds = ensureArray(request.targets || resource.targets);
      let targets = targetSeeds
        .map((target) => sanitizePubKey(typeof target === 'string' ? target : target?.pub || target?.nknPub))
        .filter(Boolean);
      if (!targets.length && filters) {
        targets = selectTargets(st, filters);
      }

      const packet = {
        type: 'hybrid-bridge-resource',
        nodeId,
        issuer: {
          graphId: st.graphId || CFG?.graphId || '',
          nodeId,
          address: st.cfg?.address || st.cfg?.targetAddr || ''
        },
        resource,
        filters: filters || null,
        ts: nowMs()
      };

      const ok = await sendBridgePayload(nodeId, st, packet, { targets });
      Router.sendFrom(nodeId, 'events', {
        nodeId,
        peer: targets,
        type: 'resource-dispatched',
        payload: {
          resource,
          filters,
          success: ok,
          targets
        }
      });
      if (ok) maybeBadge(st, `Resource sent to ${targets?.length || 1} peer${targets?.length === 1 ? '' : 's'}`);

      // Log resource data
      const resType = resource.type || 'unknown';
      const resLabel = resource.label || 'unlabeled';
      logToNode(nodeId, `📤 Resource out: ${resType} "${resLabel}" → ${targets?.length || 0} peer(s)`, 'success');
    },

    async onCommand(nodeId, payload) {
      const st = ensureNodeState(nodeId);
      if (!st) return;
      if (!payload || typeof payload !== 'object') return;
      const packet = {
        type: 'hybrid-bridge-command',
        nodeId,
        command: payload,
        ts: nowMs()
      };
      await sendBridgePayload(nodeId, st, packet);
    },

    async onAudioOutput(nodeId, payload) {
      // Handle audio packets from TTS node to send to NoClip Smart Objects
      const st = ensureNodeState(nodeId);
      if (!st || !payload) return;

      const packet = {
        type: 'smart-object-audio-output',
        nodeId,
        audioPacket: payload,
        ts: nowMs()
      };

      await sendBridgePayload(nodeId, st, packet);

      // Log audio data
      const dataSize = payload.data?.length || 0;
      logToNode(nodeId, `📤 Audio out: ${dataSize} samples @ ${payload.sampleRate || 'unknown'}Hz`, 'success');
    },

    async onAudioInput(nodeId, payload) {
      // This is called when audio comes from NoClip - route it to connected nodes
      // The actual routing happens in handleDiscoveryDm via 'smart-object-audio' type
      // This handler could be used for outgoing audio requests if needed
      const st = ensureNodeState(nodeId);
      if (!st || !payload) return;

      // For now, just log reception - actual routing happens in handleDiscoveryDm
      log?.(`[noclip-bridge] Received audio input for node ${nodeId}`);
    },

    requestFriend(nodeId, note) {
      const st = ensureNodeState(nodeId);
      if (!st) return;
      const packet = {
        type: 'hybrid-friend-request',
        nodeId,
        note: typeof note === 'string' ? note.slice(0, 240) : '',
        ts: nowMs()
      };
      sendBridgePayload(nodeId, st, packet);
    },

    dispose(nodeId) {
      const key = String(nodeId || '').trim();
      if (!key) return;
      const st = NODE_STATE.get(key);
      if (st?.targetPub) unregisterTarget(key, st.targetPub);
      st?.sessions?.clear?.();
      st?.sessionIndex?.clear?.();
      if (st?.pendingAcks) {
        st.pendingAcks.forEach((entry) => clearTimeout(entry.timer));
        st.pendingAcks.clear();
      }
      NODE_STATE.delete(key);
    },

    registerSyncAdapter(adapter) {
      syncAdapter = adapter;
    },

    onSessionUpdate(session) {
      handleSessionUpdate(session);
    },

    async sendSmartObjectState(nodeId, data = {}) {
      const st = ensureNodeState(nodeId);
      if (!st) return { ok: false, messageId: null };
      const packet = {
        type: 'smart-object-state',
        sessionId: data.sessionId || data.session?.sessionId || '',
        objectUuid: data.objectUuid || data.objectId || '',
        objectId: data.objectUuid || data.objectId || '',
        position: data.position || null,
        state: data.state || null,
        capabilities: data.capabilities || null,
        metadata: data.metadata || null
      };
      return sendTypedBridgeMessage(nodeId, st, packet, {
        expectAck: data.expectAck !== false,
        targets: data.targets
      });
    },

    async sendDecisionResult(nodeId, data = {}) {
      const st = ensureNodeState(nodeId);
      if (!st) return { ok: false, messageId: null };
      const packet = {
        type: 'decision-result',
        sessionId: data.sessionId || data.session?.sessionId || '',
        objectUuid: data.objectUuid || data.objectId || '',
        objectId: data.objectUuid || data.objectId || '',
        result: data.result || null,
        decision: data.decision || null,
        summary: data.summary || null,
        metadata: data.metadata || null
      };
      return sendTypedBridgeMessage(nodeId, st, packet, {
        expectAck: data.expectAck !== false,
        targets: data.targets
      });
    },

    async sendGraphQuery(nodeId, data = {}) {
      const st = ensureNodeState(nodeId);
      if (!st) return { ok: false, messageId: null };
      const packet = {
        type: 'graph-query',
        sessionId: data.sessionId || '',
        objectUuid: data.objectUuid || '',
        queryId: data.queryId || data.query?.id || null,
        query: data.query || null,
        variables: data.variables || null,
        metadata: data.metadata || null
      };
      return sendTypedBridgeMessage(nodeId, st, packet, {
        expectAck: data.expectAck !== false,
        targets: data.targets
      });
    },

    async sendGraphResponse(nodeId, data = {}) {
      const st = ensureNodeState(nodeId);
      if (!st) return { ok: false, messageId: null };
      const packet = {
        type: 'graph-response',
        sessionId: data.sessionId || '',
        objectUuid: data.objectUuid || '',
        queryId: data.queryId || '',
        result: data.result || null,
        errors: data.errors || null,
        metadata: data.metadata || null
      };
      return sendTypedBridgeMessage(nodeId, st, packet, {
        expectAck: data.expectAck === true,
        targets: data.targets
      });
    },

    // UI helper methods
    logToNode,
    refreshPeerDropdown,
    getDiscoveredNoClipPeers,
    listSessions
  };
}

export { createNoClipBridge };
