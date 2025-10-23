import { createDiscovery as createNatsDiscovery } from './nats.js';

const DEFAULT_SERVERS = ['wss://demo.nats.io:8443'];
const CONNECT_TIMEOUT_MS = 12000;

function createNoClipBridge({ NodeStore, Router, Net, setBadge, log }) {
  const NODE_STATE = new Map();
  const TARGET_INDEX = new Map();
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
    if (/^[0-9a-f]{64}$/i.test(text)) return `web.${text.toLowerCase()}`;
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

  const ensureNodeState = (nodeId) => {
    const key = String(nodeId || '').trim();
    if (!key) return null;
    if (!NODE_STATE.has(key)) {
      const record = NodeStore.ensure(key, 'NoClipBridge') || { config: {} };
      const cfg = record.config || {};
      NODE_STATE.set(key, {
        cfg,
        targetPub: sanitizePubKey(cfg.targetPub),
        targetAddr: sanitizeAddr(cfg.targetAddr) || null,
        remotePeers: new Map(),
        sessionId: '',
        lastHandshakeAt: 0,
        lastState: null,
        badgeLimiter: 0
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
        // fan peer lists to nodes watching this pub
        const normalized = sanitizePubKey(peer?.nknPub);
        if (!normalized) return;
        const watchers = TARGET_INDEX.get(normalized);
        if (!watchers || !watchers.size) return;
        for (const nodeId of watchers) {
          const st = ensureNodeState(nodeId);
          if (!st) continue;
          st.remotePeers.set(normalized, {
            addr: peer.addr || '',
            meta: peer.meta || {},
            last: peer.last || 0
          });
          Router.sendFrom(nodeId, 'peers', {
            nodeId,
            peers: Array.from(st.remotePeers.entries()).map(([key, entry]) => ({
              nknPub: key,
              addr: entry.addr || '',
              last: entry.last || 0,
              meta: entry.meta || {}
            }))
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
        if (msg.peers) {
          Router.sendFrom(nodeId, 'peers', {
            nodeId,
            peer: from,
            peers: msg.peers
          });
        }
      }
    } else if (type === 'hybrid-friend-response') {
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

  async function sendBridgePayload(nodeId, state, payload) {
    if (!state?.targetPub) {
      maybeBadge(state || {}, 'No target peer configured', false);
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
    try {
      await disco.dm(state.targetPub, payload);
      return true;
    } catch (err) {
      log?.(`[noclip] send failed: ${err?.message || err}`);
      maybeBadge(state, 'Bridge send failed', false);
      return false;
    }
  }

  return {
    init(nodeId) {
      const st = ensureNodeState(nodeId);
      if (!st) return;
      if (normalizeBoolean(st.cfg?.autoConnect, true)) {
        ensureHandshake(nodeId, st);
      }
    },

    refresh(nodeId) {
      const st = ensureNodeState(nodeId);
      if (!st) return;
      const record = NodeStore.ensure(nodeId, 'NoClipBridge');
      const cfg = record?.config || {};
      st.cfg = cfg;
      overrideRoom = sanitizeRoomName(cfg.room || overrideRoom || 'hybrid-bridge');
      st.targetPub = sanitizePubKey(cfg.targetPub);
      st.targetAddr = sanitizeAddr(cfg.targetAddr) || null;
      st.remotePeers.clear();
      registerTarget(nodeId, st.targetPub);
      if (normalizeBoolean(cfg.autoConnect, true) && st.targetPub) {
        ensureHandshake(nodeId, st);
      }
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
      let resource = null;
      if (typeof payload === 'string') {
        resource = { type: 'text', value: payload };
      } else if (payload && typeof payload === 'object') {
        resource = payload;
      }
      if (!resource) return;
      const packet = {
        type: 'hybrid-bridge-resource',
        nodeId,
        resource,
        ts: nowMs()
      };
      await sendBridgePayload(nodeId, st, packet);
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
      NODE_STATE.delete(key);
    }
  };
}

export { createNoClipBridge };
