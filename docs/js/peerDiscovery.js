import { qs, LS } from './utils.js';
import { createDiscovery as createNoClipDiscovery } from './nats.js';

const NATS_MODULE_URL = 'https://cdn.jsdelivr.net/npm/nats.ws/+esm';
const DEFAULT_SERVERS = ['wss://demo.nats.io:8443'];
const DEFAULT_HEARTBEAT_SEC = 12;
const OFFLINE_AFTER_SECONDS = 150; // allow longer grace to reduce list flapping
const PING_TIMEOUT_MS = 8000;
const USERNAME_KEY = 'hydra.peer.username';

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const nowSeconds = () => Math.floor(Date.now() / 1000);

const normalizeKey = (value) => {
  if (value == null) return '';
  let text = typeof value === 'string' ? value : String(value);
  try {
    text = text.normalize('NFKC');
  } catch (_) {
    // ignore
  }
  return text.trim().toLowerCase();
};

const stripGraphPrefix = (value) => {
  if (!value) return '';
  return String(value).replace(/^graph\./i, '');
};

const normalizePubKey = (value) => normalizeKey(stripGraphPrefix(value));

const sanitizeUsername = (value) => {
  if (typeof value !== 'string') return '';
  let text = value.normalize?.('NFKC') ?? value;
  text = text.replace(/[^\w\s.\-]/g, '');
  text = text.trim();
  if (text.length > 48) text = text.slice(0, 48);
  return text;
};

class Store {
  constructor(room) {
    this.key = `hydra.peerstore.${room}`;
    this.memo = new Map();
    this.disabled = typeof localStorage === 'undefined';
    if (this.disabled) return;
    try {
      const raw = localStorage.getItem(this.key);
      if (raw) {
        const arr = JSON.parse(raw);
        if (Array.isArray(arr)) {
          arr.forEach((entry) => {
            if (entry?.nknPub) this.memo.set(entry.nknPub, entry);
          });
        }
      }
    } catch (_) {
      // ignore storage failures
    }
  }

  all() {
    return Array.from(this.memo.values());
  }

  upsert(peer) {
    if (!peer?.nknPub) return null;
    const key = normalizePubKey(peer.nknPub);
    if (!key) return null;
    const prev = this.memo.get(key) || {};
    const merged = {
      ...prev,
      ...peer,
      nknPub: key,
      last: typeof peer.last === 'number' ? peer.last : nowSeconds()
    };
    this.memo.set(key, merged);
    this.flush();
    return merged;
  }

  flush() {
    if (this.disabled) return;
    try {
      localStorage.setItem(this.key, JSON.stringify(this.all()));
    } catch (_) {
      // ignore quota errors
    }
  }

  remove(pub) {
    if (!pub || !this.memo.size) return;
    const targetKey = normalizePubKey(pub);
    if (!targetKey) return;
    const toDelete = [];
    this.memo.forEach((value, key) => {
      const keyNorm = normalizePubKey(key);
      const pubNorm = normalizePubKey(value?.nknPub);
      const addrNorm = normalizePubKey(value?.addr);
      if (keyNorm === targetKey || pubNorm === targetKey || addrNorm === targetKey) {
        toDelete.push(key);
      }
    });
    if (!toDelete.length) return;
    toDelete.forEach((key) => this.memo.delete(key));
    this.flush();
  }
}

class EventHub {
  constructor() {
    this.handlers = new Map();
  }

  on(evt, fn) {
    if (!this.handlers.has(evt)) this.handlers.set(evt, new Set());
    this.handlers.get(evt).add(fn);
    return () => this.off(evt, fn);
  }

  off(evt, fn) {
    this.handlers.get(evt)?.delete(fn);
  }

  emit(evt, data) {
    const list = this.handlers.get(evt);
    if (!list || !list.size) return;
    list.forEach((fn) => {
      try {
        fn(data);
      } catch (_) {
        // ignore handler failures
      }
    });
  }
}

let natsPromise = null;
async function loadNats() {
  if (!natsPromise) {
    natsPromise = import(NATS_MODULE_URL);
  }
  return natsPromise;
}

function sanitizeRoomName(value) {
  const raw = String(value || 'default');
  return raw.replace(/[^a-zA-Z0-9_.-]+/g, '_');
}

class DiscoveryClient extends EventHub {
  constructor({ servers, room, me, heartbeatSec, store }) {
    super();
    this.servers = Array.isArray(servers) && servers.length ? servers : DEFAULT_SERVERS;
    this.room = sanitizeRoomName(room);
    this.me = { ...(me || {}) };
    this.heartbeatSec = typeof heartbeatSec === 'number' && heartbeatSec > 0 ? heartbeatSec : DEFAULT_HEARTBEAT_SEC;

    const providedStore =
      store && typeof store.all === 'function' && typeof store.upsert === 'function' ? store : null;
    this.store = providedStore || new Store(this.room);
    this.nc = null;
    this.sc = null;
    this._hbTimer = null;

    // Use unified 'discovery.' prefix to be compatible with NoClip
    this.presenceSubject = `discovery.${this.room}.presence`;
    this.dmSubject = (pub) => `discovery.${this.room}.dm.${pub}`;
  }

  get peers() {
    return this.store
      .all()
      .filter((p) => p.nknPub && p.nknPub !== this.me.nknPub)
      .sort((a, b) => (b.last || 0) - (a.last || 0));
  }

  async connect() {
    if (this.nc) return this;
    const { connect, StringCodec, Events } = await loadNats();
    const nameSuffix = (this.me?.nknPub || '').slice(-6) || Math.random().toString(36).slice(-4);
    const nc = await connect({
      servers: this.servers,
      name: `hydra-${nameSuffix}`
    });
    this.nc = nc;
    this.sc = StringCodec();

    const presenceSub = await nc.subscribe(this.presenceSubject);
    (async () => {
      try {
        for await (const msg of presenceSub) {
          const payload = this._decode(msg);
          if (!payload || payload.type !== 'presence') continue;
          if (payload.pub === this.me.nknPub) continue;
          const peer = this.store.upsert({
            nknPub: payload.pub,
            addr: payload.addr || payload.pub,
            meta: payload.meta || {},
            last: payload.ts || nowSeconds()
          });
          this.emit('peer', peer);
        }
      } catch (err) {
        this.emit('status', { type: 'presence-error', err });
      }
    })();

    const dmSub = await nc.subscribe(this.dmSubject(this.me.nknPub));
    (async () => {
      try {
        for await (const msg of dmSub) {
          const payload = this._decode(msg);
          if (!payload?.type) continue;
          if (payload.type === 'peer-ping') {
            const peer = this.store.upsert({
              nknPub: payload.pub,
              addr: payload.addr || payload.pub,
              meta: payload.meta || {},
              last: payload.ts || nowSeconds()
            });
            this.emit('peer', peer);
            try {
              await this.dm(payload.pub, {
                type: 'peer-pong',
                pub: this.me?.nknPub,
                addr: this.me?.addr || this.me?.nknPub,
                meta: this.me?.meta || {},
                ts: nowSeconds()
              });
            } catch (_) {
              // ignore ping reply errors
            }
            continue;
          }
          if (payload.type === 'peer-pong') {
            const peer = this.store.upsert({
              nknPub: payload.pub,
              addr: payload.addr || payload.pub,
              meta: payload.meta || {},
              last: payload.ts || nowSeconds()
            });
            this.emit('peer', peer);
            continue;
          }
          if (payload.type === 'peer-meta') {
            const peer = this.store.upsert({
              nknPub: payload.pub,
              addr: payload.addr || payload.pub,
              meta: payload.meta || {},
              last: payload.ts || nowSeconds()
            });
            this.emit('peer', peer);
          } else {
            this.emit('dm', payload);
          }
        }
      } catch (err) {
        this.emit('status', { type: 'dm-error', err });
      }
    })();

    (async () => {
      try {
        for await (const s of nc.status()) {
          if (s.type === Events.Disconnect) {
            this.emit('status', { type: 'disconnect', data: s.data });
          } else if (s.type === Events.Reconnect) {
            this.emit('status', { type: 'reconnect', data: s.data });
          } else if (s.type === Events.Update) {
            this.emit('status', { type: 'update', data: s.data });
          } else {
            this.emit('status', { type: s.type, data: s.data });
          }
        }
      } catch (err) {
        this.emit('status', { type: 'status-error', err });
      }
    })();

    return this;
  }

  async presence(extraMeta = {}) {
    if (!this.nc || !this.sc) throw new Error('not connected');
    const payload = {
      type: 'presence',
      pub: this.me?.nknPub,
      addr: this.me?.addr || this.me?.nknPub,
      meta: { ...(this.me?.meta || {}), ...extraMeta },
      ts: nowSeconds()
    };
    this.nc.publish(this.presenceSubject, this._encode(payload));
  }

  async startHeartbeat(meta = {}) {
    await this.presence(meta);
    if (this._hbTimer) return;
    this._hbTimer = setInterval(() => {
      this.presence(meta).catch(() => {});
    }, this.heartbeatSec * 1000);
  }

  stopHeartbeat() {
    if (this._hbTimer) {
      clearInterval(this._hbTimer);
      this._hbTimer = null;
    }
  }

  async close() {
    this.stopHeartbeat();
    if (!this.nc) return;
    const done = this.nc.closed();
    try {
      await this.nc.drain();
    } catch (err) {
      try {
        this.nc.close();
      } catch (_) {
        // ignore
      }
    }
    await done.catch(() => {});
    this.nc = null;
  }

  dm(pub, payload) {
    if (!this.nc || !this.sc) throw new Error('not connected');
    if (!pub) return;
    const subj = this.dmSubject(pub);
    const msg = { ...(payload || {}), pub: this.me?.nknPub, ts: nowSeconds() };
    this.nc.publish(subj, this._encode(msg));
  }

  _encode(obj) {
    try {
      return this.sc.encode(JSON.stringify(obj));
    } catch (err) {
      return this.sc.encode('{}');
    }
  }

  _decode(msg) {
    try {
      return JSON.parse(this.sc.decode(msg.data));
    } catch (err) {
      return null;
    }
  }
}

function createPeerDiscovery({ Net, CFG, WorkspaceSync, setBadge, log, NoClip }) {
  // Use 'nexus' as the shared discovery room for cross-application peer discovery
  // This enables Hydra (hydra.nexus) and NoClip (noclip.nexus) to discover each other
  const derivedRoom = 'nexus';
  const sharedStore = new Store(derivedRoom);
  const noclipPeerListeners = new Set();
  const notifyNoclipListeners = () => {
    const snapshot = Array.from(state.noclip.peers.values());
    noclipPeerListeners.forEach((fn) => {
      try {
        fn(snapshot);
      } catch (err) {
        console.warn('[PeerDiscovery] noclip listener error:', err);
      }
    });
  };

  const state = {
    discovery: null,
    connecting: null,
    meAddr: '',
    mePub: '',
    username: '',
    room: derivedRoom,
    peers: new Map(),
    pendingPings: new Map(),
    statusOverride: null,
    renderScheduled: false,
    statusInterval: null,
    directoryTimer: null,
    sharedHashes: new Map(),
    remoteHashes: new Map(),
    peerOrder: new Map(),
    peerSeq: 0,
    filters: {
      search: '',
      onlyFavorites: false
    },
    layout: {
      activePane: 'list',
      activeNetwork: 'hydra'
    },
    chat: {
      activePeer: null,
      sessions: new Map(),
      typingPeers: new Map(),
      typingTimers: new Map(),
      favorites: new Set(),
      promptMessage: null
    },
    noclip: {
      discovery: null,
      connecting: null,
      peers: new Map(),
      status: { state: 'idle', detail: '' }
    },
    pokeNoticeTimer: null,
    nknListenerAttached: false,
    nknMessageHandler: null,
    nknClient: null
  };
  state.store = sharedStore;

  const FAVORITES_KEY = `hydra.favorites.${state.room}`;
  const historyKey = (peerKey) => {
    const normalized = normalizePubKey(peerKey);
    return normalized ? `hydra.chat.history.${state.room}.${normalized}` : '';
  };
  const sessionKey = (peerKey) => {
    const normalized = normalizePubKey(peerKey);
    return normalized ? `hydra.chat.session.${state.room}.${normalized}` : '';
  };

  const loadFavorites = () => {
    if (typeof localStorage === 'undefined') return new Set();
    try {
      const raw = localStorage.getItem(FAVORITES_KEY);
      if (!raw) return new Set();
      const list = JSON.parse(raw);
      return new Set(Array.isArray(list) ? list.map((value) => normalizePubKey(value)).filter(Boolean) : []);
    } catch (_) {
      return new Set();
    }
  };

  const saveFavorites = () => {
    if (typeof localStorage === 'undefined') return;
    try {
      localStorage.setItem(FAVORITES_KEY, JSON.stringify([...state.chat.favorites]));
    } catch (_) {
      // ignore quota issues
    }
  };

  const loadHistory = (peerKey) => {
    if (typeof localStorage === 'undefined') return [];
    const key = historyKey(peerKey);
    if (!key) return [];
    try {
      const raw = localStorage.getItem(key);
      const arr = raw ? JSON.parse(raw) : [];
      if (!Array.isArray(arr)) return [];
      return arr
        .map((item) => {
          if (!item || typeof item !== 'object') return null;
          const dir = item.dir === 'out' ? 'out' : 'in';
          const id = typeof item.id === 'string' ? item.id : '';
          const ts = typeof item.ts === 'number' ? item.ts : nowSeconds();
          const text = typeof item.text === 'string' ? item.text : '';
          return { id, dir, ts, text };
        })
        .filter(Boolean);
    } catch (_) {
      return [];
    }
  };

  const saveHistory = (peerKey, messages) => {
    if (typeof localStorage === 'undefined') return;
    const key = historyKey(peerKey);
    if (!key) return;
    try {
      localStorage.setItem(key, JSON.stringify(messages));
    } catch (_) {
      // ignore quota issues
    }
  };

  const loadSession = (peerKey) => {
    if (typeof localStorage === 'undefined') return null;
    const key = sessionKey(peerKey);
    if (!key) return null;
    try {
      const raw = localStorage.getItem(key);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      const status = typeof parsed?.status === 'string' ? parsed.status : 'idle';
      const allowed = ['idle', 'pending', 'accepted', 'declined'];
      const normalizedStatus = allowed.includes(status) ? status : 'idle';
      const record = { status: normalizedStatus };
      if (typeof parsed?.lastTs === 'number') record.lastTs = parsed.lastTs;
      return record;
    } catch (_) {
      return null;
    }
  };

  const saveSession = (peerKey, session) => {
    if (typeof localStorage === 'undefined') return;
    const key = sessionKey(peerKey);
    if (!key) return;
    try {
      localStorage.setItem(key, JSON.stringify(session));
    } catch (_) {
      // ignore quota issues
    }
  };

  function normalizeAddressForSend(value) {
    if (!value) return '';
    try {
      const text = typeof value === 'string' ? value : String(value);
      return text.trim();
    } catch (_) {
      return '';
    }
  }

  function ensurePeerOrder(key) {
    const normalized = normalizePubKey(key);
    if (!normalized) return;
    if (state.peerOrder.has(normalized)) return;
    state.peerSeq += 1;
    state.peerOrder.set(normalized, state.peerSeq);
  }

  function parseNknPeerPayload(packet, payload) {
    let data = payload;
    if (packet && typeof packet === 'object' && packet.payload !== undefined) {
      data = packet.payload;
    }
    if (data == null) return null;
    try {
      const text = data && typeof data === 'object' && data.toString ? data.toString() : String(data);
      return JSON.parse(text);
    } catch (_) {
      return null;
    }
  }

  // --- ADD (top-level inside createPeerDiscovery, not nested) ---
function handlePeerMessage(payload, { transport = 'nats', sourceAddr = '' } = {}) {
  if (!payload || typeof payload !== 'object') return false;
  const type = payload.type;
  if (!type || !PEER_MESSAGE_TYPES.has(type)) return false;

  const senderAddr = normalizeAddressForSend(sourceAddr || payload.addr || payload.pub || payload.from);
  const senderKey = normalizePubKey(payload.pub || payload.from || senderAddr);
  if (!senderKey) return false;
  try {
    console.debug('[peers] recv', { type, from: senderAddr, via: transport, normalizedKey: senderKey });
  } catch (_) {
    // ignore logging issues
  }

  const timestamp = typeof payload.ts === 'number' ? payload.ts : nowSeconds();
  const meta = payload.meta && typeof payload.meta === 'object' ? { ...payload.meta } : undefined;

  const upsertResult = upsertPeer(
    {
      nknPub: senderKey,
      addr: senderAddr || senderKey,
      originalPub: payload.pub || senderAddr || senderKey,
      meta,
      last: timestamp
    },
    { online: true, probing: false }
  );
  const peer = upsertResult.entry;
  if (peer) {
    peer.online = true;
    peer.probing = false;
    peer.last = timestamp;
    peer.lastSeenAt = timestamp * 1000;
    state.peers.set(senderKey, peer);
  }

  switch (type) {
    case 'peer-ping': {
      state.pendingPings.delete(senderKey);
      sendPeerPayload(senderAddr || senderKey, { type: 'peer-pong' }, { attachMeta: true }).catch(() => {});
      refreshStatus();
      scheduleRender();
      return true;
    }
    case 'peer-pong': {
      state.pendingPings.delete(senderKey);
      refreshStatus();
      scheduleRender();
      return true;
    }
    case 'peer-meta': {
      refreshStatus();
      scheduleRender();
      return true;
    }
    case 'peer-poke': {
      if (els.button) {
        els.button.classList.add('poke-notice');
        clearTimeout(state.pokeNoticeTimer);
        state.pokeNoticeTimer = setTimeout(() => {
          els.button?.classList?.remove('poke-notice');
        }, 8000);
      }
      if (peer) {
        peer.online = true;
        state.peers.set(senderKey, peer);
        refreshStatus();
      }
      const label = getDisplayName(peer || { addr: senderAddr || senderKey });
      setBadge?.(`Poke from ${label}`);
      scheduleRender();
      return true;
    }
    case 'chat-request': {
      const label = getDisplayName(peer || { addr: senderAddr || senderKey });
      state.chat.promptMessage = { key: senderKey, text: `${label} wants to chat. Accept?` };
      setSession(senderKey, { status: 'pending', lastTs: nowSeconds() });
      setActiveChat(senderKey);
      if (isMobileView()) setActivePane('chat');
      setBadge?.(`Chat request from ${label}`);
      showInlineChatDecision(senderKey);
      renderChat();
      scheduleRender();
      return true;
    }
    case 'chat-response': {
      const accepted = !!payload.accepted;
      const label = getDisplayName(peer || { addr: senderAddr || senderKey });
      setSession(senderKey, { status: accepted ? 'accepted' : 'declined', lastTs: nowSeconds() });
      if (state.chat.promptMessage?.key === senderKey) state.chat.promptMessage = null;
      setBadge?.(accepted ? `Chat accepted by ${label}` : `Chat declined by ${label}`, accepted);
      clearInlineChatDecision();
      if (state.chat.activePeer === senderKey) renderChat();
      scheduleRender();
      return true;
    }
    case 'chat-typing': {
      if (payload.isTyping) {
        state.chat.typingPeers.set(senderKey, Date.now());
        clearTimeout(state.chat.typingTimers.get(senderKey));
        if (state.chat.activePeer === senderKey && els.chatTyping) {
          els.chatTyping.classList.remove('hidden');
        }
        const timeout = setTimeout(() => {
          state.chat.typingPeers.delete(senderKey);
          if (state.chat.activePeer === senderKey && els.chatTyping) {
            els.chatTyping.classList.add('hidden');
          }
          state.chat.typingTimers.delete(senderKey);
        }, 1500);
        state.chat.typingTimers.set(senderKey, timeout);
      } else {
        state.chat.typingPeers.delete(senderKey);
        const existingTimer = state.chat.typingTimers.get(senderKey);
        if (existingTimer) clearTimeout(existingTimer);
        state.chat.typingTimers.delete(senderKey);
        if (state.chat.activePeer === senderKey && els.chatTyping) {
          els.chatTyping.classList.add('hidden');
        }
      }
      return true;
    }
    case 'chat-message': {
      const label = getDisplayName(peer || { addr: senderAddr || senderKey });
      appendHistory(senderKey, {
        id:
          typeof payload.id === 'string'
            ? payload.id
            : `${payload.ts || nowSeconds()}-${Math.random().toString(36).slice(2, 10)}`,
        dir: 'in',
        text: typeof payload.text === 'string' ? payload.text : '',
        ts: payload.ts || nowSeconds()
      });
      state.chat.typingPeers.delete(senderKey);
      const timer = state.chat.typingTimers.get(senderKey);
      if (timer) clearTimeout(timer);
      state.chat.typingTimers.delete(senderKey);
      setSession(senderKey, { status: 'accepted', lastTs: nowSeconds() });
      if (state.chat.promptMessage?.key === senderKey) state.chat.promptMessage = null;
      if (state.chat.activePeer === senderKey) {
        renderChat();
      } else {
        setBadge?.(`New message from ${label}`);
      }
      scheduleRender();
      return true;
    }
    default:
      return false;
  }
}

  function attachNknMessageHandler(client) {
    if (!client) return;
    if (state.nknListenerAttached && state.nknClient === client) return;
    // reset listener state if client instance changed
    if (state.nknClient && state.nknClient !== client) {
      state.nknListenerAttached = false;
      state.nknMessageHandler = null;
    }
    const handler = (packet, payload) => {
      const msg = parseNknPeerPayload(packet, payload);
      if (!msg || typeof msg !== 'object') return;
      const type = msg.type || msg.event;
      if (!type || !PEER_MESSAGE_TYPES.has(type)) return;
      const sourceAddr = normalizeAddressForSend(
        (packet && packet.src) || msg.from || msg.pub || msg.addr || msg.address || ''
      );
      handlePeerMessage(msg, { transport: 'nkn', sourceAddr });
    };
    try {
      client.on('message', handler);
      state.nknListenerAttached = true;
      state.nknMessageHandler = handler;
      state.nknClient = client;
    } catch (err) {
      log?.(`[peers] failed to attach NKN listener: ${err?.message || err}`);
    }
  }

  async function waitForNknClient(timeout = 20000) {
    try {
      Net.ensureNkn();
    } catch (_) {
      // ignore; ensureNkn may throw when SDK absent
    }
    if (Net.nkn?.client && Net.nkn.ready) {
      attachNknMessageHandler(Net.nkn.client);
      return Net.nkn.client;
    }
    const start = Date.now();
    return new Promise((resolve, reject) => {
      const check = () => {
        try {
          Net.ensureNkn();
        } catch (_) {
          // ignore, we'll retry
        }
        if (Net.nkn?.client && Net.nkn.ready) {
          attachNknMessageHandler(Net.nkn.client);
          resolve(Net.nkn.client);
          return;
        }
        if (Date.now() - start >= timeout) {
          reject(new Error('NKN client unavailable'));
          return;
        }
        setTimeout(check, 250);
      };
      check();
    });
  }

  async function sendPeerPayload(targetAddr, payload, { fallback = true, attachMeta = false, timeout = 20000 } = {}) {
    const destination = normalizeAddressForSend(targetAddr);
    if (!destination) return false;
    const message = { ...(payload || {}) };
    if (!state.discovery && !state.connecting) {
      try {
        await ensureDiscovery();
      } catch (_) {
        // discovery may still be unavailable; proceed with best effort
      }
    }
    if (!message.ts) message.ts = nowSeconds();
    let fromAddr = state.meAddr || Net.nkn?.addr || '';
    if (!fromAddr) {
      try {
        fromAddr = await waitForNknAddress(Math.min(timeout, 20000));
        if (!state.meAddr) {
          state.meAddr = fromAddr;
          updateSelf();
        }
      } catch (err) {
        if (fallback && state.discovery) {
          try {
            const natsTarget = normalizePubKey(destination);
            if (!natsTarget) throw new Error('invalid nats target');
            message.transport = 'nats';
            state.discovery.dm(natsTarget, message);
            try {
              console.debug('[peers] send', {
                dest: destination,
                via: 'nats',
                normalizedNatsKey: natsTarget,
                payload: message.type
              });
            } catch (_) {
              // ignore console issues
            }
            return true;
          } catch (_) {
            // ignore fallback failure
          }
        }
        log?.(`[peers] nkn send failed: ${err?.message || err}`);
        return false;
      }
    }
    if (fromAddr && !message.pub) message.pub = fromAddr;
    if (fromAddr && !message.addr) message.addr = fromAddr;
    if ((attachMeta || message.meta) && !message.meta) {
      message.meta = getPresenceMeta();
    }
    message.transport = 'nkn';
    try {
      const client = await waitForNknClient(timeout);
      await client.send(destination, JSON.stringify(message), { noReply: true, maxHoldingSeconds: 120 });
      try {
        console.debug('[peers] send', {
          dest: destination,
          via: 'nkn',
          normalizedNatsKey: normalizePubKey(destination),
          payload: message.type
        });
      } catch (_) {
        // ignore console issues
      }
      return true;
    } catch (err) {
      if (fallback && state.discovery) {
        try {
          const natsTarget = normalizePubKey(destination);
          if (!natsTarget) throw new Error('invalid nats target');
          message.transport = 'nats';
          state.discovery.dm(natsTarget, message);
          try {
            console.debug('[peers] send', {
              dest: destination,
              via: 'nats',
              normalizedNatsKey: natsTarget,
              payload: message.type
            });
          } catch (_) {
            // ignore console issues
          }
          return true;
        } catch (_) {
          // ignore fallback failure
        }
      }
      log?.(`[peers] nkn send failed: ${err?.message || err}`);
      return false;
    }
  }

  try {
    const storedName = LS.get(USERNAME_KEY, '');
    if (storedName) state.username = sanitizeUsername(storedName);
  } catch (_) {
    // ignore storage load issues
  }
  state.username = sanitizeUsername(state.username || '');
  state.chat.favorites = loadFavorites();

  const PEER_MESSAGE_TYPES = new Set([
    'peer-ping',
    'peer-pong',
    'peer-meta',
    'peer-poke',
    'chat-request',
    'chat-response',
    'chat-typing',
    'chat-message'
  ]);

  // === HOISTED: shared peer-message dispatcher (used by both NKN and Discovery/NATS) ===
function handlePeerMessage(payload, { transport = 'nats', sourceAddr = '' } = {}) {
  if (!payload || typeof payload !== 'object') return false;
  const type = payload.type;
  if (!type || !PEER_MESSAGE_TYPES.has(type)) return false;

  const senderAddr = normalizeAddressForSend(sourceAddr || payload.addr || payload.pub || payload.from);
  const senderKey = normalizePubKey(payload.pub || payload.from || senderAddr);
  if (!senderKey) return false;
  try {
    console.debug('[peers] recv', { type, from: senderAddr, via: transport, normalizedKey: senderKey });
  } catch (_) {}

  const timestamp = typeof payload.ts === 'number' ? payload.ts : nowSeconds();
  const meta = payload.meta && typeof payload.meta === 'object' ? { ...payload.meta } : undefined;

  const upsertResult = upsertPeer(
    {
      nknPub: senderKey,
      addr: senderAddr || senderKey,
      originalPub: payload.pub || senderAddr || senderKey,
      meta,
      last: timestamp
    },
    { online: true, probing: false }
  );
  const peer = upsertResult.entry;
  if (peer) {
    peer.online = true;
    peer.probing = false;
    peer.last = timestamp;
    peer.lastSeenAt = timestamp * 1000;
    state.peers.set(senderKey, peer);
  }

  switch (type) {
    case 'peer-ping': {
      state.pendingPings.delete(senderKey);
      sendPeerPayload(senderAddr || senderKey, { type: 'peer-pong' }, { attachMeta: true }).catch(() => {});
      refreshStatus();
      scheduleRender();
      return true;
    }
    case 'peer-pong': {
      state.pendingPings.delete(senderKey);
      refreshStatus();
      scheduleRender();
      return true;
    }
    case 'peer-meta': {
      refreshStatus();
      scheduleRender();
      return true;
    }
    case 'peer-poke': {
      if (els.button) {
        els.button.classList.add('poke-notice');
        clearTimeout(state.pokeNoticeTimer);
        state.pokeNoticeTimer = setTimeout(() => {
          els.button?.classList?.remove('poke-notice');
        }, 8000);
      }
      if (peer) {
        peer.online = true;
        state.peers.set(senderKey, peer);
        refreshStatus();
      }
      const label = getDisplayName(peer || { addr: senderAddr || senderKey });
      setBadge?.(`Poke from ${label}`);
      scheduleRender();
      return true;
    }
    case 'chat-request': {
      const label = getDisplayName(peer || { addr: senderAddr || senderKey });
      state.chat.promptMessage = { key: senderKey, text: `${label} wants to chat. Accept?` };
      setSession(senderKey, { status: 'pending', lastTs: nowSeconds() });
      setActiveChat(senderKey);
      if (isMobileView()) setActivePane('chat');
      setBadge?.(`Chat request from ${label}`);
      showInlineChatDecision(senderKey);
      renderChat();
      scheduleRender();
      return true;
    }
    case 'chat-response': {
      const accepted = !!payload.accepted;
      const label = getDisplayName(peer || { addr: senderAddr || senderKey });
      setSession(senderKey, { status: accepted ? 'accepted' : 'declined', lastTs: nowSeconds() });
      if (state.chat.promptMessage?.key === senderKey) state.chat.promptMessage = null;
      setBadge?.(accepted ? `Chat accepted by ${label}` : `Chat declined by ${label}`, accepted);
      clearInlineChatDecision();
      if (state.chat.activePeer === senderKey) renderChat();
      scheduleRender();
      return true;
    }
    case 'chat-typing': {
      if (payload.isTyping) {
        state.chat.typingPeers.set(senderKey, Date.now());
        clearTimeout(state.chat.typingTimers.get(senderKey));
        if (state.chat.activePeer === senderKey && els.chatTyping) {
          els.chatTyping.classList.remove('hidden');
        }
        const timeout = setTimeout(() => {
          state.chat.typingPeers.delete(senderKey);
          if (state.chat.activePeer === senderKey && els.chatTyping) {
            els.chatTyping.classList.add('hidden');
          }
          state.chat.typingTimers.delete(senderKey);
        }, 1500);
        state.chat.typingTimers.set(senderKey, timeout);
      } else {
        state.chat.typingPeers.delete(senderKey);
        const existingTimer = state.chat.typingTimers.get(senderKey);
        if (existingTimer) clearTimeout(existingTimer);
        state.chat.typingTimers.delete(senderKey);
        if (state.chat.activePeer === senderKey && els.chatTyping) {
          els.chatTyping.classList.add('hidden');
        }
      }
      return true;
    }
    case 'chat-message': {
      const label = getDisplayName(peer || { addr: senderAddr || senderKey });
      appendHistory(senderKey, {
        id: typeof payload.id === 'string'
          ? payload.id
          : `${payload.ts || nowSeconds()}-${Math.random().toString(36).slice(2, 10)}`,
        dir: 'in',
        text: typeof payload.text === 'string' ? payload.text : '',
        ts: payload.ts || nowSeconds()
      });
      state.chat.typingPeers.delete(senderKey);
      const timer = state.chat.typingTimers.get(senderKey);
      if (timer) clearTimeout(timer);
      state.chat.typingTimers.delete(senderKey);
      setSession(senderKey, { status: 'accepted', lastTs: nowSeconds() });
      if (state.chat.promptMessage?.key === senderKey) state.chat.promptMessage = null;
      if (state.chat.activePeer === senderKey) {
        renderChat();
      } else {
        setBadge?.(`New message from ${label}`);
      }
      scheduleRender();
      return true;
    }
    default:
      return false;
  }
}


  function getPresenceMeta() {
    return {
      graphId: CFG.graphId || '',
      origin: window.location.origin || '',
      username: sanitizeUsername(state.username || ''),
      network: 'hydra'  // Identify as Hydra peer for cross-app discovery
    };
  }

  sharedStore.all().forEach((peer) => {
    const normalized = normalizePeerEntry(peer);
    if (!normalized) return;
    upsertPeer({ ...normalized, lastSeenAt: normalized.last * 1000 }, { online: false, probing: false });
  });

  const els = {
    button: null,
    modal: null,
    backdrop: null,
    close: null,
    body: null,
    list: null,
    status: null,
    self: null,
    chatWrap: null,
    chatName: null,
    chatStatus: null,
    chatMsgs: null,
    chatTyping: null,
    chatInput: null,
    chatSend: null,
    chatFav: null,
    search: null,
    favToggle: null,
    tabs: null,
    tabButtons: []
  };

  function assignElements() {
    els.button = qs('#peerListButton');
    els.modal = qs('#peerModal');
    els.backdrop = qs('#peerBackdrop');
    els.close = qs('#peerClose');
    els.body = qs('#peerModalBody');
    els.list = qs('#peerList');
    els.status = qs('#peerStatus');
    els.self = qs('#peerSelf');
    els.nameInput = qs('#peerUsernameInput');
    els.nameApply = qs('#peerUsernameApply');
    els.nameBadge = qs('#peerSelfName');
    els.badgeCount = qs('#peerOnlineBadge');
    els.noclipBadge = qs('#noclipPeerBadge');
    els.chatWrap = qs('#chatWrap');
    els.chatName = qs('#chatPeerName');
    els.chatStatus = qs('#chatStatus');
    els.chatMsgs = qs('#chatMessages');
    els.chatTyping = qs('#chatTyping');
    els.chatInput = qs('#chatInput');
    els.chatSend = qs('#chatSend');
    els.chatFav = qs('#chatFavoriteBtn');
    els.search = qs('#peerSearchInput');
    els.favToggle = qs('#peerFavoritesToggle');
    els.tabs = qs('#peerTabs');
    els.tabButtons = els.tabs ? Array.from(els.tabs.querySelectorAll('.peer-tab')) : [];
    els.networkTabs = qs('#peerNetworkTabs');
    els.networkButtons = els.networkTabs ? Array.from(els.networkTabs.querySelectorAll('[data-network]')) : [];
    if (els.nameInput) els.nameInput.value = state.username || '';
    if (els.search) els.search.value = state.filters.search || '';
    if (els.favToggle) els.favToggle.classList.toggle('active', !!state.filters.onlyFavorites);
    setActivePane(state.layout.activePane);
    setActiveNetwork(state.layout.activeNetwork, { render: false });
    updateSelf();
    updateBadge();
    renderChat();
  }

  function showModal() {
    if (!els.modal) return;
    els.modal.classList.remove('hidden');
    els.modal.setAttribute('aria-hidden', 'false');
  }

  function hideModal() {
    if (!els.modal) return;
    els.modal.classList.add('hidden');
    els.modal.setAttribute('aria-hidden', 'true');
  }

  function formatAddress(addr) {
    if (!addr) return '(unknown)';
    const raw = String(addr).replace(/^graph\./i, '');
    if (raw.length <= 16) return raw;
    return `${raw.slice(0, 8)}...${raw.slice(-6)}`;
  }

  function formatLast(last) {
    if (!last) return 'unknown';
    const delta = Math.max(0, nowSeconds() - last);
    if (delta < 10) return 'just now';
    if (delta < 60) return `${delta}s ago`;
    if (delta < 3600) return `${Math.floor(delta / 60)}m ago`;
    return `${Math.floor(delta / 3600)}h ago`;
  }

  function setStatus(text, sticky = false) {
    if (sticky) state.statusOverride = text;
    else state.statusOverride = null;
    if (els.status) els.status.textContent = text;
    updateBadge();
  }

  function clearStatusOverride() {
    if (state.statusOverride == null) return;
    state.statusOverride = null;
    refreshStatus();
  }

  function refreshStatus() {
    if (!els.status) return;
    if (state.statusOverride != null) {
      els.status.textContent = state.statusOverride;
      updateBadge();
      return;
    }
    const total = state.peers.size;
    const online = Array.from(state.peers.values()).filter((peer) => peer?.online).length;
    const message = state.discovery
      ? `Peers: ${online}/${total} online`
      : total
        ? `Offline • ${total} known`
        : 'Discovery offline';
    els.status.textContent = message;
    updateBadge();
  }

  function updateBadge() {
    if (els.badgeCount) {
      const hydraOnline = Array.from(state.peers.values()).filter((peer) => peer?.online).length;
      if (hydraOnline > 0) {
        els.badgeCount.textContent = String(hydraOnline);
        els.badgeCount.classList.remove('hidden');
      } else {
        els.badgeCount.classList.add('hidden');
      }
    }

    if (els.noclipBadge) {
      const noclipCount = state.noclip.peers.size;
      if (noclipCount > 0) {
        els.noclipBadge.textContent = String(noclipCount);
        els.noclipBadge.classList.remove('hidden');
      } else {
        els.noclipBadge.classList.add('hidden');
      }
    }
  }

  function pruneSelfEntries() {
    const selfKey = normalizePubKey(state.mePub || state.meAddr);
    if (!selfKey) return false;
    const toDelete = [];
    state.peers.forEach((peer, key) => {
      const keyNorm = normalizePubKey(key);
      const peerKey = normalizePubKey(peer?.nknPub);
      const addrKey = normalizePubKey(peer?.addr);
      const originalKey = normalizePubKey(peer?.originalPub);
      if (keyNorm === selfKey || peerKey === selfKey || addrKey === selfKey || originalKey === selfKey) {
        toDelete.push(key);
      }
    });
    if (state.store?.remove) state.store.remove(state.mePub || state.meAddr);
    if (!toDelete.length) return false;
    toDelete.forEach((key) => {
      state.peers.delete(key);
      state.peerOrder.delete(normalizePubKey(key));
    });
    return true;
  }

  function updateDiscoveryMeta() {
    if (!state.discovery) return;
    const meta = getPresenceMeta();
    state.discovery.me = state.discovery.me || {};
    state.discovery.me.nknPub = normalizePubKey(state.mePub || state.meAddr);
    state.discovery.me.addr = state.meAddr || '';
    state.discovery.me.meta = meta;
  }

  function setUsername(value) {
    const sanitized = sanitizeUsername(value);
    if (sanitized === state.username) return sanitized;
    state.username = sanitized;
    try {
      if (sanitized) LS.set(USERNAME_KEY, sanitized);
      else LS.del(USERNAME_KEY);
    } catch (_) {
      // ignore persistence failures
    }
    updateSelf();
    updateDiscoveryMeta();
      if (state.meAddr) {
        try {
          state.store.upsert({
            nknPub: state.mePub || normalizePubKey(state.meAddr),
            addr: state.meAddr,
            meta: { username: sanitized },
            last: nowSeconds()
          });
        } catch (_) {
          // ignore storage issues
        }
        const removedSelf = pruneSelfEntries();
        if (removedSelf) updateBadge();
        const selfKey = normalizePubKey(state.mePub || state.meAddr);
        state.peers.forEach((peer, key) => {
          if (!Array.isArray(peer.sharedSources) || !peer.sharedSources.length) return;
          let changed = false;
          const updatedSources = peer.sharedSources.map((source) => {
          if (!source) return source;
          if (source.key === selfKey) {
            const nextLabel = sanitized || source.key;
            if (source.label !== nextLabel) {
              changed = true;
              return { ...source, label: nextLabel };
            }
          }
          return source;
        });
        if (changed) {
          const updated = {
            ...peer,
            sharedSources: updatedSources,
            sharedFrom: updatedSources[0]?.key || peer.sharedFrom,
            sharedFromLabel: updatedSources[0]?.label || peer.sharedFromLabel
          };
          state.peers.set(key, updated);
        }
      });
    }
    if (state.discovery) {
      try {
        state.discovery.presence(getPresenceMeta()).catch(() => {});
      } catch (_) {
        // ignore failures sending presence
      }
    }
    scheduleDirectoryBroadcast(120);
    scheduleRender();
    if (typeof setBadge === 'function') {
      const message = sanitized ? `Display name set to "${sanitized}"` : 'Display name cleared';
      setBadge(message);
    }
    return sanitized;
  }

  function scheduleRender() {
    if (state.renderScheduled) return;
    state.renderScheduled = true;
    requestAnimationFrame(() => {
      state.renderScheduled = false;
      renderPeers();
    });
  }

  function normalizePeerEntry(entry) {
    if (!entry || typeof entry !== 'object') return null;
    const rawPub = entry.nknPub || entry.addr || entry.pub;
    const nknPub = normalizePubKey(rawPub);
    if (!nknPub) return null;
    const addrValue = entry.addr || entry.address || rawPub || '';
    const addrTrimmed = typeof addrValue === 'string' ? addrValue.trim() : String(addrValue || '').trim();
    const meta = entry.meta && typeof entry.meta === 'object' ? { ...entry.meta } : {};
    if (meta.username) meta.username = sanitizeUsername(meta.username);
    const sharedRaw = [];
    if (Array.isArray(entry.sharedSources)) sharedRaw.push(...entry.sharedSources);
    if (Array.isArray(entry.sharedFrom)) sharedRaw.push(...entry.sharedFrom);
    else if (entry.sharedFrom) sharedRaw.push(entry.sharedFrom);
    const sharedSources = [];
    sharedRaw.forEach((value) => {
      if (!value) return;
      let label = '';
      let sourceKey = '';
      if (typeof value === 'object') {
        label = value.label || value.username || value.addr || value.from || value.key || value.id || '';
        sourceKey = normalizePubKey(value.key || value.addr || value.from || label);
      } else {
        label = String(value || '').trim();
        sourceKey = normalizePubKey(label);
      }
      label = label ? label.trim() : '';
      if (!sourceKey) sourceKey = normalizePubKey(label);
      if (!sourceKey || sourceKey === nknPub) return;
      sharedSources.push({ key: sourceKey, label: label || sourceKey });
    });
    return {
      nknPub,
      addr: addrTrimmed || rawPub || nknPub,
      originalPub: typeof rawPub === 'string' ? rawPub.trim() : String(rawPub || '').trim() || nknPub,
      meta,
      last: typeof entry.last === 'number' ? entry.last : nowSeconds(),
      sharedSources
    };
  }

  function shareDirectory(list, targetPub) {
    if (!state.discovery || !Array.isArray(list) || !list.length) return;
    const targetKey = normalizePubKey(targetPub);
    if (!targetKey) return;
    if (targetKey === normalizePubKey(state.mePub || state.meAddr)) return;
    const peers = list
      .map(normalizePeerEntry)
      .filter(
        (entry) =>
          entry &&
          entry.nknPub !== targetKey &&
          (entry.originalPub ? normalizePubKey(entry.originalPub) !== targetKey : true)
      );
    if (!peers.length) return;
    const payload = {
      type: 'peer-directory',
      pub: state.meAddr,
      meta: { username: sanitizeUsername(state.username || ''), graphId: CFG.graphId || '' },
      peers
    };
    try {
      state.discovery.dm(targetKey, payload);
    } catch (err) {
      log?.(`[peers] share directory failed: ${err?.message || err}`);
    }
  }

  function collectDirectoryEntries() {
    const entries = [];
    state.peers.forEach((peer) => {
      if (!peer?.nknPub) return;
      const username = sanitizeUsername(peer.meta?.username || peer.meta?.name || peer.displayName || '');
      entries.push({
        nknPub: peer.originalPub || peer.nknPub,
        addr: peer.addr || peer.originalPub || peer.nknPub,
        meta: {
          ...(peer.meta || {}),
          username
        },
        last: peer.last || nowSeconds(),
        sharedSources: (peer.sharedSources || []).map((src) => ({
          key: src.key,
          label: src.label || src.key
        }))
      });
    });
    if (state.meAddr) {
      entries.push({
        nknPub: state.mePub || state.meAddr,
        addr: state.meAddr,
        meta: { self: true, username: sanitizeUsername(state.username || '') },
        last: nowSeconds(),
        sharedSources: []
      });
    }
    return entries;
  }

  function hashDirectory(entries) {
    try {
      const sorted = entries
        .map((entry) => ({
          nknPub: entry.nknPub,
          addr: entry.addr,
          last: entry.last || 0,
          username: sanitizeUsername(entry.meta?.username || ''),
          shared: Array.isArray(entry.sharedSources)
            ? entry.sharedSources
                .map((src) => (typeof src === 'object' ? src.key || src.label || '' : String(src || '')))
                .sort()
            : []
        }))
        .sort((a, b) => a.nknPub.localeCompare(b.nknPub));
      return JSON.stringify(sorted);
    } catch (_) {
      return '';
    }
  }

  function broadcastDirectory(targetPub = null) {
    if (!state.discovery) return;
    const entries = collectDirectoryEntries();
    if (!entries.length) return;
    const hash = hashDirectory(entries);
    const targets = targetPub
      ? [normalizePubKey(targetPub)]
      : Array.from(state.peers.keys());
    targets.forEach((key) => {
      if (!key || key === normalizePubKey(state.mePub || state.meAddr)) return;
      const peer = state.peers.get(key);
      const address = peer?.addr || peer?.originalPub || key;
      const normalizedTarget = normalizePubKey(address) || normalizePubKey(key);
      if (!normalizedTarget) return;
      if (!targetPub && hash && state.sharedHashes.get(key) === hash) return;
      shareDirectory(entries, normalizedTarget);
      if (hash) state.sharedHashes.set(key, hash);
    });
  }

  function scheduleDirectoryBroadcast(delay = 300) {
    if (state.directoryTimer) return;
    state.directoryTimer = setTimeout(() => {
      state.directoryTimer = null;
      broadcastDirectory();
    }, delay);
  }

  function upsertPeer(peer, { online, probing, sharedFrom, sharedSources } = {}) {
    const key = normalizePubKey(peer?.nknPub || peer?.addr || '');
    if (!key) return { added: false, entry: null };
    const selfKey = normalizePubKey(state.mePub || state.meAddr);
    const peerAddrKey = normalizePubKey(peer?.addr);
    const originalKey = normalizePubKey(peer?.originalPub);
    if (selfKey && (key === selfKey || peerAddrKey === selfKey || originalKey === selfKey)) {
      state.peers.delete(key);
      if (state.store?.remove) state.store.remove(state.mePub || state.meAddr);
      return { added: false, entry: null };
    }
    const existing = state.peers.get(key) || {};
    const added = !existing.nknPub;
    ensurePeerOrder(key);
    const lastSeconds = typeof peer.last === 'number' ? peer.last : existing.last || 0;
    const lastSeenAt =
      typeof peer.lastSeenAt === 'number'
        ? peer.lastSeenAt
        : lastSeconds
          ? lastSeconds * 1000
          : existing.lastSeenAt || 0;
    const merged = {
      ...existing,
      ...peer,
      nknPub: key,
      last: lastSeconds,
      lastSeenAt,
      addr: peer.addr || existing.addr || peer.nknPub || key,
      originalPub: peer.nknPub || existing.originalPub || key
    };
    if (peer.meta && typeof peer.meta === 'object') {
      const prevMeta = existing.meta && typeof existing.meta === 'object' ? { ...existing.meta } : {};
      const nextMeta = { ...prevMeta, ...peer.meta };
      if (!nextMeta.username && prevMeta.username) nextMeta.username = prevMeta.username;
      if (nextMeta.username) nextMeta.username = sanitizeUsername(nextMeta.username);
      merged.meta = nextMeta;
    } else if (existing.meta) {
      merged.meta = existing.meta;
    } else if (!merged.meta) {
      merged.meta = {};
    }
    if (online !== undefined) merged.online = online;
    if (probing !== undefined) merged.probing = probing;

    const sharedMap = new Map();
    const addSource = (value) => {
      if (!value) return;
      let label = '';
      let sourceKey = '';
      if (typeof value === 'object') {
        label = value.label || value.username || value.addr || value.from || value.key || value.id || '';
        sourceKey = normalizePubKey(value.key || value.addr || value.from || label);
      } else {
        label = String(value || '').trim();
        sourceKey = normalizePubKey(label);
      }
      label = label ? label.trim() : '';
      if (!sourceKey) sourceKey = normalizePubKey(label);
      if (!sourceKey || sourceKey === merged.nknPub) return;
      if (!label) label = sourceKey;
      const known = state.peers.get(sourceKey);
      if (known?.meta?.username) {
        const clean = sanitizeUsername(known.meta.username);
        if (clean) label = clean;
      }
      if (!sharedMap.has(sourceKey)) sharedMap.set(sourceKey, label);
    };

    if (Array.isArray(existing.sharedSources)) existing.sharedSources.forEach(addSource);
    if (Array.isArray(peer.sharedSources)) peer.sharedSources.forEach(addSource);
    if (Array.isArray(sharedSources)) sharedSources.forEach(addSource);
    if (peer.sharedFrom) addSource(peer.sharedFrom);
    if (sharedFrom) addSource(sharedFrom);

    const sharedArray = Array.from(sharedMap.entries()).map(([srcKey, label]) => ({ key: srcKey, label }));
    merged.sharedSources = sharedArray;
    merged.sharedFrom = sharedArray[0]?.key || null;
    merged.sharedFromLabel = sharedArray[0]?.label || '';

    state.peers.set(key, merged);
    try {
      state.store.upsert({
        nknPub: merged.nknPub,
        addr: merged.addr,
        meta: merged.meta || {},
        last: merged.last || nowSeconds(),
        sharedSources: sharedArray
      });
    } catch (_) {
      // ignore persistence errors
    }
    return { added, entry: merged };
  }

  function updatePeerStatuses() {
    const now = Date.now();
    let changed = false;
    state.peers.forEach((peer, pub) => {
      const lastMs = peer.lastSeenAt || (peer.last ? peer.last * 1000 : 0);
      const online = lastMs && now - lastMs <= OFFLINE_AFTER_SECONDS * 1000;
      if (peer.online !== online) {
        peer.online = online;
        if (!online) peer.probing = false;
        state.peers.set(pub, peer);
        changed = true;
      }
    });
    if (changed) renderPeers();
    else refreshStatus();
    updateBadge();
  }

  function pingPeer(pub) {
    const key = normalizePubKey(pub);
    if (!key) return;
    if (state.pendingPings.has(key)) return;
    const peer = state.peers.get(key);
    const target = peer?.addr || peer?.originalPub || pub;
    if (!target) return;
    state.pendingPings.set(key, Date.now());
    if (peer) {
      peer.probing = true;
      state.peers.set(key, peer);
    }
    sendPeerPayload(target, { type: 'peer-ping' }, { attachMeta: true })
      .catch((err) => {
        state.pendingPings.delete(key);
        if (peer) {
          peer.probing = false;
          state.peers.set(key, peer);
          scheduleRender();
        }
        if (err) log?.(`[peers] ping failed: ${err.message || err}`);
      });
    scheduleRender();
    setTimeout(() => {
      const started = state.pendingPings.get(key);
      if (!started) return;
      if (Date.now() - started >= PING_TIMEOUT_MS) {
        state.pendingPings.delete(key);
        const current = state.peers.get(key);
        if (current && !current.online) {
          current.probing = false;
          state.peers.set(key, current);
          scheduleRender();
        }
      }
    }, PING_TIMEOUT_MS);
  }

  function pingAllPeers() {
    state.peers.forEach((peer) => {
      if (!peer?.nknPub) return;
      if (peer.online) return;
      pingPeer(peer.originalPub || peer.addr || peer.nknPub);
    });
  }

  function updateSelf() {
    if (!els.self) return;
    if (!state.meAddr) {
      els.self.textContent = '';
      els.self.classList.add('hidden');
      return;
    }
    els.self.textContent = `Your address: ${state.meAddr}`;
    els.self.classList.remove('hidden');
    if (els.nameInput && document.activeElement !== els.nameInput) {
      els.nameInput.value = state.username || '';
    }
    if (els.nameBadge) {
      const name = (state.username || '').trim();
      els.nameBadge.textContent = name ? `Presenting as ${name}` : '';
      els.nameBadge.classList.toggle('hidden', !name);
    }
  }

  function normalizePeerKey(peer) {
    if (!peer) return '';
    if (typeof peer === 'string') return normalizePubKey(peer);
    return normalizePubKey(peer.addr || peer.originalPub || peer.nknPub);
  }

  function peerByKey(key) {
    const normalized = normalizePubKey(key);
    if (!normalized) return null;
    return state.peers.get(normalized) || null;
  }

  function targetAddress(keyOrPeer) {
    if (!keyOrPeer) return '';
    if (typeof keyOrPeer === 'string') {
      const peer = peerByKey(keyOrPeer);
      if (!peer) return '';
      return peer.addr || peer.originalPub || peer.nknPub || '';
    }
    return keyOrPeer.addr || keyOrPeer.originalPub || keyOrPeer.nknPub || '';
  }

  function ensureChatSession(peerKey) {
    const normalized = normalizePubKey(peerKey);
    if (!normalized) return null;
    const existing = state.chat.sessions.get(normalized);
    if (existing) return existing;
    const stored = loadSession(normalized);
    const session = stored ? { ...stored } : { status: 'idle' };
    state.chat.sessions.set(normalized, session);
    return session;
  }

  function setSession(peerKey, nextSession) {
    const normalized = normalizePubKey(peerKey);
    if (!normalized) return null;
    const base = ensureChatSession(normalized) || { status: 'idle' };
    const allowed = ['idle', 'pending', 'accepted', 'declined'];
    const statusCandidate = typeof nextSession?.status === 'string' ? nextSession.status : base.status;
    const status = allowed.includes(statusCandidate) ? statusCandidate : 'idle';
    const record = { status };
    if (typeof nextSession?.lastTs === 'number') record.lastTs = nextSession.lastTs;
    else if (typeof base.lastTs === 'number') record.lastTs = base.lastTs;
    state.chat.sessions.set(normalized, record);
    saveSession(normalized, record);
    return record;
  }

  function setActiveChat(key) {
    const normalized = normalizePubKey(key);
    state.chat.activePeer = normalized || null;
    if (normalized) ensureChatSession(normalized);
    renderChat();
  }

  function appendHistory(peerKey, message) {
    const normalized = normalizePubKey(peerKey);
    if (!normalized || !message) return [];
    const history = loadHistory(normalized);
    if (message.id && history.some((item) => item.id === message.id)) return history;
    const record = {
      id: typeof message.id === 'string' ? message.id : String(Date.now()),
      dir: message.dir === 'out' ? 'out' : 'in',
      text: typeof message.text === 'string' ? message.text : '',
      ts: typeof message.ts === 'number' ? message.ts : nowSeconds()
    };
    history.push(record);
    saveHistory(normalized, history);
    return history;
  }

  function isMobileView() {
    if (typeof window === 'undefined' || !window.matchMedia) return false;
    try {
      return window.matchMedia('(max-width: 768px)').matches;
    } catch (_) {
      return false;
    }
  }

  function setActivePane(pane) {
    const normalized = pane === 'chat' ? 'chat' : 'list';
    state.layout.activePane = normalized;
    if (els.body) {
      els.body.classList.remove('mobile-pane-list', 'mobile-pane-chat');
      els.body.classList.add(`mobile-pane-${normalized}`);
    }
    if (els.tabButtons && els.tabButtons.length) {
      els.tabButtons.forEach((btn) => {
        const target = (btn.dataset?.pane || 'list') === normalized;
        btn.classList.toggle('active', target);
      });
    }
  }

  function setActiveNetwork(network, { render = true } = {}) {
    const normalized = network === 'noclip' ? 'noclip' : 'hydra';
    if (state.layout.activeNetwork === normalized) {
      if (render) {
        renderPeers();
        renderChat();
      }
      return;
    }
    state.layout.activeNetwork = normalized;
    if (els.networkButtons && els.networkButtons.length) {
      els.networkButtons.forEach((btn) => {
        const match = (btn.dataset?.network || 'hydra') === normalized;
        btn.classList.toggle('active', match);
        btn.setAttribute('aria-pressed', match ? 'true' : 'false');
      });
    }
    if (normalized === 'noclip') {
      ensureNoclipDiscovery().catch(() => {});
    }
    if (render) {
      renderPeers();
      renderChat();
    }
  }

  function clearInlineChatDecision() {
    const row = els.chatWrap?.querySelector('.chat-decision-row');
    if (row) row.remove();
  }

  async function respondToChat(peerKey, accept) {
    const normalized = normalizePubKey(peerKey);
    if (!normalized) return;
    if (state.chat.promptMessage?.key === normalized) state.chat.promptMessage = null;
    const addr = targetAddress(normalized);
    let ok = false;
    if (addr) {
      ok = await sendPeerPayload(addr, { type: 'chat-response', accepted: !!accept });
    }
    if (!ok) {
      setBadge?.('Chat response failed to send', false);
      return;
    }
    const session = setSession(normalized, { status: accept ? 'accepted' : 'declined', lastTs: nowSeconds() });
    if (accept) {
      setBadge?.(`Chat accepted with ${getDisplayName(peerByKey(normalized) || { addr: addr || normalized })}`);
    } else {
      setBadge?.(`Chat declined for ${getDisplayName(peerByKey(normalized) || { addr: addr || normalized })}`, false);
    }
    if (state.chat.activePeer === normalized) {
      clearInlineChatDecision();
      renderChat();
    }
  }

  function showInlineChatDecision(fromKey) {
    if (!els.chatWrap) return;
    let row = els.chatWrap.querySelector('.chat-decision-row');
    if (!row) {
      row = document.createElement('div');
      row.className = 'row chat-decision-row';
      row.style.marginTop = '8px';
      const accept = document.createElement('button');
      accept.className = 'secondary';
      accept.textContent = 'Accept';
      accept.addEventListener('click', async () => {
        await respondToChat(fromKey, true);
      });
      const decline = document.createElement('button');
      decline.className = 'ghost';
      decline.textContent = 'Decline';
      decline.addEventListener('click', async () => {
        await respondToChat(fromKey, false);
      });
      row.appendChild(accept);
      row.appendChild(decline);
      els.chatStatus?.insertAdjacentElement('afterend', row);
    }
  }

  function renderChat() {
    if (!els.chatWrap) return;
    const network = state.layout.activeNetwork || 'hydra';
    const isHydra = network === 'hydra';
    if (!isHydra) {
      state.chat.activePeer = null;
      if (els.chatName) els.chatName.textContent = 'Bridge required';
      if (els.chatStatus) els.chatStatus.textContent = 'Use a NoClip Bridge node to exchange chat.';
      if (els.chatMsgs) els.chatMsgs.innerHTML = '';
      if (els.chatInput) {
        els.chatInput.value = '';
        els.chatInput.disabled = true;
      }
      if (els.chatSend) els.chatSend.disabled = true;
      if (els.chatFav) els.chatFav.classList.remove('active');
      if (els.chatTyping) els.chatTyping.classList.add('hidden');
      clearInlineChatDecision();
      return;
    }
    const key = state.chat.activePeer;
    const discoveryReady = !!state.discovery;
    if (!key) {
      if (els.chatName) els.chatName.textContent = 'Select a peer…';
      if (els.chatStatus) {
        els.chatStatus.textContent = discoveryReady ? 'Pick a peer to start chatting' : 'Discovery unavailable';
      }
      if (els.chatMsgs) els.chatMsgs.innerHTML = '';
      if (els.chatInput) {
        els.chatInput.value = '';
        els.chatInput.disabled = true;
      }
      if (els.chatSend) els.chatSend.disabled = true;
      if (els.chatFav) els.chatFav.classList.remove('active');
      if (els.chatTyping) els.chatTyping.classList.add('hidden');
      clearInlineChatDecision();
      return;
    }

    const peer = peerByKey(key);
    if (els.chatName) els.chatName.textContent = peer ? getDisplayName(peer) : key;
    if (els.chatFav) els.chatFav.classList.toggle('active', state.chat.favorites.has(key));

    const history = loadHistory(key);
    if (els.chatMsgs) {
      els.chatMsgs.innerHTML = '';
      history.forEach((msg) => {
        const row = document.createElement('div');
        row.className = `chat-message ${msg.dir === 'out' ? 'out' : 'in'}`;
        row.textContent = msg.text || '';
        const time = document.createElement('span');
        time.className = 'time';
        time.textContent = new Date((msg.ts || nowSeconds()) * 1000).toLocaleTimeString();
        row.appendChild(time);
        els.chatMsgs.appendChild(row);
      });
      els.chatMsgs.scrollTop = els.chatMsgs.scrollHeight;
    }

    const session = ensureChatSession(key);
    const prompt = state.chat.promptMessage;
    if (prompt?.key === key && session?.status !== 'pending') state.chat.promptMessage = null;
    if (session?.status !== 'pending') clearInlineChatDecision();

    let statusText = 'No chat yet';
    if (state.chat.promptMessage?.key === key) statusText = state.chat.promptMessage.text;
    else if (!discoveryReady) statusText = 'Discovery unavailable';
    else if (session?.status === 'accepted') statusText = 'Connected';
    else if (session?.status === 'pending') statusText = 'Awaiting peer…';
    else if (session?.status === 'declined') statusText = 'Declined';
    if (els.chatStatus) els.chatStatus.textContent = statusText;

    const canSend = discoveryReady && session?.status === 'accepted';
    if (els.chatInput) els.chatInput.disabled = !canSend;
    if (els.chatSend) els.chatSend.disabled = !canSend;

    if (els.chatTyping && state.chat.typingPeers.has(key)) {
      const last = state.chat.typingPeers.get(key) || 0;
      const recently = Date.now() - last < 1500;
      els.chatTyping.classList.toggle('hidden', !recently);
      if (!recently) state.chat.typingPeers.delete(key);
    } else if (els.chatTyping) {
      els.chatTyping.classList.add('hidden');
    }
  }

  async function openChatWithPeer(peer) {
    const key = normalizePeerKey(peer);
    if (!key) return;
    setActiveChat(key);
    if (isMobileView()) setActivePane('chat');
    try {
      await ensureDiscovery();
    } catch (_) {
      // discovery may fail; continue to attempt NKN send so the user can retry later
    }
    const session = ensureChatSession(key);
    if (session?.status === 'accepted' || session?.status === 'pending') {
      renderChat();
      return;
    }
    const addr = targetAddress(peer) || targetAddress(key);
    if (!addr) {
      renderChat();
      return;
    }
    const fromName = sanitizeUsername(state.username || '');
    const ok = await sendPeerPayload(addr, { type: 'chat-request', fromName });
    if (ok) {
      setSession(key, { status: 'pending', lastTs: nowSeconds() });
    } else {
      setBadge?.('Chat request failed', false);
    }
    renderChat();
  }

  function getDisplayName(peer) {
    const name = peer.meta?.username && sanitizeUsername(peer.meta.username);
    if (name) return name;
    return formatAddress(peer.addr || peer.originalPub || peer.nknPub);
  }

  function getSortName(peer) {
    const name = peer.meta?.username && sanitizeUsername(peer.meta.username);
    if (name) return name.toLowerCase();
    return (peer.addr || peer.originalPub || peer.nknPub || '').toLowerCase();
  }

  function scrollToPeer(peerKey) {
    const key = normalizePubKey(peerKey);
    if (!key || !els.list) return;
    const selectorKey = typeof CSS !== 'undefined' && CSS.escape
      ? CSS.escape(key)
      : key.replace(/[^a-zA-Z0-9_-]/g, (c) => `\\${c}`);
    const target = els.list.querySelector(`[data-peer-id="${selectorKey}"]`);
    if (!target) return;
    target.classList.add('peer-highlight');
    try {
      target.scrollIntoView({ block: 'center', behavior: 'smooth' });
    } catch (_) {
      target.scrollIntoView({ block: 'center' });
    }
    setTimeout(() => target.classList.remove('peer-highlight'), 1500);
  }

  function renderPeers() {
    if (!els.list) return;
    els.list.innerHTML = '';
    setActivePane(state.layout.activePane);
    if (els.search && els.search.value !== state.filters.search) {
      els.search.value = state.filters.search || '';
    }
    const network = state.layout.activeNetwork || 'hydra';
    const isHydra = network === 'hydra';
    if (els.favToggle) {
      els.favToggle.disabled = !isHydra;
      els.favToggle.classList.toggle('active', isHydra && !!state.filters.onlyFavorites);
    }
    const now = nowSeconds();
    const peersMap = isHydra ? state.peers : state.noclip.peers;
    let peers = Array.from(peersMap.values()).filter((peer) => {
      if (!peer?.nknPub) return false;

      // Additional prefix filtering to ensure correct tab displays correct peers
      const addr = peer.addr || peer.nknPub || '';
      const addrLower = addr.toLowerCase();

      if (isHydra) {
        // Hydra tab: ONLY show hydra./graph. peers, exclude noclip. peers
        const isNoclipPeer = addrLower.startsWith('noclip.');
        return !isNoclipPeer; // Show everything except noclip. peers
      } else {
        // NoClip tab: ONLY show noclip. peers, exclude hydra./graph. peers
        const isNoclipPeer = addrLower.startsWith('noclip.');
        return isNoclipPeer; // Show only noclip. peers
      }
    });

    if (isHydra && state.filters.onlyFavorites) {
      peers = peers.filter((peer) => state.chat.favorites.has(normalizePeerKey(peer)));
    }
    const searchTerm = (state.filters.search || '').trim().toLowerCase();
    if (searchTerm) {
      peers = peers.filter((peer) => {
        const display = (getDisplayName(peer) || '').toLowerCase();
        const addr = (peer.addr || peer.originalPub || peer.nknPub || '').toLowerCase();
        return display.includes(searchTerm) || addr.includes(searchTerm);
      });
    }
    peers.sort((a, b) => {
      const onlineA = a.online ? 1 : 0;
      const onlineB = b.online ? 1 : 0;
      if (onlineA !== onlineB) return onlineB - onlineA;
      const orderA = isHydra
        ? state.peerOrder.get(normalizePubKey(a.nknPub || a.addr)) || Number.MAX_SAFE_INTEGER
        : Number.MAX_SAFE_INTEGER;
      const orderB = isHydra
        ? state.peerOrder.get(normalizePubKey(b.nknPub || b.addr)) || Number.MAX_SAFE_INTEGER
        : Number.MAX_SAFE_INTEGER;
      if (orderA !== orderB) return orderA - orderB;
      const lastA = a.last || now;
      const lastB = b.last || now;
      if (lastA !== lastB) return lastB - lastA;
      const nameA = getSortName(a);
      const nameB = getSortName(b);
      if (nameA !== nameB) return nameA.localeCompare(nameB);
      return (a.addr || a.nknPub || '').localeCompare(b.addr || b.nknPub || '');
    });
    if (!peers.length) {
      const empty = document.createElement('div');
      empty.className = 'peer-empty';
      if (isHydra && state.filters.onlyFavorites && !state.chat.favorites.size) {
        empty.textContent = 'No favorites saved yet.';
      } else if (searchTerm) {
        empty.textContent = 'No peers match your search.';
      } else if (isHydra) {
        empty.textContent = state.discovery ? 'No peers discovered yet.' : 'Discovery offline.';
      } else {
        const status = state.noclip.status?.state || 'idle';
        if (status === 'error') empty.textContent = 'NoClip discovery unavailable.';
        else if (status === 'connecting') empty.textContent = 'Connecting to NoClip discovery…';
        else empty.textContent = 'No NoClip peers discovered yet.';
      }
      els.list.appendChild(empty);
      refreshStatus();
      return;
    }
    const frag = document.createDocumentFragment();
    peers.forEach((peer) => {
      const entry = document.createElement('div');
      entry.className = 'peer-entry';
      entry.dataset.peerId = peer.nknPub;
      entry.classList.toggle('offline', !peer.online);
      entry.classList.toggle('probing', !!peer.probing);

      const meta = document.createElement('div');
      meta.className = 'peer-meta';

      const nameEl = document.createElement('div');
      nameEl.className = 'peer-name';
      const displayName = getDisplayName(peer);
      const isFavorite = isHydra && state.chat.favorites.has(normalizePeerKey(peer));
      nameEl.textContent = `${isFavorite ? '★ ' : ''}${displayName}`;
      meta.appendChild(nameEl);

      const address = peer.addr || peer.originalPub || peer.nknPub;
      if (address) {
        const addrEl = document.createElement('div');
        addrEl.className = 'peer-address';
        addrEl.textContent = address;
        meta.appendChild(addrEl);
      }

      const infoEl = document.createElement('div');
      infoEl.className = 'peer-info';
      const parts = [];
      const statusText = peer.online ? 'Online' : peer.probing ? 'Checking...' : 'Offline';
      const lastSeconds = peer.last
        ? peer.last
        : peer.lastSeenAt
          ? Math.floor(peer.lastSeenAt / 1000)
          : 0;
      parts.push(statusText);
      parts.push(`Last seen ${formatLast(lastSeconds)}`);
      const graphLabel = peer.meta?.graphId;
      if (graphLabel) parts.push(`Graph ${String(graphLabel).slice(0, 8)}`);
      if (!isHydra) parts.push('NoClip network');
      infoEl.textContent = parts.join(' • ');
      meta.appendChild(infoEl);

      entry.appendChild(meta);

      const actions = document.createElement('div');
      actions.className = 'peer-actions';
      if (isHydra) {
        const syncBtn = document.createElement('button');
        syncBtn.className = 'secondary';
        syncBtn.textContent = 'Sync';
        syncBtn.addEventListener('click', (e) => {
          e.preventDefault();
          const target = peer.addr || peer.originalPub || peer.nknPub;
          if (!target) return;
          WorkspaceSync.promptSync(target);
          hideModal();
        });
        actions.appendChild(syncBtn);
      }

      const pokeBtn = document.createElement('button');
      pokeBtn.className = 'ghost';
      pokeBtn.textContent = 'Poke';
      pokeBtn.title = 'Send a notification ping';
      pokeBtn.addEventListener('click', async (e) => {
        e.preventDefault();
        const target = peer.addr || peer.originalPub || peer.nknPub;
        if (!target) return;
        const ok = await sendPeerPayload(target, { type: 'peer-poke', note: '👋' });
        if (ok) setBadge?.('Poke sent');
        else setBadge?.('Poke failed', false);
      });
      actions.appendChild(pokeBtn);

      if (isHydra) {
        const chatBtn = document.createElement('button');
        chatBtn.className = 'ghost';
        chatBtn.textContent = 'Chat';
        chatBtn.title = 'Request to chat';
        chatBtn.addEventListener('click', async (e) => {
          e.preventDefault();
          await openChatWithPeer(peer);
        });
        actions.appendChild(chatBtn);
      } else {
        if (NoClip?.requestSync) {
          const syncBtn = document.createElement('button');
          syncBtn.className = 'secondary';
          syncBtn.textContent = 'Sync';
          syncBtn.title = 'Send bridge sync request';
          syncBtn.addEventListener('click', async (e) => {
            e.preventDefault();
            const normalized = normalizePubKey(peer?.nknPub || peer?.addr || peer?.originalPub);
            if (!normalized) {
              setBadge?.('No peer identifier available', false);
              return;
            }
            syncBtn.disabled = true;
            syncBtn.dataset.busy = 'true';
            try {
              await NoClip.requestSync(null, normalized);
            } catch (err) {
              console.error('[peers] sync request failed', err);
              setBadge?.(`Sync request failed: ${err?.message || err}`, false);
            } finally {
              delete syncBtn.dataset.busy;
              syncBtn.disabled = false;
            }
          });
          actions.appendChild(syncBtn);
        }
        const bridgeBtn = document.createElement('button');
        bridgeBtn.className = 'ghost';
        bridgeBtn.textContent = 'Bridge';
        bridgeBtn.title = 'Configure a NoClip Bridge node for this peer';
        bridgeBtn.addEventListener('click', (e) => {
          e.preventDefault();
          promptNoClipBridge(peer);
        });
        actions.appendChild(bridgeBtn);
      }

      entry.appendChild(actions);
      frag.appendChild(entry);
    });
    els.list.appendChild(frag);
    refreshStatus();
    renderChat();
  }

  function promptNoClipBridge(peer) {
    const target = normalizePubKey(peer?.nknPub || peer?.addr || peer?.originalPub);
    if (!target) {
      setBadge?.('No peer identifier available', false);
      return;
    }
    setBadge?.(`Configure a NoClip Bridge node with target ${target.slice(0, 8)}…`, true);
    hideModal();
  }

  function rememberPeersFromDiscovery() {
    if (!state.discovery) return;
    const nowSec = nowSeconds();
    state.discovery.peers.forEach((peer) => {
      if (!peer?.nknPub) return;
      const last = typeof peer.last === 'number' ? peer.last : 0;
      const online = last ? nowSec - last <= OFFLINE_AFTER_SECONDS : false;
      const result = upsertPeer(
        {
          ...peer,
          last,
          lastSeenAt: last ? last * 1000 : peer.lastSeenAt
        },
        { online, probing: false }
      );
      if (result.added) scheduleDirectoryBroadcast();
    });
    scheduleRender();
  }

  function attachDiscoveryEvents(client) {
    if (!client) return;
    client.on('peer', (peer) => {
      if (!peer?.nknPub) return;

      // Determine peer network based on addr/nknPub prefix
      const addr = peer.addr || peer.nknPub || '';
      const isNoclipPeer = addr.toLowerCase().startsWith('noclip.');
      const isHydraPeer = addr.toLowerCase().startsWith('hydra.') || addr.toLowerCase().startsWith('graph.');

      // If no recognizable prefix, default to hydra for backward compatibility
      const shouldAddToHydra = isHydraPeer || (!isNoclipPeer && !isHydraPeer);

      if (isNoclipPeer) {
        // Route noclip peers to the noclip peer store
        const normalized = normalizePeerEntry(peer);
        if (normalized) {
          const key = normalizePubKey(normalized.nknPub || normalized.addr);
          if (key) {
            state.noclip.peers.set(key, {
              ...normalized,
              source: 'noclip',
              online: true,
              last: typeof peer.last === 'number' ? peer.last : nowSeconds(),
              lastSeenAt: (typeof peer.last === 'number' ? peer.last : nowSeconds()) * 1000
            });
            if (state.layout.activeNetwork === 'noclip') scheduleRender();
          }
        }
        return; // Don't add to hydra peers
      }

      // Add hydra peers to main peer store
      if (shouldAddToHydra) {
        const last = typeof peer.last === 'number' ? peer.last : nowSeconds();
        const result = upsertPeer(
          {
            ...peer,
            last,
            lastSeenAt: last * 1000
          },
          { online: true, probing: false }
        );
        if (peer?.nknPub) state.pendingPings.delete(normalizePubKey(peer.nknPub));
        clearStatusOverride();
        scheduleRender();
        if (result.added) {
          broadcastDirectory(peer.nknPub);
          scheduleDirectoryBroadcast();
        }
      }
    });
    client.on('status', (info) => {
      if (!info) return;
      if (info.type === 'disconnect') {
        setStatus('Reconnecting to discovery...', true);
      } else if (info.type === 'reconnect') {
        clearStatusOverride();
        refreshStatus();
      }
    });


    client.on('dm', (payload) => {
      if (!payload || typeof payload !== 'object') return;
      if (payload.type === 'peer-directory') {
        const sender = normalizePubKey(payload.pub || payload.from);
        if (sender) {
          state.remoteHashes.set(sender, hashDirectory(payload.peers || []));
          if (sender !== normalizePubKey(state.mePub || state.meAddr)) {
            upsertPeer(
              {
                nknPub: sender,
                addr: payload.pub || payload.from || sender,
                last: payload.ts || nowSeconds(),
                meta: payload.meta || {}
              },
              {}
            );
          }
        }
        const list = Array.isArray(payload.peers) ? payload.peers : [];
        let added = false;
        list.forEach((entry) => {
          const normalized = normalizePeerEntry(entry);
          if (!normalized) return;
          if (normalized.nknPub === normalizePubKey(state.mePub || state.meAddr)) return;
          const sourceLabel =
            sanitizeUsername(payload.meta?.username || state.peers.get(sender)?.meta?.username || '') ||
            payload.from ||
            payload.pub ||
            sender;
          const sourceDescriptor = sender ? { key: sender, label: sourceLabel || sender } : null;
          const result = upsertPeer(normalized, {
            sharedFrom: sourceDescriptor,
            sharedSources: normalized.sharedSources
          });
          if (result.added) added = true;
        });
        const entries = collectDirectoryEntries();
        const localHash = hashDirectory(entries);
        if (sender && localHash) {
          const lastSent = state.sharedHashes.get(sender);
          if (localHash !== lastSent) {
            const peerInfo = state.peers.get(sender);
            const targetAddr = peerInfo?.addr || peerInfo?.originalPub || payload.pub || payload.from;
            if (targetAddr) {
              shareDirectory(entries, targetAddr);
              state.sharedHashes.set(sender, localHash);
            }
          }
        }
        if (added) scheduleDirectoryBroadcast();
        scheduleRender();
        return;
      }
      const sourceAddr = payload.pub || payload.from || payload.addr || '';
      if (handlePeerMessage(payload, { transport: 'nats', sourceAddr })) return;
    });

  }

  async function waitForNknAddress(timeoutMs = 20000) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      try {
        Net.ensureNkn();
      } catch (_) {
        // ignore ensure failures and retry
      }
      const addr = Net.nkn?.addr;
      if (addr) {
        if (Net.nkn?.client) attachNknMessageHandler(Net.nkn.client);
        return addr;
      }
      await wait(250);
    }
    throw new Error('NKN address unavailable');
  }

  async function ensureDiscovery() {
    const ensureNknFirst = waitForNknClient().catch(() => null);
    if (state.discovery) {
      await ensureNknFirst;
      return state.discovery;
    }
    if (state.connecting) {
      await ensureNknFirst;
      return state.connecting;
    }
    const promise = (async () => {
      setStatus('Waiting for NKN...', true);
      const addr = await waitForNknAddress();
      const pub = normalizePubKey(addr);
      state.meAddr = addr;
      state.mePub = pub;
      updateSelf();
      if (state.username) {
        try {
          state.store.upsert({
            nknPub: pub,
            addr,
            meta: { username: state.username },
            last: nowSeconds()
          });
        } catch (_) {
          // ignore storage errors
        }
      }
      const removedSelf = pruneSelfEntries();
      if (removedSelf) {
        scheduleRender();
        updateBadge();
      }

      const meta = getPresenceMeta();

      const client = new DiscoveryClient({
        servers: DEFAULT_SERVERS,
        room: state.room,
        me: { nknPub: pub, addr, meta },
        heartbeatSec: DEFAULT_HEARTBEAT_SEC,
        store: sharedStore
      });

      attachDiscoveryEvents(client);

      await client.connect();
      await client.startHeartbeat(meta);

      state.discovery = client;

      // Attach any external DM handlers that were registered
      if (state.externalDmHandlers && state.externalDmHandlers.length > 0) {
        state.externalDmHandlers.forEach(handler => {
          client.on('dm', handler);
        });
        console.log(`[PeerDiscovery] Attached ${state.externalDmHandlers.length} external DM handler(s)`);
      }

      await ensureNknFirst;
      await waitForNknClient().catch(() => null);
      updateDiscoveryMeta();
      rememberPeersFromDiscovery();
      pingAllPeers();
      updatePeerStatuses();
      clearStatusOverride();
      scheduleRender();
      scheduleDirectoryBroadcast(150);
      return client;
    })().catch((err) => {
      log?.(`[peers] discovery connect failed: ${err?.message || err}`);
      setStatus('Discovery unavailable', true);
      throw err;
    }).finally(() => {
      state.connecting = null;
    });

    state.connecting = promise;
    return promise;
  }

  function handleNoclipDm(evt) {
    if (!evt) return;
    const msg = evt.msg || {};
    const from = normalizePubKey(evt.from || msg.pub || msg.from);
    if (!from) return;
    const existing = state.noclip.peers.get(from) || { nknPub: from };
    const merged = {
      ...existing,
      nknPub: from,
      addr: msg.addr || existing.addr || evt.from || from,
      last: msg.ts || nowSeconds(),
      meta: { ...(existing.meta || {}), lastMessageType: msg.type || existing.meta?.lastMessageType || '' }
    };
    state.noclip.peers.set(from, merged);
    if (state.layout.activeNetwork === 'noclip') scheduleRender();
    updateBadge();
    notifyNoclipListeners();
  }

  async function ensureNoclipDiscovery() {
    if (state.noclip.discovery) return state.noclip.discovery;
    if (state.noclip.connecting) return state.noclip.connecting;
    const promise = (async () => {
      try {
        await waitForNknAddress(6000).catch(() => null);
      } catch (_) {
        // ignore
      }
      const addr = state.meAddr || Net.nkn?.addr || '';
      const pub = normalizePubKey(state.mePub || addr);
      const roomName = 'nexus';
      const discovery = await createNoClipDiscovery({
        room: roomName,
        servers: DEFAULT_SERVERS,
        me: {
          nknPub: pub || `hydra-${Math.random().toString(36).slice(2, 10)}`,
          addr,
          meta: { username: state.username || '', network: 'hydra' }
        }
      });
      discovery.on('peer', (peer) => {
        const normalized = normalizePeerEntry(peer);
        if (!normalized) return;
        const key = normalizePubKey(normalized.nknPub || normalized.addr);
        if (!key) return;
        state.noclip.peers.set(key, { ...normalized, source: 'noclip' });
        if (state.layout.activeNetwork === 'noclip') scheduleRender();
        updateBadge();
        notifyNoclipListeners();
      });
      discovery.on('dm', (evt) => handleNoclipDm(evt));
      discovery.on('status', (ev) => {
        if (!ev) return;
        state.noclip.status = { state: ev.type || 'update', detail: ev.data ? String(ev.data) : '' };
        if (state.layout.activeNetwork === 'noclip') scheduleRender();
      });
      state.noclip.status = { state: 'ready', detail: '' };
      state.noclip.discovery = discovery;
      return discovery;
    })().catch((err) => {
      state.noclip.status = { state: 'error', detail: err?.message || 'connect failed' };
      throw err;
    }).finally(() => {
      state.noclip.connecting = null;
    });
    state.noclip.connecting = promise;
    return promise;
  }

  function bindUI() {
    if (els.button && !els.button._peerBound) {
      els.button.addEventListener('click', (e) => {
        e.preventDefault();
        els.button.classList.remove('poke-notice');
        showModal();
        updateSelf();
        scheduleRender();
        ensureDiscovery()
          .then(() => {
            pingAllPeers();
            updatePeerStatuses();
          })
          .catch(() => {});
      });
      els.button._peerBound = true;
    }

    if (els.nameApply && !els.nameApply._peerBound) {
      els.nameApply.addEventListener('click', (e) => {
        e.preventDefault();
        const name = setUsername(els.nameInput?.value || '');
        if (els.nameInput) els.nameInput.value = name;
      });
      els.nameApply._peerBound = true;
    }

    if (els.nameInput && !els.nameInput._peerBound) {
      const commit = () => {
        const name = setUsername(els.nameInput.value || '');
        els.nameInput.value = name;
      };
      els.nameInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          e.preventDefault();
          commit();
        }
      });
      els.nameInput.addEventListener('blur', commit);
      els.nameInput._peerBound = true;
    }

    [els.backdrop, els.close].forEach((el) => {
      if (el && !el._peerBound) {
        el.addEventListener('click', (e) => {
          e.preventDefault();
          hideModal();
        });
        el._peerBound = true;
      }
    });

    if (els.search && !els.search._peerBound) {
      els.search.addEventListener('input', (e) => {
        const value = (e.target?.value || '').slice(0, 120);
        if (state.filters.search === value) return;
        state.filters.search = value;
        scheduleRender();
      });
      els.search._peerBound = true;
    }

    if (els.favToggle && !els.favToggle._peerBound) {
      els.favToggle.addEventListener('click', (e) => {
        e.preventDefault();
        state.filters.onlyFavorites = !state.filters.onlyFavorites;
        els.favToggle.classList.toggle('active', state.filters.onlyFavorites);
        scheduleRender();
      });
      els.favToggle._peerBound = true;
    }

    if (els.tabButtons && els.tabButtons.length) {
      els.tabButtons.forEach((btn) => {
        if (btn._peerBound) return;
        btn.addEventListener('click', (e) => {
          e.preventDefault();
          const pane = btn.dataset?.pane === 'chat' ? 'chat' : 'list';
          setActivePane(pane);
        });
        btn._peerBound = true;
      });
    }

    if (els.networkButtons && els.networkButtons.length) {
      els.networkButtons.forEach((btn) => {
        if (btn._peerNetworkBound) return;
        btn.addEventListener('click', (e) => {
          e.preventDefault();
          const network = btn.dataset?.network || 'hydra';
          setActiveNetwork(network);
        });
        btn._peerNetworkBound = true;
      });
    }

    if (els.chatFav && !els.chatFav._peerBound) {
      els.chatFav.addEventListener('click', () => {
        const key = state.chat.activePeer;
        if (!key) return;
        if (state.chat.favorites.has(key)) state.chat.favorites.delete(key);
        else state.chat.favorites.add(key);
        saveFavorites();
        renderPeers();
        renderChat();
      });
      els.chatFav._peerBound = true;
    }

    if (els.chatSend && !els.chatSend._peerBound) {
      const sendMessage = async () => {
        const key = state.chat.activePeer;
        if (!key) return;
        const value = (els.chatInput?.value || '').trim();
        if (!value) return;
        const addr = targetAddress(key);
        if (!addr) return;
        const id =
          (typeof crypto !== 'undefined' && crypto.randomUUID?.()) ||
          `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const ok = await sendPeerPayload(addr, { type: 'chat-message', id, text: value });
        if (ok) {
          appendHistory(key, { id, dir: 'out', text: value, ts: nowSeconds() });
        } else {
          setBadge?.('Message failed to send', false);
        }
        if (els.chatInput) els.chatInput.value = '';
        renderChat();
      };

      const typingState = { timer: null };
      const typingNotify = () => {
        const key = state.chat.activePeer;
        if (!key) return;
        const session = ensureChatSession(key);
        if (!session || session.status !== 'accepted') return;
        const addr = targetAddress(key);
        if (!addr) return;
        sendPeerPayload(addr, { type: 'chat-typing', isTyping: true }).catch(() => {});
        if (typingState.timer) clearTimeout(typingState.timer);
        typingState.timer = setTimeout(() => {
          sendPeerPayload(addr, { type: 'chat-typing', isTyping: false }).catch(() => {});
          typingState.timer = null;
        }, 1200);
      };

      els.chatSend.addEventListener('click', async (e) => {
        e.preventDefault();
        await sendMessage();
      });
      if (els.chatInput) {
        els.chatInput.addEventListener('keydown', async (e) => {
          if (e.key === 'Enter') {
            e.preventDefault();
            await sendMessage();
          }
        });
        els.chatInput.addEventListener('input', typingNotify);
      }
      els.chatSend._peerBound = true;
    }
  }

  async function init() {
    assignElements();
    bindUI();
    renderPeers();
    refreshStatus();
    if (!state.statusInterval) {
      state.statusInterval = setInterval(updatePeerStatuses, 15000);
    }
    try {
      await ensureDiscovery();
      updatePeerStatuses();
    } catch (_) {
      // swallow: modal will retry on demand
    }
  }

  /**
   * Register an external DM handler
   * This allows other modules to listen for DM messages
   */
  function registerDmHandler(handler) {
    if (typeof handler !== 'function') {
      console.warn('[PeerDiscovery] registerDmHandler requires a function');
      return;
    }

    // Store the handler to be attached when discovery is ready
    state.externalDmHandlers = state.externalDmHandlers || [];
    state.externalDmHandlers.push(handler);

    // If discovery is already active, attach immediately
    if (state.discovery) {
      state.discovery.on('dm', handler);
      console.log('[PeerDiscovery] External DM handler attached to active discovery');
    }
  }

  /**
   * Send a DM to a peer (proxy to discovery)
   */
  async function sendDm(pub, payload) {
    if (!state.discovery) {
      // Try to ensure discovery is ready
      await ensureDiscovery().catch(() => {
        throw new Error('Discovery not available');
      });
    }

    if (state.discovery && state.discovery.dm) {
      state.discovery.dm(pub, payload);
    } else {
      throw new Error('Discovery DM not available');
    }
  }

  function getNoclipPeers() {
    return Array.from(state.noclip.peers.values());
  }

  function subscribeNoclipPeers(listener) {
    if (typeof listener !== 'function') return () => {};
    noclipPeerListeners.add(listener);
    try {
      listener(getNoclipPeers());
    } catch (err) {
      console.warn('[PeerDiscovery] initial noclip listener error:', err);
    }
    ensureNoclipDiscovery().catch(() => {});
    return () => {
      noclipPeerListeners.delete(listener);
    };
  }

  return {
    init,
    registerDmHandler,
    sendDm,
    getNoclipPeers,
    subscribeNoclipPeers
  };

}


export { createPeerDiscovery };
