import { qs, LS } from './utils.js';

const NATS_MODULE_URL = 'https://cdn.jsdelivr.net/npm/nats.ws/+esm';
const DEFAULT_SERVERS = ['wss://demo.nats.io:8443'];
const DEFAULT_HEARTBEAT_SEC = 12;
const OFFLINE_AFTER_SECONDS = 45;
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
    const prev = this.memo.get(peer.nknPub) || {};
    const merged = {
      ...prev,
      ...peer,
      last: typeof peer.last === 'number' ? peer.last : nowSeconds()
    };
    this.memo.set(merged.nknPub, merged);
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
    const targetKey = normalizeKey(pub);
    if (!targetKey) return;
    const toDelete = [];
    this.memo.forEach((value, key) => {
      const keyNorm = normalizeKey(key);
      const pubNorm = normalizeKey(value?.nknPub);
      const addrNorm = normalizeKey(value?.addr);
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

    this.presenceSubject = `hydra.${this.room}.presence`;
    this.dmSubject = (pub) => `hydra.${this.room}.dm.${pub}`;
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

function createPeerDiscovery({ Net, CFG, WorkspaceSync, setBadge, log }) {
  const derivedRoom = sanitizeRoomName(
    `${window.location.host || 'local'}${(window.location.pathname || '').replace(/\//g, '-')}`
  );
  const sharedStore = new Store(derivedRoom);
  const state = {
    discovery: null,
    connecting: null,
    meAddr: '',
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
    chat: {
      activePeer: null,
      sessions: new Map(),
      typingPeers: new Map(),
      typingTimers: new Map(),
      favorites: new Set(),
      promptMessage: null
    },
    pokeNoticeTimer: null
  };
  state.store = sharedStore;

  const FAVORITES_KEY = `hydra.favorites.${state.room}`;
  const historyKey = (peerKey) => {
    const normalized = normalizeKey(peerKey);
    return normalized ? `hydra.chat.history.${state.room}.${normalized}` : '';
  };
  const sessionKey = (peerKey) => {
    const normalized = normalizeKey(peerKey);
    return normalized ? `hydra.chat.session.${state.room}.${normalized}` : '';
  };

  const loadFavorites = () => {
    if (typeof localStorage === 'undefined') return new Set();
    try {
      const raw = localStorage.getItem(FAVORITES_KEY);
      if (!raw) return new Set();
      const list = JSON.parse(raw);
      return new Set(Array.isArray(list) ? list.map((value) => normalizeKey(value)).filter(Boolean) : []);
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

  try {
    const storedName = LS.get(USERNAME_KEY, '');
    if (storedName) state.username = sanitizeUsername(storedName);
  } catch (_) {
    // ignore storage load issues
  }
  state.username = sanitizeUsername(state.username || '');
  state.chat.favorites = loadFavorites();

  function getPresenceMeta() {
    return {
      graphId: CFG.graphId || '',
      origin: window.location.origin || '',
      username: sanitizeUsername(state.username || '')
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
    chatFav: null
  };

  function assignElements() {
    els.button = qs('#peerListButton');
    els.modal = qs('#peerModal');
    els.backdrop = qs('#peerBackdrop');
    els.close = qs('#peerClose');
    els.list = qs('#peerList');
    els.status = qs('#peerStatus');
    els.self = qs('#peerSelf');
    els.nameInput = qs('#peerUsernameInput');
    els.nameApply = qs('#peerUsernameApply');
    els.nameBadge = qs('#peerSelfName');
    els.badgeCount = qs('#peerOnlineBadge');
    els.chatWrap = qs('#chatWrap');
    els.chatName = qs('#chatPeerName');
    els.chatStatus = qs('#chatStatus');
    els.chatMsgs = qs('#chatMessages');
    els.chatTyping = qs('#chatTyping');
    els.chatInput = qs('#chatInput');
    els.chatSend = qs('#chatSend');
    els.chatFav = qs('#chatFavoriteBtn');
    if (els.nameInput) els.nameInput.value = state.username || '';
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
    if (!els.badgeCount) return;
    const online = Array.from(state.peers.values()).filter((peer) => peer?.online).length;
    if (online > 0) {
      els.badgeCount.textContent = String(online);
      els.badgeCount.classList.remove('hidden');
    } else {
      els.badgeCount.classList.add('hidden');
    }
  }

  function pruneSelfEntries() {
    const selfKey = normalizeKey(state.meAddr);
    if (!selfKey) return false;
    const toDelete = [];
    state.peers.forEach((peer, key) => {
      const keyNorm = normalizeKey(key);
      const peerKey = normalizeKey(peer?.nknPub);
      const addrKey = normalizeKey(peer?.addr);
      const originalKey = normalizeKey(peer?.originalPub);
      if (keyNorm === selfKey || peerKey === selfKey || addrKey === selfKey || originalKey === selfKey) {
        toDelete.push(key);
      }
    });
    if (state.store?.remove) state.store.remove(state.meAddr);
    if (!toDelete.length) return false;
    toDelete.forEach((key) => state.peers.delete(key));
    return true;
  }

  function updateDiscoveryMeta() {
    if (!state.discovery) return;
    const meta = getPresenceMeta();
    state.discovery.me = state.discovery.me || {};
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
            nknPub: state.meAddr,
            addr: state.meAddr,
            meta: { username: sanitized },
            last: nowSeconds()
          });
        } catch (_) {
          // ignore storage issues
        }
        const removedSelf = pruneSelfEntries();
        if (removedSelf) updateBadge();
        const selfKey = normalizeKey(state.meAddr);
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
    const nknPub = normalizeKey(rawPub);
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
        sourceKey = normalizeKey(value.key || value.addr || value.from || label);
      } else {
        label = String(value || '').trim();
        sourceKey = normalizeKey(label);
      }
      label = label ? label.trim() : '';
      if (!sourceKey) sourceKey = normalizeKey(label);
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
    if (!targetPub) return;
    const targetKey = normalizeKey(targetPub);
    if (targetKey === normalizeKey(state.meAddr)) return;
    const peers = list
      .map(normalizePeerEntry)
      .filter(
        (entry) =>
          entry &&
          entry.nknPub !== targetKey &&
          (entry.originalPub ? normalizeKey(entry.originalPub) !== targetKey : true)
      );
    if (!peers.length) return;
    const payload = {
      type: 'peer-directory',
      pub: state.meAddr,
      meta: { username: sanitizeUsername(state.username || ''), graphId: CFG.graphId || '' },
      peers
    };
    try {
      state.discovery.dm(targetPub, payload);
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
        nknPub: state.meAddr,
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
      ? [normalizeKey(targetPub)]
      : Array.from(state.peers.keys());
    targets.forEach((key) => {
      if (!key || key === normalizeKey(state.meAddr)) return;
      const peer = state.peers.get(key);
      const address = peer?.addr || peer?.originalPub || key;
      if (!address) return;
      if (!targetPub && hash && state.sharedHashes.get(key) === hash) return;
      shareDirectory(entries, address);
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
    const key = normalizeKey(peer?.nknPub || peer?.addr || '');
    if (!key) return { added: false, entry: null };
    const selfKey = normalizeKey(state.meAddr);
    const peerAddrKey = normalizeKey(peer?.addr);
    const originalKey = normalizeKey(peer?.originalPub);
    if (selfKey && (key === selfKey || peerAddrKey === selfKey || originalKey === selfKey)) {
      state.peers.delete(key);
      if (state.store?.remove) state.store.remove(state.meAddr);
      return { added: false, entry: null };
    }
    const existing = state.peers.get(key) || {};
    const added = !existing.nknPub;
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
        sourceKey = normalizeKey(value.key || value.addr || value.from || label);
      } else {
        label = String(value || '').trim();
        sourceKey = normalizeKey(label);
      }
      label = label ? label.trim() : '';
      if (!sourceKey) sourceKey = normalizeKey(label);
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
        nknPub: merged.originalPub || merged.nknPub,
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
    const key = normalizeKey(pub);
    if (!key || !state.discovery) return;
    if (state.pendingPings.has(key)) return;
    const peer = state.peers.get(key);
    const target = peer?.addr || peer?.originalPub || pub;
    if (!target) return;
    try {
      const payload = {
        type: 'peer-ping',
        addr: state.meAddr || Net.nkn?.addr || '',
        meta: getPresenceMeta(),
        ts: nowSeconds()
      };
      state.discovery.dm(target, payload);
      state.pendingPings.set(key, Date.now());
      if (peer) {
        peer.probing = true;
        state.peers.set(key, peer);
      }
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
    } catch (err) {
      log?.(`[peers] ping failed: ${err?.message || err}`);
    }
  }

  function pingAllPeers() {
    if (!state.discovery) return;
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
    if (typeof peer === 'string') return normalizeKey(peer);
    return normalizeKey(peer.addr || peer.originalPub || peer.nknPub);
  }

  function peerByKey(key) {
    const normalized = normalizeKey(key);
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
    const normalized = normalizeKey(peerKey);
    if (!normalized) return null;
    const existing = state.chat.sessions.get(normalized);
    if (existing) return existing;
    const stored = loadSession(normalized);
    const session = stored ? { ...stored } : { status: 'idle' };
    state.chat.sessions.set(normalized, session);
    return session;
  }

  function setSession(peerKey, nextSession) {
    const normalized = normalizeKey(peerKey);
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
    const normalized = normalizeKey(key);
    state.chat.activePeer = normalized || null;
    if (normalized) ensureChatSession(normalized);
    renderChat();
  }

  function appendHistory(peerKey, message) {
    const normalized = normalizeKey(peerKey);
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

  function clearInlineChatDecision() {
    const row = els.chatWrap?.querySelector('.chat-decision-row');
    if (row) row.remove();
  }

  function respondToChat(peerKey, accept) {
    const normalized = normalizeKey(peerKey);
    if (!normalized) return;
    if (state.chat.promptMessage?.key === normalized) state.chat.promptMessage = null;
    const addr = targetAddress(normalized);
    if (addr && state.discovery) {
      try {
        state.discovery.dm(addr, { type: 'chat-response', accepted: !!accept });
      } catch (_) {
        // ignore send failure
      }
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
      accept.addEventListener('click', () => respondToChat(fromKey, true));
      const decline = document.createElement('button');
      decline.className = 'ghost';
      decline.textContent = 'Decline';
      decline.addEventListener('click', () => respondToChat(fromKey, false));
      row.appendChild(accept);
      row.appendChild(decline);
      els.chatStatus?.insertAdjacentElement('afterend', row);
    }
  }

  function renderChat() {
    if (!els.chatWrap) return;
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

  function openChatWithPeer(peer) {
    const key = normalizePeerKey(peer);
    if (!key) return;
    setActiveChat(key);
    const proceed = state.discovery ? Promise.resolve(state.discovery) : ensureDiscovery();
    proceed
      .then(() => {
        const session = ensureChatSession(key);
        if (session?.status === 'accepted' || session?.status === 'pending') {
          renderChat();
          return;
        }
        const addr = targetAddress(peer) || targetAddress(key);
        if (!addr || !state.discovery) {
          renderChat();
          return;
        }
        const fromName = sanitizeUsername(state.username || '');
        try {
          state.discovery.dm(addr, { type: 'chat-request', fromName });
          setSession(key, { status: 'pending', lastTs: nowSeconds() });
        } catch (_) {
          // ignore send failure
        }
        renderChat();
      })
      .catch(() => {
        renderChat();
      });
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
    const key = normalizeKey(peerKey);
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
    const now = nowSeconds();
    const peers = Array.from(state.peers.values()).filter((peer) => peer?.nknPub);
    peers.sort((a, b) => {
      const lastA = a.last || now;
      const lastB = b.last || now;
      const deltaA = Math.max(0, now - lastA);
      const deltaB = Math.max(0, now - lastB);
      const bucketA = Math.floor(deltaA / 60);
      const bucketB = Math.floor(deltaB / 60);
      if (bucketA !== bucketB) return bucketA - bucketB;
      const nameA = getSortName(a);
      const nameB = getSortName(b);
      if (nameA !== nameB) return nameA.localeCompare(nameB);
      return (a.addr || a.nknPub || '').localeCompare(b.addr || b.nknPub || '');
    });
    if (!peers.length) {
      const empty = document.createElement('div');
      empty.className = 'peer-empty';
      empty.textContent = state.discovery ? 'No peers discovered yet.' : 'Discovery offline.';
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
      const isFavorite = state.chat.favorites.has(normalizePeerKey(peer));
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
      if (peer.meta?.graphId) parts.push(`Graph ${String(peer.meta.graphId).slice(0, 8)}`);
      infoEl.textContent = parts.join(' • ');
      meta.appendChild(infoEl);

      entry.appendChild(meta);

      if (Array.isArray(peer.sharedSources) && peer.sharedSources.length) {
        const sharedRow = document.createElement('div');
        sharedRow.className = 'peer-shared';
        sharedRow.append('Shared from ');
        const first = peer.sharedSources[0];
        const targetKey = first?.key || normalizeKey(first?.label) || '';
        if (!targetKey) {
          return;
        }
        const label = first?.label || formatAddress(first?.key || '');
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.textContent = label || formatAddress(targetKey);
        btn.addEventListener('click', () => scrollToPeer(targetKey));
        sharedRow.appendChild(btn);
        if (peer.sharedSources.length > 1) {
          const more = document.createElement('span');
          more.textContent = ` (+${peer.sharedSources.length - 1} more)`;
          sharedRow.appendChild(more);
        }
        entry.appendChild(sharedRow);
      }

      const actions = document.createElement('div');
      actions.className = 'peer-actions';
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

      const pokeBtn = document.createElement('button');
      pokeBtn.className = 'ghost';
      pokeBtn.textContent = 'Poke';
      pokeBtn.title = 'Send a notification ping';
      pokeBtn.addEventListener('click', (e) => {
        e.preventDefault();
        const target = targetAddress(peer);
        if (!target || !state.discovery) return;
        try {
          state.discovery.dm(target, { type: 'peer-poke', note: '👋' });
          setBadge?.('Poke sent');
        } catch (_) {
          // ignore send failure
        }
      });
      actions.appendChild(pokeBtn);

      const chatBtn = document.createElement('button');
      chatBtn.className = 'ghost';
      chatBtn.textContent = 'Chat';
      chatBtn.title = 'Request to chat';
      chatBtn.addEventListener('click', (e) => {
        e.preventDefault();
        openChatWithPeer(peer);
      });
      actions.appendChild(chatBtn);

      entry.appendChild(actions);

      frag.appendChild(entry);
    });
    els.list.appendChild(frag);
    refreshStatus();
    renderChat();
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
      const last = typeof peer.last === 'number' ? peer.last : nowSeconds();
      const result = upsertPeer(
        {
          ...peer,
          last,
          lastSeenAt: last * 1000
        },
        { online: true, probing: false }
      );
      if (peer?.nknPub) state.pendingPings.delete(normalizeKey(peer.nknPub));
      clearStatusOverride();
      scheduleRender();
      if (result.added) {
        broadcastDirectory(peer.nknPub);
        scheduleDirectoryBroadcast();
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
      const senderKey = normalizeKey(payload.pub || payload.from);
      let peer = senderKey ? state.peers.get(senderKey) : null;
      if (!peer && senderKey) {
        const result = upsertPeer(
          {
            nknPub: senderKey,
            addr: payload.pub || payload.from || senderKey,
            last: payload.ts || nowSeconds(),
            meta: payload.meta || {}
          },
          {}
        );
        peer = result.entry;
        if (result.added) scheduleRender();
      } else if (peer && payload.ts) {
        peer.last = payload.ts;
        peer.lastSeenAt = payload.ts * 1000;
        peer.online = true;
        state.peers.set(senderKey, peer);
      }

      switch (payload.type) {
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
          setBadge?.(`Poke from ${getDisplayName(peer || { addr: payload.pub || senderKey })}`);
          scheduleRender();
          break;
        }
        case 'chat-request': {
          if (!senderKey) break;
          const label = getDisplayName(peer || { addr: payload.pub || senderKey });
          state.chat.promptMessage = { key: senderKey, text: `${label} wants to chat. Accept?` };
          setSession(senderKey, { status: 'pending', lastTs: nowSeconds() });
          setActiveChat(senderKey);
          setBadge?.(`Chat request from ${label}`);
          showInlineChatDecision(senderKey);
          renderChat();
          scheduleRender();
          break;
        }
        case 'chat-response': {
          if (!senderKey) break;
          const accepted = !!payload.accepted;
          const label = getDisplayName(peer || { addr: payload.pub || senderKey });
          setSession(senderKey, { status: accepted ? 'accepted' : 'declined', lastTs: nowSeconds() });
          if (state.chat.promptMessage?.key === senderKey) state.chat.promptMessage = null;
          setBadge?.(accepted ? `Chat accepted by ${label}` : `Chat declined by ${label}`, accepted);
          clearInlineChatDecision();
          if (state.chat.activePeer === senderKey) renderChat();
          scheduleRender();
          break;
        }
        case 'chat-typing': {
          if (!senderKey) break;
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
          break;
        }
        case 'chat-message': {
          if (!senderKey) break;
          const label = getDisplayName(peer || { addr: payload.pub || senderKey });
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
          break;
        }
        case 'peer-directory': {
          const sender = senderKey;
          if (sender) {
            state.remoteHashes.set(sender, hashDirectory(payload.peers || []));
            if (sender !== normalizeKey(state.meAddr)) {
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
            if (normalized.nknPub === normalizeKey(state.meAddr)) return;
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
        default:
          break;
      }
    });
  }

  async function waitForNknAddress(timeoutMs = 20000) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      Net.ensureNkn();
      const addr = Net.nkn?.addr;
      if (addr) return addr;
      await wait(250);
    }
    throw new Error('NKN address unavailable');
  }

  async function ensureDiscovery() {
    if (state.discovery) return state.discovery;
    if (state.connecting) return state.connecting;
    const promise = (async () => {
      setStatus('Waiting for NKN...', true);
      const addr = await waitForNknAddress();
      state.meAddr = addr;
      updateSelf();
      if (state.username) {
        try {
          state.store.upsert({
            nknPub: addr,
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
        me: { nknPub: addr, addr, meta },
        heartbeatSec: DEFAULT_HEARTBEAT_SEC,
        store: sharedStore
      });

      attachDiscoveryEvents(client);

      await client.connect();
      await client.startHeartbeat(meta);

      state.discovery = client;
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
      const sendMessage = () => {
        const key = state.chat.activePeer;
        if (!key) return;
        const value = (els.chatInput?.value || '').trim();
        if (!value) return;
        const addr = targetAddress(key);
        if (!addr || !state.discovery) return;
        const id =
          (typeof crypto !== 'undefined' && crypto.randomUUID?.()) ||
          `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        try {
          state.discovery.dm(addr, { type: 'chat-message', id, text: value });
          appendHistory(key, { id, dir: 'out', text: value, ts: nowSeconds() });
        } catch (_) {
          // ignore send failure
        }
        if (els.chatInput) els.chatInput.value = '';
        renderChat();
      };

      const typingState = { timer: null };
      const typingNotify = () => {
        const key = state.chat.activePeer;
        if (!key || !state.discovery) return;
        const session = ensureChatSession(key);
        if (!session || session.status !== 'accepted') return;
        const addr = targetAddress(key);
        if (!addr) return;
        try {
          state.discovery.dm(addr, { type: 'chat-typing', isTyping: true });
        } catch (_) {
          // ignore typing errors
        }
        if (typingState.timer) clearTimeout(typingState.timer);
        typingState.timer = setTimeout(() => {
          try {
            state.discovery.dm(addr, { type: 'chat-typing', isTyping: false });
          } catch (_) {
            // ignore
          }
          typingState.timer = null;
        }, 1200);
      };

      els.chatSend.addEventListener('click', (e) => {
        e.preventDefault();
        sendMessage();
      });
      if (els.chatInput) {
        els.chatInput.addEventListener('keydown', (e) => {
          if (e.key === 'Enter') {
            e.preventDefault();
            sendMessage();
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

  return {
    init
  };
}

export { createPeerDiscovery };
