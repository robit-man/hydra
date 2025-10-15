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
  return text.trim();
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
    remoteHashes: new Map()
  };
  state.store = sharedStore;

  try {
    const storedName = LS.get(USERNAME_KEY, '');
    if (storedName) state.username = sanitizeUsername(storedName);
  } catch (_) {
    state.username = sanitizeUsername(state.username || '');
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
    self: null
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
    if (els.nameInput) els.nameInput.value = state.username || '';
    updateSelf();
    updateBadge();
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
    const sharedSources = sharedRaw
      .map((value) => {
        if (!value) return '';
        if (typeof value === 'string') return value;
        if (typeof value === 'object') {
          return value.label || value.addr || value.from || value.id || '';
        }
        return String(value || '');
      })
      .map((value) => value.trim())
      .filter(Boolean);
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
    if (normalizeKey(targetPub) === normalizeKey(state.meAddr)) return;
    const payload = {
      type: 'peer-directory',
      pub: state.meAddr,
      meta: { username: sanitizeUsername(state.username || ''), graphId: CFG.graphId || '' },
      peers: list
        .map(normalizePeerEntry)
        .filter(Boolean)
    };
    if (!payload.peers.length) return;
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
        sharedSources: (peer.sharedSources || []).map(({ label }) => label)
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
          last: entry.last || 0
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
    if (online !== undefined) merged.online = online;
    if (probing !== undefined) merged.probing = probing;

    const sharedMap = new Map();
    const addSource = (value) => {
      if (!value) return;
      const label = String(value).trim();
      if (!label) return;
      const key = normalizeKey(label);
      if (!key || key === merged.nknPub) return;
      if (!sharedMap.has(key)) sharedMap.set(key, label);
    };
    if (Array.isArray(existing.sharedSources)) {
      existing.sharedSources.forEach((item) => {
        if (!item) return;
        const label = typeof item === 'string' ? item : item.label || item.addr || item.key || '';
        addSource(label);
      });
    }
    if (Array.isArray(peer.sharedSources)) peer.sharedSources.forEach(addSource);
    if (Array.isArray(sharedSources)) sharedSources.forEach(addSource);
    if (peer.sharedFrom) addSource(peer.sharedFrom);
    if (sharedFrom) addSource(sharedFrom);

    const sharedArray = Array.from(sharedMap.entries()).map(([k, label]) => ({ key: k, label }));
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
        meta: { graphId: CFG.graphId || '' },
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
      nameEl.textContent = getDisplayName(peer);
      meta.appendChild(nameEl);

      const addrEl = document.createElement('div');
      addrEl.className = 'peer-address';
      addrEl.textContent = peer.addr || peer.originalPub || peer.nknPub;
      meta.appendChild(addrEl);

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

      if (Array.isArray(peer.sharedSources) && peer.sharedSources.length) {
        const sharedRow = document.createElement('div');
        sharedRow.className = 'peer-shared';
        sharedRow.append('Shared from ');
        const first = peer.sharedSources[0];
        const targetKey = first?.key || normalizeKey(first?.label) || '';
        const label = first?.label || formatAddress(first?.key || '');
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.textContent = formatAddress(label || targetKey);
        btn.addEventListener('click', () => scrollToPeer(targetKey));
        sharedRow.appendChild(btn);
        if (peer.sharedSources.length > 1) {
          const more = document.createElement('span');
          more.textContent = ` (+${peer.sharedSources.length - 1} more)`;
          sharedRow.appendChild(more);
        }
        entry.appendChild(sharedRow);
      }

      entry.appendChild(meta);

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
      entry.appendChild(actions);

      frag.appendChild(entry);
    });
    els.list.appendChild(frag);
    refreshStatus();
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
      if (payload.type === 'peer-directory') {
        const sender = normalizeKey(payload.pub || payload.from);
        if (sender) {
          state.remoteHashes.set(sender, hashDirectory(payload.peers || []));
          if (!state.peers.has(sender) && sender !== normalizeKey(state.meAddr)) {
            upsertPeer(
              {
                nknPub: sender,
                addr: payload.pub || payload.from || sender,
                last: nowSeconds(),
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
          const result = upsertPeer(normalized, {});
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

      const meta = {
        graphId: CFG.graphId || '',
        origin: window.location.origin || ''
      };

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
        showModal();
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

    [els.backdrop, els.close].forEach((el) => {
      if (el && !el._peerBound) {
        el.addEventListener('click', (e) => {
          e.preventDefault();
          hideModal();
        });
        el._peerBound = true;
      }
    });
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
