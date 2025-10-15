import { qs } from './utils.js';

const NATS_MODULE_URL = 'https://cdn.jsdelivr.net/npm/nats.ws/+esm';
const DEFAULT_SERVERS = ['wss://demo.nats.io:8443'];
const DEFAULT_HEARTBEAT_SEC = 12;

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const nowSeconds = () => Math.floor(Date.now() / 1000);

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
  constructor({ servers, room, me, heartbeatSec }) {
    super();
    this.servers = Array.isArray(servers) && servers.length ? servers : DEFAULT_SERVERS;
    this.room = sanitizeRoomName(room);
    this.me = { ...(me || {}) };
    this.heartbeatSec = typeof heartbeatSec === 'number' && heartbeatSec > 0 ? heartbeatSec : DEFAULT_HEARTBEAT_SEC;

    this.store = new Store(this.room);
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
  const state = {
    discovery: null,
    connecting: null,
    meAddr: '',
    room: sanitizeRoomName(
      `${window.location.host || 'local'}${(window.location.pathname || '').replace(/\//g, '-')}`
    ),
    peers: new Map()
  };

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
    return `${raw.slice(0, 8)}…${raw.slice(-6)}`;
  }

  function formatLast(last) {
    if (!last) return 'just now';
    const delta = Math.max(0, nowSeconds() - last);
    if (delta < 10) return 'just now';
    if (delta < 60) return `${delta}s ago`;
    if (delta < 3600) return `${Math.floor(delta / 60)}m ago`;
    return `${Math.floor(delta / 3600)}h ago`;
  }

  function setStatus(text) {
    if (!els.status) return;
    els.status.textContent = text;
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
  }

  function renderPeers() {
    if (!els.list) return;
    els.list.innerHTML = '';
    const peers = Array.from(state.peers.values())
      .filter((peer) => peer?.nknPub && peer.nknPub !== state.meAddr)
      .sort((a, b) => (b.last || 0) - (a.last || 0));
    if (!peers.length) {
      const empty = document.createElement('div');
      empty.className = 'peer-empty';
      empty.textContent = state.discovery ? 'No peers discovered yet.' : 'Discovery offline.';
      els.list.appendChild(empty);
      return;
    }
    const frag = document.createDocumentFragment();
    peers.forEach((peer) => {
      const entry = document.createElement('div');
      entry.className = 'peer-entry';

      const meta = document.createElement('div');
      meta.className = 'peer-meta';

      const nameEl = document.createElement('div');
      nameEl.className = 'peer-name';
      nameEl.textContent = formatAddress(peer.addr || peer.nknPub);
      meta.appendChild(nameEl);

      const infoEl = document.createElement('div');
      infoEl.className = 'peer-info';
      const parts = [];
      if (peer.meta?.graphId) parts.push(`Graph ${String(peer.meta.graphId).slice(0, 8)}`);
      parts.push(`Last seen ${formatLast(peer.last)}`);
      infoEl.textContent = parts.join(' • ');
      meta.appendChild(infoEl);

      entry.appendChild(meta);

      const actions = document.createElement('div');
      actions.className = 'peer-actions';
      const syncBtn = document.createElement('button');
      syncBtn.className = 'secondary';
      syncBtn.textContent = 'Sync';
      syncBtn.addEventListener('click', (e) => {
        e.preventDefault();
        const target = peer.addr || peer.nknPub;
        if (!target) return;
        WorkspaceSync.promptSync(target);
        hideModal();
      });
      actions.appendChild(syncBtn);
      entry.appendChild(actions);

      frag.appendChild(entry);
    });
    els.list.appendChild(frag);
  }

  function rememberPeersFromDiscovery() {
    if (!state.discovery) return;
    state.peers.clear();
    state.discovery.peers.forEach((peer) => {
      state.peers.set(peer.nknPub, peer);
    });
  }

  function attachDiscoveryEvents(client) {
    if (!client) return;
    client.on('peer', (peer) => {
      if (!peer?.nknPub) return;
      state.peers.set(peer.nknPub, peer);
      setStatus(`Online • ${state.peers.size} peer${state.peers.size === 1 ? '' : 's'}`);
      renderPeers();
    });
    client.on('status', (info) => {
      if (!info) return;
      if (info.type === 'disconnect') {
        setStatus('Reconnecting to discovery…');
      } else if (info.type === 'reconnect') {
        setStatus(`Online • ${state.peers.size} peer${state.peers.size === 1 ? '' : 's'}`);
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
      setStatus('Waiting for NKN…');
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
        heartbeatSec: DEFAULT_HEARTBEAT_SEC
      });

      attachDiscoveryEvents(client);

      await client.connect();
      await client.startHeartbeat(meta);

      state.discovery = client;
      rememberPeersFromDiscovery();
      setStatus(`Online • ${state.peers.size} peer${state.peers.size === 1 ? '' : 's'}`);
      renderPeers();
      return client;
    })().catch((err) => {
      log?.(`[peers] discovery connect failed: ${err?.message || err}`);
      setStatus('Discovery unavailable');
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
        ensureDiscovery().catch(() => {});
        renderPeers();
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
    try {
      await ensureDiscovery();
    } catch (_) {
      // swallow: modal will retry on demand
    }
  }

  return {
    init
  };
}

export { createPeerDiscovery };
