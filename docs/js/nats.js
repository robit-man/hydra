// nats.js — Tiny NATS-backed discovery for NKN peers (browser or node)
// - Presence beacons on discovery.<room>.presence
// - Encrypted DMs are optional; this file keeps it plaintext for simplicity
//   (you can layer nacl-box on top if you want)
// - Persists discovered peers (Node: ~/.nats_discovery/peers.<room>.json; Browser: localStorage)
//
// Usage (Node):
//   import { NatsDiscovery } from './nats.js'
//   const disco = new NatsDiscovery({ room: 'public', me: { nknPub: '<YOUR_NKN_ADDR>', meta:{ver:'1'} } })
//   await disco.connect()
//   disco.on('peer', (p)=> console.log('peer:', p))
//   await disco.startHeartbeat()  // begin presence every 10s
//
// Usage (Browser):
//   import { NatsDiscovery } from '/nats.js'
//   const disco = new NatsDiscovery({ room: 'public', me: { nknPub: 'NKNxxxxx' } })
//   await disco.connect()
//   disco.on('peer', console.log)
//   await disco.startHeartbeat()
//
// Minimal API exported at bottom.

const proc = typeof process !== 'undefined' ? process : null;

const DEFAULTS = {
  // Public demo NATS websocket (works from browsers too)
  servers: ['wss://demo.nats.io:8443'],
  heartbeatSec: 10,
  persistDir: proc?.env?.NATS_DISCOVERY_DIR || (proc?.platform ? `${require('os').homedir()}/.nats_discovery` : null),
};

function nowS() { return Math.floor(Date.now() / 1000); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ---------------------------------------------------------------------------
// Storage adapter (Node FS or browser localStorage)
// ---------------------------------------------------------------------------
class Store {
  constructor(room, persistDir) {
    this.room = room;
    this.persistDir = persistDir;
    this.memo = new Map();
    if (typeof window === 'undefined' && persistDir) {
      const fs = require('fs');
      const path = require('path');
      this.fs = fs; this.path = path;
      fs.mkdirSync(persistDir, { recursive: true });
      this.file = path.join(persistDir, `peers.${room}.json`);
      try {
        const raw = fs.readFileSync(this.file, 'utf8');
        const arr = JSON.parse(raw);
        arr.forEach(p => this.memo.set(p.nknPub, p));
      } catch (_) {}
    } else if (typeof window !== 'undefined') {
      this.lsKey = `nats_discovery:${room}`;
      try {
        const raw = localStorage.getItem(this.lsKey);
        const arr = raw ? JSON.parse(raw) : [];
        arr.forEach(p => this.memo.set(p.nknPub, p));
      } catch (_) {}
    }
  }
  all() { return Array.from(this.memo.values()); }
  upsert(peer) {
    const prev = this.memo.get(peer.nknPub) || {};
    const merged = { ...prev, ...peer, last: peer.last || nowS() };
    this.memo.set(merged.nknPub, merged);
    this.flush();
    return merged;
  }
  flush() {
    const arr = this.all();
    if (this.file && this.fs) {
      try { this.fs.writeFileSync(this.file, JSON.stringify(arr, null, 2)); } catch (_) {}
    } else if (this.lsKey && typeof localStorage !== 'undefined') {
      try { localStorage.setItem(this.lsKey, JSON.stringify(arr)); } catch (_) {}
    }
  }
}

// ---------------------------------------------------------------------------
// Tiny Event Emitter
// ---------------------------------------------------------------------------
class Evt {
  constructor() { this.handlers = new Map(); }
  on(evt, fn) { if (!this.handlers.has(evt)) this.handlers.set(evt, new Set()); this.handlers.get(evt).add(fn); return () => this.off(evt, fn); }
  off(evt, fn) { this.handlers.get(evt)?.delete(fn); }
  emit(evt, data) { this.handlers.get(evt)?.forEach(fn => { try { fn(data); } catch {} }); }
}

// ---------------------------------------------------------------------------
// NATS client (nats.ws). For Node we provide a WebSocket factory using 'ws'.
// ---------------------------------------------------------------------------
async function loadNats() {
  // dynamic import to work in both Node and Browser
  const mod = await import('nats.ws'); // ESM
  return mod;
}
function nodeWsFactory(url, opts) {
  const WS = require('ws');
  return new WS(url, undefined, opts);
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------
export class NatsDiscovery extends Evt {
  constructor(opts) {
    super();
    const {
      servers = DEFAULTS.servers,
      room = 'public',
      me = { nknPub: 'unknown', meta: {} },
      heartbeatSec = DEFAULTS.heartbeatSec,
      persistDir = DEFAULTS.persistDir,
      name = `disco-${(me.nknPub||'anon').slice(0, 8)}`
    } = opts || {};
    this.servers = servers;
    this.room = room;
    this.me = { ...me };
    this.heartbeatSec = heartbeatSec;
    this.persist = new Store(room, persistDir);
    this.nc = null;
    this.sc = null; // String codec
    this._hb = null;

    // subjects
    this.subPresence = `discovery.${room}.presence`;
    this.subDM = (pub) => `discovery.dm.${pub}`;
  }

  get peers() {
    return this.persist.all().sort((a,b)=> (b.last||0)-(a.last||0));
  }

  async connect() {
    const { connect, StringCodec, Events } = await loadNats();
    const connOpts = {
      servers: this.servers,
      name: this.name,
      // For Node, nats.ws needs a websocket factory:
      ...(typeof window === 'undefined' ? { websocketFactory: nodeWsFactory } : {})
    };
    this.nc = await connect(connOpts);
    this.sc = StringCodec();

    // Presence fan-in
    const subP = await this.nc.subscribe(this.subPresence);
    (async () => {
      for await (const m of subP) {
        const msg = this._decode(m);
        if (!msg || msg.type !== 'presence') continue;
        if (msg.pub === this.me.nknPub) continue; // ignore self
        const peer = this.persist.upsert({
          nknPub: msg.pub,
          addr: msg.addr || msg.pub, // for parity with your Python code
          meta: msg.meta || {},
          last: msg.ts || nowS()
        });
        this.emit('peer', peer);
      }
    })();

    // Personal DM inbox
    const subD = await this.nc.subscribe(this.subDM(this.me.nknPub));
    (async () => {
      for await (const m of subD) {
        const msg = this._decode(m);
        if (!msg || !msg.type) continue;
        if (msg.type === 'handshake') {
          const peer = this.persist.upsert({
            nknPub: msg.pub,
            addr: msg.addr || msg.pub,
            meta: msg.meta || {},
            last: msg.ts || nowS()
          });
          this.emit('handshake', peer);
          // optional auto-ack
          if (msg.wantAck) await this.dm(msg.pub, { type: 'handshake_ack', pub: this.me.nknPub, meta: this.me.meta || {}, ts: nowS() });
          this.emit('peer', peer);
        } else if (msg.type === 'handshake_ack') {
          const peer = this.persist.upsert({
            nknPub: msg.pub,
            last: msg.ts || nowS()
          });
          this.emit('handshake_ack', peer);
        } else {
          this.emit('dm', { from: msg.pub, msg });
        }
      }
    })();

    // lifecycle
    (async () => {
      for await (const s of this.nc.status()) {
        if (s.type === Events.Disconnect) this.emit('status', { type: 'disconnect', data: s.data });
        if (s.type === Events.Reconnect) this.emit('status', { type: 'reconnect', data: s.data });
        if (s.type === Events.Update) this.emit('status', { type: 'update', data: s.data });
      }
    })();

    return this;
  }

  // Publish a presence beacon
  async presence(extraMeta = {}) {
    if (!this.nc) throw new Error('not connected');
    const payload = {
      type: 'presence',
      pub: this.me.nknPub,
      addr: this.me.addr || this.me.nknPub,
      meta: { ...(this.me.meta || {}), ...extraMeta },
      ts: nowS()
    };
    this.nc.publish(this.subPresence, this._encode(payload));
  }

  // Start periodic presence
  async startHeartbeat() {
    if (this._hb) return;
    await this.presence();
    this._hb = setInterval(() => { this.presence().catch(()=>{}); }, this.heartbeatSec * 1000);
  }

  stopHeartbeat() { if (this._hb) { clearInterval(this._hb); this._hb = null; } }

  // Change room (resubscribes)
  async setRoom(room) {
    this.stopHeartbeat();
    this.room = room;
    this.persist = new Store(room, this.persist.persistDir);
    this.subPresence = `discovery.${room}.presence`;
    // Reconnect/re-sub simplest path:
    await this.close();
    await this.connect();
  }

  // Send a direct message (JSON object) to a peer
  async dm(toPub, obj) {
    if (!this.nc) throw new Error('not connected');
    const subj = this.subDM(toPub);
    const msg = { ...obj, pub: this.me.nknPub, ts: nowS() };
    this.nc.publish(subj, this._encode(msg));
  }

  // Initiate a handshake to one peer (they'll store you; you store them on ack)
  async handshake(toPub, meta = {}, { wantAck = true } = {}) {
    await this.dm(toPub, {
      type: 'handshake',
      pub: this.me.nknPub,
      addr: this.me.addr || this.me.nknPub,
      meta,
      wantAck
    });
  }

  // Broadcast handshake to all currently visible peers
  async handshakeAll(meta = {}, { wantAck = false } = {}) {
    const seen = new Set();
    this.peers.forEach(p => { if (p.nknPub !== this.me.nknPub) seen.add(p.nknPub); });
    // Also probe recent presence listeners by blasting a presence first:
    await this.presence();
    await sleep(200); // tiny settle
    for (const pub of seen) await this.handshake(pub, meta, { wantAck });
  }

  // Helpers
  _encode(obj) { return this.sc.encode(JSON.stringify(obj)); }
  _decode(msg) { try { return JSON.parse(this.sc.decode(msg.data)); } catch { return null; } }

  async close() {
    this.stopHeartbeat();
    if (this.nc) {
      const done = this.nc.closed();
      await this.nc.drain().catch(()=>this.nc.close());
      await done.catch(()=>{});
      this.nc = null;
    }
  }
}

// ---------------------------------------------------------------------------
// Convenience tiny facade for “simple API” consumers
// ---------------------------------------------------------------------------
export async function createDiscovery({ room, me, servers, heartbeatSec, persistDir, name } = {}) {
  const d = new NatsDiscovery({ room, me, servers, heartbeatSec, persistDir, name });
  await d.connect();
  await d.startHeartbeat();
  return d;
}
