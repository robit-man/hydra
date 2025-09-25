import { CFG } from './config.js';
import { setBadge, b64ToBytes, td, LS } from './utils.js';

let updateTransportButton = () => {};

const NKN_SEED_KEY = 'graph.nkn.seed';

const Net = {
  nkn: { client: null, ready: false, addr: '', pend: new Map(), streams: new Map() },

  setTransportUpdater(fn) {
    updateTransportButton = typeof fn === 'function' ? fn : () => {};
  },

  auth(headers = {}, apiKey) {
    const out = { ...headers };
    if (!out['Content-Type']) out['Content-Type'] = 'application/json';
    if (apiKey) {
      if (/^Bearer\s+/i.test(apiKey)) out['Authorization'] = apiKey;
      else out['X-API-Key'] = apiKey;
    }
    return out;
  },

  async getJSON(base, path, api, useNkn, relay) {
    if (!useNkn) {
      const headers = this.auth({}, api);
      delete headers['Content-Type'];
      headers['Accept'] = 'application/json';
      const res = await fetch(base.replace(/\/+$/, '') + path, { headers });
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      return res.json();
    }
    return this.nknFetch(base, path, 'GET', null, api, relay);
  },

  async postJSON(base, path, body, api, useNkn, relay, timeout = 45000) {
    if (!useNkn) {
      const res = await fetch(base.replace(/\/+$/, '') + path, {
        method: 'POST',
        headers: this.auth({}, api),
        body: JSON.stringify(body || {})
      });
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      return res.json();
    }
    return this.nknFetch(base, path, 'POST', body, api, relay, timeout);
  },

  async fetchBlob(fullUrl, useNkn, relay, api) {
    if (!useNkn) {
      const res = await fetch(fullUrl, { headers: this.auth({}, api) });
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      return res.blob();
    }
    return this.nknFetchBlob(fullUrl, relay, api);
  },

  ensureNkn() {
    if (CFG.transport !== 'nkn') return;
    if (this.nkn.client) return;
    if (!window.nkn || !window.nkn.MultiClient) {
      setBadge('nkn-sdk missing', false);
      return;
    }
    let seed = null;
    try {
      seed = LS.get(NKN_SEED_KEY, null);
    } catch (err) {
      seed = null;
    }

    const normalizeSeedForUse = (value) => {
      if (!value) return null;
      if (typeof value === 'string') return value;
      if (Array.isArray(value)) return Uint8Array.from(value);
      if (value instanceof Uint8Array) return value;
      if (typeof value === 'object' && typeof value.seed === 'string') return value.seed;
      return null;
    };

    let options = { identifier: 'graph', numSubClients: 4, wsConnHeartbeatTimeout: 60000 };
    const normalizedSeed = normalizeSeedForUse(seed);
    if (normalizedSeed) options.seed = normalizedSeed;
    let client;
    try {
      client = new window.nkn.MultiClient(options);
    } catch (err) {
      if (normalizedSeed) {
        try { LS.del(NKN_SEED_KEY); } catch (_) {}
        options = { identifier: 'graph', numSubClients: 4, wsConnHeartbeatTimeout: 60000 };
        client = new window.nkn.MultiClient(options);
      } else {
        throw err;
      }
    }
    const derivedSeed = client?.key?.seed || client?.key?.seedHex;
    if (derivedSeed) {
      let storeSeed = null;
      if (typeof derivedSeed === 'string') storeSeed = derivedSeed;
      else if (derivedSeed instanceof Uint8Array) storeSeed = Array.from(derivedSeed);
      if (storeSeed !== null) {
        try { LS.set(NKN_SEED_KEY, storeSeed); } catch (_) {}
      }
    }
    this.nkn.client = client;
    this.nkn.addr = client.addr || '';
    updateTransportButton();
    client.on('connect', () => {
      this.nkn.ready = true;
      this.nkn.addr = client.addr || '';
      updateTransportButton();
      setBadge('NKN ready');
    });
    client.on('close', () => {
      this.nkn.ready = false;
      updateTransportButton();
    });
    client.on('message', (a, b) => {
      let payload = (a && typeof a === 'object' && a.payload !== undefined) ? a.payload : b;
      try {
        const msg = JSON.parse((payload && payload.toString) ? payload.toString() : String(payload));
        const ev = msg.event || '';
        const id = msg.id;
        if (ev === 'relay.response' && id) {
          const pending = this.nkn.pend.get(id);
          if (pending) {
            clearTimeout(pending.t);
            this.nkn.pend.delete(id);
            pending.res(msg);
          }
          return;
        }
        if (/^relay\.response\.(begin|chunk|end|lines)$/.test(ev) && id) {
          const stream = this.nkn.streams.get(id);
          if (!stream) return;
          if (ev === 'relay.response.begin') {
            stream.onBegin && stream.onBegin(msg);
            return;
          }
          if (ev === 'relay.response.chunk') {
            const u8 = b64ToBytes(msg.b64 || '');
            stream.onChunk && stream.onChunk(u8, msg.seq | 0);
            return;
          }
          if (ev === 'relay.response.lines') {
            const lines = Array.isArray(msg.lines) ? msg.lines : [];
            for (const entry of lines) {
              try {
                stream.onLine && stream.onLine(entry.line, entry.seq | 0, entry.ts);
              } catch (err) {
                // ignore
              }
            }
            return;
          }
          if (ev === 'relay.response.end') {
            if (!stream) return;
            try {
              if (stream._lingerTimer) clearTimeout(stream._lingerTimer);
            } catch (err) {
              // ignore
            }
            stream._ended = true;
            stream._endMsg = msg;
            const linger = Math.max(0, Number(stream.lingerEndMs ?? 150));
            stream._lingerTimer = setTimeout(() => {
              try {
                stream.onEnd && stream.onEnd(stream._endMsg || msg);
              } finally {
                try {
                  clearTimeout(stream._lingerTimer);
                } catch (err) {
                  // ignore
                }
                this.nkn.streams.delete(id);
              }
            }, linger);
            return;
          }
        }
      } catch (err) {
        // ignore parse errors
      }
    });
  },

  async nknSend(req, relay, timeout = 45000) {
    if (!relay) throw new Error('No relay');
    if (!this.nkn.client) this.ensureNkn();
    const id = 'g-' + Date.now() + '-' + Math.random().toString(36).slice(2);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.nkn.pend.delete(id);
        reject(new Error('NKN relay timeout'));
      }, timeout);
      this.nkn.pend.set(id, { res: resolve, rej: reject, t: timer });
      this.nkn.client
        .send(relay, JSON.stringify({ event: 'http.request', id, req }), { noReply: true, maxHoldingSeconds: 120 })
        .catch((err) => {
          clearTimeout(timer);
          this.nkn.pend.delete(id);
          reject(err);
        });
    });
  },

  async nknStream(req, relay, handlers = {}, timeout = 300000) {
    if (!relay) throw new Error('No relay');
    if (!this.nkn.client) this.ensureNkn();
    const id = 'g-' + Date.now() + '-' + Math.random().toString(36).slice(2);
    return new Promise(async (resolve, reject) => {
      let timer = null;
      const wrapped = {
        onBegin: (...args) => {
          try {
            handlers.onBegin && handlers.onBegin(...args);
          } catch (err) {
            // ignore
          }
        },
        onLine: (...args) => {
          try {
            handlers.onLine && handlers.onLine(...args);
          } catch (err) {
            // ignore
          }
        },
        onChunk: (...args) => {
          try {
            handlers.onChunk && handlers.onChunk(...args);
          } catch (err) {
            // ignore
          }
        },
        onEnd: (...args) => {
          try {
            handlers.onEnd && handlers.onEnd(...args);
          } finally {
            clearTimeout(timer);
            this.nkn.streams.delete(id);
            resolve();
          }
        },
        onError: (err) => {
          clearTimeout(timer);
          this.nkn.streams.delete(id);
          reject(err instanceof Error ? err : new Error(String(err || 'nkn stream error')));
        }
      };
      wrapped.lingerEndMs = handlers.lingerEndMs ?? 150;
      this.nkn.streams.set(id, wrapped);
      try {
        await this.nkn.client.send(
          relay,
          JSON.stringify({ event: 'http.request', id, req: Object.assign({ stream: 'chunks' }, req) }),
          { noReply: true, maxHoldingSeconds: 120 }
        );
      } catch (err) {
        this.nkn.streams.delete(id);
        return reject(err);
      }
      timer = setTimeout(() => {
        const stream = this.nkn.streams.get(id);
        if (stream) {
          this.nkn.streams.delete(id);
          reject(new Error('NKN stream timeout'));
        }
      }, timeout);
    });
  },

  async nknFetch(base, path, method, json, api, relay, timeout = 45000) {
    const headers = this.auth({}, api);
    if (method === 'GET') {
      delete headers['Content-Type'];
      headers['Accept'] = 'application/json';
    }
    const req = { url: base.replace(/\/+$/, '') + path, method, headers, timeout_ms: timeout };
    if (json !== null) req.json = json;
    const res = await this.nknSend(req, relay, timeout);
    if (!res || res.ok === false) throw new Error((res && res.error) || ('HTTP ' + (res && res.status)));
    if (res.json !== undefined && res.json !== null) return res.json;
    if (res.body_b64) {
      const u8 = b64ToBytes(res.body_b64);
      return JSON.parse(td.decode(u8));
    }
    return null;
  },

  async nknFetchBlob(fullUrl, relay, api) {
    const parts = [];
    let contentType = 'application/octet-stream';
    await this.nknStream(
      { url: fullUrl, method: 'GET', headers: this.auth({ 'X-Relay-Stream': 'chunks' }, api), timeout_ms: 10 * 60 * 1000 },
      relay,
      {
        onBegin: (meta) => {
          const headers = meta.headers || {};
          contentType = headers['content-type'] || headers['Content-Type'] || contentType;
        },
        onChunk: (chunk) => parts.push(chunk),
        onEnd: () => {}
      }
    );
    return new Blob(parts, { type: contentType });
  }
};

export { Net };
