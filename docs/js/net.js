import { CFG } from './config.js';
import { setBadge, b64ToBytes, td, LS } from './utils.js';

let updateTransportButton = () => {};

const NKN_SEED_KEY = 'graph.nkn.seed';

const Net = {
  nkn: { client: null, ready: false, addr: '', pend: new Map(), streams: new Map() },
  uploadChunkBytes: 80 * 1024, // default; per-call chunkSize can override
  uploadCache: new Map(), // uploadId -> { chunks, total, url, headers, timeout },

  setTransportUpdater(fn) {
    updateTransportButton = typeof fn === 'function' ? fn : () => {};
  },

  setUploadChunkKB(kb) {
    const val = Number(kb);
    if (Number.isFinite(val) && val > 4) {
      this.uploadChunkBytes = Math.max(4 * 1024, Math.floor(val * 1024));
    }
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

  _sleep(ms = 0) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  },

  async waitForReady(timeout = 15000) {
    if (this.nkn.ready && this.nkn.client) return;
    this.ensureNkn();
    if (this.nkn.ready && this.nkn.client) return;
    return new Promise((resolve, reject) => {
      const to = setTimeout(() => reject(new Error('NKN not ready')), timeout);
      const check = () => {
        if (this.nkn.ready && this.nkn.client) {
          clearTimeout(to);
          resolve();
        } else {
          setTimeout(check, 150);
        }
      };
      check();
    });
  },

  async _sendNkn(relay, payload, opts = {}, timeout = 15000) {
    if (!relay) throw new Error('No relay');
    await this.waitForReady(timeout);
    const data = typeof payload === 'string' ? payload : JSON.stringify(payload);
    let lastErr = null;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        await this.nkn.client.send(relay, data, { noReply: true, maxHoldingSeconds: 240, ...opts });
        return;
      } catch (err) {
        lastErr = err;
        const msg = String(err || '');
        if (msg.includes('readyState') || msg.includes('not open') || msg.toLowerCase().includes('connection')) {
          await this._sleep(200 * (attempt + 1));
          await this.waitForReady(timeout);
          continue;
        }
        if (attempt < 2) await this._sleep(150 * (attempt + 1));
      }
    }
    throw lastErr || new Error('NKN send failed');
  },

  _clearUploadCache(id) {
    if (!id) return;
    this.uploadCache.delete(id);
  },

  async _handleUploadMissing(uploadId, msg) {
    if (!uploadId) return;
    const entry = this.uploadCache.get(uploadId);
    if (!entry) return;
    const missing = Array.isArray(msg.missing) ? msg.missing.map((n) => Number(n) || 0).filter((n) => n > 0) : [];
    if (!missing.length) return;
    const { relay, timeout = 120000, url, path = '', headers, chunks, total, service = '', useServiceTarget = false } = entry;
    const reqMeta = {
      url: useServiceTarget ? '' : url,
      path,
      method: 'POST',
      headers,
      timeout_ms: timeout,
      stream: 'chunks'
    };
    if (service) reqMeta.service = service;
    for (const seq of missing) {
      const chunk = chunks[seq - 1];
      if (!chunk) continue;
      const payload = {
        event: 'http.upload.chunk',
        id: uploadId,
        upload_id: uploadId,
        seq,
        total: total,
        b64: chunk
      };
      if (seq === 1) {
        payload.req = reqMeta;
        payload.content_type = headers['Content-Type'] || headers['content-type'] || 'application/json';
      }
      try {
        await this._sendNkn(relay, payload, { noReply: true, maxHoldingSeconds: 240 }, timeout);
        await this._sleep(4);
      } catch (err) {
        // swallow resend errors; cleanup will handle if still missing
      }
    }
  },

  _chunkBase64(b64, maxChars = 60000) {
    // Split base64 text at boundaries that are multiples of 4 chars to keep each chunk decodable
    const chunks = [];
    const len = b64.length;
    let i = 0;
    const safeSize = Math.max(4, Math.floor(maxChars / 4) * 4); // ensure divisible by 4
    while (i < len) {
      let end = Math.min(len, i + safeSize);
      // ensure end - i is a multiple of 4 (except possibly final which is still valid)
      const span = end - i;
      const mod = span % 4;
      if (mod !== 0 && end < len) {
        end = Math.max(i + 4, end - mod);
      }
      chunks.push(b64.slice(i, end));
      i = end;
    }
    return chunks;
  },

  async getJSON(base, path, api, useNkn, relay, options = {}) {
    const { forceRelay = false, service = '', useServiceTarget = false } = options || {};
    const useRelay = useNkn && relay;
    const direct = async () => {
      const headers = this.auth({}, api);
      delete headers['Content-Type'];
      headers['Accept'] = 'application/json';
      const res = await fetch(base.replace(/\/+$/, '') + path, { headers });
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      return res.json();
    };
    if (!useRelay) {
      if (forceRelay) throw new Error('Relay address required for remote endpoint');
      return direct();
    }
    try {
      return await this.nknFetch(base, path, 'GET', null, api, relay, 45000, { service, useServiceTarget });
    } catch (err) {
      if (forceRelay) throw err;
      // fallback to direct if relay fails
      return direct();
    }
  },

  async postJSONChunked(base, path, body, api, useNkn, relay, timeout = 120000, chunkSize = 60000, progressCb = null, options = {}) {
    const { forceRelay = false, service = '', useServiceTarget = false } = options || {};
    const useRelay = useNkn && relay;
    if (!useRelay) {
      if (forceRelay) throw new Error('Relay address required for remote endpoint');
      return this.postJSON(base, path, body, api, false, '', timeout);
    }
    // Stream upload over NKN in multiple DMs
    if (!this.nkn.client) this.ensureNkn();
    const uploadId = 'up-' + Date.now() + '-' + Math.random().toString(36).slice(2);
    const headers = this.auth({}, api);
    headers['Content-Type'] = 'application/json';
    headers['X-Relay-Stream'] = 'chunks';
    await this.waitForReady(20000);
    const payloadStr = JSON.stringify(body || {});
    const b64 = btoa(unescape(encodeURIComponent(payloadStr)));
    // Keep chunk size small to survive stricter relays (router default limit is set separately)
    const limitBytes = Math.min(Math.max(chunkSize || this.uploadChunkBytes || 80 * 1024, 8 * 1024), 96 * 1024);
    const chunkChars = Math.max(2048, Math.floor(limitBytes * 4 / 3));
    const chunks = this._chunkBase64(b64, chunkChars);
    const total = chunks.length;
    const url = base.replace(/\/+$/, '') + path;
    const reqMeta = {
      url: useServiceTarget ? '' : url,
      path,
      method: 'POST',
      headers,
      timeout_ms: timeout,
      stream: 'chunks'
    };
    if (service) reqMeta.service = service;
    this.uploadCache.set(uploadId, { chunks, total, url, path, headers, relay, timeout, service, useServiceTarget });

    const responsePromise = this._awaitResponseStream(uploadId, timeout, relay);

    const throttleMs = total > 8 ? 6 : 0;
    const sendMsg = async (msg) => {
      await this._sendNkn(relay, msg, { noReply: true, maxHoldingSeconds: 240 }, timeout);
      if (throttleMs) await this._sleep(throttleMs);
    };

    if (typeof progressCb === 'function') {
      try { progressCb(0, total, 'begin'); } catch (_) { /* ignore */ }
    }
    const beginPayload = {
      event: 'http.upload.begin',
      id: uploadId,
      upload_id: uploadId,
      req: reqMeta,
      total_chunks: total,
      content_type: 'application/json'
    };
    await sendMsg(beginPayload);
    // Send a second begin as a safeguard in case the first DM is dropped
    await this._sleep(10);
    await sendMsg(beginPayload);
    let seq = 1;
    for (const chunk of chunks) {
      const payload = {
        event: 'http.upload.chunk',
        id: uploadId,
        upload_id: uploadId,
        seq,
        total: total,
        b64: chunk
      };
      // Include req on first chunk so router can recover if begin was dropped
      if (seq === 1) {
        payload.req = reqMeta;
        payload.content_type = 'application/json';
      }
      await sendMsg(payload);
      if (typeof progressCb === 'function') {
        try { progressCb(seq, total, 'chunk'); } catch (_) { /* ignore */ }
      }
      seq += 1;
    }
    await sendMsg({
      event: 'http.upload.end',
      id: uploadId,
      upload_id: uploadId,
      total: total
    });
    if (typeof progressCb === 'function') {
      try { progressCb(total, total, 'end'); } catch (_) { /* ignore */ }
    }

    try {
      return await responsePromise;
    } catch (err) {
      if (forceRelay) throw err;
      // fallback to direct HTTP if relay upload fails
      return this.postJSON(base, path, body, api, false, '', timeout);
    } finally {
      this._clearUploadCache(uploadId);
    }
  },

  async _awaitResponseStream(id, timeout = 180000, relay = null) {
    if (!this.nkn.client) this.ensureNkn();
    return new Promise((resolve, reject) => {
      const chunks = [];
      let maxSeq = 0;
      let ok = false;
      let status = 0;
      let headers = {};
      let timer = setTimeout(() => {
        this.nkn.streams.delete(id);
        this._clearUploadCache(id);
        reject(new Error('NKN stream timeout'));
      }, timeout);
      const handlers = {
        onBegin: (meta) => {
          ok = meta?.ok !== false;
          status = meta?.status || 0;
          headers = meta?.headers || {};
        },
        onChunk: (chunk, seq = 0) => {
          const idx = (Number(seq) || 0) - 1;
          if (idx >= 0) {
            chunks[idx] = chunk;
            maxSeq = Math.max(maxSeq, idx + 1);
          } else {
            chunks.push(chunk);
            maxSeq = Math.max(maxSeq, chunks.length);
          }
        },
        onLine: (lineObj) => {
          try {
            const text = lineObj?.line || '';
            chunks.push(new TextEncoder().encode(text + '\\n'));
            maxSeq = Math.max(maxSeq, chunks.length);
          } catch (_) {}
        },
        onEnd: () => {
          clearTimeout(timer);
          this.nkn.streams.delete(id);
          this._clearUploadCache(id);
          const missing = [];
          for (let i = 0; i < maxSeq; i++) {
            if (!chunks[i]) missing.push(i + 1);
          }
          const finalize = () => {
            const merged = chunks.length ? new Blob(chunks.filter(Boolean)) : new Blob([]);
            merged.arrayBuffer().then((arr) => {
              const text = td.decode(new Uint8Array(arr));
              if (!ok) return reject(new Error(`HTTP ${status || 0}`));
              try {
                return resolve(JSON.parse(text));
              } catch (err) {
                if (headers && typeof headers['content-type'] === 'string' && headers['content-type'].includes('application/json')) {
                  return reject(err);
                }
                return resolve({ body: text, status, headers });
              }
            }).catch(reject);
          };
          if (missing.length && relay) {
            const attempts = 2;
            const requestMissing = async (left) => {
              if (!left || !missing.length) return finalize();
              try {
                await this._sendNkn(relay, { event: 'relay.response.missing', id, missing }, { noReply: true, maxHoldingSeconds: 30 }, timeout);
              } catch (_) {
                return finalize();
              }
              setTimeout(() => {
                const stillMissing = [];
                for (const seq of missing) {
                  const idx = seq - 1;
                  if (!chunks[idx]) stillMissing.push(seq);
                }
                missing.splice(0, missing.length, ...stillMissing);
                requestMissing(left - 1);
              }, 300);
            };
            requestMissing(attempts);
          } else {
            finalize();
          }
        },
        onError: (err) => {
          clearTimeout(timer);
          this.nkn.streams.delete(id);
          this._clearUploadCache(id);
          reject(err instanceof Error ? err : new Error(String(err || 'NKN stream error')));
        },
        lingerEndMs: 50
      };
      this.nkn.streams.set(id, handlers);
    });
  },

  async postJSON(base, path, body, api, useNkn, relay, timeout = 45000, options = {}) {
    const { forceRelay = false, service = '', useServiceTarget = false } = options || {};
    const useRelay = useNkn && relay;
    const direct = async () => {
      const res = await fetch(base.replace(/\/+$/, '') + path, {
        method: 'POST',
        headers: this.auth({}, api),
        body: JSON.stringify(body || {})
      });
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      return res.json();
    };
    if (!useRelay) {
      if (forceRelay) throw new Error('Relay address required for remote endpoint');
      return direct();
    }
    try {
      return await this.nknFetch(base, path, 'POST', body, api, relay, timeout, { service, useServiceTarget });
    } catch (err) {
      if (forceRelay) throw err;
      return direct();
    }
  },

  async nknFetchForm(base, path, formData, api, relay, timeout = 60000) {
    return this.nknFetch(base, path, 'POST_FORM', formData, api, relay, timeout);
  },

  async fetchBlob(fullUrl, useNkn, relay, api, options = {}) {
    const { forceRelay = false, service = '', useServiceTarget = false } = options || {};
    const useRelay = useNkn && relay;
    if (!useRelay) {
      if (forceRelay) throw new Error('Relay address required for remote endpoint');
      const res = await fetch(fullUrl, { headers: this.auth({}, api) });
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      return res.blob();
    }
    return this.nknFetchBlob(fullUrl, relay, api, { service, useServiceTarget });
  },

  ensureNkn() {
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

    // Use hydra. prefix for Hydra peers (changed from graph.)
    let options = { identifier: 'hydra', numSubClients: 4, wsConnHeartbeatTimeout: 60000 };
    const normalizedSeed = normalizeSeedForUse(seed);
    if (normalizedSeed) options.seed = normalizedSeed;
    let client;
    try {
      client = new window.nkn.MultiClient(options);
    } catch (err) {
      if (normalizedSeed) {
        try { LS.del(NKN_SEED_KEY); } catch (_) {}
        options = { identifier: 'hydra', numSubClients: 4, wsConnHeartbeatTimeout: 60000 };
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
        if (ev === 'http.upload.missing') {
          const uid = msg.upload_id || id;
          this._handleUploadMissing(uid, msg);
          return;
        }
        if (ev === 'relay.health' && id) {
          const pending = this.nkn.pend.get(id);
          if (pending) {
            clearTimeout(pending.t);
            this.nkn.pend.delete(id);
            pending.res(msg);
          }
          return;
        }
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
      this._sendNkn(relay, { event: 'http.request', id, req }, { noReply: true, maxHoldingSeconds: 120 }, timeout)
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
        await this._sendNkn(relay, { event: 'http.request', id, req: Object.assign({ stream: 'chunks' }, req) }, { noReply: true, maxHoldingSeconds: 120 }, timeout);
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

  async nknFetch(base, path, method, json, api, relay, timeout = 45000, options = {}) {
    const { service = '', useServiceTarget = false } = options || {};
    const headers = this.auth({}, api);
    if (method === 'GET') {
      delete headers['Content-Type'];
      headers['Accept'] = 'application/json';
    }
    const req = {
      url: useServiceTarget ? '' : base.replace(/\/+$/, '') + path,
      path,
      method,
      headers,
      timeout_ms: timeout
    };
    if (service) req.service = service;
    if (json !== null) {
      if (method === 'POST_FORM') {
        // NKN cannot stream binary form; send as JSON map (only works for text fields)
        const obj = {};
        if (json && typeof json.forEach === 'function') {
          json.forEach((v, k) => { obj[k] = v; });
        }
        req.json = obj;
      } else {
        req.json = json;
      }
    }
    const res = await this.nknSend(req, relay, timeout);
    if (!res || res.ok === false) throw new Error((res && res.error) || ('HTTP ' + (res && res.status)));
    if (res.json !== undefined && res.json !== null) return res.json;
    if (res.body_b64) {
      const u8 = b64ToBytes(res.body_b64);
      return JSON.parse(td.decode(u8));
    }
    return null;
  },

  async nknFetchBlob(fullUrl, relay, api, options = {}) {
    const { service = '', useServiceTarget = false } = options || {};
    let path = '';
    try {
      const parsed = new URL(fullUrl);
      path = parsed.pathname + (parsed.search || '');
    } catch (_) {
      path = fullUrl;
    }
    const parts = [];
    let contentType = 'application/octet-stream';
    await this.nknStream(
      {
        url: useServiceTarget ? '' : fullUrl,
        path,
        method: 'GET',
        service,
        headers: this.auth({ 'X-Relay-Stream': 'chunks' }, api),
        timeout_ms: 10 * 60 * 1000
      },
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
  },

  async relayHealth(relay, timeout = 15000) {
    const addr = (relay || '').trim();
    if (!addr) throw new Error('No relay provided');
    if (!this.nkn.client) this.ensureNkn();
    const id = 'h-' + Date.now() + '-' + Math.random().toString(36).slice(2);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.nkn.pend.delete(id);
        reject(new Error('NKN health timeout'));
      }, timeout);
      this.nkn.pend.set(id, { res: resolve, rej: reject, t: timer });
      this._sendNkn(addr, { event: 'relay.health', id }, { noReply: true, maxHoldingSeconds: 30 }, timeout)
        .catch((err) => {
          clearTimeout(timer);
          this.nkn.pend.delete(id);
          reject(err);
        });
    });
  }
};

export { Net };
