import { CFG } from './config.js';
import { setBadge, b64ToBytes, td, LS } from './utils.js';

let updateTransportButton = () => {};

const NKN_SEED_KEY = 'graph.nkn.seed';

const Net = {
  nkn: {
    client: null,
    ready: false,
    addr: '',
    pend: new Map(),
    streams: new Map(),
    resolvePend: new Map(),
    rpcPend: new Map(),
    telemetry: {
      resolve: {
        total: 0,
        staleRejected: 0,
        last: null
      }
    }
  },
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

  _rejectPending(reason = 'NKN connection closed') {
    const err = reason instanceof Error ? reason : new Error(String(reason || 'NKN connection closed'));
    try {
      for (const [id, pending] of this.nkn.pend.entries()) {
        try { clearTimeout(pending.t); } catch (_) {}
        try { pending.rej && pending.rej(err); } catch (_) {}
        this.nkn.pend.delete(id);
      }
      for (const [id, pending] of this.nkn.resolvePend.entries()) {
        try { clearTimeout(pending.t); } catch (_) {}
        try { pending.rej && pending.rej(err); } catch (_) {}
        this.nkn.resolvePend.delete(id);
      }
      for (const [id, pending] of this.nkn.rpcPend.entries()) {
        try { clearTimeout(pending.t); } catch (_) {}
        try { pending.rej && pending.rej(err); } catch (_) {}
        this.nkn.rpcPend.delete(id);
      }
      for (const [id, stream] of this.nkn.streams.entries()) {
        try { stream.onError && stream.onError(err); } catch (_) {}
        this.nkn.streams.delete(id);
      }
    } catch (_) {
      // best effort
    }
  },

  _sleep(ms = 0) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  },

  _normalizeTimeoutMs(value, fallback = 15000, minimum = 1000, maximum = 300000) {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return Math.max(minimum, Math.min(maximum, Math.round(n)));
  },

  isCloudflareTunnelUrl(value) {
    const text = String(value || '').trim();
    if (!text) return false;
    try {
      const parsed = new URL(text);
      const host = String(parsed.hostname || '').toLowerCase();
      return host.endsWith('.trycloudflare.com') || host.endsWith('.cfargotunnel.com');
    } catch (_) {
      return false;
    }
  },

  classifyTunnelFailure(input, context = {}) {
    const ctx = context && typeof context === 'object' ? context : {};
    let status = Number(ctx.status || 0) || 0;
    let url = String(ctx.url || '').trim();
    const candidate = String(ctx.candidate || ctx.transport || '').trim().toLowerCase();
    let message = '';

    if (input && typeof input === 'object') {
      if (!status) status = Number(input.status || input.statusCode || 0) || 0;
      if (!url) url = String(input.url || '').trim();
      if (!message) {
        if (typeof input.message === 'string') message = input.message;
        else if (typeof input.error === 'string') message = input.error;
      }
    }
    if (!message) message = String(input || '');

    const lowerMsg = message.toLowerCase();
    const cloudflare = this.isCloudflareTunnelUrl(url)
      || candidate === 'cloudflare'
      || /trycloudflare|cfargotunnel|cloudflare/.test(lowerMsg);
    const expiredHint = /stale|expired|offline|tunnel process is not running|url is stale|tunnel.*(dead|down)/.test(lowerMsg);
    const unavailableHint = /failed to fetch|networkerror|network error|connection reset|temporar|gateway|unreachable|timeout/.test(lowerMsg);

    const stale = status === 530 || (cloudflare && (expiredHint || unavailableHint));
    const reason = stale
      ? (status === 530 ? 'cloudflare_530' : 'cloudflare_tunnel_stale')
      : (status ? `http_${status}` : '');

    return {
      stale,
      cloudflare,
      candidate,
      status,
      url,
      reason,
      message
    };
  },

  _firstEndpoint(...values) {
    for (const value of values) {
      if (!value) continue;
      if (typeof value === 'string') {
        const text = value.trim();
        if (text) return text;
        continue;
      }
      if (typeof value !== 'object') continue;
      const endpoint = String(
        value.base_url ||
        value.baseUrl ||
        value.http_endpoint ||
        value.httpEndpoint ||
        value.public_base_url ||
        value.nkn_address ||
        value.address ||
        value.url ||
        ''
      ).trim();
      if (endpoint) return endpoint;
    }
    return '';
  },

  _extractResolveCandidates(entry = {}) {
    const fallback = entry && typeof entry.fallback === 'object' ? entry.fallback : {};
    const candidates = entry && typeof entry.candidates === 'object' ? entry.candidates : {};
    return {
      cloudflare: this._firstEndpoint(
        candidates.cloudflare,
        entry.cloudflare,
        entry.tunnel_url,
        entry.stale_tunnel_url,
        fallback.cloudflare
      ),
      nkn: this._firstEndpoint(
        candidates.nkn,
        entry.nkn,
        fallback.nkn
      ),
      local: this._firstEndpoint(
        candidates.local,
        entry.local,
        entry.base_url,
        fallback.local
      )
    };
  },

  _collectResolveDiagnostics(reply = {}) {
    const resolved = (reply && typeof reply.resolved === 'object')
      ? reply.resolved
      : (reply?.snapshot && typeof reply.snapshot.resolved === 'object' ? reply.snapshot.resolved : {});
    const staleRejections = [];
    const selectedTransports = {};
    for (const [service, rawEntry] of Object.entries(resolved || {})) {
      if (!rawEntry || typeof rawEntry !== 'object') continue;
      const entry = rawEntry;
      const selected = String(entry.selected_transport || entry.transport || '').trim().toLowerCase();
      if (selected) selectedTransports[service] = selected;
      const candidates = this._extractResolveCandidates(entry);
      const tunnelError = String(entry.tunnel_error || entry.error || '').trim();
      const tunnelStatus = Number(entry.tunnel_status || 0) || 0;
      const staleHint = this.classifyTunnelFailure(
        tunnelError || entry,
        { status: tunnelStatus, url: entry.tunnel_url || entry.stale_tunnel_url, candidate: 'cloudflare' }
      );
      const staleByPayload = !!entry.stale_tunnel_url || staleHint.stale;
      if (selected === 'cloudflare' && staleByPayload && (candidates.nkn || candidates.local)) {
        staleRejections.push({
          service,
          selected_transport: selected,
          stale_reason: staleHint.reason || (entry.stale_tunnel_url ? 'stale_tunnel_url' : 'cloudflare_tunnel_stale'),
          candidates
        });
      }
    }
    return {
      selected_transports: selectedTransports,
      stale_rejections: staleRejections,
      stale_rejection_count: staleRejections.length
    };
  },

  _normalizeResolveReply(reply = {}) {
    const out = reply && typeof reply === 'object' ? { ...reply } : {};
    const diagnostics = this._collectResolveDiagnostics(out);
    out.discovery_source = String(
      out.discovery_source ||
      out.source ||
      out.mode ||
      (out.source_address ? 'nkn' : 'local')
    ).trim().toLowerCase();
    out.transport_decision = diagnostics.selected_transports;
    out.stale_rejections = diagnostics.stale_rejections;
    out.stale_rejection_count = diagnostics.stale_rejection_count;
    this.nkn.telemetry.resolve.total = Number(this.nkn.telemetry.resolve.total || 0) + 1;
    this.nkn.telemetry.resolve.staleRejected = Number(this.nkn.telemetry.resolve.staleRejected || 0) + diagnostics.stale_rejection_count;
    this.nkn.telemetry.resolve.last = {
      ts: Date.now(),
      source: out.discovery_source || '',
      stale_rejection_count: diagnostics.stale_rejection_count,
      request_id: String(out.request_id || '')
    };
    return out;
  },

  isStaleTunnelFailure(input, context = {}) {
    return !!this.classifyTunnelFailure(input, context).stale;
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
    const sendOpts = { ...(opts || {}) };
    const attempts = Math.max(1, Math.min(5, Number(sendOpts._attempts ?? 3) || 3));
    delete sendOpts._attempts;
    let lastErr = null;
    for (let attempt = 0; attempt < attempts; attempt++) {
      try {
        await this.nkn.client.send(relay, data, { noReply: true, maxHoldingSeconds: 240, ...sendOpts });
        return;
      } catch (err) {
        lastErr = err;
        const msg = String(err || '');
        if (msg.includes('readyState') || msg.includes('not open') || msg.toLowerCase().includes('connection')) {
          if (attempt < (attempts - 1)) {
            await this._sleep(200 * (attempt + 1));
            await this.waitForReady(timeout);
          }
          continue;
        }
        if (attempt < (attempts - 1)) await this._sleep(150 * (attempt + 1));
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

  async reconnectNkn(options = {}) {
    const timeoutMs = this._normalizeTimeoutMs(options.timeout ?? options.timeoutMs, 20000, 2000, 120000);
    const waitReady = options.waitReady !== false;
    const prev = this.nkn.client;

    this._rejectPending(new Error('NKN reconnect requested'));
    this.nkn.ready = false;

    if (prev) {
      try {
        if (typeof prev.close === 'function') prev.close();
      } catch (_) {
        // ignore close failures
      }
    }

    this.nkn.client = null;
    this.nkn.addr = '';
    updateTransportButton();

    this.ensureNkn();
    if (!waitReady) return '';
    await this.waitForReady(timeoutMs);
    return this.nkn.addr || '';
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
      this._rejectPending(new Error('NKN transport closed'));
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
        if (ev === 'resolve_tunnels_result') {
          const rid = msg.request_id || id;
          if (rid) {
            const pending = this.nkn.resolvePend.get(rid);
            if (pending) {
              clearTimeout(pending.t);
              this.nkn.resolvePend.delete(rid);
              pending.res(msg);
              return;
            }
          }
        }
        if (ev === 'service_rpc_result') {
          const rid = msg.request_id || id;
          if (rid) {
            const pending = this.nkn.rpcPend.get(rid);
            if (pending) {
              clearTimeout(pending.t);
              this.nkn.rpcPend.delete(rid);
              pending.res(msg);
              return;
            }
          }
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
    const timeoutMs = this._normalizeTimeoutMs(timeout, 15000);
    if (!this.nkn.client) this.ensureNkn();
    const id = 'h-' + Date.now() + '-' + Math.random().toString(36).slice(2);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.nkn.pend.delete(id);
        reject(new Error('NKN health timeout'));
      }, timeoutMs);
      this.nkn.pend.set(id, { res: resolve, rej: reject, t: timer });
      this._sendNkn(addr, { event: 'relay.health', id }, { noReply: true, maxHoldingSeconds: 30 }, timeoutMs)
        .catch((err) => {
          clearTimeout(timer);
          this.nkn.pend.delete(id);
          reject(err);
        });
    });
  },

  async nknResolveTunnels(targetAddress, timeout = 20000) {
    const target = (targetAddress || '').trim();
    if (!target) throw new Error('No target router address');
    const timeoutMs = this._normalizeTimeoutMs(timeout, 20000);
    if (!this.nkn.client) this.ensureNkn();
    await this.waitForReady(timeoutMs);
    const requestId = 'rt-' + Date.now() + '-' + Math.random().toString(36).slice(2);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.nkn.resolvePend.delete(requestId);
        reject(new Error('NKN resolve timeout'));
      }, timeoutMs);
      this.nkn.resolvePend.set(requestId, { res: resolve, rej: reject, t: timer });
      const payload = {
        event: 'resolve_tunnels',
        request_id: requestId,
        from: this.nkn.addr || '',
        timestamp_ms: Date.now()
      };
      this._sendNkn(target, payload, { noReply: true, maxHoldingSeconds: 45 }, timeoutMs)
        .catch((err) => {
          clearTimeout(timer);
          this.nkn.resolvePend.delete(requestId);
          reject(err);
        });
    }).then((reply) => this._normalizeResolveReply(reply || {}));
  },

  async nknServiceRpc(arg1, arg2, arg3, arg4 = undefined) {
    // Supports both signatures:
    // 1) nknServiceRpc(service, path, options)
    // 2) nknServiceRpc(relay, service, path, options) (legacy compatibility)
    const legacySignature = (typeof arg3 === 'string') || (arg4 !== undefined);
    const options = (legacySignature ? arg4 : arg3) && typeof (legacySignature ? arg4 : arg3) === 'object'
      ? { ...(legacySignature ? arg4 : arg3) }
      : {};
    const target = String(
      legacySignature
        ? (arg1 || '')
        : (options.relay || CFG.routerTargetNknAddress || '')
    ).trim();
    if (!target) throw new Error('No relay/router address');

    const svc = String(legacySignature ? arg2 : arg1 || '').trim();
    if (!svc) throw new Error('Missing service');
    const rpcPath = String(legacySignature ? arg3 : arg2 || '').trim();
    if (!rpcPath) throw new Error('Missing path');
    const method = String(options.method || 'GET').toUpperCase();
    const timeout = this._normalizeTimeoutMs(options.timeout_ms ?? options.timeout, 45000);
    const maxPayloadB = Math.max(1024, Number(options.maxPayloadB || (512 * 1024)) || (512 * 1024));
    const sendAttempts = Math.max(1, Math.min(3, Number(options.sendAttempts || 1) || 1));
    if (!this.nkn.client) this.ensureNkn();
    await this.waitForReady(timeout);
    const requestId = 'rpc-' + Date.now() + '-' + Math.random().toString(36).slice(2);
    const rpcPromise = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.nkn.rpcPend.delete(requestId);
        reject(new Error('NKN service RPC timeout'));
      }, timeout);
      this.nkn.rpcPend.set(requestId, { res: resolve, rej: reject, t: timer });
      const payload = {
        event: 'service_rpc_request',
        request_id: requestId,
        from: this.nkn.addr || '',
        service: svc,
        path: rpcPath.startsWith('/') ? rpcPath : ('/' + rpcPath),
        method,
        timeout_ms: timeout,
        headers: options.headers || {}
      };
      if (options.json !== undefined) payload.json = options.json;
      if (options.body_b64 !== undefined) payload.body_b64 = options.body_b64;
      try {
        const payloadBytes = new TextEncoder().encode(JSON.stringify(payload)).length;
        if (Number.isFinite(maxPayloadB) && maxPayloadB > 0 && payloadBytes > maxPayloadB) {
          clearTimeout(timer);
          this.nkn.rpcPend.delete(requestId);
          reject(new Error(`RPC payload too large (${payloadBytes} > ${maxPayloadB})`));
          return;
        }
      } catch (_) {}
      this._sendNkn(target, payload, { noReply: true, maxHoldingSeconds: 120, _attempts: sendAttempts }, timeout)
        .catch((err) => {
          clearTimeout(timer);
          this.nkn.rpcPend.delete(requestId);
          reject(err);
        });
    });
    return rpcPromise.then((result) => {
      const out = result && typeof result === 'object' ? { ...result } : result;
      if (out && typeof out === 'object' && out.result && typeof out.result === 'object' && out.ok === undefined) {
        // Compatibility with nested teleoperation-style result payloads.
        Object.assign(out, out.result);
      }
      if (out && typeof out === 'object') {
        if (!out.discovery_source) {
          out.discovery_source = String(out.source || out.mode || (out.source_address ? 'nkn' : '')).trim().toLowerCase();
        }
        if (!out.selected_transport && out.selectedTransport) {
          out.selected_transport = out.selectedTransport;
        }
      }
      if (options.decodeBody && out && out.body_b64 && out.json == null) {
        try {
          const bodyBytes = b64ToBytes(out.body_b64);
          const text = td.decode(bodyBytes);
          out.body_text = text;
          const ctype = String((out.headers && (out.headers['content-type'] || out.headers['Content-Type'])) || '').toLowerCase();
          if (ctype.includes('application/json')) {
            try { out.body_json = JSON.parse(text); } catch (_) {}
          }
        } catch (_) {
          // keep raw payload if decode fails
        }
      }
      if (options.throwOnError && out && out.ok === false) {
        const status = Number(out.status || 0);
        const msg = String(out.error || `RPC failed with status ${status}`);
        const err = new Error(msg);
        err.status = status;
        err.payload = out;
        throw err;
      }
      return out;
    });
  }
};

export { Net };
