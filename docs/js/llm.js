import { ensureLocalNetworkAccess } from './localNetwork.js';

function createLLM({
  getNode,
  NodeStore,
  Router,
  Net,
  CFG,
  createSentenceMux,
  makeNdjsonPump,
  stripEOT,
  log,
  setRelayState = () => {}
}) {
  const MODEL_PROMISE = new Map();
  const MODEL_CACHE = new Map();
  const MODEL_REFRESH_TIMER = new Map();
  const MODEL_LISTENERS = new Map();
  const MODEL_REFRESH_MS = 60 * 1000;
  const IMAGE_PAYLOADS = new Map();
  const MODEL_INFO_CACHE = new Map();
  const MODEL_INFO_PROMISES = new Map();
  const TOOL_PAYLOADS = new Map();

  function stripLicense(value) {
    if (Array.isArray(value)) return value.map(stripLicense);
    if (!value || typeof value !== 'object') return value;
    const out = {};
    let removed = false;
    for (const [key, val] of Object.entries(value)) {
      if (typeof key === 'string' && key.toLowerCase() === 'license') {
        removed = true;
        continue;
      }
      out[key] = stripLicense(val);
    }
    if (removed) out.license = '[omitted]';
    return out;
  }

  function normalizeCapabilities(value) {
    if (!value) return [];
    if (Array.isArray(value)) return value.map((v) => String(v).trim()).filter(Boolean);
    if (typeof value === 'string') {
      return value
        .split(/[,\s]+/)
        .map((v) => v.trim())
        .filter(Boolean);
    }
    return [];
  }

  function normalizeImageSources(input, mimeHint) {
    const out = [];
    const visit = (value, hint) => {
      if (value == null) return;
      if (Array.isArray(value)) {
        value.forEach((item) => visit(item, hint));
        return;
      }
      if (typeof value === 'string') {
        let trimmed = value.trim();
        if (!trimmed) return;
        if (/^data:image\//i.test(trimmed)) {
          const base64 = trimmed.includes(',') ? trimmed.split(',')[1] : trimmed;
          if (base64) out.push(base64.trim());
          return;
        }
        if (/^https?:\/\//i.test(trimmed)) {
          return;
        }
        trimmed = trimmed.replace(/\s+/g, '');
        if (trimmed) out.push(trimmed);
        return;
      }
      if (typeof value === 'object') {
        const obj = value;
        const nextHint = obj.mime || obj.type || hint || null;
        if (obj.images != null) {
          visit(obj.images, nextHint);
          return;
        }
        if (obj.data != null) {
          visit(obj.data, nextHint);
          return;
        }
        if (obj.image != null) {
          visit(obj.image, nextHint);
          return;
        }
        if (obj.b64 != null) {
          visit(obj.b64, nextHint);
          return;
        }
        if (obj.base64 != null) {
          visit(obj.base64, nextHint);
          return;
        }
        if (obj.url != null) {
          visit(obj.url, nextHint);
          return;
        }
        if (obj.src != null) {
          visit(obj.src, nextHint);
          return;
        }
        if (obj.payload != null) {
          visit(obj.payload, nextHint);
          return;
        }
        if (obj.value != null) {
          visit(obj.value, nextHint);
          return;
        }
        if (obj.path != null) {
          visit(obj.path, nextHint);
        }
      }
    };
    visit(input, mimeHint);
    return out.map((str) => String(str || '').trim()).filter(Boolean);
  }

  function mergeEntry(map, entry) {
    const existing = map.get(entry.id);
    if (!existing) {
      map.set(entry.id, entry);
      return;
    }
    const mergedCaps = new Set([...(existing.capabilities || []), ...(entry.capabilities || [])]);
    existing.capabilities = Array.from(mergedCaps);
    existing.raw = Object.assign({}, entry.raw || {}, existing.raw || {});
    existing.source = existing.source || entry.source;
    map.set(existing.id, existing);
  }

  function normalizeModelEntry(rawEntry, source) {
    if (rawEntry == null) return null;
    if (typeof rawEntry === 'string') {
      const id = rawEntry.trim();
      if (!id) return null;
      return {
        id,
        label: id,
        raw: { name: id },
        source,
        capabilities: []
      };
    }
    if (typeof rawEntry !== 'object') return null;
    const id = String(rawEntry.id || rawEntry.name || rawEntry.model || '').trim();
    if (!id) return null;
    const label = String(rawEntry.name || rawEntry.id || id).trim() || id;
    const capabilities = normalizeCapabilities(rawEntry.capabilities || rawEntry.supports || rawEntry.features);
    return {
      id,
      label,
      raw: rawEntry,
      source,
      capabilities
    };
  }

  const makeCacheKey = (nodeId, base, relay, api) => `${nodeId}::${base}::${relay}::${api}`;

  const getCachedMeta = (nodeId) => MODEL_CACHE.get(nodeId) || null;

  const setCachedMeta = (nodeId, meta) => {
    if (!meta) {
      MODEL_CACHE.delete(nodeId);
      return;
    }
    MODEL_CACHE.set(nodeId, meta);
    const subs = MODEL_LISTENERS.get(nodeId);
    if (subs) {
      const list = (meta.list || []).slice();
      subs.forEach((fn) => {
        try { fn(list); } catch (err) { /* noop */ }
      });
    }
  };

  const clearModelRefresh = (nodeId) => {
    const timer = MODEL_REFRESH_TIMER.get(nodeId);
    if (timer) {
      clearTimeout(timer);
      MODEL_REFRESH_TIMER.delete(nodeId);
    }
  };

  const scheduleModelRefresh = (nodeId) => {
    clearModelRefresh(nodeId);
    const entry = getCachedMeta(nodeId);
    if (!entry || !entry.base) return;
    const timer = setTimeout(() => {
      MODEL_REFRESH_TIMER.delete(nodeId);
      try {
        const rec = NodeStore.ensure(nodeId, 'LLM');
        const cfg = rec?.config || {};
        const endpoint = resolveEndpointConfig(cfg);
        const needsOverride = entry.base !== endpoint.base || entry.relay !== endpoint.relay || entry.api !== endpoint.api || (entry.mode || 'auto') !== endpoint.mode;
        const override = needsOverride
          ? { base: entry.base, relay: entry.relay, api: entry.api, endpointMode: entry.mode }
          : null;
        const opts = override ? { force: true, override } : { force: true };
        ensureModelMetadata(nodeId, cfg, opts).catch(() => {});
      } catch (err) {
        // ignore refresh issues
      }
    }, MODEL_REFRESH_MS);
    MODEL_REFRESH_TIMER.set(nodeId, timer);
  };

  const subscribeModels = (nodeId, fn) => {
    if (typeof fn !== 'function') return () => {};
    let subs = MODEL_LISTENERS.get(nodeId);
    if (!subs) {
      subs = new Set();
      MODEL_LISTENERS.set(nodeId, subs);
    }
    subs.add(fn);
    return () => {
      const set = MODEL_LISTENERS.get(nodeId);
      if (!set) return;
      set.delete(fn);
      if (!set.size) MODEL_LISTENERS.delete(nodeId);
    };
  };

  function normalizeModelShow(model, raw) {
    const data = stripLicense(raw || {});
    const det = data.details || {};
    const mi = data.modelinfo || {};

    const paramsObj = {};
    if (typeof data.parameters === 'string') {
      data.parameters.split(/\n+/).forEach((line) => {
        const m = line.match(/^\s*([A-Za-z0-9_]+)\s+(.+?)\s*$/);
        if (m) paramsObj[m[1]] = m[2];
      });
    }

    const paramNum = (key) => {
      const value = paramsObj[key];
      const num = Number(value);
      return Number.isFinite(num) ? num : null;
    };

    const capabilities = (() => {
      if (Array.isArray(data.capabilities)) return data.capabilities.map(String);
      if (Array.isArray(det.capabilities)) return det.capabilities.map(String);
      return [];
    })();

    const ctxFromModelInfo = (() => {
      if (!mi || typeof mi !== 'object') return null;
      for (const [k, v] of Object.entries(mi)) {
        if (/context_length$/i.test(k)) {
          const num = Number(v);
          if (Number.isFinite(num)) return num;
        }
      }
      return null;
    })();

    return {
      id: data.model || model,
      label: data.model || model,
      name: data.model || model,
      modified_at: data.modified_at || null,
      format: det.format || null,
      family: det.family || null,
      families: Array.isArray(det.families) ? det.families.slice() : (det.family ? [det.family] : []),
      parameter_size: det.parameter_size || null,
      quantization: det.quantization || det.quantization_level || null,
      num_ctx: ctxFromModelInfo || paramNum('num_ctx') || det.num_ctx || null,
      license: data.license || null,
      capabilities,
      raw: data,
      source: 'show'
    };
  }

  function resolveEndpointConfig(baseCfg, override = null) {
    const effective = Object.assign({}, baseCfg || {}, override || {});
    const base = String(effective.base || '').trim();
    const relayRaw = String(effective.relay || '').trim();
    const api = String(effective.api || '').trim();
    const mode = String(effective.endpointMode || 'auto').toLowerCase();
    const hasRelay = !!relayRaw;
    let useRelay = false;
    if (mode === 'remote') useRelay = hasRelay;
    else if (mode === 'auto') useRelay = hasRelay;
    else useRelay = false;
    return {
      base,
      api,
      relay: useRelay ? relayRaw : '',
      rawRelay: relayRaw,
      viaNkn: useRelay && hasRelay,
      mode,
      relayOnly: mode === 'remote'
    };
  }

  function cacheModelInfo(nodeId, modelId, info) {
    if (!modelId || !info) return;
    const key = `${nodeId}:${modelId}`;
    MODEL_INFO_CACHE.set(key, info);
    const meta = getCachedMeta(nodeId);
    const list = meta?.list;
    if (list && Array.isArray(list)) {
      const entry = list.find((item) => item.id === modelId);
      if (entry) {
        entry.capabilities = Array.isArray(info.capabilities) ? info.capabilities.map(String) : [];
        entry.raw = entry.raw || {};
        entry.raw.details = entry.raw.details || {};
        entry.raw.details.capabilities = entry.capabilities;
      }
    }
  }

  async function streamHttpNdjson(url, options, onEvent) {
    await ensureLocalNetworkAccess();
    const res = await fetch(url, options);
    if (!res.ok) {
      let detail = '';
      try {
        detail = await res.text();
      } catch (err) {
        detail = '';
      }
      const msg = detail ? `${res.status} ${res.statusText}: ${detail}` : `${res.status} ${res.statusText}`;
      throw new Error(msg);
    }
    if (!res.body) {
      try {
        const obj = await res.json();
        if (obj) onEvent?.(obj);
      } catch (err) {
        // ignore when no JSON body is present
      }
      return;
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let idx = buffer.indexOf('\n');
      while (idx >= 0) {
        const line = buffer.slice(0, idx).trim();
        buffer = buffer.slice(idx + 1);
        if (line) {
          try {
            onEvent?.(JSON.parse(line));
          } catch (err) {
            // ignore malformed chunks
          }
        }
        idx = buffer.indexOf('\n');
      }
    }
    const tail = buffer.trim();
    if (tail) {
      try {
        onEvent?.(JSON.parse(tail));
      } catch (err) {
        // ignore final partial line
      }
    }
  }

  async function fetchModelInfo(nodeId, modelId, options = {}) {
    if (!modelId) return null;
    const { force = false, override = null } = options || {};
    const rec = NodeStore.ensure(nodeId, 'LLM');
    const cfg = rec?.config || {};
    const endpoint = resolveEndpointConfig(cfg, override);
    const { base, relay, api, viaNkn, relayOnly } = endpoint;
    if (!base) return null;

    let skipCache = force;
    if (!skipCache && override) {
      const normalizedOverride = {
        base: String(override.base || '').trim(),
        relay: String(override.relay || '').trim(),
        api: String(override.api || '').trim(),
        endpointMode: String(override.endpointMode || '').trim().toLowerCase()
      };
      if (normalizedOverride.base && normalizedOverride.base !== String(cfg.base || '').trim()) skipCache = true;
      if (normalizedOverride.relay && normalizedOverride.relay !== String(cfg.relay || '').trim()) skipCache = true;
      if (normalizedOverride.api && normalizedOverride.api !== String(cfg.api || '').trim()) skipCache = true;
      if (normalizedOverride.endpointMode && normalizedOverride.endpointMode !== String(cfg.endpointMode || 'auto').trim().toLowerCase()) skipCache = true;
    }

    const key = `${nodeId}:${modelId}`;
    if (!skipCache && MODEL_INFO_CACHE.has(key)) return MODEL_INFO_CACHE.get(key);
    if (!skipCache && MODEL_INFO_PROMISES.has(key)) return MODEL_INFO_PROMISES.get(key);

    const promise = Net.postJSON(
      base.replace(/\/+$/, ''),
      '/api/show',
      { name: modelId },
      api,
      viaNkn,
      relay,
      undefined,
      { forceRelay: relayOnly }
    )
      .then((data) => {
        try {
          console.log('[LLM] model show response', {
            nodeId,
            modelId,
            raw: data
          });
        } catch (err) {
          // ignore logging failures
        }
        const info = normalizeModelShow(modelId, data || {});
        cacheModelInfo(nodeId, modelId, info);
        return info;
      })
      .catch((err) => {
        MODEL_INFO_CACHE.delete(key);
        throw err;
      })
      .finally(() => {
        MODEL_INFO_PROMISES.delete(key);
      });

    MODEL_INFO_PROMISES.set(key, promise);
    return promise;
  }

  async function fetchModelMetadataList(base, api, viaNkn, relay, options = {}) {
    const { forceRelay = false } = options || {};
    const cleaned = (base || '').replace(/\/+$/, '');
    if (!cleaned) return [];
    const collected = new Map();

    const tryFetch = async (path) => {
      try {
        const res = await Net.getJSON(cleaned, path, api, viaNkn, relay, { forceRelay });
        if (!res) return;
        const pushEntry = (entry) => {
          const normalized = normalizeModelEntry(entry, path);
          if (normalized) mergeEntry(collected, normalized);
        };
        if (Array.isArray(res)) {
          res.forEach(pushEntry);
        } else if (Array.isArray(res?.models)) {
          res.models.forEach(pushEntry);
        } else if (Array.isArray(res?.data)) {
          res.data.forEach(pushEntry);
        }
      } catch (err) {
        // ignore; we'll fall back to other endpoints
      }
    };

    await tryFetch('/v1/models');
    await tryFetch('/models');
    await tryFetch('/api/tags');

    return Array.from(collected.values()).sort((a, b) => a.label.localeCompare(b.label));
  }

  async function ensureModelMetadata(nodeId, cfg, { force = false, override = null } = {}) {
    const endpoint = resolveEndpointConfig(cfg, override);
    const { base, relay, api, viaNkn, mode, relayOnly } = endpoint;
    if (!base) {
      setCachedMeta(nodeId, { list: [], base: '', relay: '', api: '', fetchedAt: Date.now(), mode });
      clearModelRefresh(nodeId);
      return [];
    }
    const cache = getCachedMeta(nodeId);
    const now = Date.now();
    if (!force && cache && cache.base === base && cache.relay === relay && cache.api === api && (now - cache.fetchedAt) < MODEL_REFRESH_MS) {
      return cache.list.slice();
    }
    const key = makeCacheKey(nodeId, base, relay, api);
    if (MODEL_PROMISE.has(key)) {
      return MODEL_PROMISE.get(key);
    }
    const task = (async () => {
      const list = await fetchModelMetadataList(base, api, viaNkn, relay, { forceRelay: relayOnly });
      setCachedMeta(nodeId, { list, base, relay, api, fetchedAt: Date.now(), mode });
      return list.slice();
    })();
    MODEL_PROMISE.set(key, task);
    try {
      return await task;
    } finally {
      MODEL_PROMISE.delete(key);
      const entry = getCachedMeta(nodeId);
      if (entry && entry.base) scheduleModelRefresh(nodeId);
      else clearModelRefresh(nodeId);
    }
  }

  async function ensureModelConfigured(nodeId, cfg) {
    const base = (cfg.base || '').trim();
    if (!base) return cfg;
    const metadata = await ensureModelMetadata(nodeId, cfg);
    if (!metadata.length) return cfg;
    const currentId = (cfg.model || '').trim();
    const chosen = metadata.find((item) => item.id === currentId) || metadata[0];
    const patch = {
      type: 'LLM',
      model: chosen.id,
      capabilities: chosen.capabilities || []
    };
    NodeStore.update(nodeId, patch);
    return NodeStore.ensure(nodeId, 'LLM').config || cfg;
  }

  async function ensureModels(nodeId, options = {}) {
    const rec = NodeStore.ensure(nodeId, 'LLM');
    const cfg = rec.config || {};
    return ensureModelMetadata(nodeId, cfg, options) || [];
  }

  function listModels(nodeId) {
    const entry = getCachedMeta(nodeId);
    return entry && Array.isArray(entry.list) ? entry.list.slice() : [];
  }

  function getModelInfo(nodeId, modelId) {
    const cached = MODEL_INFO_CACHE.get(`${nodeId}:${modelId}`);
    if (cached) return cached;
    const metadata = listModels(nodeId);
    return metadata.find((item) => item.id === modelId) || null;
  }

  async function pullModel(nodeId, params = {}) {
    const { model, insecure = false, override = null, onEvent, signal } = params || {};
    const modelName = String(model || '').trim();
    if (!modelName) throw new Error('Model name is required');

    await ensureLocalNetworkAccess({ requireGesture: true });

    const rec = NodeStore.ensure(nodeId, 'LLM');
    const cfg = rec?.config || {};
    const endpoint = resolveEndpointConfig(cfg, override);
    const { base, api, relay, viaNkn, mode } = endpoint;
    if (!base) throw new Error('Base URL is required');
    if (mode === 'remote' && !viaNkn) throw new Error('Relay address required for remote mode');

    const body = { model: modelName, stream: true };
    if (insecure) body.insecure = true;

    let success = false;
    let lastError = null;
    const handleEvent = (evt) => {
      if (!evt || typeof evt !== 'object') return;
      try { onEvent?.(evt); } catch (err) { /* ignore */ }
      if (typeof evt.status === 'string') {
        const status = evt.status.toLowerCase();
        if (status === 'success') success = true;
        if (status === 'error' && !lastError) lastError = evt.error || evt.message || 'Pull failed';
      }
      if (evt.error && !lastError) lastError = evt.error;
    };

    const baseUrl = base.replace(/\/+$/, '');
    const timeout = 10 * 60 * 1000;

    if (viaNkn) {
      const headers = Net.auth({ 'X-Relay-Stream': 'lines' }, api);
      await Net.nknStream(
        {
          url: `${baseUrl}/api/pull`,
          method: 'POST',
          headers,
          json: body,
          stream: 'lines',
          timeout_ms: timeout
        },
        relay,
        {
          onLine: (line) => {
            if (!line) return;
            try {
              handleEvent(JSON.parse(line));
            } catch (err) {
              // ignore malformed JSON fragments
            }
          },
          onError: (err) => {
            if (!lastError) lastError = err?.message || String(err || 'Pull failed');
          },
          onEnd: (meta) => {
            try {
              if (meta && meta.json) handleEvent(meta.json);
            } catch (err) {
              // ignore
            }
          }
        },
        timeout
      );
    } else {
      const headers = Net.auth({ Accept: 'application/x-ndjson', 'Content-Type': 'application/json' }, api);
      await streamHttpNdjson(
        `${baseUrl}/api/pull`,
        { method: 'POST', headers, body: JSON.stringify(body), signal },
        handleEvent
      );
    }

    if (!success) {
      const message = lastError || 'Model pull did not complete';
      throw new Error(message);
    }

    await ensureModelMetadata(nodeId, cfg, { force: true, override });
    return { ok: true };
  }

  async function refreshModels(nodeId, override = null, options = {}) {
    const rec = NodeStore.ensure(nodeId, 'LLM');
    const cfg = rec.config || {};
    const merged = Object.assign({ force: true }, options || {});
    if (override) merged.override = override;
    await ensureLocalNetworkAccess({ requireGesture: true });
    return ensureModelMetadata(nodeId, cfg, merged);
  }

  function dispose(nodeId) {
    clearModelRefresh(nodeId);
    MODEL_CACHE.delete(nodeId);
    MODEL_LISTENERS.delete(nodeId);
    for (const key of Array.from(MODEL_PROMISE.keys())) {
      if (key.startsWith(`${nodeId}::`)) MODEL_PROMISE.delete(key);
    }
    for (const key of Array.from(MODEL_INFO_CACHE.keys())) {
      if (key.startsWith(`${nodeId}:`)) MODEL_INFO_CACHE.delete(key);
    }
    for (const key of Array.from(MODEL_INFO_PROMISES.keys())) {
      if (key.startsWith(`${nodeId}:`)) MODEL_INFO_PROMISES.delete(key);
    }
    IMAGE_PAYLOADS.delete(nodeId);
    TOOL_PAYLOADS.delete(nodeId);
  }

  async function onPrompt(nodeId, payload) {
    const node = getNode(nodeId);
    if (!node) return;
    await ensureLocalNetworkAccess({ requireGesture: true });
    const rec = NodeStore.ensure(nodeId, 'LLM');
    let cfg = rec.config || {};
    cfg = await ensureModelConfigured(nodeId, cfg);
    const endpoint = resolveEndpointConfig(cfg);
    const { base, api, relay, viaNkn, relayOnly } = endpoint;
    if (relayOnly && !viaNkn) throw new Error('Relay address required for remote mode');
    const model = (cfg.model || '').trim();
    const stream = !!cfg.stream;
    const sysUse = !!cfg.useSystem;
    const sysTxt = (cfg.system || '').trim();
    const memOn = !!cfg.memoryOn;
    const persist = !!cfg.persistMemory;
    const maxTurns = Number.isFinite(cfg.maxTurns) ? cfg.maxTurns : 16;
    const usingNkn = viaNkn;
    const updateRelayState = (state, message) => {
      if (!usingNkn) return;
      setRelayState(nodeId, { state, message });
    };

    const capabilitySet = new Set((cfg.capabilities || []).map((cap) => String(cap).toLowerCase()));
    const supportsThinking = capabilitySet.has('thinking') || capabilitySet.has('think') || capabilitySet.has('reasoning');
    const supportsImages = capabilitySet.has('images') || capabilitySet.has('image') || capabilitySet.has('vision');
    const thinkingEnabled = supportsThinking && !!cfg.think;
    const imagePayload = supportsImages ? IMAGE_PAYLOADS.get(nodeId) : null;
    if (imagePayload) IMAGE_PAYLOADS.delete(nodeId);

    if (usingNkn) updateRelayState('warn', 'Awaiting response');

    const text = String(payload && (payload.text ?? payload.prompt ?? payload) || '');
    const memory = Array.isArray(cfg.memory) ? cfg.memory.slice() : [];

    function buildMessages(latest) {
      const msgs = [];
      let startIdx = 0;
      if (sysUse && sysTxt) msgs.push({ role: 'system', content: sysTxt });
      if (memOn) {
        if (memory.length && memory[0].role === 'system') {
          if (!msgs.length) msgs.push(memory[0]);
          startIdx = 1;
        }
        for (let i = startIdx; i < memory.length; i++) msgs.push(memory[i]);
      }
      if (latest?.trim()) msgs.push({ role: 'user', content: latest.trim() });
      if (memOn) {
        let userCount = msgs.filter((m) => m.role === 'user').length;
        while (userCount > maxTurns) {
          const sysIndex = msgs[0]?.role === 'system' ? 1 : 0;
          const userIdx = msgs.findIndex((m, idx) => idx >= sysIndex && m.role === 'user');
          if (userIdx < 0) break;
          msgs.splice(userIdx, 1);
          if (msgs[userIdx]?.role === 'assistant') msgs.splice(userIdx, 1);
          userCount--;
        }
      }
      return msgs;
    }

    const messages = buildMessages(text);
    const mux = createSentenceMux(250);
    let full = '';

    const outEl = node.el?.querySelector('[data-llm-out]');
    let uiLines = [];
    let uiBuf = '';
    const setOut = (value) => {
      if (!outEl) return;
      outEl.textContent = value || '';
      outEl.scrollTop = outEl.scrollHeight;
    };
    setOut('');
    uiLines = [];
    uiBuf = '';

    const emitSentenceFinal = (s) => {
      Router.sendFrom(nodeId, 'final', { nodeId, text: s, eos: true });
    };
    const emitSentenceDelta = (s) => {
      Router.sendFrom(nodeId, 'delta', { nodeId, type: 'text', text: s, eos: true });
    };
    const emitBoth = (s) => {
      emitSentenceDelta(s);
      emitSentenceFinal(s);
      const line = String(s || '').trim();
      if (line) {
        uiLines.push(line);
        uiBuf = '';
        setOut(uiLines.join('\n'));
      }
    };

    const normalizedImages = supportsImages ? normalizeImageSources(imagePayload) : [];

    const buildRequestBody = (streamMode) => {
      const preparedMessages = messages.map((msg) => ({ ...msg }));

      if (normalizedImages.length) {
        let injected = false;
        for (let i = preparedMessages.length - 1; i >= 0; i--) {
          const msg = preparedMessages[i];
          if (!msg || msg.role !== 'user') continue;
          const clone = { ...msg };
          let textContent = clone.content;
          if (Array.isArray(textContent)) {
            textContent = textContent
              .map((part) => {
                if (typeof part === 'string') return part;
                if (part && typeof part === 'object') {
                  if (typeof part.text === 'string') return part.text;
                  if (typeof part.content === 'string') return part.content;
                }
                return '';
              })
              .filter(Boolean)
              .join('\n');
          } else if (textContent && typeof textContent === 'object') {
            textContent = JSON.stringify(textContent);
          }
          if (typeof textContent !== 'string') textContent = '';
          clone.content = textContent;
          clone.images = normalizedImages.slice();
          preparedMessages[i] = clone;
          injected = true;
          break;
        }
        if (!injected) {
          const fallbackText = typeof text === 'string' ? text.trim() : '';
          preparedMessages.push({ role: 'user', content: fallbackText, images: normalizedImages.slice() });
        }
      }

      const body = { model, messages: preparedMessages, stream: streamMode };
      if (thinkingEnabled) {
        body.thinking = true;
        body.options = Object.assign({}, body.options, { thinking: true });
      }
      const toolPayload = TOOL_PAYLOADS.get(nodeId) ?? cfg.tools;
      if (Array.isArray(toolPayload) ? toolPayload.length : !!toolPayload) {
        body.tools = toolPayload;
        body.options = Object.assign({}, body.options, { tools: toolPayload });
      }
      return body;
    };

    try {
      if (stream) {
        const pump = makeNdjsonPump((line) => {
          try {
            const obj = JSON.parse(line);
            let delta =
              (obj.message && typeof obj.message.content === 'string' && obj.message.content) ||
              (typeof obj.response === 'string' && obj.response) ||
              (typeof obj.delta === 'string' && obj.delta) || '';
            if (!delta && (obj.done || obj.complete) && typeof obj.final === 'string') delta = obj.final;
            if (!delta && (obj.done || obj.complete) && obj.message && typeof obj.message.content === 'string') delta = obj.message.content;
            if (delta) {
              const clean = stripEOT(delta);
              full += clean;
              uiBuf += clean;
              setOut(uiLines.length ? `${uiLines.join('\n')}\n${uiBuf}` : uiBuf);
              mux.push(clean, emitBoth);
            }
          } catch (err) {
            // ignore parse errors per chunk
          }
        });

        let relayMarkedOk = false;
        const markRelayOk = (msg) => {
          if (!usingNkn || relayMarkedOk) return;
          relayMarkedOk = true;
          updateRelayState('ok', msg);
        };

        if (viaNkn) {
          let expected = null;
          const stash = new Map();
          const seen = new Set();
          const flushReorder = () => {
            while (expected != null && stash.has(expected)) {
              pump.push(`${stash.get(expected)}\n`);
              stash.delete(expected);
              expected++;
            }
          };
        await Net.nknStream(
          {
            url: base.replace(/\/+$/, '') + '/api/chat',
            method: 'POST',
            headers: Net.auth({ 'X-Relay-Stream': 'chunks', Accept: 'application/x-ndjson' }, api),
            json: buildRequestBody(true),
            timeout_ms: 180000
          },
            relay,
            {
              onBegin: () => {},
              onLine: (line, seqRaw) => {
                markRelayOk('Streaming response');
                const seq = seqRaw | 0;
                if (seen.has(seq)) return;
                seen.add(seq);
                if (expected == null) expected = seq;
                if (seq === expected) {
                  pump.push(`${line}\n`);
                  expected++;
                  flushReorder();
                } else if (seq > expected) {
                  stash.set(seq, line);
                }
              },
              onChunk: (bytes) => {
                markRelayOk('Streaming response');
                pump.push(bytes);
              },
              onEnd: () => {
                flushReorder();
                pump.flush();
                mux.flush(emitBoth);
              }
            },
            180000
          );
          if (usingNkn) {
            const completionMessage = relayMarkedOk ? 'Response complete' : 'No response received';
            updateRelayState(relayMarkedOk ? 'ok' : 'warn', completionMessage);
          }
        } else {
          const res = await fetch(base.replace(/\/+$/, '') + '/api/chat', {
            method: 'POST',
            headers: Net.auth({ Accept: 'application/x-ndjson', 'Content-Type': 'application/json' }, api),
            body: JSON.stringify(buildRequestBody(true))
          });
          if (!res.ok || !res.body) throw new Error(`${res.status} ${res.statusText}`);
          const reader = res.body.getReader();
          while (true) {
            const { value, done } = await reader.read();
            if (done) break;
            if (value && value.byteLength) pump.push(value);
          }
          pump.flush();
          mux.flush(emitBoth);
        }
      } else {
        const data = await Net.postJSON(
          base,
          '/api/chat',
          buildRequestBody(false),
          api,
          viaNkn,
          relay,
          120000,
          { forceRelay: relayOnly }
        );
        if (usingNkn) updateRelayState('ok', 'Response ready');
        full = stripEOT((data?.message?.content) || (data?.response) || '') || '';
        if (full) {
          uiLines = [full.trim()];
          uiBuf = '';
          setOut(uiLines.join('\n'));
          mux.push(`${full}\n`, emitBoth);
          mux.flush(emitBoth);
        }
      }

      if (cfg.memoryOn) {
        const newMemory = (memory || []).slice();
        if (sysUse && sysTxt && !(newMemory.length && newMemory[0].role === 'system')) {
          newMemory.unshift({ role: 'system', content: sysTxt });
        }
        if (text?.trim()) newMemory.push({ role: 'user', content: text.trim() });
        const finalText = full.trim();
        if (finalText) newMemory.push({ role: 'assistant', content: finalText });
        let pairs = 0;
        const pruned = [];
        let i = newMemory.length && newMemory[0].role === 'system' ? 1 : 0;
        for (let k = newMemory.length - 1; k >= i; k--) {
          pruned.push(newMemory[k]);
          if (newMemory[k].role === 'user') {
            pairs++;
            if (pairs >= maxTurns) break;
          }
        }
        pruned.reverse();
        const out = newMemory[0] && newMemory[0].role === 'system' ? [newMemory[0], ...pruned] : pruned;
        NodeStore.update(nodeId, { memory: out });
        Router.sendFrom(nodeId, 'memory', { type: 'updated', size: out.length });
      }
    } catch (err) {
      log(`[llm ${nodeId}] ${err.message}`);
      if (usingNkn) updateRelayState('err', err?.message || 'LLM relay failed');
    }
  }

  function onSystem(nodeId, payload) {
    const text = String(payload && (payload.text ?? payload.prompt ?? payload) || '').trim();
    NodeStore.update(nodeId, { system: text, useSystem: true });
  }

  function onImage(nodeId, payload) {
    const images = normalizeImageSources(payload);
    if (!images.length) return;
    IMAGE_PAYLOADS.set(nodeId, images);
  }

  function onTools(nodeId, payload) {
    if (!nodeId) return;
    if (payload == null || (typeof payload === 'string' && !payload.trim())) {
      TOOL_PAYLOADS.delete(nodeId);
      NodeStore.update(nodeId, { type: 'LLM', tools: [] });
      return;
    }
    let normalized = payload;
    if (typeof payload === 'string') {
      try {
        normalized = JSON.parse(payload);
      } catch (err) {
        normalized = payload;
      }
    }
    TOOL_PAYLOADS.set(nodeId, normalized);
    NodeStore.update(nodeId, { type: 'LLM', tools: normalized });
  }

  async function ensureDefaults(nodeId) {
    try {
      const rec = NodeStore.ensure(nodeId, 'LLM');
      const cfg = rec?.config || {};
      await ensureModels(nodeId);
      const updated = await ensureModelConfigured(nodeId, cfg);
      const selected = (updated.model || '').trim();
      if (selected) {
        const info = getModelInfo(nodeId, selected);
        if (info) NodeStore.update(nodeId, { type: 'LLM', capabilities: info.capabilities || [] });
      }
    } catch (err) {
      // ignore background discovery failures
    }
  }

  return {
    onPrompt,
    onSystem,
    onImage,
    onTools,
    ensureDefaults,
    ensureModels,
    listModels,
    getModelInfo,
    fetchModelInfo,
    pullModel,
    refreshModels,
    subscribeModels,
    dispose
  };
}

export { createLLM };
