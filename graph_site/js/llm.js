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
  const MODEL_METADATA = new Map();
  const IMAGE_PAYLOADS = new Map();

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

  async function fetchModelMetadataList(base, api, viaNkn, relay) {
    const cleaned = (base || '').replace(/\/+$/, '');
    if (!cleaned) return [];
    const collected = new Map();

    const tryFetch = async (path) => {
      try {
        const res = await Net.getJSON(cleaned, path, api, viaNkn, relay);
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

  async function ensureModelMetadata(nodeId, cfg, { force = false } = {}) {
    const base = (cfg.base || '').trim();
    if (!base) {
      MODEL_METADATA.set(nodeId, []);
      return [];
    }
    if (!force && MODEL_METADATA.has(nodeId)) return MODEL_METADATA.get(nodeId);
    if (MODEL_PROMISE.has(nodeId)) {
      try {
        return await MODEL_PROMISE.get(nodeId);
      } catch (err) {
        throw err;
      }
    }
    const viaNkn = CFG.transport === 'nkn';
    const relay = (cfg.relay || '').trim();
    const api = (cfg.api || '').trim();
    const task = (async () => {
      const list = await fetchModelMetadataList(base, api, viaNkn, relay);
      MODEL_METADATA.set(nodeId, list);
      return list;
    })();
    MODEL_PROMISE.set(nodeId, task);
    try {
      return await task;
    } finally {
      MODEL_PROMISE.delete(nodeId);
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
    return MODEL_METADATA.get(nodeId) || [];
  }

  function getModelInfo(nodeId, modelId) {
    const metadata = listModels(nodeId);
    return metadata.find((item) => item.id === modelId) || null;
  }

  async function onPrompt(nodeId, payload) {
    const node = getNode(nodeId);
    if (!node) return;
    const rec = NodeStore.ensure(nodeId, 'LLM');
    let cfg = rec.config || {};
    cfg = await ensureModelConfigured(nodeId, cfg);
    const base = (cfg.base || '').trim();
    const api = (cfg.api || '').trim();
    const relay = (cfg.relay || '').trim();
    const model = (cfg.model || '').trim();
    const viaNkn = CFG.transport === 'nkn';
    const stream = !!cfg.stream;
    const sysUse = !!cfg.useSystem;
    const sysTxt = (cfg.system || '').trim();
    const memOn = !!cfg.memoryOn;
    const persist = !!cfg.persistMemory;
    const maxTurns = Number.isFinite(cfg.maxTurns) ? cfg.maxTurns : 16;
    const usingNkn = viaNkn && !!relay;
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

    const buildRequestBody = (streamMode) => {
      const body = { model, messages, stream: streamMode };
      if (thinkingEnabled) {
        body.thinking = true;
        body.options = Object.assign({}, body.options, { thinking: true });
      }
      if (imagePayload) {
        body.images = Array.isArray(imagePayload) ? imagePayload : [imagePayload];
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
        const data = await Net.postJSON(base, '/api/chat', buildRequestBody(false), api, viaNkn, relay, 120000);
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

  function extractImagePayload(payload) {
    if (payload == null) return null;
    if (typeof payload === 'string') return payload;
    if (payload.data != null) return payload.data;
    if (payload.image != null) return payload.image;
    if (payload.payload != null) return payload.payload;
    return payload;
  }

  function onImage(nodeId, payload) {
    const data = extractImagePayload(payload);
    if (data == null) return;
    IMAGE_PAYLOADS.set(nodeId, data);
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
        if (info) NodeStore.update(nodeId, { capabilities: info.capabilities || [] });
      }
    } catch (err) {
      // ignore background discovery failures
    }
  }

  return { onPrompt, onSystem, onImage, ensureDefaults, ensureModels, listModels, getModelInfo };
}

export { createLLM };
