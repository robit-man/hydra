import { ensureLocalNetworkAccess } from './localNetwork.js';

function createTTS({ getNode, NodeStore, Net, CFG, log, b64ToBytes, setRelayState = () => {}, Router }) {
  const state = new Map();
  const MODEL_PROMISE = new Map();
  const MODEL_CACHE = new Map();
  const MODEL_REFRESH_TIMER = new Map();
  const MODEL_LISTENERS = new Map();
  const MODEL_REFRESH_MS = 60 * 1000;
  const SIGNAL_TRUE_FALSE = 'true/false';
  const SIGNAL_TRUE_EMPTY = 'true/empty';
  const muteRequests = new Map();
  const lastActiveByNode = new Map();
  const DEFAULT_FILTER_TOKENS = ['#'];
  const DEFAULT_WASM_PRESET = 'piper_en_US_libritts_r_medium';
  const DEFAULT_PIPER_MODELS = [
    {
      id: 'piper_en_US_libritts_r_medium',
      label: 'English (US) • LibriTTS R Medium',
      modelUrl:
        'https://huggingface.co/rhasspy/piper-voices/resolve/main/en/en_US/libritts_r/medium/en_US-libritts_r-medium.onnx',
      configUrl:
        'https://huggingface.co/rhasspy/piper-voices/resolve/main/en/en_US/libritts_r/medium/en_US-libritts_r-medium.onnx.json',
      sizeMB: 420
    },
    {
      id: 'piper_en_US_ljspeech_medium',
      label: 'English (US) • LJSpeech Medium',
      modelUrl:
        'https://huggingface.co/rhasspy/piper-voices/resolve/main/en/en_US/ljspeech/medium/en_US-ljspeech-medium.onnx',
      configUrl:
        'https://huggingface.co/rhasspy/piper-voices/resolve/main/en/en_US/ljspeech/medium/en_US-ljspeech-medium.onnx.json',
      sizeMB: 380
    },
    {
      id: 'piper_en_GB_cori_high',
      label: 'English (UK) • Cori High',
      modelUrl:
        'https://huggingface.co/rhasspy/piper-voices/resolve/main/en/en_GB/cori/high/en_GB-cori-high.onnx',
      configUrl:
        'https://huggingface.co/rhasspy/piper-voices/resolve/main/en/en_GB/cori/high/en_GB-cori-high.onnx.json',
      sizeMB: 510
    },
    {
      id: 'piper_es_ES_ana_medium',
      label: 'Spanish (ES) • Ana Medium',
      modelUrl:
        'https://huggingface.co/rhasspy/piper-voices/resolve/main/es/es_ES/ana/medium/es_ES-ana-medium.onnx',
      configUrl:
        'https://huggingface.co/rhasspy/piper-voices/resolve/main/es/es_ES/ana/medium/es_ES-ana-medium.onnx.json',
      sizeMB: 400
    }
  ];
  const isWasmMode = (cfg) => cfg && (cfg.wasm === true || String(cfg.wasm).toLowerCase() === 'true');
  let wasmModulePromise = null;
  const loadWasmModule = async () => {
    if (!wasmModulePromise) wasmModulePromise = import('./wasm/ttsWasm.js');
    return wasmModulePromise;
  };
  const wasmVoicesCache = new Map();
  const getCustomVoices = (cfg) => {
    if (!cfg || !Array.isArray(cfg.wasmCustomVoices)) return [];
    return cfg.wasmCustomVoices
      .map((v, idx) => {
        const id = v.id || v.key || `custom_${idx}`;
        const label = v.label || v.name || id;
        const modelUrl = v.modelUrl || v.model || '';
        const configUrl = v.configUrl || v.config || '';
        const sizeMB = Number(v.sizeMB);
        return { id, label, modelUrl, configUrl, sizeMB: Number.isFinite(sizeMB) ? sizeMB : undefined, source: 'custom' };
      })
      .filter((v) => v.modelUrl && v.configUrl);
  };

  const getWasmVoiceOptions = (cfg) => {
    const custom = getCustomVoices(cfg);
    const defaults = DEFAULT_PIPER_MODELS.map((v) => ({ ...v, source: 'default' }));
    return [...custom, ...defaults];
  };

  const resolveWasmVoice = (cfg) => {
    const options = getWasmVoiceOptions(cfg);
    const presetId = cfg?.wasmVoicePreset || DEFAULT_WASM_PRESET;
    const found = options.find((o) => o.id === presetId);
    const voice = found || options[0] || {
      id: DEFAULT_WASM_PRESET,
      label: 'Default Piper Voice',
      modelUrl: cfg?.wasmPiperModelUrl,
      configUrl: cfg?.wasmPiperConfigUrl,
      sizeMB: 400,
      source: 'fallback'
    };
    const modelUrl = voice.modelUrl || cfg?.wasmPiperModelUrl;
    const configUrl = voice.configUrl || cfg?.wasmPiperConfigUrl;
    return { ...voice, modelUrl, configUrl };
  };

  const checkMemoryForVoice = (voice) => {
    const devMem = typeof navigator !== 'undefined' ? Number(navigator.deviceMemory || 0) : 0;
    const estMB = Number(voice?.sizeMB || 0) || 600;
    if (!devMem) return true;
    const budget = devMem * 700; // rough usable MB
    if (estMB > budget) {
      throw new Error(`Voice model (~${Math.round(estMB)} MB) may exceed device memory (${devMem} GB). Use remote TTS or smaller model.`);
    }
    return true;
  };
  const stripEmoji = (() => {
    try {
      const propertyRegex = new RegExp('[\\p{Extended_Pictographic}\\p{Emoji_Presentation}]', 'gu');
      const modifierRegex = /[\u200D\uFE0F]/g;
      return (text) => {
        if (!text) return '';
        return text.replace(propertyRegex, '').replace(modifierRegex, '');
      };
    } catch (err) {
      const ranges = [
        [0x1F000, 0x1FFFF],
        [0x1F1E6, 0x1F1FF],
        [0x2600, 0x27BF],
        [0x2300, 0x23FF],
        [0xFE00, 0xFE0F],
        [0xE0020, 0xE007F]
      ];
      const singles = new Set([0x00A9, 0x00AE, 0x203C, 0x2049, 0x2122, 0x2139, 0x3030, 0x303D, 0x3297, 0x3299, 0x200D]);
      return (text) => {
        if (!text) return '';
        let out = '';
        for (const ch of text) {
          const cp = ch.codePointAt(0);
          if (!cp) continue;
          let remove = singles.has(cp);
          if (!remove) {
            for (const [start, end] of ranges) {
              if (cp >= start && cp <= end) {
                remove = true;
                break;
              }
            }
          }
          if (!remove) out += ch;
        }
        return out;
      };
    }
  })();

  const clampVolume = (value) => {
    const num = Number(value);
    if (!Number.isFinite(num)) return 1;
    return Math.max(0, Math.min(1, num));
  };

  const coerceFromPayload = (raw) => {
    if (raw == null) return null;
    if (typeof raw === 'boolean') return raw;
    if (typeof raw === 'number') return raw !== 0;
    if (typeof raw === 'string') {
      const text = raw.trim().toLowerCase();
      if (!text) return false;
      if (['true', '1', 'yes', 'on', 'start', 'active'].includes(text)) return true;
      if (['false', '0', 'no', 'off', 'stop', 'inactive'].includes(text)) return false;
      return true;
    }
    if (typeof raw === 'object') {
      if ('value' in raw) return coerceFromPayload(raw.value);
      if ('text' in raw) return coerceFromPayload(raw.text);
      if ('data' in raw) return coerceFromPayload(raw.data);
      if ('state' in raw) return coerceFromPayload(raw.state);
    }
    return Boolean(raw);
  };

  const interpretSignalInput = (payload, mode = SIGNAL_TRUE_FALSE) => {
    if (payload == null) return false;
    const coerced = coerceFromPayload(payload);
    if (coerced === null) return false;
    return Boolean(coerced);
  };

  const getSignalMode = (nodeId, key, fallback = SIGNAL_TRUE_FALSE) => {
    try {
      const rec = NodeStore.ensure(nodeId, 'TTS');
      const cfg = rec?.config || {};
      const raw = cfg[key];
      if (typeof raw === 'string' && raw) return raw;
    } catch (err) {
      // ignore lookup issues
    }
    return fallback;
  };

  const getActiveMode = (nodeId) => getSignalMode(nodeId, 'activeSignalMode');
  const getMuteMode = (nodeId) => getSignalMode(nodeId, 'muteSignalMode');

  const emitActive = (nodeId, isActive) => {
    if (!nodeId) return;
    const prev = lastActiveByNode.get(nodeId);
    if (prev === isActive) return;
    lastActiveByNode.set(nodeId, isActive);
    if (isActive) {
      Router?.sendFrom?.(nodeId, 'active', true);
      return;
    }
    if (getActiveMode(nodeId) === SIGNAL_TRUE_FALSE) {
      Router?.sendFrom?.(nodeId, 'active', false);
    }
  };

  const setActiveState = (nodeId, st, active) => {
    const final = !!(active && !st?.muted);
    if (st && st.active === final) return;
    if (st) st.active = final;
    emitActive(nodeId, final);
  };

  const currentVolumeValue = (st) => {
    if (!st) return 1;
    const slider = st.volumeControl;
    if (slider) return clampVolume(slider.value);
    return clampVolume(st.volume);
  };

  const applyMuteState = (nodeId, st, muted) => {
    if (!st) return;
    const next = !!muted;
    st.muted = next;
    const value = currentVolumeValue(st);
    if (st.gain) st.gain.gain.value = next ? 0 : value;
    if (st.audioEl) {
      st.audioEl.muted = next;
      st.audioEl.volume = value;
    }
    if (next) setActiveState(nodeId, st, false);
  };

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
        // ignore
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
            // ignore
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
        // ignore trailing fragment
      }
    }
  }

  function normalizeModelEntry(raw) {
    if (raw == null) return null;
    if (typeof raw === 'string') {
      const id = raw.trim();
      if (!id) return null;
      return { id, label: id, raw: { name: id } };
    }
    if (typeof raw !== 'object') return null;
    const id = String(raw.id || raw.name || raw.model || raw.voice || '').trim();
    if (!id) return null;
    const label = String(raw.name || raw.id || raw.voice || id).trim() || id;
    return { id, label, raw };
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
      mode
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
        const rec = NodeStore.ensure(nodeId, 'TTS');
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

  async function fetchModelMetadata(base, api, viaNkn, relay) {
    const cleaned = (base || '').replace(/\/+$/, '');
    if (!cleaned) return [];
    try {
      const res = await Net.getJSON(cleaned, '/models', api, viaNkn, relay);
      const list = [];
      const push = (entry) => {
        const normalized = normalizeModelEntry(entry);
        if (normalized) list.push(normalized);
      };
      if (Array.isArray(res?.models)) res.models.forEach(push);
      else if (Array.isArray(res?.data)) res.data.forEach(push);
      else if (Array.isArray(res)) res.forEach(push);
      const dedup = new Map();
      for (const item of list) {
        if (!dedup.has(item.id)) dedup.set(item.id, item);
      }
      return Array.from(dedup.values()).sort((a, b) => a.label.localeCompare(b.label));
    } catch (err) {
      return [];
    }
  }

  async function ensureModelMetadata(nodeId, cfg, { force = false, override = null } = {}) {
    const endpoint = resolveEndpointConfig(cfg, override);
    const { base, relay, api, viaNkn } = endpoint;
    if (!base) {
      setCachedMeta(nodeId, { list: [], base: '', relay: '', api: '', fetchedAt: Date.now(), mode: endpoint.mode });
      clearModelRefresh(nodeId);
      return [];
    }
    const cache = getCachedMeta(nodeId);
    const now = Date.now();
    if (!force && cache && cache.base === base && cache.relay === relay && cache.api === api && (now - cache.fetchedAt) < MODEL_REFRESH_MS) {
      return cache.list.slice();
    }
    const key = makeCacheKey(nodeId, base, relay, api);
    if (MODEL_PROMISE.has(key)) return MODEL_PROMISE.get(key);
    const task = (async () => {
      const list = await fetchModelMetadata(base, api, viaNkn, relay);
      setCachedMeta(nodeId, { list, base, relay, api, fetchedAt: Date.now(), mode: endpoint.mode });
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
    NodeStore.update(nodeId, { type: 'TTS', model: chosen.id });
    return NodeStore.ensure(nodeId, 'TTS').config || cfg;
  }

  async function ensureModels(nodeId, options = {}) {
    const rec = NodeStore.ensure(nodeId, 'TTS');
    const cfg = rec?.config || {};
    return ensureModelMetadata(nodeId, cfg, options) || [];
  }

  function listModels(nodeId) {
    const entry = getCachedMeta(nodeId);
    return entry && Array.isArray(entry.list) ? entry.list.slice() : [];
  }

  function getModelInfo(nodeId, modelId) {
    const models = listModels(nodeId);
    return models.find((item) => item.id === modelId) || null;
  }

  async function pullModel(nodeId, params = {}) {
    const { onnxJsonUrl, onnxModelUrl, name, override = null, onEvent, signal } = params || {};
    const jsonUrl = String(onnxJsonUrl || '').trim();
    const modelUrl = String(onnxModelUrl || '').trim();
    if (!jsonUrl || !modelUrl) throw new Error('Both JSON and ONNX URLs are required');

    await ensureLocalNetworkAccess({ requireGesture: true });

    const rec = NodeStore.ensure(nodeId, 'TTS');
    const cfg = rec?.config || {};
    const endpoint = resolveEndpointConfig(cfg, override);
    const { base, relay, api, viaNkn, mode } = endpoint;
    if (!base) throw new Error('Base URL is required');
    if (mode === 'remote' && !viaNkn) throw new Error('Relay address required for remote mode');

    const body = {
      onnx_json_url: jsonUrl,
      onnx_model_url: modelUrl
    };
    const trimmedName = String(name || '').trim();
    if (trimmedName) body.name = trimmedName;

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
          url: `${baseUrl}/models/pull`,
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
              // ignore
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
        `${baseUrl}/models/pull`,
        { method: 'POST', headers, body: JSON.stringify(body), signal },
        handleEvent
      );
    }

    if (!success) {
      const message = lastError || 'Voice pull did not complete';
      throw new Error(message);
    }

    await ensureModelMetadata(nodeId, cfg, { force: true, override });
    return { ok: true };
  }

  async function refreshModels(nodeId, override = null, options = {}) {
    const rec = NodeStore.ensure(nodeId, 'TTS');
    const cfg = rec?.config || {};
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
  }

  function ensure(nodeId) {
    let entry = state.get(nodeId);
    if (entry) return entry;

    const cfgEntry = NodeStore.ensure(nodeId, 'TTS');
    const cfg = cfgEntry?.config || {};
    const initialVolume = clampVolume(cfg.volume ?? 1);

    const ac = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 22050 });
    const node = ac.createScriptProcessor
      ? ac.createScriptProcessor(4096, 1, 1)
      : (window.ScriptProcessorNode
        ? new window.ScriptProcessorNode(ac, { bufferSize: 4096, numberOfInputChannels: 1, numberOfOutputChannels: 1 })
        : null);
    if (!node) throw new Error('ScriptProcessorNode not supported');
    const analyser = ac.createAnalyser();
    analyser.fftSize = 2048;
    analyser.smoothingTimeConstant = 0.85;
    const gain = ac.createGain();
    gain.gain.value = initialVolume;
    node.connect(analyser);
    analyser.connect(gain);
    gain.connect(ac.destination);

    const st = {
      ac,
      node,
      an: analyser,
      gain,
      q: [],
      queued: 0,
      sr: ac.sampleRate || 22050,
      underruns: 0,
      canvas: null,
      ctx: null,
      raf: null,
      _resizeObs: null,
      audioEl: null,
      volumeControl: null,
      volume: initialVolume,
      chain: Promise.resolve(),
      muted: muteRequests.get(nodeId) === true,
      active: false,
      lastProducedAt: 0,
      audioSeq: 0
    };

    node.onaudioprocess = (evt) => {
      const out = evt.outputBuffer.getChannelData(0);
      let produced = false;
      if (!st.q.length) {
        out.fill(0);
        st.underruns++;
      } else {
        let need = out.length;
        let offset = 0;
        while (need > 0) {
          if (!st.q.length) {
            out.fill(0, offset);
            st.underruns++;
            break;
          }
          const head = st.q[0];
          const take = Math.min(need, head.length);
          if (take > 0) {
            out.set(head.subarray(0, take), offset);
            produced = produced || take > 0;
          }
          if (take === head.length) st.q.shift();
          else st.q[0] = head.subarray(take);
          st.queued -= take;
          offset += take;
          need -= take;
        }
      }
      const now = performance.now();
      if (produced && !st.muted) {
        st.lastProducedAt = now;
        setActiveState(nodeId, st, true);

        // Emit raw audio packet through the audio output port for Smart Objects
        if (Router && typeof Router.sendFrom === 'function') {
          // Convert Float32Array to Int16Array (PCM16) for transmission
          const pcm16 = new Int16Array(out.length);
          for (let i = 0; i < out.length; i++) {
            const s = Math.max(-1, Math.min(1, out[i]));
            pcm16[i] = s < 0 ? s * 0x8000 : s * 0x7FFF;
          }

          const audioPacket = {
            type: 'audio',
            format: 'pcm16',
            sampleRate: st.sr || 22050,
            channels: 1,
            samples: out.length,
            data: Array.from(pcm16),
            timestamp: now,
            sequence: st.audioSeq || 0
          };

          st.audioSeq = (st.audioSeq || 0) + 1;
          Router.sendFrom(nodeId, 'audio', audioPacket);
        }
      } else {
        const elapsed = now - (st.lastProducedAt || 0);
        if (st.muted) {
          setActiveState(nodeId, st, false);
        } else if (st.active && elapsed > 250 && (!st.q.length || st.queued <= 0)) {
          setActiveState(nodeId, st, false);
        }
      }
    };

    const graphNode = getNode(nodeId);
    const body = graphNode?.el?.querySelector('.body');
    if (body) {
      const canvas = body.querySelector('[data-tts-vis]');
      if (canvas) {
        st.canvas = canvas;
        st.ctx = canvas.getContext('2d');
      } else {
        const el = document.createElement('canvas');
        el.dataset.ttsVis = '';
        el.style.cssText = 'margin-top:4px;width:100%;height:56px;background:rgba(0,0,0,.25);border-radius:4px';
        body.appendChild(el);
        st.canvas = el;
        st.ctx = el.getContext('2d');
      }

      const audio = body.querySelector('[data-tts-audio]');
      if (audio) {
        st.audioEl = audio;
      } else {
        const el = document.createElement('audio');
        el.dataset.ttsAudio = '';
        el.controls = true;
        el.style.marginTop = '6px';
        el.style.display = 'none';
        body.appendChild(el);
        st.audioEl = el;
      }

      if (st.audioEl && !st.audioEl._ttsActiveBound) {
        st.audioEl.addEventListener('play', () => setActiveState(nodeId, st, !st.muted));
        st.audioEl.addEventListener('playing', () => setActiveState(nodeId, st, !st.muted));
        st.audioEl.addEventListener('pause', () => setActiveState(nodeId, st, false));
        st.audioEl.addEventListener('ended', () => setActiveState(nodeId, st, false));
        st.audioEl._ttsActiveBound = true;
      }

      const vol = body.querySelector('[data-tts-volume]');
      if (vol) {
        st.volumeControl = vol;
        vol.value = String(initialVolume);
        if (!vol._ttsBound) {
          vol.addEventListener('input', () => {
            const value = clampVolume(vol.value);
            vol.value = String(value);
            st.volume = value;
            if (st.audioEl) {
              st.audioEl.volume = value;
              st.audioEl.muted = st.muted;
            }
            if (st.gain) st.gain.gain.value = st.muted ? 0 : value;
            NodeStore.update(nodeId, { type: 'TTS', volume: value });
          });
          vol._ttsBound = true;
        }
        if (st.audioEl) {
          st.audioEl.volume = initialVolume;
          st.audioEl.muted = st.muted;
        }
        if (st.gain) st.gain.gain.value = st.muted ? 0 : initialVolume;
      }
    }

    if (st.audioEl) st.audioEl.volume = initialVolume;
    if (st.audioEl) st.audioEl.muted = st.muted;
    if (st.gain) st.gain.gain.value = st.muted ? 0 : initialVolume;

    try {
      const cfg = NodeStore.ensure(nodeId, 'TTS').config || {};
      if ((cfg.mode || 'stream') === 'stream') showStreamUI(st);
      else showFileUI(st);
    } catch (err) {
      // ignore init issues
    }

    applyMuteState(nodeId, st, st.muted);
    emitActive(nodeId, false);

    state.set(nodeId, st);
    return st;
  }

  function startVis(st) {
    if (!st.canvas || !st.ctx || !st.an || st.raf) return;
    const resize = () => {
      try {
        const bounds = st.canvas.getBoundingClientRect();
        st.canvas.width = Math.max(150, Math.floor(bounds.width));
        st.canvas.height = 56;
      } catch (err) {
        // ignore
      }
    };
    if ('ResizeObserver' in window) {
      st._resizeObs = new ResizeObserver(resize);
      st._resizeObs.observe(st.canvas.parentNode || st.canvas);
    }
    resize();

    const buf = new Uint8Array(st.an.fftSize);
    const draw = () => {
      st.raf = requestAnimationFrame(draw);
      st.an.getByteTimeDomainData(buf);
      const { width: w, height: h } = st.canvas;
      const c = st.ctx;
      c.clearRect(0, 0, w, h);
      c.fillStyle = 'rgba(0,0,0,0.15)';
      c.fillRect(0, 0, w, h);
      c.lineWidth = 2;
      c.strokeStyle = 'rgba(255,255,255,0.9)';
      c.beginPath();
      const step = Math.max(1, Math.floor(buf.length / w));
      for (let x = 0, i = 0; x < w; x++, i += step) {
        const v = buf[i] / 128.0;
        const y = (v * 0.5) * h;
        if (x === 0) c.moveTo(x, y);
        else c.lineTo(x, y);
      }
      c.stroke();
      c.strokeStyle = 'rgba(255,255,255,0.25)';
      c.lineWidth = 1;
      c.beginPath();
      c.moveTo(0, h / 2);
      c.lineTo(w, h / 2);
      c.stroke();
    };
    draw();
  }

  function stopVis(st) {
    if (st.raf) {
      cancelAnimationFrame(st.raf);
      st.raf = null;
    }
    if (st._resizeObs) {
      try {
        st._resizeObs.disconnect();
      } catch (err) {
        // ignore
      }
      st._resizeObs = null;
    }
  }

  function showStreamUI(st) {
    if (st.audioEl) st.audioEl.style.display = 'none';
    if (st.canvas) {
      st.canvas.style.display = 'block';
      startVis(st);
      const value = st.volumeControl ? clampVolume(st.volumeControl.value) : clampVolume(st.volume);
      if (st.gain) st.gain.gain.value = st.muted ? 0 : value;
    }
  }

  function showFileUI(st) {
    stopVis(st);
    if (st.canvas) st.canvas.style.display = 'none';
    if (st.audioEl) {
      st.audioEl.style.display = 'block';
      const value = st.volumeControl ? clampVolume(st.volumeControl.value) : clampVolume(st.volume);
      st.audioEl.volume = value;
      st.audioEl.muted = st.muted;
    }
  }

  function f32FromI16(int16) {
    const out = new Float32Array(int16.length);
    for (let i = 0; i < int16.length; i++) out[i] = Math.max(-1, Math.min(1, int16[i] / 32768));
    return out;
  }

  function resampleLinear(input, fromRate, toRate) {
    if (fromRate === toRate) return input;
    const ratio = toRate / fromRate;
    const length = Math.round(input.length * ratio);
    const out = new Float32Array(length);
    for (let i = 0; i < length; i++) {
      const pos = i / ratio;
      const i0 = Math.floor(pos);
      const i1 = Math.min(i0 + 1, input.length - 1);
      const t = pos - i0;
      out[i] = input[i0] * (1 - t) + input[i1] * t;
    }
    return out;
  }

  function enqueue(st, f32) {
    st.q.push(f32);
    st.queued += f32.length;
  }

  function sanitize(text) {
    if (!text) return '';
    try {
      text = text.normalize('NFKC');
    } catch (err) {
      // ignore
    }
    let out = text
      .replace(/[\u2019\u2018]/g, "'")
      .replace(/\bhttps?:\/\/\S+/gi, ' ')
      .replace(/[*_~`]+/g, ' ')
      .replace(/\u2026/g, '.').replace(/\.{3,}/g, '.')
      .replace(/[“”"«»‹›„‟]/g, ' ')
      .replace(/[\[\](){}<>]/g, ' ');
    out = out.replace(/[^\S\r\n]+/g, ' ').replace(/\s*([.,?!])\s*/g, '$1 ').trim();
    return out;
  }

  function resolveFilterTokens(cfg) {
    const source = Array.isArray(cfg?.filterTokens) ? cfg.filterTokens : DEFAULT_FILTER_TOKENS;
    const seen = new Set();
    const out = [];
    source.forEach((token) => {
      if (token == null) return;
      let value = typeof token === 'string' ? token : String(token);
      try {
        value = value.normalize('NFKC');
      } catch (err) {
        // ignore
      }
      value = value.trim();
      if (!value || seen.has(value)) return;
      seen.add(value);
      out.push(value);
    });
    return out;
  }

  function applySpeakFilters(text, cfg) {
    if (!text) return '';
    let result = stripEmoji(text);
    const tokens = resolveFilterTokens(cfg);
    for (const token of tokens) {
      if (!token) continue;
      result = result.split(token).join('');
    }
    result = result.replace(/[^\S\r\n]+/g, ' ').replace(/\s*([.,?!])\s*/g, '$1 ').trim();
    return result;
  }

  function refreshUI(nodeId) {
    const st = ensure(nodeId);
    const cfg = NodeStore.ensure(nodeId, 'TTS').config || {};
    if ((cfg.mode || 'stream') === 'stream') showStreamUI(st);
    else showFileUI(st);
    const shouldMute = muteRequests.get(nodeId) === true;
    applyMuteState(nodeId, st, shouldMute);
  }

  async function onText(nodeId, payload) {
    const node = getNode(nodeId);
    if (!node) return;
    let cfg = NodeStore.ensure(nodeId, 'TTS').config || {};
    const wasmEnabled = isWasmMode(cfg);
    if (!wasmEnabled) {
      await ensureLocalNetworkAccess({ requireGesture: true });
      cfg = await ensureModelConfigured(nodeId, cfg);
    }
    const endpoint = wasmEnabled ? { base: '', api: '', relay: '', viaNkn: false } : resolveEndpointConfig(cfg);
    const { base, api, relay, viaNkn } = endpoint;
    const model = (cfg.model || '').trim();
    const mode = cfg.mode || 'stream';
    const usingNkn = viaNkn;
    const updateRelayState = (state, message) => {
      if (!usingNkn) return;
      setRelayState(nodeId, { state, message });
    };

    const raw = (payload && (payload.text || payload)) || '';
    const eos = !!(payload && payload.eos);
    if (!raw) return;

    const sanitized = sanitize(String(raw));
    if (!sanitized) return;
    const speakText = applySpeakFilters(sanitized, cfg);
    if (!speakText) return;

    const st = ensure(nodeId);

    const speakWasmOnce = async () => {
      try {
        await st.ac.resume();
        showStreamUI(st);
        enqueue(st, new Float32Array(Math.round((st.sr || 22050) * 0.04)));
        const wasm = await loadWasmModule();
        const voice = resolveWasmVoice(cfg);
        checkMemoryForVoice(voice);
        const modelUrl = (voice.modelUrl || wasm.DEFAULT_PIPER_MODEL_URL || '').trim() || wasm.DEFAULT_PIPER_MODEL_URL;
        const configUrl = (voice.configUrl || wasm.DEFAULT_PIPER_CONFIG_URL || '').trim() || wasm.DEFAULT_PIPER_CONFIG_URL;
        const speakerIdRaw = cfg.wasmSpeakerId ?? 0;
        const speakerId = Number.isFinite(Number(speakerIdRaw)) ? Number(speakerIdRaw) : 0;
        const threads = cfg.wasmThreads;
        const { audioData, sampleRate } = await wasm.synthesize(speakText, { modelUrl, configUrl, speakerId, threads });
        let f32 = audioData;
        if (st.sr !== sampleRate) f32 = resampleLinear(f32, sampleRate, st.sr);
        enqueue(st, f32);
        enqueue(st, new Float32Array(Math.round((st.sr || sampleRate) * 0.03)));
      } catch (err) {
        log(`[tts ${nodeId}] ${err?.message || err}`);
      }
    };

    const speakOnce = async () => {
      if (usingNkn) updateRelayState('warn', mode === 'stream' ? 'Awaiting audio stream' : 'Synthesizing audio');
      if (mode === 'stream') {
        await st.ac.resume();
        showStreamUI(st);
        enqueue(st, new Float32Array(Math.round((st.sr || 22050) * 0.04)));
        const req = { text: speakText, mode: 'stream', format: 'raw', ...(model ? { model, voice: model } : {}) };
        const handleBytes = (u8) => {
          if (!u8 || !u8.length) return;
          const even = (u8.length >> 1) << 1;
          if (!even) return;
          const body = u8.subarray(0, even);
          const frames = body.length >> 1;
          const dv = new DataView(body.buffer, body.byteOffset, body.length);
          const i16 = new Int16Array(frames);
          for (let i = 0; i < frames; i++) i16[i] = dv.getInt16(i * 2, true);
          let f32 = f32FromI16(i16);
          if (st.sr !== 22050) f32 = resampleLinear(f32, 22050, st.sr);
          enqueue(st, f32);

          // Emit audio packet to 'audio' output port for Smart Objects
          if (Router?.sendFrom) {
            const audioPacket = {
              format: 'pcm16',
              sampleRate: st.sr || 22050,
              channels: 1,
              data: Array.from(i16), // Convert Int16Array to regular array for serialization
              timestamp: Date.now()
            };
            Router.sendFrom(nodeId, 'audio', audioPacket);
          }
        };

        try {
          if (viaNkn) {
            let expected = null;
            const stash = new Map();
            const seen = new Set();
            let relayMarkedOk = false;
            const markRelayOk = (msg) => {
              if (!usingNkn || relayMarkedOk) return;
              relayMarkedOk = true;
              updateRelayState('ok', msg);
            };
            const flush = () => {
              while (expected != null && stash.has(expected)) {
                handleBytes(stash.get(expected));
                stash.delete(expected);
                expected++;
              }
            };
            await Net.nknStream(
              {
                url: base.replace(/\/+$/, '') + '/speak',
                method: 'POST',
                headers: Net.auth({ 'X-Relay-Stream': 'chunks' }, api),
                json: req,
                timeout_ms: 120000
              },
              relay,
              {
                onBegin: () => {},
                onChunk: (u8, seqRaw) => {
                  markRelayOk('Streaming audio');
                  const seq = seqRaw | 0;
                  if (seen.has(seq)) return;
                  seen.add(seq);
                  if (expected == null) expected = seq;
                  if (seq === expected) {
                    handleBytes(u8);
                    expected++;
                    flush();
                  } else if (seq > expected) {
                    stash.set(seq, u8);
                  }
                },
                onEnd: () => {
                  if (usingNkn) {
                    const completionMessage = relayMarkedOk ? 'Stream complete' : 'No audio received';
                    updateRelayState(relayMarkedOk ? 'ok' : 'warn', completionMessage);
                  }
                  flush();
                },
                lingerEndMs: 350
              },
              120000
            );
          } else {
            const res = await fetch(base.replace(/\/+$/, '') + '/speak', {
              method: 'POST',
              headers: Net.auth({}, api),
              body: JSON.stringify(req)
            });
            if (!res.ok || !res.body) throw new Error(`${res.status} ${res.statusText}`);
            const reader = res.body.getReader();
            let leftover = new Uint8Array(0);
            while (true) {
              const { value, done } = await reader.read();
              if (done) break;
              if (!value || !value.byteLength) continue;
              const merged = new Uint8Array(leftover.length + value.length);
              merged.set(leftover, 0);
              merged.set(value, leftover.length);
              const even = (merged.length >> 1) << 1;
              if (even) {
                handleBytes(merged.subarray(0, even));
                leftover = merged.subarray(even);
              } else {
                leftover = merged;
              }
            }
          }
        } catch (err) {
          log(`[tts ${nodeId}] ${err.message}`);
          if (usingNkn) updateRelayState('err', err?.message || 'TTS relay failed');
        }

        enqueue(st, new Float32Array(Math.round((st.sr || 22050) * 0.03)));
      } else {
        showFileUI(st);
        try {
          const data = await Net.postJSON(
            base,
            '/speak',
            { text: speakText, mode: 'file', format: 'ogg', ...(model ? { model, voice: model } : {}) },
            api,
            viaNkn,
            relay,
            10 * 60 * 1000
          );
          let blob = null;
          let mime = 'audio/ogg';
          if (data?.files?.[0]?.url) {
            const fileUrl = base.replace(/\/+$/, '') + data.files[0].url;
            blob = await Net.fetchBlob(fileUrl, viaNkn, relay, api);
            mime = blob.type || mime;
          } else if (data?.audio_b64) {
            const u8 = b64ToBytes(data.audio_b64);
            blob = new Blob([u8], { type: mime });
          } else {
            throw new Error('no audio');
          }
          if (usingNkn) updateRelayState('ok', 'Audio ready');
          const url = URL.createObjectURL(blob);
          if (st.audioEl) {
            await new Promise((resolve) => {
              const onEnd = () => {
                st.audioEl.removeEventListener('ended', onEnd);
                resolve();
              };
              st.audioEl.addEventListener('ended', onEnd);
              st.audioEl.src = url;
              st.audioEl.play().catch(() => {});
            });
          }
        } catch (err) {
          log(`[tts ${nodeId}] ${err.message}`);
          if (usingNkn) updateRelayState('err', err?.message || 'TTS relay failed');
        }
      }
    };

    const task = wasmEnabled ? speakWasmOnce : speakOnce;
    st.chain = st.chain.then(task).catch((err) => log(`[tts ${nodeId}] ${err?.message || err}`));
    return st.chain;
  }

  async function ensureDefaults(nodeId) {
    try {
      const rec = NodeStore.ensure(nodeId, 'TTS');
      const cfg = rec?.config || {};
      if (!cfg.wasmVoicePreset) NodeStore.update(nodeId, { type: 'TTS', wasmVoicePreset: DEFAULT_WASM_PRESET });
      if (isWasmMode(cfg)) return;
      await ensureModels(nodeId);
      await ensureModelConfigured(nodeId, cfg);
    } catch (err) {
      // ignore background discovery failures
    }
  }

  async function listWasmVoices(nodeId) {
    const rec = NodeStore.ensure(nodeId, 'TTS');
    const cfg = rec?.config || {};
    const wasm = await loadWasmModule();
    const voice = resolveWasmVoice(cfg);
    const modelUrl = (voice.modelUrl || wasm.DEFAULT_PIPER_MODEL_URL || '').trim() || wasm.DEFAULT_PIPER_MODEL_URL;
    const configUrl = (voice.configUrl || wasm.DEFAULT_PIPER_CONFIG_URL || '').trim() || wasm.DEFAULT_PIPER_CONFIG_URL;
    const threads = cfg.wasmThreads;
    const key = `${modelUrl}|${configUrl}|${threads}`;
    if (wasmVoicesCache.has(key)) return wasmVoicesCache.get(key);
    const voices = await wasm.listSpeakers({ modelUrl, configUrl, threads });
    wasmVoicesCache.set(key, voices);
    return voices;
  }

  function onMute(nodeId, payload) {
    if (!nodeId) return;
    const mode = getMuteMode(nodeId);
    const shouldMute = interpretSignalInput(payload, mode);
    muteRequests.set(nodeId, shouldMute);
    const st = state.get(nodeId);
    if (st) {
      applyMuteState(nodeId, st, shouldMute);
      if (shouldMute) setActiveState(nodeId, st, false);
    } else if (shouldMute) {
      emitActive(nodeId, false);
    }
  }

  function refreshConfig(nodeId) {
    if (!nodeId) return;
    const st = state.get(nodeId);
    const shouldMute = muteRequests.get(nodeId) === true;
    const previousActive = lastActiveByNode.get(nodeId) === true;
    if (st) applyMuteState(nodeId, st, shouldMute);
    lastActiveByNode.delete(nodeId);
    const finalActive = st ? st.active : (shouldMute ? false : previousActive);
    emitActive(nodeId, !!finalActive);
  }

  return {
    ensure,
    refreshUI,
    onText,
    ensureDefaults,
    getWasmVoiceOptions,
    resolveWasmVoice,
    ensureModels,
    listWasmVoices,
    listModels,
    getModelInfo,
    pullModel,
    refreshModels,
    subscribeModels,
    dispose,
    onMute,
    refreshConfig
  };
}

export { createTTS };
