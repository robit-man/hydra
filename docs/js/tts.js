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
        const cfgBase = String(cfg.base || '').trim();
        const cfgRelay = String(cfg.relay || '').trim();
        const cfgApi = String(cfg.api || '').trim();
        const override = (entry.base !== cfgBase || entry.relay !== cfgRelay || entry.api !== cfgApi)
          ? { base: entry.base, relay: entry.relay, api: entry.api }
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
    const effective = Object.assign({}, cfg || {}, override || {});
    const base = String(effective.base || '').trim();
    const relay = String(effective.relay || '').trim();
    const api = String(effective.api || '').trim();
    if (!base) {
      setCachedMeta(nodeId, { list: [], base: '', relay: '', api: '', fetchedAt: Date.now() });
      clearModelRefresh(nodeId);
      return [];
    }
    const viaNkn = !!relay;
    const cache = getCachedMeta(nodeId);
    const now = Date.now();
    if (!force && cache && cache.base === base && cache.relay === relay && cache.api === api && (now - cache.fetchedAt) < MODEL_REFRESH_MS) {
      return cache.list.slice();
    }
    const key = makeCacheKey(nodeId, base, relay, api);
    if (MODEL_PROMISE.has(key)) return MODEL_PROMISE.get(key);
    const task = (async () => {
      const list = await fetchModelMetadata(base, api, viaNkn, relay);
      setCachedMeta(nodeId, { list, base, relay, api, fetchedAt: Date.now() });
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

  function refreshModels(nodeId, override = null, options = {}) {
    const rec = NodeStore.ensure(nodeId, 'TTS');
    const cfg = rec?.config || {};
    const merged = Object.assign({ force: true }, options || {});
    if (override) merged.override = override;
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
      lastProducedAt: 0
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
    cfg = await ensureModelConfigured(nodeId, cfg);
    const base = (cfg.base || '').trim();
    const api = (cfg.api || '').trim();
    const relay = (cfg.relay || '').trim();
    const model = (cfg.model || '').trim();
    const viaNkn = !!relay;
    const mode = cfg.mode || 'stream';
    const usingNkn = viaNkn;
    const updateRelayState = (state, message) => {
      if (!usingNkn) return;
      setRelayState(nodeId, { state, message });
    };

    const raw = (payload && (payload.text || payload)) || '';
    const eos = !!(payload && payload.eos);
    if (!raw) return;

    const st = ensure(nodeId);
    const clean = sanitize(String(raw));
    if (!clean) return;

    const speakOnce = async () => {
      if (usingNkn) updateRelayState('warn', mode === 'stream' ? 'Awaiting audio stream' : 'Synthesizing audio');
      if (mode === 'stream') {
        await st.ac.resume();
        showStreamUI(st);
        enqueue(st, new Float32Array(Math.round((st.sr || 22050) * 0.04)));
        const req = { text: clean, mode: 'stream', format: 'raw', ...(model ? { model, voice: model } : {}) };
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
            { text: clean, mode: 'file', format: 'ogg', ...(model ? { model, voice: model } : {}) },
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

    st.chain = st.chain.then(speakOnce).catch((err) => log(`[tts ${nodeId}] ${err?.message || err}`));
    return st.chain;
  }

  async function ensureDefaults(nodeId) {
    try {
      const rec = NodeStore.ensure(nodeId, 'TTS');
      const cfg = rec?.config || {};
      await ensureModels(nodeId);
      await ensureModelConfigured(nodeId, cfg);
    } catch (err) {
      // ignore background discovery failures
    }
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
    ensureModels,
    listModels,
    getModelInfo,
    refreshModels,
    subscribeModels,
    dispose,
    onMute,
    refreshConfig
  };
}

export { createTTS };
