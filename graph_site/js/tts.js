function createTTS({ getNode, NodeStore, Net, CFG, log, b64ToBytes }) {
  const state = new Map();

  function ensure(nodeId) {
    let entry = state.get(nodeId);
    if (entry) return entry;

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
    node.connect(analyser);
    analyser.connect(ac.destination);

    const st = {
      ac,
      node,
      an: analyser,
      q: [],
      queued: 0,
      sr: ac.sampleRate || 22050,
      underruns: 0,
      canvas: null,
      ctx: null,
      raf: null,
      _resizeObs: null,
      audioEl: null,
      chain: Promise.resolve()
    };

    node.onaudioprocess = (evt) => {
      const out = evt.outputBuffer.getChannelData(0);
      if (!st.q.length) {
        out.fill(0);
        st.underruns++;
        return;
      }
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
        out.set(head.subarray(0, take), offset);
        if (take === head.length) st.q.shift();
        else st.q[0] = head.subarray(take);
        st.queued -= take;
        offset += take;
        need -= take;
      }
    };

    const graphNode = getNode(nodeId);
    const body = graphNode?.el?.querySelector('.body');
    if (body) {
      if (!body.querySelector('[data-tts-vis]')) {
        const canvas = document.createElement('canvas');
        canvas.dataset.ttsVis = '';
        canvas.style.cssText = 'margin-top:6px;width:100%;height:56px;background:rgba(0,0,0,.25);border-radius:4px;display:none';
        body.appendChild(canvas);
        st.canvas = canvas;
        st.ctx = canvas.getContext('2d');
      }
      if (!body.querySelector('audio')) {
        const audio = document.createElement('audio');
        audio.controls = true;
        audio.style.marginTop = '6px';
        body.appendChild(audio);
        st.audioEl = audio;
      } else {
        st.audioEl = body.querySelector('audio');
      }
    }

    try {
      const cfg = NodeStore.ensure(nodeId, 'TTS').config || {};
      if ((cfg.mode || 'stream') === 'stream') showStreamUI(st);
      else showFileUI(st);
    } catch (err) {
      // ignore init issues
    }

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
    }
  }

  function showFileUI(st) {
    stopVis(st);
    if (st.canvas) st.canvas.style.display = 'none';
    if (st.audioEl) st.audioEl.style.display = 'block';
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
  }

  async function onText(nodeId, payload) {
    const node = getNode(nodeId);
    if (!node) return;
    const cfg = NodeStore.ensure(nodeId, 'TTS').config || {};
    const base = (cfg.base || '').trim();
    const api = (cfg.api || '').trim();
    const relay = (cfg.relay || '').trim();
    const model = (cfg.model || '').trim();
    const viaNkn = CFG.transport === 'nkn';
    const mode = cfg.mode || 'stream';

    const raw = (payload && (payload.text || payload)) || '';
    const eos = !!(payload && payload.eos);
    if (!raw) return;

    const st = ensure(nodeId);
    const clean = sanitize(String(raw));
    if (!clean) return;

    const speakOnce = async () => {
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
        }
      }
    };

    st.chain = st.chain.then(speakOnce).catch((err) => log(`[tts ${nodeId}] ${err?.message || err}`));
    return st.chain;
  }

  return {
    ensure,
    refreshUI,
    onText
  };
}

export { createTTS };
