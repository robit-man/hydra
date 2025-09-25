function createASR({
  getNode,
  Router,
  NodeStore,
  Net,
  CFG,
  SIGNOFF_RE,
  td,
  setBadge,
  log
}) {
  const ASR = {
    ownerId: null,
    running: false,
    ac: null,
    media: null,
    source: null,
    node: null,
    an: null,
    _vis: new Map(),
    bufF32: new Float32Array(0),
    out: 0,
    sid: null,
    _startingSid: false,
    _rate: 16000,
    _chunk: 120,
    _base: '',
    _api: '',
    _relay: '',
    _viaNkn: false,
    _live: true,
    _sawSpeech: false,
    finalizing: false,
    _vadWatch: null,
    _lastFinal: { text: '', at: 0 },
    _DEDUP_MS: 1500,
    vad: { ema: 0, state: 'silence', lastVoice: 0, lastSilence: 0 },
    aggr: { prev: '', pend: '', pendStart: 0, lastChange: 0, lastEmit: 0 },
    _uplinkOn: false,
    _tailDeadline: 0,
    _preMs: 450,
    _preMaxSamples: 0,
    _preSamples: 0,
    _preChunks: [],
    _sawPartialForUplink: false,
    _lastPartialAt: 0,
    _lastPostAt: 0,
    _ignorePartials: false,
    _silenceSince: 0,
    _lingerMs: 700,
    _minTailMs: 350,
    _forceQuietMaxMs: 2800,

    _nodeEl() {
      return this.ownerId ? getNode(this.ownerId)?.el : null;
    },

    _setVU(p) {
      const width = Math.max(0, Math.min(100, Math.round(p * 150)));
      const el = this._nodeEl()?.querySelector('.vu > b');
      if (el) el.style.width = `${width}%`;
    },

    _setPartial(text) {
      const el = this._nodeEl()?.querySelector('[data-asr-partial]');
      if (el) el.textContent = text || '';
    },

    _setFinal(text) {
      const el = this._nodeEl()?.querySelector('[data-asr-final]');
      if (!el) return;
      el.textContent = (el.textContent ? `${el.textContent}\n` : '') + text;
      el.scrollTop = el.scrollHeight;
    },

    pushF32(f32) {
      const a = this.bufF32;
      const b = f32;
      const out = new Float32Array(a.length + b.length);
      out.set(a, 0);
      out.set(b, a.length);
      this.bufF32 = out;
    },

    drainF32(n) {
      const a = this.bufF32;
      const take = Math.min(a.length, n);
      const head = a.subarray(0, take);
      this.bufF32 = a.subarray(take).slice(0);
      return head;
    },

    resample(f32, fromRate, toRate) {
      if (fromRate === toRate) return f32.slice(0);
      const ratio = toRate / fromRate;
      const length = Math.round(f32.length * ratio);
      const out = new Float32Array(length);
      for (let i = 0; i < length; i++) {
        const pos = i / ratio;
        const i0 = Math.floor(pos);
        const i1 = Math.min(i0 + 1, f32.length - 1);
        const t = pos - i0;
        out[i] = f32[i0] * (1 - t) + f32[i1] * t;
      }
      return out;
    },

    i16(f32) {
      const out = new Int16Array(f32.length);
      for (let i = 0; i < f32.length; i++) {
        const v = Math.max(-1, Math.min(1, f32[i]));
        out[i] = v < 0 ? v * 0x8000 : v * 0x7FFF;
      }
      return new Uint8Array(out.buffer);
    },

    _preInit(rate) {
      this._preMaxSamples = Math.max(1, Math.round(rate * (this._preMs / 1000)));
      this._preSamples = 0;
      this._preChunks = [];
    },

    _prePush(f32) {
      this._preChunks.push(f32);
      this._preSamples += f32.length;
      while (this._preSamples > this._preMaxSamples && this._preChunks.length) {
        const drop = this._preChunks.shift();
        this._preSamples -= drop.length;
      }
    },

    _preFlush() {
      for (const chunk of this._preChunks) this.pushF32(chunk);
      this._preChunks = [];
      this._preSamples = 0;
    },

    _openUplink(now, tailMs, current) {
      if (this._uplinkOn) return;
      this._uplinkOn = true;
      this._sawPartialForUplink = false;
      this._lastPartialAt = 0;
      this._ignorePartials = false;
      this._tailDeadline = now + tailMs;
      this._preFlush();
      if (current) this.pushF32(current);
    },

    _extendTail(now, tailMs) {
      this._tailDeadline = now + tailMs;
    },

    _closeUplinkAndMaybeFinalize() {
      if (!this._uplinkOn) return;
      this._uplinkOn = false;
      this._ignorePartials = true;
      this._drainAndEnd().catch((err) => log('[asr] finalize(gate) ' + (err?.message || err)));
    },

    _startVis(nodeId) {
      const graphNode = getNode(nodeId);
      const canvas = graphNode?.el?.querySelector('[data-asr-vis]');
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      const st = { canvas, ctx, raf: null, ro: null };
      const resize = () => {
        try {
          const bounds = canvas.getBoundingClientRect();
          canvas.width = Math.max(150, Math.floor(bounds.width));
          canvas.height = 56;
        } catch (err) {
          // ignore
        }
      };
      if ('ResizeObserver' in window) {
        st.ro = new ResizeObserver(resize);
        st.ro.observe(canvas.parentNode || canvas);
      }
      resize();

      const draw = () => {
        st.raf = requestAnimationFrame(draw);
        const { width: w, height: h } = canvas;
        ctx.clearRect(0, 0, w, h);
        ctx.fillStyle = 'rgba(0,0,0,0.15)';
        ctx.fillRect(0, 0, w, h);
        ctx.strokeStyle = 'rgba(255,255,255,0.25)';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(0, h / 2);
        ctx.lineTo(w, h / 2);
        ctx.stroke();
        if (!ASR.an) return;
        const buf = new Uint8Array(ASR.an.fftSize || 2048);
        ASR.an.getByteTimeDomainData(buf);
        ctx.lineWidth = 2;
        ctx.strokeStyle = 'rgba(255,255,255,0.9)';
        ctx.beginPath();
        const step = Math.max(1, Math.floor(buf.length / w));
        for (let x = 0, i = 0; x < w; x++, i += step) {
          const v = buf[i] / 128.0;
          const y = (v * 0.5) * h;
          if (x === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        }
        ctx.stroke();
      };

      draw();
      this._vis.set(nodeId, st);
    },

    _stopVis(nodeId) {
      const st = this._vis.get(nodeId);
      if (!st) return;
      if (st.raf) {
        cancelAnimationFrame(st.raf);
        st.raf = null;
      }
      if (st.ro) {
        try {
          st.ro.disconnect();
        } catch (err) {
          // ignore
        }
        st.ro = null;
      }
      this._vis.delete(nodeId);
    },

    resetAggr() {
      this.aggr.prev = '';
      this.aggr.pend = '';
      this.aggr.pendStart = 0;
      this.aggr.lastChange = 0;
    },

    handlePartialForPhrases(text, cfg) {
      if (!cfg.phraseOn) return;
      const minWords = cfg.phraseMin | 0 || 3;
      const stableMs = cfg.phraseStable | 0 || 350;
      const prev = this.aggr.prev || '';
      if (!text.startsWith(prev)) {
        this.aggr.prev = text;
        this.aggr.pend = '';
        this.aggr.pendStart = performance.now();
        this.aggr.lastChange = this.aggr.pendStart;
        return;
      }
      const added = text.slice(prev.length);
      const now = performance.now();
      if (added.length > 0) {
        if (!this.aggr.pend) this.aggr.pendStart = now;
        this.aggr.pend += added;
        this.aggr.prev = text;
        this.aggr.lastChange = now;
      } else {
        const words = (this.aggr.pend.trim().match(/\S+/g) || []).length;
        const punct = /[.!?;:,]$/.test(this.aggr.pend.trim());
        if (this.aggr.pend && words >= minWords && (punct || (now - this.aggr.lastChange) >= stableMs)) {
          const phrase = this.aggr.pend.trim();
          this._routePhrase(phrase);
          this.aggr.pend = '';
          this.aggr.pendStart = 0;
          this.aggr.lastChange = now;
        }
      }
    },

    handleFinalFlush(text) {
      if (this.aggr.pend) {
        this._routePhrase(this.aggr.pend.trim());
        this.aggr.pend = '';
      } else if (text.startsWith(this.aggr.prev || '')) {
        const extra = text.slice((this.aggr.prev || '').length).trim();
        if (extra) this._routePhrase(extra);
      }
      this.aggr.prev = text;
    },

    _routePartial(text) {
      if (this.ownerId) Router.sendFrom(this.ownerId, 'partial', { type: 'text', text: text || '' });
    },

    _routePhrase(text) {
      if (!text) return;
      if (this.ownerId) Router.sendFrom(this.ownerId, 'phrase', { type: 'text', text });
    },

    _routeFinal(text) {
      if (!text) return;
      if (this.ownerId) Router.sendFrom(this.ownerId, 'final', { type: 'text', text, eos: true });
    },

    printPartial(text, cfg) {
      this._setPartial(text || '');
      this._routePartial(text || '');
      this.handlePartialForPhrases(text || '', cfg);
    },

    appendFinal(text) {
      const trimmed = (text || '').trim().replace(/\s+/g, ' ');
      if (!trimmed) return;
      const now = performance.now();
      if (trimmed === this._lastFinal.text && (now - this._lastFinal.at) <= this._DEDUP_MS) return;
      this._lastFinal.text = trimmed;
      this._lastFinal.at = now;
      this._setFinal(trimmed);
      this._routeFinal(trimmed);
    },

    shouldDropAsHallucination(text, meta) {
      if (!SIGNOFF_RE.test(text)) return false;
      const words = (text.match(/\S+/g) || []).length;
      const shortGeneric = words <= 7;
      const sawNoSpeech = !this._sawSpeech;
      const inSilence = this.vad?.state === 'silence';
      const noSpeechProb = (meta && typeof meta.no_speech_prob === 'number') ? meta.no_speech_prob : null;
      const lowConfidence = (noSpeechProb !== null && noSpeechProb > 0.6) ||
        (meta && typeof meta.avg_logprob === 'number' && meta.avg_logprob < -1.0) ||
        (meta && typeof meta.compression_ratio === 'number' && meta.compression_ratio > 2.4);
      return shortGeneric && (sawNoSpeech || inSilence || lowConfidence);
    },

    async _ensureLiveSession(cfg) {
      if (!this._live || this.sid || this._startingSid || !this.running) return;
      this._startingSid = true;
      try {
        const prompt = (cfg.prompt || '').trim();
        const mode = cfg.mode || 'fast';
        const preview_model = (cfg.prevModel || '').trim();
        const preview_window_s = cfg.prevWin ? Number(cfg.prevWin) : undefined;
        const preview_step_s = cfg.prevStep ? Number(cfg.prevStep) : undefined;
        const model = (cfg.model || '').trim();
        const body = {
          ...(prompt ? { prompt } : {}),
          mode,
          ...(preview_model ? { preview_model } : {}),
          ...(Number.isFinite(preview_window_s) ? { preview_window_s } : {}),
          ...(Number.isFinite(preview_step_s) ? { preview_step_s } : {}),
          temperature: 0.0,
          condition_on_previous_text: false,
          no_speech_threshold: 0.6,
          logprob_threshold: -1.0,
          ...(model ? { model } : {})
        };
        const data = await Net.postJSON(this._base, '/recognize/stream/start', body, this._api, this._viaNkn, this._relay, 45000);
        this.sid = (data && (data.sid || data.id || data.session)) || null;
        if (!this.sid) throw new Error('no sid');
        const sid = this.sid;
        this.openEvents(sid, this._viaNkn, this._base, this._relay, this._api, cfg).catch(() => {});
        this.flushLoop(this._rate, this._chunk, this._viaNkn, this._base, this._relay, this._api).catch(() => {});
      } catch (err) {
        log('[asr] start(lazy) ' + (err?.message || err));
      } finally {
        this._startingSid = false;
      }
    },

    async _drainAndEnd() {
      if (this.finalizing || !this.sid) return;
      this.finalizing = true;
      const oldSid = this.sid;
      this._ignorePartials = true;
      const softDeadline = performance.now() + Math.max(900, this._lingerMs + 600);
      while (performance.now() < softDeadline) {
        const now = performance.now();
        const postQuiet = (now - this._lastPostAt) >= this._lingerMs;
        const partialQuiet = (!this._sawPartialForUplink) || (now - this._lastPartialAt) >= this._lingerMs;
        const hardQuiet = (now - Math.max(this._lastPostAt, this._lastPartialAt || 0)) >= this._forceQuietMaxMs;
        if ((postQuiet && partialQuiet && this.out === 0 && this.bufF32.length === 0) || hardQuiet) break;
        await new Promise((resolve) => setTimeout(resolve, 12));
      }
      try {
        const resp = await Net.postJSON(this._base, `/recognize/stream/${encodeURIComponent(oldSid)}/end`, {}, this._api, this._viaNkn, this._relay, 20000);
        const finalText = resp?.final?.text || resp?.final?.result?.text || resp?.text || resp?.result?.text || '';
        if (finalText) {
          this.handleFinalFlush(finalText);
          this.appendFinal(finalText);
        }
      } catch (err) {
        log('[asr] finalize-live ' + (err?.message || err));
      } finally {
        this.sid = null;
        this.finalizing = false;
      }
    },

    async start(nodeId) {
      if (this.running && this.ownerId === nodeId) {
        setBadge('ASR already running');
        return;
      }
      if (this.running && this.ownerId && this.ownerId !== nodeId) {
        await this.stop();
      }
      const rec = NodeStore.ensure(nodeId, 'ASR');
      const cfg = rec.config || {};
      this.ownerId = nodeId;
      this._rate = cfg.rate | 0 || 16000;
      this._chunk = cfg.chunk | 0 || 120;
      this._live = !!cfg.live;
      this._viaNkn = CFG.transport === 'nkn';
      this._base = cfg.base || '';
      this._relay = cfg.relay || '';
      this._api = cfg.api || '';
      this._sawSpeech = false;
      this.finalizing = false;
      this._uplinkOn = false;
      this._tailDeadline = 0;
      this._sawPartialForUplink = false;
      this._lastPartialAt = 0;
      this._lastPostAt = 0;
      this._ignorePartials = false;
      this._silenceSince = 0;
      this._preInit(this._rate);
      this._lastFinal = { text: '', at: 0 };
      this.resetAggr();

      try {
        this.ac = new (window.AudioContext || window.webkitAudioContext)();
        this.media = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: false, noiseSuppression: false, autoGainControl: false }, video: false });
        this.source = this.ac.createMediaStreamSource(this.media);
        this.node = (this.ac.createScriptProcessor
          ? this.ac.createScriptProcessor(2048, 1, 1)
          : new window.ScriptProcessorNode(this.ac, { bufferSize: 2048, numberOfInputChannels: 1, numberOfOutputChannels: 1 }));
        this.an = this.ac.createAnalyser();
        this.an.fftSize = 2048;
        this.an.smoothingTimeConstant = 0.85;
        this.source.connect(this.node);
        this.source.connect(this.an);
        this.node.connect(this.ac.destination);

        const from = (this.ac.sampleRate | 0) || this._rate;
        const bufDurMs = (this.node.bufferSize / from) * 1000;
        const emaMs = (cfg.emaMs | 0) || 120;
        const alpha = 1 - Math.exp(-bufDurMs / Math.max(emaMs, 1));
        const rmsTh = parseFloat(cfg.rms) || 0.2;
        const holdMs = (cfg.hold | 0) || 250;
        const silence = (cfg.silence | 0) || 900;
        const tailBaseMs = Math.max(this._minTailMs, silence);
        const rate = (cfg.rate | 0) || 16000;
        const chunkMs = (cfg.chunk | 0) || 120;
        let batchQuietSince = null;
        this.vad.ema = 0;
        this.vad.state = 'silence';
        this.vad.lastSilence = performance.now();
        this._silenceSince = performance.now();

        this.node.onaudioprocess = (ev) => {
          const ch = ev.inputBuffer.getChannelData(0);
          let sum = 0;
          for (let i = 0; i < ch.length; i++) sum += ch[i] * ch[i];
          const rmsCur = Math.sqrt(sum / ch.length);
          this.vad.ema = (1 - alpha) * this.vad.ema + alpha * rmsCur;
          const ema = this.vad.ema;
          const onTh = rmsTh;
          const offTh = rmsTh * 0.7;
          const now = performance.now();
          const tailMs = tailBaseMs;
          const f32 = (from === this._rate) ? ch.slice(0) : this.resample(ch, from, this._rate);
          this._prePush(f32);

          if (this.vad.state === 'silence') {
            if (ema >= onTh) {
              this.vad.state = 'voice';
              this.vad.lastVoice = now;
              this._sawSpeech = true;
              this._ignorePartials = false;
              this._ensureLiveSession(cfg).then(() => {
                if (this.sid) this._openUplink(now, tailMs, f32);
              });
            } else if (this._uplinkOn) {
              if (now > this._tailDeadline) {
                const postQuiet = (now - this._lastPostAt) >= this._lingerMs;
                const partialQuiet = (!this._sawPartialForUplink) || ((now - this._lastPartialAt) >= this._lingerMs);
                const hardQuiet = (now - Math.max(this._lastPostAt, this._lastPartialAt || 0)) >= this._forceQuietMaxMs;
                if ((postQuiet && partialQuiet) || hardQuiet) this._closeUplinkAndMaybeFinalize();
              }
            }
          } else {
            if (ema >= offTh) {
              this.vad.lastVoice = now;
              if (this._uplinkOn) {
                this._extendTail(now, tailMs);
                this.pushF32(f32);
              }
            } else {
              const sinceVoice = now - (this.vad.lastVoice || now);
              if (sinceVoice >= holdMs) {
                this.vad.state = 'silence';
                this.vad.lastSilence = now;
                this._silenceSince = now;
                this._ignorePartials = true;
                if (this.aggr.pend) {
                  this._routePhrase(this.aggr.pend.trim());
                  this.aggr.pend = '';
                }
              } else if (this._uplinkOn) {
                this.pushF32(f32);
              }
            }
          }

          this._setVU(ema);

          if (!this._live) {
            if (ema < offTh) {
              batchQuietSince = batchQuietSince ?? now;
              if (now - batchQuietSince >= silence) {
                batchQuietSince = null;
                this.finalizeOnce(this._rate).catch(() => {});
              }
            } else {
              batchQuietSince = null;
            }
          }
        };

        this.source.connect(this.node);
        this.node.connect(this.ac.destination);
        this._startVis(nodeId);
        clearInterval(this._vadWatch);
        if (this._live) {
          this._vadWatch = setInterval(() => {
            if (!this.sid || this.finalizing || !this._uplinkOn) return;
            const now = performance.now();
            if (this.vad.state !== 'silence') return;
            if (now <= this._tailDeadline) return;
            const postQuiet = (now - this._lastPostAt) >= this._lingerMs;
            const partialQuiet = (!this._sawPartialForUplink) || ((now - this._lastPartialAt) >= this._lingerMs);
            const hardQuiet = (now - Math.max(this._lastPostAt, this._lastPartialAt || 0)) >= this._forceQuietMaxMs;
            if ((postQuiet && partialQuiet) || hardQuiet) this._closeUplinkAndMaybeFinalize();
          }, Math.max(50, Math.min(200, this._chunk)));
        }
      } catch (err) {
        log('[asr] ' + err.message);
        return;
      }

      this._setPartial('');
      const finalBox = this._nodeEl()?.querySelector('[data-asr-final]');
      if (finalBox) finalBox.textContent = '';
      this.running = true;
    },

    async stop() {
      if (!this.running) return;
      this.running = false;
      const visId = this.ownerId;
      try {
        if (this.sid) {
          await Net.postJSON(this._base, `/recognize/stream/${encodeURIComponent(this.sid)}/end`, {}, this._api, this._viaNkn, this._relay, 20000);
        }
      } catch (err) {
        // ignore
      }
      this.sid = null;
      this._uplinkOn = false;
      try {
        this.node && this.node.disconnect();
      } catch (err) {}
      try {
        this.source && this.source.disconnect();
      } catch (err) {}
      try {
        this.media && this.media.getTracks().forEach((t) => t.stop());
      } catch (err) {}
      try {
        this.ac && this.ac.close();
      } catch (err) {}
      this.an = null;
      this.node = this.source = this.media = this.ac = null;
      clearInterval(this._vadWatch);
      this._vadWatch = null;
      this.bufF32 = new Float32Array(0);
      this._preInit(this._rate || 16000);
      if (visId) this._stopVis(visId);
      this._setVU(0);
      this.ownerId = null;
    },

    async finalizeOnce(rate) {
      if (!this.bufF32.length) return;
      const pcm = this.bufF32;
      const buf = new ArrayBuffer(44 + pcm.length * 2);
      const view = new DataView(buf);
      let offset = 0;
      const W4 = (s) => { for (let i = 0; i < 4; i++) view.setUint8(offset++, s.charCodeAt(i)); };
      const U32 = (n) => { view.setUint32(offset, n, true); offset += 4; };
      const U16 = (n) => { view.setUint16(offset, n, true); offset += 2; };
      const sr = rate;
      const bps = 16;
      const ch = 1;
      const ba = ch * bps / 8;
      const br = sr * ba;
      W4('RIFF');
      U32(36 + pcm.length * 2);
      W4('WAVE');
      W4('fmt ');
      U32(16);
      U16(1);
      U16(1);
      U32(sr);
      U32(br);
      U16(ba);
      U16(bps);
      W4('data');
      U32(pcm.length * 2);
      for (let i = 0; i < pcm.length; i++) {
        view.setInt16(44 + i * 2, Math.max(-1, Math.min(1, pcm[i])) < 0 ? pcm[i] * 0x8000 : pcm[i] * 0x7FFF, true);
      }
      const b64 = btoa(String.fromCharCode(...new Uint8Array(buf)));
      this.bufF32 = new Float32Array(0);
      try {
        const cfg = NodeStore.ensure(this.ownerId, 'ASR').config || {};
        const prompt = (cfg.prompt || '').trim();
        const body = Object.assign({ body_b64: b64, format: 'wav', sample_rate: rate }, prompt ? { prompt } : {});
        const data = await Net.postJSON(this._base, '/recognize', body, this._api, this._viaNkn, this._relay, 120000);
        const txt = (data && (data.text || data.transcript)) || '';
        if (txt) this.appendFinal(txt);
      } catch (err) {
        log('[asr] finalize ' + err.message);
      }
    },

    async openEvents(sid, viaNkn, base, relay, api, cfg) {
      const pump = (() => {
        let buffer = '';
        return (u8) => {
          buffer += td.decode(u8, { stream: true });
          let index;
          while ((index = buffer.indexOf('\n\n')) >= 0) {
            const chunk = buffer.slice(0, index);
            buffer = buffer.slice(index + 2);
            let ev = null;
            let data = '';
            for (const line of chunk.split(/\r?\n/)) {
              if (!line) continue;
              if (line.startsWith(':')) continue;
              if (line.startsWith('event:')) ev = line.slice(6).trim();
              else if (line.startsWith('data:')) data += (data ? '\n' : '') + line.slice(5).trim();
            }
            if (data) {
              try {
                const obj = JSON.parse(data);
                const type = (obj.type || obj.event || '').toLowerCase();
                if (type === 'asr.partial' || type === 'partial') {
                  const text = obj.text || (obj.result && obj.result.text) || '';
                  const sameSid = (sid === ASR.sid);
                  const ok = sameSid && !ASR.finalizing && ASR._uplinkOn && ASR.vad.state === 'voice' && !ASR._ignorePartials;
                  if (ok) {
                    ASR.printPartial(text, cfg);
                    if (text) {
                      ASR._sawSpeech = true;
                      ASR._lastPartialAt = performance.now();
                      ASR._sawPartialForUplink = true;
                    }
                  }
                } else if (type === 'asr.detected' || type === 'detected') {
                  const s = obj.text || (obj.result && obj.result.text) || '';
                  if (s) {
                    const meta = obj.result || obj;
                    if (!ASR.shouldDropAsHallucination(s, meta)) ASR._routePhrase(s);
                  }
                } else if (type === 'asr.final' || type === 'final') {
                  const s = (obj.result && obj.result.text) || obj.text || '';
                  const meta = obj.result || obj;
                  if (s) {
                    if (ASR.shouldDropAsHallucination(s, meta)) {
                      log('[asr] dropped probable sign-off: "' + s + '"');
                    } else {
                      ASR._ignorePartials = true;
                      ASR.appendFinal(s);
                      ASR.handleFinalFlush(s);
                    }
                  }
                }
              } catch (err) {
                // ignore parse errors
              }
            }
          }
        };
      })();

      if (viaNkn) {
        await Net.nknStream(
          { url: base.replace(/\/+$/, '') + `/recognize/stream/${encodeURIComponent(sid)}/events`, method: 'GET', headers: Net.auth({ 'X-Relay-Stream': 'chunks' }, api), timeout_ms: 10 * 60 * 1000 },
          relay,
          { onBegin: () => {}, onChunk: (u8) => pump(u8), onEnd: () => {} }
        );
      } else {
        const res = await fetch(base.replace(/\/+$/, '') + `/recognize/stream/${encodeURIComponent(sid)}/events`, { headers: Net.auth({}, api) });
        if (!res.ok || !res.body) return;
        const reader = res.body.getReader();
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          if (value && value.byteLength) pump(value);
        }
      }
    },

    async flushLoop(rate, chunk, viaNkn, base, relay, api) {
      const need = () => Math.round(rate * (chunk / 1000));
      while (this.running && this.sid) {
        if (this.bufF32.length >= need() && this.out < 4 && this._uplinkOn) {
          const f32 = this.drainF32(need());
          const bytes = this.i16(f32);
          const url = base.replace(/\/+$/, '') + `/recognize/stream/${encodeURIComponent(this.sid)}/audio?format=pcm16&sr=${rate}`;
          this.out++;
          try {
            this._lastPostAt = performance.now();
            if (viaNkn) {
              await Net.nknSend({ url, method: 'POST', headers: Net.auth({ 'Content-Type': 'application/octet-stream' }, api), body_b64: btoa(String.fromCharCode(...bytes)), timeout_ms: 20000 }, relay, 20000);
            } else {
              const res = await fetch(url, { method: 'POST', headers: Net.auth({ 'Content-Type': 'application/octet-stream' }, api), body: bytes });
              if (!res.ok) throw new Error(res.status + ' ' + res.statusText);
            }
            this._lastPostAt = performance.now();
          } catch (err) {
            log('[asr] audio send ' + (err && err.message || err));
          } finally {
            this.out--;
          }
        }
        await new Promise((resolve) => setTimeout(resolve, Math.max(10, chunk / 2)));
      }
    }
  };

  return ASR;
}

export { createASR };
