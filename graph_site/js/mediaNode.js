const arrayBufferToBase64 = (buffer) => {
  const bytes = new Uint8Array(buffer);
  let binary = '';
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  return btoa(binary);
};

const base64ToBlob = (b64, mime) => {
  const binary = atob(b64);
  const len = binary.length;
  const bytes = new Uint8Array(len);
  for (let i = 0; i < len; i++) bytes[i] = binary.charCodeAt(i);
  return new Blob([bytes], { type: mime || 'application/octet-stream' });
};

const base64ToArrayBuffer = (b64) => {
  const binary = atob(b64);
  const len = binary.length;
  const bytes = new Uint8Array(len);
  for (let i = 0; i < len; i++) bytes[i] = binary.charCodeAt(i);
  return bytes.buffer;
};

function createMediaNode({ getNode, Router, NodeStore, setBadge, log }) {
  const state = new Map();

  function ensureState(id) {
    if (!state.has(id)) {
      state.set(id, {
        running: false,
        stream: null,
        frameTimer: null,
        recorder: null,
        recorderHandler: null,
        canvas: null,
        lastSendTs: 0,
        lastRemoteFrom: null,
        audioCtx: null,
        audioQueueTime: 0,
        frameSeq: 0,
        audioSeq: 0
      });
    }
    return state.get(id);
  }

  function nodeElements(id) {
    const node = getNode(id);
    if (!node || !node.el) return {};
    return {
      container: node.el,
      localVideo: node.el.querySelector('[data-media-local]'),
      remoteImg: node.el.querySelector('[data-media-remote]'),
      remoteInfo: node.el.querySelector('[data-media-remote-info]'),
      status: node.el.querySelector('[data-media-status]'),
      audioOut: node.el.querySelector('[data-media-audio]')
    };
  }

  function config(id) {
    return NodeStore.ensure(id, 'MediaStream').config || {};
  }

  function qualityFromCompression(compression) {
    const value = Number(compression);
    if (!Number.isFinite(value)) return 0.5;
    const clamped = Math.max(0, Math.min(100, value));
    return Math.max(0.05, (100 - clamped) / 100);
  }

  function updateStatus(id, text) {
    const { status } = nodeElements(id);
    if (status) status.textContent = text;
  }

  function updateButtonGlyph(id) {
    const node = getNode(id);
    if (!node || !node.el) return;
    const btn = node.el.querySelector('.mediaToggle');
    const st = ensureState(id);
    if (btn) btn.textContent = st.running ? '■' : '▶';
  }

  function stopRecorder(st) {
    if (st.recorder) {
      try {
        st.recorder.stop();
      } catch (err) {
        // ignore
      }
      st.recorder = null;
    }
    if (st.recorderHandler) {
      st.recorderHandler();
      st.recorderHandler = null;
    }
  }

  function stopCapture(id) {
    const st = ensureState(id);
    if (!st.running) return;
    if (st.frameTimer) {
      clearInterval(st.frameTimer);
      st.frameTimer = null;
    }
    stopRecorder(st);
    if (st.stream) {
      try {
        st.stream.getTracks().forEach((track) => track.stop());
      } catch (err) {
        // ignore
      }
      st.stream = null;
    }
    if (st.audioCtx) {
      try { st.audioCtx.close(); } catch (_) {}
      st.audioCtx = null;
      st.audioQueueTime = 0;
    }
    st.running = false;
    updateStatus(id, 'Stopped');
    updateButtonGlyph(id);
    NodeStore.update(id, { type: 'MediaStream', running: false });
  }

  async function startCapture(id) {
    const st = ensureState(id);
    if (st.running) return;
    const cfg = config(id);
    const includeVideo = !!cfg.includeVideo;
    const includeAudio = !!cfg.includeAudio;
    if (!includeVideo && !includeAudio) {
      setBadge('Enable audio or video before starting', false);
      return;
    }

    const constraints = {};
    if (includeVideo) {
      constraints.video = {
        width: { ideal: Number(cfg.videoWidth) || 640 },
        height: { ideal: Number(cfg.videoHeight) || 480 },
        frameRate: { ideal: Number(cfg.frameRate) || 8, max: Number(cfg.frameRate) || 8 }
      };
    } else {
      constraints.video = false;
    }
    if (includeAudio) {
      const sr = Number(cfg.audioSampleRate) || 48000;
      constraints.audio = {
        sampleRate: sr,
        channelCount: Number(cfg.audioChannels) || 1,
        noiseSuppression: true,
        echoCancellation: true
      };
    } else {
      constraints.audio = false;
    }

    let stream;
    try {
      stream = await navigator.mediaDevices.getUserMedia(constraints);
    } catch (err) {
      setBadge(`Media access denied: ${err?.message || err}`, false);
      log(`[media] getUserMedia failed: ${err?.stack || err}`);
      return;
    }

    st.stream = stream;
    st.running = true;
    NodeStore.update(id, { type: 'MediaStream', running: true });

    const { localVideo } = nodeElements(id);
    if (localVideo) {
      try {
        localVideo.srcObject = stream;
        localVideo.play().catch(() => {});
      } catch (err) {
        // ignore
      }
    }

    if (includeVideo) startVideoLoop(id, cfg);
    if (includeAudio) startAudioLoop(id, cfg);

    updateStatus(id, includeVideo ? 'Streaming video' : 'Streaming audio');
    updateButtonGlyph(id);
    setBadge('Media stream started');
  }

  function startVideoLoop(id, cfg) {
    const st = ensureState(id);
    const fps = Math.max(1, Math.min(30, Number(cfg.frameRate) || 8));
    const quality = qualityFromCompression(cfg.compression);
    const { localVideo } = nodeElements(id);
    if (!localVideo) return;
    if (!st.canvas) st.canvas = document.createElement('canvas');
    const ctx = st.canvas.getContext('2d');
    st.frameSeq = 0;

    const sendFrame = () => {
      if (!st.running || !st.stream) return;
      if (!localVideo.videoWidth || !localVideo.videoHeight) return;
      st.canvas.width = localVideo.videoWidth;
      st.canvas.height = localVideo.videoHeight;
      ctx.drawImage(localVideo, 0, 0, st.canvas.width, st.canvas.height);
      let dataUrl;
      try {
        dataUrl = st.canvas.toDataURL('image/webp', quality);
      } catch (err) {
        dataUrl = st.canvas.toDataURL();
      }
      if (!dataUrl) return;
      const idx = dataUrl.indexOf(',');
      const b64 = idx >= 0 ? dataUrl.slice(idx + 1) : dataUrl;
      const mime = dataUrl.slice(5, dataUrl.indexOf(';')) || 'image/webp';
      const ts = Date.now();
      const seq = (st.frameSeq = (st.frameSeq || 0) + 1);
      const packet = {
        type: 'nkndm.media',
        op: 'video',
        kind: 'video',
        mime,
        b64,
        image: b64,
        width: st.canvas.width,
        height: st.canvas.height,
        ts,
        seq,
        nodeId: id,
        route: 'media.video'
      };
      Router.sendFrom(id, 'packet', packet);
      Router.sendFrom(id, 'media', packet);
    };

    if (st.frameTimer) clearInterval(st.frameTimer);
    st.frameTimer = setInterval(sendFrame, Math.round(1000 / fps));
  }

  function startAudioLoop(id, cfg) {
    const st = ensureState(id);
    const stream = st.stream;
    if (!stream) return;
    st.audioSeq = 0;

    const supports = (m) => {
      try {
        return typeof MediaRecorder !== 'undefined' &&
          typeof MediaRecorder.isTypeSupported === 'function' &&
          MediaRecorder.isTypeSupported(m);
      } catch (err) {
        return false;
      }
    };

    let targetMime = cfg.audioFormat === 'pcm16' ? 'audio/pcm' : 'audio/webm;codecs=opus';
    if (!supports(targetMime)) {
      if (cfg.audioFormat === 'pcm16' && supports('audio/webm;codecs=opus')) {
        targetMime = 'audio/webm;codecs=opus';
        setBadge('PCM capture unsupported, falling back to Opus', false);
      } else {
        setBadge('Audio recorder unsupported', false);
        return;
      }
    }
    const usePcm = targetMime.includes('pcm');

    const bits = Math.max(16000, Math.min(256000, Number(cfg.audioBitsPerSecond) || 32000));
    const recorder = new MediaRecorder(stream, { mimeType: targetMime, audioBitsPerSecond: bits });
    st.recorder = recorder;

    const handleData = async (event) => {
      if (!event.data || !event.data.size) return;
      try {
        const buffer = await event.data.arrayBuffer();
        const b64 = arrayBufferToBase64(buffer);
        const ts = Date.now();
        const seq = (st.audioSeq = (st.audioSeq || 0) + 1);
        const sr = Number(cfg.audioSampleRate) || 48000;
        const channels = Number(cfg.audioChannels) || 1;
        const packet = {
          type: 'nkndm.media',
          op: 'audio',
          kind: 'audio',
          mime: event.data.type || targetMime,
          b64,
          ts,
          route: 'media.audio',
          format: usePcm ? 'pcm16' : 'opus',
          sr,
          channels,
          nodeId: id,
          seq
        };
        Router.sendFrom(id, 'packet', packet);
        Router.sendFrom(id, 'media', packet);
      } catch (err) {
        log(`[media] audio encode error: ${err?.message || err}`);
      }
    };

    recorder.addEventListener('dataavailable', handleData);
    recorder.start(Math.max(200, Math.min(1000, Math.round(1000 / ((Number(cfg.frameRate) || 8) / 2)))));
    st.recorderHandler = () => recorder.removeEventListener('dataavailable', handleData);
  }

  function handleIncoming(id, payload) {
    if (payload == null) return;
    const els = nodeElements(id);
    if (!els.container) return;

    let data = null;
    if (payload && typeof payload === 'object') {
      if (payload.parsed && typeof payload.parsed === 'object') {
        data = payload.parsed;
      } else if (payload.raw && typeof payload.raw === 'object') {
        if (payload.raw.payload && typeof payload.raw.payload === 'object') {
          data = payload.raw.payload;
        } else if (typeof payload.raw.text === 'string') {
          try { data = JSON.parse(payload.raw.text); }
          catch (_) { data = null; }
        }
      }
      if (!data && typeof payload.text === 'string') {
        try { data = JSON.parse(payload.text); }
        catch (_) { data = null; }
      }
      if (!data && payload.payload && typeof payload.payload === 'object') data = payload.payload;
      if (!data && payload.message && typeof payload.message === 'object') data = payload.message;
      if (!data && payload.kind) data = payload;
    } else if (typeof payload === 'string') {
      try { data = JSON.parse(payload); }
      catch (_) { return; }
    }

    if (data && typeof data === 'object' && data.payload && typeof data.payload === 'object') {
      data = data.payload;
    }

    if (!data || typeof data !== 'object') return;

    if (data.type === 'nkndm.media' && data.payload && typeof data.payload === 'object') {
      data = { ...data, ...data.payload };
    }
    if (!data.kind && typeof data.op === 'string') data.kind = data.op;
    if (!data.route && typeof payload?.route === 'string') data.route = payload.route;
    if (!data.from && typeof payload?.from === 'string') data.from = payload.from;
    if (!data.nodeId && typeof payload?.nodeId === 'string') data.nodeId = payload.nodeId;

    const kind = data.kind || data.type;
    if (kind === 'video' && data.b64) {
      const mime = data.mime || 'image/webp';
      if (els.remoteImg) {
        const url = `data:${mime};base64,${data.b64}`;
        els.remoteImg.src = url;
        els.remoteImg.dataset.empty = 'false';
      }
      if (els.remoteInfo) {
        const src = data.from || data.nodeId || payload.from || payload.nodeId || '?';
        els.remoteInfo.textContent = `Frame • ${mime} • ${data.width || '?'}×${data.height || '?'} • from ${src}`;
      }
      const current = NodeStore.ensure(id, 'MediaStream').config || {};
      const nextFrom = data.from || data.nodeId || payload.from || payload.nodeId || '';
      if (current.lastRemoteFrom !== nextFrom) {
        NodeStore.update(id, { type: 'MediaStream', lastRemoteFrom: nextFrom });
      }
      return;
    }

    if (kind === 'audio' && data.b64) {
      const src = data.from || data.nodeId || payload.from || payload.nodeId || '?';
      const current = NodeStore.ensure(id, 'MediaStream').config || {};
      if (current.lastRemoteFrom !== src) NodeStore.update(id, { type: 'MediaStream', lastRemoteFrom: src });
      const format = String(data.format || data.mime || '').toLowerCase();
      const routeInfo = data.route || payload.route || '';

      if (format.includes('pcm16') || format.includes('pcm')) {
        const sr = Number(data.sr || payload.sr || current.audioSampleRate || 48000);
        const channels = Number(data.channels || payload.channels || current.audioChannels || 1);
        playPcmAudio(id, data.b64, sr, channels);
        if (els.remoteInfo) {
          els.remoteInfo.textContent = `Audio • PCM • ${sr} Hz • ch ${channels} • from ${src}`;
        }
        return;
      }

      if (!els.audioOut) return;
      try {
        const blob = base64ToBlob(data.b64, data.mime || 'audio/webm');
        const url = URL.createObjectURL(blob);
        els.audioOut.src = url;
        const play = els.audioOut.play();
        if (play && typeof play.catch === 'function') play.catch(() => {});
        els.audioOut.onended = () => URL.revokeObjectURL(url);
        if (els.remoteInfo) {
          const note = routeInfo ? ` • ${routeInfo}` : '';
          els.remoteInfo.textContent = `Audio • ${data.mime || 'audio/webm'}${note} • from ${src}`;
        }
      } catch (err) {
        log(`[media] audio playback error: ${err?.message || err}`);
      }
    }
  }

  function playPcmAudio(nodeId, b64, sr, channels) {
    const st = ensureState(nodeId);
    const buffer = base64ToArrayBuffer(b64);
    if (!buffer) return;
    const safeChannels = Math.max(1, Math.min(2, Number(channels) || 1));
    const safeSr = Number(sr) || 48000;

    const Ctx = window.AudioContext || window.webkitAudioContext;
    if (!Ctx) return;
    if (!st.audioCtx || st.audioCtx.sampleRate !== safeSr) {
      if (st.audioCtx) {
        try { st.audioCtx.close(); } catch (_) {}
      }
      st.audioCtx = new Ctx({ sampleRate: safeSr });
      st.audioQueueTime = 0;
    }
    const ctx = st.audioCtx;
    if (!ctx) return;
    if (ctx.state === 'suspended') ctx.resume().catch(() => {});

    const int16 = new Int16Array(buffer);
    if (!int16.length) return;
    const frameCount = Math.floor(int16.length / safeChannels);
    const audioBuffer = ctx.createBuffer(safeChannels, frameCount, safeSr);
    for (let ch = 0; ch < safeChannels; ch++) {
      const channelData = audioBuffer.getChannelData(ch);
      for (let i = 0; i < frameCount; i++) {
        const sample = int16[i * safeChannels + ch];
        channelData[i] = Math.max(-1, Math.min(1, sample / 32768));
      }
    }

    const source = ctx.createBufferSource();
    source.buffer = audioBuffer;
    source.connect(ctx.destination);

    const startAt = Math.max(ctx.currentTime, st.audioQueueTime || ctx.currentTime);
    try {
      source.start(startAt);
      st.audioQueueTime = startAt + audioBuffer.duration;
    } catch (err) {
      try {
        source.start();
        st.audioQueueTime = ctx.currentTime + audioBuffer.duration;
      } catch (_) {}
    }
  }

  function init(id) {
    ensureState(id);
    updateButtonGlyph(id);
    updateStatus(id, 'Ready');
    const cfg = config(id);
    if (cfg.lastRemoteFrom) {
      const els = nodeElements(id);
      if (els && els.remoteInfo) {
        els.remoteInfo.textContent = `Last from ${cfg.lastRemoteFrom}`;
      }
    }
  }

  function toggle(id) {
    const st = ensureState(id);
    if (st.running) stopCapture(id);
    else startCapture(id);
  }

  function refresh(nodeId) {
    const st = ensureState(nodeId);
    const cfg = config(nodeId);
    if (st.running) {
      // restart to apply new constraints
      stopCapture(nodeId);
      setTimeout(() => startCapture(nodeId), 250);
    }
    const { container } = nodeElements(nodeId);
    if (container) {
      container.dataset.mediaIncludeVideo = cfg.includeVideo ? 'true' : 'false';
      container.dataset.mediaIncludeAudio = cfg.includeAudio ? 'true' : 'false';
    }
  }

  function dispose(id) {
    stopCapture(id);
    state.delete(id);
  }

  return {
    init,
    toggle,
    refresh,
    onInput: handleIncoming,
    dispose,
    isRunning: (id) => ensureState(id).running
  };
}

export { createMediaNode };
