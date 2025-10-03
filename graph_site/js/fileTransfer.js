function createFileTransfer({ getNode, NodeStore, Router, log, setBadge }) {
  const encoder = new TextEncoder();
  const state = new Map();

  const DEFAULT_ITERATIONS = 120000;
  const DEFAULT_IV_BYTES = 12;

  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  const toBase64 = (bytes) => {
    if (!bytes) return '';
    if (bytes instanceof ArrayBuffer) bytes = new Uint8Array(bytes);
    if (!(bytes instanceof Uint8Array)) bytes = new Uint8Array(bytes);
    let binary = '';
    for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
    return btoa(binary);
  };

  const fromBase64 = (b64) => {
    if (typeof b64 !== 'string' || !b64) return new Uint8Array();
    const binary = atob(b64);
    const out = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
    return out;
  };

  const formatBytes = (bytes) => {
    if (!Number.isFinite(bytes)) return '0 B';
    const thresh = 1024;
    if (bytes < thresh) return `${bytes} B`;
    const units = ['KB', 'MB', 'GB', 'TB'];
    let unit = -1;
    let value = bytes;
    do {
      value /= thresh;
      unit += 1;
    } while (value >= thresh && unit < units.length - 1);
    return `${value.toFixed(1)} ${units[unit]}`;
  };

  const randomId = () => `ft-${Math.random().toString(36).slice(2, 8)}-${Date.now().toString(36)}`;

  function ensureState(id) {
    if (!state.has(id)) {
      state.set(id, {
        sending: null,
        incoming: new Map(),
        activeIncoming: null,
        fileUrl: null,
        ui: null,
        lastPass: ''
      });
    }
    return state.get(id);
  }

  function config(id) {
    return NodeStore.ensure(id, 'FileTransfer').config || {};
  }

  function pickPassphrase(id) {
    const st = ensureState(id);
    if (st?.ui?.pass) {
      const val = st.ui.pass.value.trim();
      if (val) return val;
    }
    const cfg = config(id);
    if (cfg.defaultKey && cfg.defaultKey.trim()) return cfg.defaultKey.trim();
    if (st.lastPass) return st.lastPass;
    return '';
  }

  async function deriveEncryption(passphrase, saltB64, iterations = DEFAULT_ITERATIONS) {
    if (!passphrase) return null;
    const cryptoObj = globalThis.crypto;
    if (!cryptoObj?.subtle) {
      setBadge('WebCrypto unavailable for encryption', false);
      return null;
    }
    const salt = saltB64 ? fromBase64(saltB64) : cryptoObj.getRandomValues(new Uint8Array(16));
    const baseKey = await cryptoObj.subtle.importKey('raw', encoder.encode(passphrase), 'PBKDF2', false, ['deriveKey', 'deriveBits']);
    const key = await cryptoObj.subtle.deriveKey(
      { name: 'PBKDF2', salt, iterations, hash: 'SHA-256' },
      baseKey,
      { name: 'AES-GCM', length: 256 },
      false,
      ['encrypt', 'decrypt']
    );
    const rawKey = new Uint8Array(await cryptoObj.subtle.exportKey('raw', key));
    const hash = new Uint8Array(await cryptoObj.subtle.digest('SHA-256', rawKey));
    return {
      key,
      salt,
      saltB64: toBase64(salt),
      iterations,
      keyHash: toBase64(hash)
    };
  }

  async function encryptChunk(encInfo, chunkBytes) {
    const cryptoObj = globalThis.crypto;
    const iv = cryptoObj.getRandomValues(new Uint8Array(DEFAULT_IV_BYTES));
    const encrypted = await cryptoObj.subtle.encrypt({ name: 'AES-GCM', iv }, encInfo.key, chunkBytes);
    return {
      data: toBase64(new Uint8Array(encrypted)),
      iv: toBase64(iv)
    };
  }

  async function decryptChunk(encInfo, ivB64, dataB64) {
    try {
      const iv = fromBase64(ivB64);
      const cipherBytes = fromBase64(dataB64);
      const cryptoObj = globalThis.crypto;
      const decrypted = await cryptoObj.subtle.decrypt({ name: 'AES-GCM', iv }, encInfo.key, cipherBytes);
      return new Uint8Array(decrypted);
    } catch (err) {
      throw new Error('Decryption failed');
    }
  }

  function nodeElements(id) {
    const node = getNode(id);
    if (!node?.el) return {};
    const el = node.el;
    return {
      root: el,
      drop: el.querySelector('[data-ft-drop]'),
      dropLabel: el.querySelector('[data-ft-drop-label]'),
      input: el.querySelector('[data-ft-input]'),
      send: el.querySelector('[data-ft-send]'),
      cancel: el.querySelector('[data-ft-cancel]'),
      progressWrap: el.querySelector('[data-ft-progress]'),
      progressBar: el.querySelector('[data-ft-progress-bar]'),
      progressText: el.querySelector('[data-ft-progress-text]'),
      statusLine: el.querySelector('[data-ft-status]'),
      info: el.querySelector('[data-ft-info]'),
      save: el.querySelector('[data-ft-save]'),
      clear: el.querySelector('[data-ft-clear]'),
      route: el.querySelector('[data-ft-route]'),
      pass: el.querySelector('[data-ft-pass]')
    };
  }

  function bindUi(id) {
    const st = ensureState(id);
    const ui = nodeElements(id);
    if (!ui.root) return;
    st.ui = ui;

    const cfg = config(id);
    if (ui.route) ui.route.value = cfg.preferRoute || '';
    if (ui.pass) ui.pass.value = cfg.defaultKey || '';

    if (ui.drop) {
      ui.drop.addEventListener('click', () => {
        ui.input?.click();
      });
      ui.drop.addEventListener('dragover', (e) => {
        e.preventDefault();
        ui.drop.classList.add('dragover');
      });
      ui.drop.addEventListener('dragleave', () => ui.drop.classList.remove('dragover'));
      ui.drop.addEventListener('drop', (e) => {
        e.preventDefault();
        ui.drop.classList.remove('dragover');
        const files = e.dataTransfer?.files;
        if (files?.length) {
          handleSelectedFile(id, files[0]);
        }
      });
    }

    if (ui.input) {
      ui.input.addEventListener('change', () => {
        const files = ui.input?.files;
        if (files?.length) handleSelectedFile(id, files[0]);
      });
    }

    if (ui.send) {
      ui.send.addEventListener('click', async () => {
        try {
          await beginSend(id);
        } catch (err) {
          setBadge(`File send failed: ${err?.message || err}`, false);
          log(`[fileTransfer] send failed: ${err?.stack || err}`);
        }
      });
    }

    if (ui.cancel) {
      ui.cancel.addEventListener('click', () => {
        cancelSend(id, 'sender-cancel');
      });
    }

    if (ui.save) {
      ui.save.addEventListener('click', () => saveIncomingFile(id));
    }

    if (ui.clear) {
      ui.clear.addEventListener('click', () => clearIncoming(id));
    }

    if (ui.route) {
      ui.route.addEventListener('change', () => {
        NodeStore.update(id, { type: 'FileTransfer', preferRoute: ui.route.value.trim() });
      });
    }

    if (ui.pass) {
      ui.pass.addEventListener('change', () => {
        const value = ui.pass.value.trim();
        st.lastPass = value;
        NodeStore.update(id, { type: 'FileTransfer', defaultKey: value });
        attemptDecrypt(id);
      });
    }

    updateSendUi(id, null);
    updateReceiveUi(id, null);
  }

  function updateSendUi(id, sending) {
    const st = ensureState(id);
    const ui = st.ui;
    if (!ui) return;
    if (!sending) {
      ui.send?.setAttribute('disabled', 'disabled');
      ui.cancel?.classList.add('hidden');
      ui.progressWrap?.classList.add('hidden');
      if (ui.progressBar) ui.progressBar.style.setProperty('--pct', '0%');
      if (ui.progressText) ui.progressText.textContent = '0%';
      if (ui.dropLabel) ui.dropLabel.textContent = 'Drop file or click to select';
      ui.drop?.classList.toggle('selected', false);
      st.pendingFile = null;
      return;
    }
    const pct = sending.totalChunks ? Math.floor((sending.sentChunks / sending.totalChunks) * 100) : 0;
    ui.progressWrap?.classList.toggle('hidden', false);
    if (ui.progressBar) ui.progressBar.style.setProperty('--pct', `${pct}%`);
    if (ui.progressText) ui.progressText.textContent = `${pct}% (${sending.sentChunks}/${sending.totalChunks})`;
    if (ui.send) ui.send.setAttribute('disabled', 'disabled');
    if (ui.cancel) ui.cancel.classList.remove('hidden');
  }

  function updateReceiveUi(id, incoming) {
    const st = ensureState(id);
    const ui = st.ui;
    if (!ui) return;
    const entry = incoming || (st.activeIncoming ? st.incoming.get(st.activeIncoming) : null);
    if (!entry) {
      ui.statusLine && (ui.statusLine.textContent = 'Waiting for file…');
      ui.info && (ui.info.textContent = '');
      ui.save?.setAttribute('disabled', 'disabled');
      ui.clear?.setAttribute('disabled', 'disabled');
      return;
    }
    const pct = entry.totalChunks ? Math.floor((entry.receivedCount / entry.totalChunks) * 100) : 0;
    ui.statusLine && (ui.statusLine.textContent = entry.statusText || `Receiving ${pct}%`);
    const infoParts = [];
    infoParts.push(`${entry.name || 'file'} • ${formatBytes(entry.size || 0)} • ${entry.totalChunks} chunks`);
    if (entry.from) infoParts.push(`from ${entry.from}`);
    if (entry.route) infoParts.push(`route ${entry.route}`);
    if (entry.encrypted) infoParts.push('encrypted');
    ui.info && (ui.info.textContent = infoParts.join(' | '));
    if (entry.ready) {
      ui.save?.removeAttribute('disabled');
    } else {
      ui.save?.setAttribute('disabled', 'disabled');
    }
    ui.clear?.removeAttribute('disabled');
  }

  function setSendSelectable(id, file) {
    const st = ensureState(id);
    const ui = st.ui;
    st.pendingFile = file || null;
    if (!ui) return;
    if (file) {
      if (ui.dropLabel) ui.dropLabel.textContent = `${file.name} (${formatBytes(file.size)})`;
      ui.drop?.classList.add('selected');
      ui.send?.removeAttribute('disabled');
    } else {
      if (ui.dropLabel) ui.dropLabel.textContent = 'Drop file or click to select';
      ui.drop?.classList.remove('selected');
      ui.send?.setAttribute('disabled', 'disabled');
    }
  }

  function handleSelectedFile(id, file) {
    if (!(file instanceof File)) {
      setBadge('Unsupported file selection', false);
      return;
    }
    const st = ensureState(id);
    if (st.sending) {
      setBadge('Already sending a file', false);
      return;
    }
    setSendSelectable(id, file);
  }

  async function beginSend(id) {
    const st = ensureState(id);
    const ui = st.ui;
    if (!st.pendingFile) {
      setBadge('Select a file first', false);
      return;
    }
    if (st.sending) {
      setBadge('Transfer already in progress', false);
      return;
    }

    const file = st.pendingFile;
    const cfg = config(id);
    const route = (ui?.route?.value || cfg.preferRoute || '').trim();
    const passphrase = pickPassphrase(id);
    const chunkBytes = Math.max(512, Math.min(32768, Number(cfg.chunkSize) || 1024));
    const totalChunks = Math.max(1, Math.ceil(file.size / chunkBytes));
    const transferId = randomId();

    let encInfo = null;
    if (passphrase) {
      try {
        encInfo = await deriveEncryption(passphrase, null, DEFAULT_ITERATIONS);
      } catch (err) {
        setBadge(`Encryption setup failed: ${err?.message || err}`, false);
        encInfo = null;
      }
    }

    const sending = {
      id: transferId,
      file,
      fileName: file.name,
      mime: file.type || 'application/octet-stream',
      size: file.size,
      chunkBytes,
      totalChunks,
      sentChunks: 0,
      route,
      passphrase,
      encInfo,
      cancelled: false,
      chunks: new Map()
    };
    st.sending = sending;
    updateSendUi(id, sending);
    emitStatus(id, {
      direction: 'outgoing',
      transferId,
      op: 'start',
      name: file.name,
      size: file.size,
      totalChunks,
      route,
      encrypted: Boolean(encInfo),
      ts: Date.now()
    });

    await sendHeader(id, sending);
    await sendChunks(id, sending);
    if (!sending.cancelled) await sendComplete(id, sending);

    if (!sending.cancelled) {
      setBadge(`File sent: ${file.name}`, true);
    }
    st.sending = null;
    setSendSelectable(id, null);
    updateSendUi(id, null);
  }

  async function sendHeader(id, sending) {
    const header = {
      type: 'nkndm.file',
      op: 'header',
      transferId: sending.id,
      name: sending.fileName,
      size: sending.size,
      mime: sending.mime,
      totalChunks: sending.totalChunks,
      chunkSize: sending.chunkBytes,
      route: sending.route,
      ts: Date.now(),
      version: 1
    };
    if (sending.encInfo) {
      header.encryption = {
        type: 'aes-gcm',
        salt: sending.encInfo.saltB64,
        iterations: sending.encInfo.iterations,
        keyHash: sending.encInfo.keyHash,
        ivBytes: DEFAULT_IV_BYTES
      };
    }
    Router.sendFrom(id, 'outgoing', JSON.stringify(header));
    await sleep(10);
  }

  async function sendChunks(id, sending) {
    for (let idx = 0; idx < sending.totalChunks; idx += 1) {
      if (sending.cancelled) return;
      const start = idx * sending.chunkBytes;
      const end = Math.min(start + sending.chunkBytes, sending.size);
      const blob = sending.file.slice(start, end);
      const buffer = await blob.arrayBuffer();
      const raw = new Uint8Array(buffer);
      let payloadData;
      let encMeta = null;
      if (sending.encInfo) {
        const encrypted = await encryptChunk(sending.encInfo, raw);
        payloadData = encrypted.data;
        encMeta = { iv: encrypted.iv, type: 'aes-gcm' };
      } else {
        payloadData = toBase64(raw);
      }
      const seq = idx + 1;
      sending.chunks.set(seq, { data: payloadData, enc: encMeta, rawLength: raw.length });
      const chunkMessage = {
        type: 'nkndm.file',
        op: 'chunk',
        transferId: sending.id,
        seq,
        totalChunks: sending.totalChunks,
        data: payloadData,
        size: sending.size,
        chunkSize: raw.length,
        route: sending.route,
        ts: Date.now()
      };
      if (encMeta) chunkMessage.encryption = encMeta;
      Router.sendFrom(id, 'outgoing', JSON.stringify(chunkMessage));
      sending.sentChunks = seq;
      updateSendUi(id, sending);
      emitStatus(id, {
        direction: 'outgoing',
        transferId: sending.id,
        op: 'chunk',
        seq,
        totalChunks: sending.totalChunks,
        progress: sending.sentChunks / sending.totalChunks,
        ts: Date.now()
      });
      await sleep(6);
    }
  }

  async function sendComplete(id, sending) {
    const message = {
      type: 'nkndm.file',
      op: 'complete',
      transferId: sending.id,
      totalChunks: sending.totalChunks,
      size: sending.size,
      route: sending.route,
      ts: Date.now()
    };
    Router.sendFrom(id, 'outgoing', JSON.stringify(message));
    emitStatus(id, {
      direction: 'outgoing',
      transferId: sending.id,
      op: 'complete',
      totalChunks: sending.totalChunks,
      ts: Date.now()
    });
  }

  function cancelSend(id, reason = 'cancelled') {
    const st = ensureState(id);
    if (!st.sending) return;
    st.sending.cancelled = true;
    const message = {
      type: 'nkndm.file',
      op: 'cancel',
      transferId: st.sending.id,
      reason,
      ts: Date.now()
    };
    Router.sendFrom(id, 'outgoing', JSON.stringify(message));
    emitStatus(id, {
      direction: 'outgoing',
      transferId: st.sending.id,
      op: 'cancel',
      reason,
      ts: Date.now()
    });
    setBadge('Transfer cancelled', false);
    st.sending = null;
    setSendSelectable(id, null);
    updateSendUi(id, null);
  }

  function emitStatus(id, payload) {
    Router.sendFrom(id, 'status', { nodeId: id, ...payload });
  }

  function ensureIncomingEntry(id, data, payload) {
    const st = ensureState(id);
    const map = st.incoming;
    const transferId = data.transferId || data.id || 'unknown';
    if (!map.has(transferId)) {
      const cfg = config(id);
      map.set(transferId, {
        id,
        transferId,
        name: data.name || 'file',
        mime: data.mime || 'application/octet-stream',
        size: Number(data.size) || 0,
        totalChunks: Number(data.totalChunks || data.total) || 0,
        chunkSize: Number(data.chunkSize) || 0,
        from: payload?.from || '',
        route: data.route || payload?.route || '',
        receivedCount: 0,
        chunks: [],
        encryption: data.encryption || null,
        encrypted: Boolean(data.encryption),
        ready: false,
        statusText: '',
        requested: new Set(),
        completed: false,
        digest: data.checksum || '',
        createdAt: Date.now(),
        autoAccept: cfg.autoAccept !== false,
        missingTries: 0
      });
    }
    const entry = map.get(transferId);
    if (data.name) entry.name = data.name;
    if (data.mime) entry.mime = data.mime;
    if (data.size) entry.size = Number(data.size);
    if (data.totalChunks || data.total) entry.totalChunks = Number(data.totalChunks || data.total);
    if (data.chunkSize) entry.chunkSize = Number(data.chunkSize);
    if (data.route) entry.route = data.route;
    if (payload?.from) entry.from = payload.from;
    st.activeIncoming = transferId;
    return entry;
  }

  async function onIncoming(id, payload) {
    if (!payload) return;
    const data = payload.payload || payload.parsed || (() => {
      try {
        return JSON.parse(payload.text || '{}');
      } catch (_) {
        return null;
      }
    })();
    if (!data || data.type !== 'nkndm.file') return;
    const op = data.op || data.action || 'chunk';
    if (op === 'header') {
      handleIncomingHeader(id, data, payload);
    } else if (op === 'chunk') {
      await handleIncomingChunk(id, data, payload);
    } else if (op === 'complete') {
      handleIncomingComplete(id, data, payload);
    } else if (op === 'request') {
      handleIncomingRequest(id, data, payload);
    } else if (op === 'cancel') {
      handleIncomingCancel(id, data, payload);
    } else if (op === 'ack') {
      // future use: progress acknowledgements
    }
  }

  function handleIncomingHeader(id, data, payload) {
    const entry = ensureIncomingEntry(id, data, payload);
    entry.statusText = `Incoming ${entry.name} (${formatBytes(entry.size)})`;
    emitStatus(id, {
      direction: 'incoming',
      transferId: entry.transferId,
      op: 'header',
      name: entry.name,
      size: entry.size,
      totalChunks: entry.totalChunks,
      route: entry.route,
      encrypted: entry.encrypted,
      from: entry.from,
      ts: Date.now()
    });
    updateReceiveUi(id, entry);
  }

  async function handleIncomingChunk(id, data, payload) {
    const entry = ensureIncomingEntry(id, data, payload);
    const seq = Number(data.seq || data.index || 0);
    if (!seq) return;
    if (!entry.totalChunks) entry.totalChunks = Number(data.totalChunks || data.total) || 0;
    if (!entry.size) entry.size = Number(data.size) || 0;
    if (!entry.chunkSize) entry.chunkSize = Number(data.chunkSize) || 0;
    if (!entry.chunks[seq - 1]) entry.receivedCount += 1;
    entry.chunks[seq - 1] = { data: data.data, encryption: data.encryption || null };
    entry.statusText = `Receiving chunk ${seq}/${entry.totalChunks}`;
    emitStatus(id, {
      direction: 'incoming',
      transferId: entry.transferId,
      op: 'chunk',
      seq,
      totalChunks: entry.totalChunks,
      progress: entry.receivedCount / entry.totalChunks,
      from: entry.from,
      ts: Date.now()
    });
    updateReceiveUi(id, entry);
    if (entry.completed) {
      assembleIncoming(id, entry).catch((err) => {
        entry.statusText = `Assemble failed: ${err?.message || err}`;
        updateReceiveUi(id, entry);
      });
    }
  }

  function handleIncomingComplete(id, data, payload) {
    const entry = ensureIncomingEntry(id, data, payload);
    entry.completed = true;
    entry.statusText = 'Completing transfer';
    assembleIncoming(id, entry).catch((err) => {
      entry.statusText = `Assemble failed: ${err?.message || err}`;
      setBadge(entry.statusText, false);
      updateReceiveUi(id, entry);
    });
  }

  function handleIncomingCancel(id, data, payload) {
    const st = ensureState(id);
    const transferId = data.transferId;
    const incoming = transferId ? st.incoming.get(transferId) : null;
    if (incoming) {
      incoming.statusText = data.reason ? `Transfer cancelled (${data.reason})` : 'Transfer cancelled';
      incoming.cancelled = true;
      incoming.ready = false;
      incoming.chunks = [];
      updateReceiveUi(id, incoming);
    }
    const sending = st.sending;
    if (sending && transferId && sending.id === transferId) {
      setBadge('Peer cancelled transfer', false);
      st.sending = null;
      updateSendUi(id, null);
    }
    emitStatus(id, {
      direction: 'incoming',
      transferId: transferId || 'unknown',
      op: 'cancel',
      reason: data.reason,
      from: payload?.from,
      ts: Date.now()
    });
  }

  function handleIncomingRequest(id, data) {
    const st = ensureState(id);
    const sending = st.sending;
    if (!sending || sending.id !== data.transferId) return;
    const missing = Array.isArray(data.missing) ? data.missing.map((n) => Number(n)).filter((n) => n >= 1) : [];
    if (!missing.length) return;
    missing.forEach((seq) => {
      const chunk = sending.chunks.get(seq);
      if (!chunk) return;
      const message = {
        type: 'nkndm.file',
        op: 'chunk',
        transferId: sending.id,
        seq,
        totalChunks: sending.totalChunks,
        data: chunk.data,
        size: sending.size,
        chunkSize: chunk.rawLength || sending.chunkBytes,
        route: sending.route,
        ts: Date.now()
      };
      if (chunk.enc) message.encryption = chunk.enc;
      Router.sendFrom(id, 'outgoing', JSON.stringify(message));
    });
    emitStatus(id, {
      direction: 'outgoing',
      transferId: sending.id,
      op: 'resend',
      missing,
      ts: Date.now()
    });
  }

  function sendMissingRequest(id, entry, missing) {
    if (!missing.length) return;
    const message = {
      type: 'nkndm.file',
      op: 'request',
      transferId: entry.transferId,
      missing,
      ts: Date.now()
    };
    Router.sendFrom(id, 'outgoing', JSON.stringify(message));
    entry.missingTries += 1;
    emitStatus(id, {
      direction: 'incoming',
      transferId: entry.transferId,
      op: 'request',
      missing,
      tries: entry.missingTries,
      ts: Date.now()
    });
  }

  async function assembleIncoming(id, entry) {
    const total = entry.totalChunks;
    if (!total) {
      throw new Error('Missing chunk metadata');
    }
    const missing = [];
    for (let i = 0; i < total; i += 1) {
      if (!entry.chunks[i]) missing.push(i + 1);
    }
    if (missing.length) {
      entry.statusText = `Missing ${missing.length} chunk(s), requesting resend`;
      updateReceiveUi(id, entry);
      sendMissingRequest(id, entry, missing);
      return;
    }

    let resultBuffer;
    if (entry.size > 0) {
      resultBuffer = new Uint8Array(entry.size);
    } else {
      const chunkSize = entry.chunks[0]?.data ? fromBase64(entry.chunks[0].data).length : 0;
      resultBuffer = new Uint8Array(chunkSize * entry.totalChunks);
    }

    let offset = 0;
    for (let i = 0; i < entry.totalChunks; i += 1) {
      const chunk = entry.chunks[i];
      if (!chunk) continue;
      let bytes;
      if (entry.encrypted || chunk.encryption) {
        const encInfo = await getDecryptionInfo(id, entry);
        if (!encInfo) {
          entry.statusText = 'Encrypted file - provide passphrase';
          updateReceiveUi(id, entry);
          return;
        }
        bytes = await decryptChunk(encInfo, chunk.encryption?.iv, chunk.data);
      } else {
        bytes = fromBase64(chunk.data);
      }
      if (offset + bytes.length > resultBuffer.length) {
        const next = new Uint8Array(offset + bytes.length);
        next.set(resultBuffer.subarray(0, offset), 0);
        resultBuffer = next;
      }
      resultBuffer.set(bytes, offset);
      offset += bytes.length;
    }
    const finalBuffer = resultBuffer.subarray(0, offset);
    entry.ready = true;
    entry.statusText = `Ready (${formatBytes(offset)})`;
    entry.resultBuffer = finalBuffer.buffer.slice(0);
    entry.resultBlob = new Blob([finalBuffer], { type: entry.mime });
    updateReceiveUi(id, entry);
    emitStatus(id, {
      direction: 'incoming',
      transferId: entry.transferId,
      op: 'complete',
      size: offset,
      ts: Date.now()
    });
    Router.sendFrom(id, 'file', {
      nodeId: id,
      transferId: entry.transferId,
      name: entry.name,
      mime: entry.mime,
      size: offset,
      route: entry.route,
      from: entry.from,
      buffer: entry.resultBuffer,
      blob: entry.resultBlob,
      encrypted: entry.encrypted,
      ts: Date.now()
    });
  }

  async function getDecryptionInfo(id, entry) {
    if (!entry.encrypted) return null;
    if (entry.decryptInfo) return entry.decryptInfo;
    const passphrase = pickPassphrase(id);
    if (!passphrase) return null;
    try {
      const info = await deriveEncryption(passphrase, entry.encryption?.salt, entry.encryption?.iterations || DEFAULT_ITERATIONS);
      if (info && entry.encryption?.keyHash && info.keyHash !== entry.encryption.keyHash) {
        throw new Error('Passphrase mismatch');
      }
      entry.decryptInfo = info;
      return info;
    } catch (err) {
      setBadge(`File decrypt failed: ${err?.message || err}`, false);
      entry.statusText = 'Decryption error';
      updateReceiveUi(id, entry);
      return null;
    }
  }

  function attemptDecrypt(id) {
    const st = ensureState(id);
    if (!st.activeIncoming) return;
    const entry = st.incoming.get(st.activeIncoming);
    if (!entry || !entry.encrypted || entry.ready) return;
    if (!entry.completed) return;
    assembleIncoming(id, entry).catch((err) => {
      entry.statusText = `Assemble failed: ${err?.message || err}`;
      updateReceiveUi(id, entry);
    });
  }

  function saveIncomingFile(id) {
    const st = ensureState(id);
    if (!st.activeIncoming) {
      setBadge('No file ready', false);
      return;
    }
    const entry = st.incoming.get(st.activeIncoming);
    if (!entry?.ready || !entry.resultBlob) {
      setBadge('File not ready yet', false);
      return;
    }
    if (st.fileUrl) URL.revokeObjectURL(st.fileUrl);
    st.fileUrl = URL.createObjectURL(entry.resultBlob);
    const link = document.createElement('a');
    link.href = st.fileUrl;
    link.download = entry.name || 'file.bin';
    link.click();
  }

  function clearIncoming(id) {
    const st = ensureState(id);
    if (st.fileUrl) {
      URL.revokeObjectURL(st.fileUrl);
      st.fileUrl = null;
    }
    st.incoming.clear();
    st.activeIncoming = null;
    updateReceiveUi(id, null);
  }

  function dispose(id) {
    const st = ensureState(id);
    if (st.fileUrl) URL.revokeObjectURL(st.fileUrl);
    state.delete(id);
  }

  function onFilePayload(id, payload) {
    if (!payload) return;
    if (payload instanceof File) {
      handleSelectedFile(id, payload);
      return;
    }
    if (payload?.blob instanceof Blob) {
      const file = new File([payload.blob], payload.name || 'file.bin', { type: payload.blob.type || payload.mime || 'application/octet-stream' });
      handleSelectedFile(id, file);
      return;
    }
    if (payload?.buffer instanceof ArrayBuffer) {
      const blob = new Blob([payload.buffer], { type: payload.mime || 'application/octet-stream' });
      const file = new File([blob], payload.name || 'file.bin', { type: blob.type });
      handleSelectedFile(id, file);
      return;
    }
  }

  return {
    init: bindUi,
    dispose,
    onIncoming,
    onFilePayload,
    cancelSend
  };
}

export { createFileTransfer };
