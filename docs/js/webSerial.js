import { toBoolean } from './utils.js';

const DEFAULT_BAUDS = [
  9600,
  14400,
  19200,
  28800,
  38400,
  57600,
  74880,
  115200,
  230400,
  460800,
  921600
];

function createWebSerial({ getNode, NodeStore, Router, log, setBadge }) {
  const sessions = new Map();
  let serialEventsBound = false;

  function stateDefaults(nodeId) {
    const cfg = getConfig(nodeId);
    return {
      nodeId,
      sessionNode: null,
      port: null,
      reader: null,
      writer: null,
      readLoopAbort: null,
      reconnectTimer: null,
      reconnectBackoff: 1000,
      userRequestedDisconnect: false,
      ui: null,
      decoder: null,
      encoder: null,
      allowed: false,
      logLines: [],
      portInfo: cfg.lastPortInfo || null,
      ready: false,
      uiBound: false,
      uiRoot: null,
      textCarry: ''
    };
  }

  function ensureSession(nodeId) {
    let entry = sessions.get(nodeId);
    if (!entry) {
      entry = { state: stateDefaults(nodeId) };
      sessions.set(nodeId, entry);
    }
    return entry;
  }

  function getConfig(nodeId) {
    const rec = NodeStore.ensure(nodeId, 'WebSerial');
    return rec?.config || {};
  }

  function saveConfig(nodeId, patch) {
    const cfg = { ...getConfig(nodeId), ...patch };
    NodeStore.saveCfg(nodeId, 'WebSerial', cfg);
    return cfg;
  }

  function booleanFromConfig(cfg, key, fallback = false) {
    const value = cfg[key];
    return typeof value === 'boolean' ? value : toBoolean(value, fallback);
  }

  function coerceNumber(value, fallback = 0) {
    const num = Number(value);
    return Number.isFinite(num) ? num : fallback;
  }

  function updateStatus(state, text, cls = '') {
    if (!state.ui?.status) return;
    state.ui.status.textContent = text;
    if (!state.ui.statusDot) return;
    state.ui.statusDot.classList.remove('ok', 'warn', 'err');
    if (cls) state.ui.statusDot.classList.add(cls);
  }

  function appendLog(state, message, cls = '') {
    if (!state.ui?.log) return;
    const maxLines = Math.max(10, coerceNumber(getConfig(state.nodeId).maxLogLines, 500));
    const line = document.createElement('div');
    line.textContent = message;
    if (cls) line.classList.add(cls);
    state.ui.log.appendChild(line);
    state.logLines.push(line);
    while (state.logLines.length > maxLines) {
      const old = state.logLines.shift();
      old?.remove();
    }
    state.ui.log.scrollTop = state.ui.log.scrollHeight;
  }

  function decodeBytes(state, buffer) {
    const cfg = getConfig(state.nodeId);
    const mode = (cfg.readMode || 'text').toLowerCase();
    if (mode === 'hex') {
      return Array.from(buffer).map((b) => b.toString(16).padStart(2, '0')).join(' ');
    }
    try {
      if (!state.decoder || state.decoderEncoding !== cfg.encoding) {
        state.decoder = new TextDecoder(cfg.encoding || 'utf-8');
        state.decoderEncoding = cfg.encoding || 'utf-8';
      }
      return state.decoder.decode(buffer);
    } catch (err) {
      return `[decode error: ${err.message}]`;
    }
  }

  function encodeText(state, text) {
    const cfg = getConfig(state.nodeId);
    const mode = (cfg.writeMode || 'text').toLowerCase();
    if (mode === 'hex') {
      const cleaned = text.replace(/[^0-9a-f]/gi, '');
      if (cleaned.length % 2 !== 0) {
        throw new Error('Hex payload must have an even number of characters');
      }
      const bytes = new Uint8Array(cleaned.length / 2);
      for (let i = 0; i < cleaned.length; i += 2) {
        bytes[i / 2] = parseInt(cleaned.substr(i, 2), 16);
      }
      return bytes;
    }
    try {
      if (!state.encoder || state.encoderEncoding !== cfg.encoding) {
        state.encoder = new TextEncoder(cfg.encoding || 'utf-8');
        state.encoderEncoding = cfg.encoding || 'utf-8';
      }
    } catch (_) {
      state.encoder = new TextEncoder();
      state.encoderEncoding = 'utf-8';
    }
    let payload = text;
    if (booleanFromConfig(cfg, 'appendNewline', false)) {
      const newline = typeof cfg.newline === 'string' ? cfg.newline : '\n';
      if (!payload.endsWith(newline)) payload += newline;
    }
    return state.encoder.encode(payload);
  }

  function buildUi(state) {
    const node = getNode(state.nodeId);
    if (!node?.el) return null;
    const wrap = node.el.querySelector('[data-webserial-root]');
    if (!wrap) return null;
    return {
      root: wrap,
      status: wrap.querySelector('[data-webserial-status]'),
      statusDot: wrap.querySelector('[data-webserial-status-dot]'),
      choose: wrap.querySelector('[data-webserial-choose]'),
      connect: wrap.querySelector('[data-webserial-connect]'),
      disconnect: wrap.querySelector('[data-webserial-disconnect]'),
      baudSelect: wrap.querySelector('[data-webserial-baud]'),
      baudCustom: wrap.querySelector('[data-webserial-baud-custom]'),
      applyBaud: wrap.querySelector('[data-webserial-baud-apply]'),
      sendInput: wrap.querySelector('[data-webserial-send-input]'),
      sendButton: wrap.querySelector('[data-webserial-send-button]'),
      log: wrap.querySelector('[data-webserial-log]'),
      clearLog: wrap.querySelector('[data-webserial-clear]')
    };
  }

  function syncBaudUi(state) {
    if (!state.ui?.baudSelect) return;
    const cfg = getConfig(state.nodeId);
    const target = coerceNumber(cfg.baudRate, 115200);
    const select = state.ui.baudSelect;
    let hasOption = false;
    Array.from(select.options).forEach((opt) => {
      if (Number(opt.value) === target) {
        hasOption = true;
        opt.selected = true;
      } else {
        opt.selected = false;
      }
    });
    if (!hasOption) {
      let customOpt = select.querySelector('option[value="custom"]');
      if (!customOpt) {
        customOpt = document.createElement('option');
        customOpt.value = 'custom';
        customOpt.textContent = 'Custom…';
        select.appendChild(customOpt);
      }
      select.value = 'custom';
      if (state.ui.baudCustom) state.ui.baudCustom.value = String(target);
    } else if (state.ui.baudCustom) {
      state.ui.baudCustom.value = '';
    }
  }

  function ensureBaudOptions(select) {
    if (!select) return;
    const existing = new Set(Array.from(select.options || []).map((opt) => String(opt.value)));
    const customOption = Array.from(select.options || []).find((opt) => opt.value === 'custom') || null;
    DEFAULT_BAUDS.forEach((rate) => {
      const value = String(rate);
      if (existing.has(value)) return;
      const opt = document.createElement('option');
      opt.value = value;
      opt.textContent = value;
      if (customOption) {
        select.insertBefore(opt, customOption);
      } else {
        select.appendChild(opt);
      }
    });
  }

  function attachUi(state) {
    const ui = buildUi(state);
    if (!ui || !ui.root) return false;
    if (state.uiBound && state.uiRoot === ui.root) return true;
    state.ui = ui;
    state.uiRoot = ui.root;
    state.logLines = [];
    if (ui.log) ui.log.textContent = '';
    state.uiBound = false;
    if (!('serial' in navigator)) {
      updateStatus(state, 'WebSerial unsupported in this browser', 'err');
      ui.choose && (ui.choose.disabled = true);
      ui.connect && (ui.connect.disabled = true);
      ui.disconnect && (ui.disconnect.disabled = true);
      ui.sendButton && (ui.sendButton.disabled = true);
      ui.sendInput && (ui.sendInput.disabled = true);
      setBadge?.('WebSerial requires a secure Chromium-based browser', false);
      state.uiBound = true;
      return true;
    }
    ensureBaudOptions(ui.baudSelect);
    syncBaudUi(state);
    updateButtons(state);
    updateStatus(state, 'Disconnected', 'warn');

    ui.choose?.addEventListener('click', async () => {
      if (!('serial' in navigator)) {
        setBadge?.('WebSerial unsupported', false);
        return;
      }
      try {
        const port = await navigator.serial.requestPort({});
        state.port = port;
        state.allowed = true;
        rememberPort(state, port);
        updateStatus(state, 'Port selected', 'warn');
        appendLog(state, 'Port selected');
        if (ui.connect) ui.connect.disabled = false;
        if (ui.sendButton) ui.sendButton.disabled = true;
        if (ui.disconnect) ui.disconnect.disabled = true;
        updateButtons(state);
      } catch (err) {
        if (err?.name !== 'NotFoundError') appendLog(state, `requestPort: ${err.message}`, 'err');
      }
    });

    ui.connect?.addEventListener('click', async () => {
      try {
        await connect(state.nodeId);
      } catch (err) {
        appendLog(state, `connect failed: ${err.message}`, 'err');
      }
    });

    ui.disconnect?.addEventListener('click', async () => {
      state.userRequestedDisconnect = true;
      cancelReconnect(state);
      await disconnect(state.nodeId);
    });

    ui.applyBaud?.addEventListener('click', () => {
      const selectValue = ui.baudSelect?.value;
      let nextBaud = coerceNumber(selectValue, NaN);
      if (selectValue === 'custom') {
        nextBaud = coerceNumber(ui.baudCustom?.value, NaN);
      }
      if (!Number.isFinite(nextBaud) || nextBaud <= 0) {
        setBadge('Enter a valid baud rate', false);
        return;
      }
      saveConfig(state.nodeId, { baudRate: nextBaud });
      setBadge(`Baud set to ${nextBaud}`);
      syncBaudUi(state);
    });

    ui.clearLog?.addEventListener('click', () => {
      state.logLines.forEach((line) => line?.remove());
      state.logLines.length = 0;
    });

    const interactiveButtons = [
      ui.choose,
      ui.connect,
      ui.disconnect,
      ui.clearLog,
      ui.sendButton
    ].filter(Boolean);
    interactiveButtons.forEach((btn) => {
      btn.addEventListener('pointerdown', (ev) => ev.stopPropagation(), { passive: false });
    });

    const send = async () => {
      const value = ui.sendInput?.value ?? '';
      if (!value) return;
      try {
        await writePayload(state.nodeId, value);
        ui.sendInput.value = '';
      } catch (err) {
        appendLog(state, `send error: ${err.message}`, 'err');
      }
    };

    ui.sendButton?.addEventListener('click', send);
    ui.sendInput?.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter' && !ev.shiftKey) {
        ev.preventDefault();
        send();
      }
    });
    state.uiBound = true;
    return true;
  }

  function rememberPort(state, port) {
    const info = port?.getInfo?.();
    if (!info) return;
    state.portInfo = info;
    const cfg = getConfig(state.nodeId);
    if (booleanFromConfig(cfg, 'rememberPort', true)) {
      saveConfig(state.nodeId, { lastPortInfo: info });
    }
  }

  function updateButtons(state) {
    if (!state.ui) return;
    const connected = Boolean(state.writer);
    const hasPort = Boolean(state.port);
    const canPrompt = Boolean(navigator.serial?.requestPort);
    if (state.ui.connect) state.ui.connect.disabled = connected || (!hasPort && !canPrompt);
    if (state.ui.disconnect) state.ui.disconnect.disabled = !connected;
    if (state.ui.sendButton) state.ui.sendButton.disabled = !connected;
  }

  async function releaseIo(state, { closePort = false } = {}) {
    if (state.reader) {
      try {
        await state.reader.cancel?.();
      } catch (_) { }
      try {
        state.reader.releaseLock?.();
      } catch (_) { }
    }
    if (state.writer) {
      try {
        await state.writer.close?.();
      } catch (_) { }
      try {
        state.writer.releaseLock?.();
      } catch (_) { }
    }
    if (closePort && state.port?.close) {
      try {
        await state.port.close();
      } catch (_) { }
    }
    state.reader = null;
    state.writer = null;
    state.textCarry = '';
    updateButtons(state);
  }

  async function connect(nodeId) {
    const entry = ensureSession(nodeId);
    const state = entry.state;
    const cfg = getConfig(nodeId);
    if (!navigator.serial) throw new Error('WebSerial not supported');
    if (state.writer) {
      appendLog(state, 'Already connected', 'warn');
      return;
    }
    if (!state.port) {
      if (cfg.lastPortInfo && booleanFromConfig(cfg, 'rememberPort', true)) {
        const ports = await navigator.serial.getPorts();
        const match = ports.find((port) => {
          const info = port.getInfo?.() || {};
          return (
            (cfg.lastPortInfo.usbVendorId == null || cfg.lastPortInfo.usbVendorId === info.usbVendorId) &&
            (cfg.lastPortInfo.usbProductId == null || cfg.lastPortInfo.usbProductId === info.usbProductId)
          );
        });
        if (match) state.port = match;
      }
      if (!state.port && navigator.serial?.requestPort) {
        try {
          const port = await navigator.serial.requestPort({});
          state.port = port;
          state.allowed = true;
          rememberPort(state, port);
          updateStatus(state, 'Port selected', 'warn');
          appendLog(state, 'Port selected');
          updateButtons(state);
        } catch (err) {
          if (err?.name === 'NotFoundError') throw new Error('No port selected');
          throw err;
        }
      }
      if (!state.port) throw new Error('No port selected');
    }
    state.userRequestedDisconnect = false;
    cancelReconnect(state);
    try {
      const openOptions = {
        baudRate: coerceNumber(cfg.baudRate, 115200),
        dataBits: 8,
        stopBits: 1,
        parity: 'none',
        bufferSize: Math.max(256, coerceNumber(cfg.bufferSize ?? 0, 0)) || 256,
        flowControl: 'none'
      };
      await state.port.open(openOptions);
    } catch (err) {
      if (state.port.readable || state.port.writable) {
        appendLog(state, 'Port already open', 'warn');
      } else {
        throw err;
      }
    }
    try {
      await state.port.setSignals?.({ dataTerminalReady: true, requestToSend: false });
    } catch (_) {
      /* setSignals not supported */
    }
    state.writer = state.port.writable?.getWriter?.() || null;
    state.reader = state.port.readable?.getReader?.() || null;
    state.allowed = true;
    state.reconnectBackoff = 1000;
    rememberPort(state, state.port);
    updateStatus(state, `Connected @ ${coerceNumber(cfg.baudRate, 115200)} baud`, 'ok');
    updateButtons(state);
    appendLog(state, 'Connected', 'ok');
    startReadLoop(state);
  }

  async function disconnect(nodeId) {
    const entry = ensureSession(nodeId);
    const state = entry.state;
    cancelReconnect(state);
    stopReadLoop(state);
    await releaseIo(state, { closePort: true });
    updateButtons(state);
    updateStatus(state, 'Disconnected', 'warn');
    appendLog(state, 'Disconnected', 'warn');
  }

  function stopReadLoop(state) {
    try {
      state.readLoopAbort?.abort?.();
    } catch (_) { }
    state.readLoopAbort = null;
  }

  function startReadLoop(state) {
    stopReadLoop(state);
    if (!state.reader || !state.port?.readable) return;
    state.readLoopAbort = new AbortController();
    const signal = state.readLoopAbort.signal;
    const cfg = getConfig(state.nodeId);
    const buffer = [];
    const readMode = (cfg.readMode || 'text').toLowerCase();
    const textEncoder = readMode === 'text' ? new TextEncoder() : null;
    state.textCarry = '';

    (async () => {
      let failureDetail = '';
      let shouldReconnect = false;
      try {
        while (true) {
          if (signal.aborted) break;
          const { value, done } = await state.reader.read();
          if (done) {
            failureDetail = failureDetail || 'port closed';
            shouldReconnect = true;
            break;
          }
          if (!value || !value.length) continue;
          buffer.push(value);
          const merged = mergeBuffers(buffer);
          buffer.length = 0;

          if (readMode === 'hex') {
            const text = decodeBytes(state, merged);
            if (text) {
              appendLog(state, text);
              Router.sendFrom(state.nodeId, 'data', buildOutputPayload(state, merged, text));
            }
            continue;
          }

          const chunk = decodeBytes(state, merged);
          if (!chunk) continue;
          state.textCarry += chunk;
          let match;
          const lineBreakRegex = /[\r\n]/g;
          while ((match = lineBreakRegex.exec(state.textCarry))) {
            const line = state.textCarry.slice(0, match.index).trim();
            state.textCarry = state.textCarry.slice(match.index + 1);
            lineBreakRegex.lastIndex = 0;
            if (!line) continue;
            appendLog(state, line);
            const rawBytes = textEncoder ? textEncoder.encode(line) : new TextEncoder().encode(line);
            Router.sendFrom(state.nodeId, 'data', buildOutputPayload(state, rawBytes, line));
          }
          if (state.textCarry.length > 2048) {
            const excess = state.textCarry.slice(-512);
            state.textCarry = excess;
          }
        }
      } catch (err) {
        if (!signal.aborted) {
          failureDetail = err?.message || String(err || 'read error');
          appendLog(state, `read error: ${failureDetail}`, 'err');
          shouldReconnect = /device has been lost/i.test(failureDetail || '') || /NetworkError/i.test(failureDetail || '');
        }
      } finally {
        const closePort = !signal.aborted || shouldReconnect;
        await releaseIo(state, { closePort });
        if (!signal.aborted) {
          updateStatus(state, 'Disconnected', 'warn');
          if (!failureDetail) appendLog(state, 'Connection closed', 'warn');
          if (!state.userRequestedDisconnect && (shouldReconnect || failureDetail)) {
            scheduleReconnect(state, failureDetail || 'port closed');
          }
        }
      }
    })();
  }

  function buildOutputPayload(state, bytes, text) {
    const payload = {
      nodeId: state.nodeId,
      timestamp: Date.now(),
      bytes: Array.from(bytes),
      text
    };
    return payload;
  }

  function mergeBuffers(chunks) {
    if (chunks.length === 1) return chunks[0];
    const total = chunks.reduce((sum, c) => sum + c.length, 0);
    const out = new Uint8Array(total);
    let offset = 0;
    chunks.forEach((chunk) => {
      out.set(chunk, offset);
      offset += chunk.length;
    });
    return out;
  }

  async function writePayload(nodeId, payload) {
    const entry = ensureSession(nodeId);
    const state = entry.state;
    if (!state.writer) throw new Error('Not connected');
    const cfg = getConfig(nodeId);
    const bytes = payload instanceof Uint8Array ? payload : encodeText(state, String(payload));
    await state.writer.write(bytes);
    if (booleanFromConfig(cfg, 'echoSent', false)) {
      appendLog(state, `(sent) ${decodeBytes(state, bytes)}`, 'ok');
    }
  }

  function scheduleReconnect(state, reason = '') {
    const cfg = getConfig(state.nodeId);
    if (!booleanFromConfig(cfg, 'autoReconnect', true)) return;
    if (state.userRequestedDisconnect) return;
    if (state.reconnectTimer) return;
    const delay = state.reconnectBackoff;
    appendLog(state, `Attempting reconnect in ${delay}ms ${reason ? `(${reason})` : ''}`, 'warn');
    state.reconnectTimer = setTimeout(async () => {
      state.reconnectTimer = null;
      try {
        await connect(state.nodeId);
        state.reconnectBackoff = 1000;
      } catch (err) {
        state.reconnectBackoff = Math.min(15000, state.reconnectBackoff * 1.5);
        scheduleReconnect(state, 'retry failed');
      }
    }, delay);
  }

  function cancelReconnect(state) {
    if (state.reconnectTimer) {
      clearTimeout(state.reconnectTimer);
      state.reconnectTimer = null;
    }
  }

  function bindSerialEvents() {
    if (serialEventsBound) return;
    if (!navigator.serial?.addEventListener) return;
    navigator.serial.addEventListener('disconnect', (event) => {
      sessions.forEach((entry) => {
        const state = entry.state;
        if (state.port && event.target && state.port !== event.target) return;
        appendLog(state, 'Port disconnected', 'warn');
        scheduleReconnect(state, 'port disconnect');
      });
    });
    navigator.serial.addEventListener('connect', (event) => {
      sessions.forEach((entry) => {
        const state = entry.state;
        if (state.port && event.target && state.port !== event.target) return;
        if (!state.port && event.target) state.port = event.target;
        appendLog(state, 'Port connected', 'ok');
        scheduleReconnect(state, 'port connect');
      });
    });
    serialEventsBound = true;
  }

  function init(nodeId) {
    const entry = ensureSession(nodeId);
    entry.state.nodeId = nodeId;
    if (!attachUi(entry.state)) {
      let attempts = 0;
      const retry = () => {
        if (attachUi(entry.state)) return;
        if (attempts++ > 20) return;
        requestAnimationFrame(retry);
      };
      requestAnimationFrame(retry);
    }
    bindSerialEvents();
    const cfg = getConfig(nodeId);
    if (booleanFromConfig(cfg, 'autoConnect', false)) {
      connect(nodeId).catch((err) => appendLog(entry.state, `auto connect failed: ${err.message}`, 'err'));
    }
  }

  function dispose(nodeId) {
    const entry = sessions.get(nodeId);
    if (!entry) return;
    entry.state.userRequestedDisconnect = true;
    cancelReconnect(entry.state);
    stopReadLoop(entry.state);
    disconnect(nodeId).catch(() => { });
    sessions.delete(nodeId);
  }

  function onSend(nodeId, payload) {
    return writePayload(nodeId, payload).catch((err) => {
      const entry = ensureSession(nodeId);
      appendLog(entry.state, `send error: ${err.message}`, 'err');
    });
  }

  function renderSettings(nodeId, container) {
    const entry = ensureSession(nodeId);
    const cfg = getConfig(nodeId);
    const summary = document.createElement('div');
    summary.textContent = `Last port: ${cfg.lastPortInfo ? JSON.stringify(cfg.lastPortInfo) : '—'}`;
    summary.className = 'tiny muted';
    summary.style.gridColumn = '1 / -1';
    summary.style.marginBottom = '6px';
    container.prepend(summary);
  }

  return {
    init,
    dispose,
    onSend,
    renderSettings
  };
}

export { createWebSerial };
