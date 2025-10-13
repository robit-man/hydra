const STYLE_ID = 'meshtastic-node-style';
const STYLE_TEXT = `
.meshtastic-node {
  font-size: 13px;
  color: #e7e8ee;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.meshtastic-node button,
.meshtastic-node select,
.meshtastic-node input[type="text"],
.meshtastic-node input[type="number"],
.meshtastic-node input[type="range"] {
  background: #1f2550;
  color: inherit;
  border: 1px solid #2c3361;
  border-radius: 6px;
  font: inherit;
  padding: 6px 8px;
}
.meshtastic-node button:hover:not(:disabled) {
  background: #2a3163;
}
.meshtastic-node button:disabled {
  opacity: 0.45;
  cursor: not-allowed;
}
.meshtastic-node .mesh-header {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.meshtastic-node .mesh-main-controls {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}
.meshtastic-node .mesh-status-line {
  display: flex;
  align-items: center;
  gap: 6px;
  color: #9aa3b2;
  font-size: 12px;
}
.meshtastic-node .mesh-tabs {
  display: flex;
  gap: 6px;
}
.meshtastic-node .mesh-tab {
  flex: 1;
  text-align: center;
  padding: 6px;
  border-radius: 6px;
  background: #1a2040;
  border: 1px solid #262b52;
  cursor: pointer;
  user-select: none;
}
.meshtastic-node .mesh-tab.active {
  background: #242d5b;
  border-color: #3a4384;
}
.meshtastic-node .mesh-views {
  position: relative;
  flex: 1;
  min-height: 160px;
}
.meshtastic-node .mesh-view {
  display: none;
  flex-direction: column;
  gap: 8px;
  height: 100%;
  min-height:250px;
}
.meshtastic-node .mesh-view.active {
  display: flex;
}
.meshtastic-node .mesh-chat-header {
  font-size: 12px;
  color: #9aa3b2;
}
.meshtastic-node .mesh-peer-bar {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  max-height: 200px;
  overflow: auto;
}
.meshtastic-node .mesh-peer-chip {
  background: #1b244d;
  border: 1px solid #29315f;
  border-radius: 999px;
  padding: 3px 8px;
  cursor: pointer;
  font-size: 12px;
  display: inline-flex;
  align-items: center;
  gap: 4px;
}
.meshtastic-node .mesh-peer-chip.active {
  background: #273375;
  border-color: #4050a7;
}
.meshtastic-node .mesh-peer-chip .bubble {
  background: #ef4444;
  color: #fff;
  border-radius: 8px;
  padding: 0 5px;
  font-size: 11px;
}
.meshtastic-node .mesh-messages {
  flex: 1;
  min-height: 80px;
  max-height: 220px;
  overflow: auto;
  background: #151a2d;
  border: 1px solid #21264a;
  border-radius: 6px;
  padding: 8px;
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.meshtastic-node .mesh-message {
  max-width: 90%;
  padding: 6px 8px;
  border-radius: 8px;
  font-size: 12px;
  white-space: pre-wrap;
  word-break: break-word;
}
.meshtastic-node .mesh-message.me {
  margin-left: auto;
  background: #293066;
}
.meshtastic-node .mesh-message.them {
  margin-right: auto;
  background: #1f244b;
}
.meshtastic-node .mesh-sharebar {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  align-items: center;
  background: #151a2d;
  border: 1px solid #21264a;
  border-radius: 6px;
  padding: 6px;
}
.meshtastic-node .mesh-sharebar-group {
  display: flex;
  gap: 6px;
  align-items: center;
}
.meshtastic-node .mesh-composer {
  display: flex;
  gap: 6px;
}
.meshtastic-node .mesh-composer input[type="text"] {
  flex: 1;
  padding: 6px 8px;
}
.meshtastic-node .mesh-map {
  flex: 1;
  min-height: 180px;
  border-radius: 6px;
  overflow: hidden;
}
.meshtastic-node .mesh-log {
  flex: 1;
  background: #0f1220;
  border: 1px solid #21264a;
  border-radius: 6px;
  padding: 6px;
  font-family: ui-monospace, Consolas, monospace;
  font-size: 12px;
  overflow: auto;
  min-height: 120px;
  max-height:350px;
}
.meshtastic-node .mesh-log .ok {
  color: #5ee3a6;
}
.meshtastic-node .mesh-log .warn {
  color: #ffd166;
}
.meshtastic-node .mesh-log .err {
  color: #ff6666;
}
.meshtastic-node .mesh-self {
  font-size: 12px;
  color: #9aa3b2;
}
.meshtastic-node .mesh-peer-cards {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.meshtastic-node .mesh-peer-card {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding: 6px;
  border-radius: 6px;
  border: 1px solid #21264a;
  background: #151a2d;
}
.meshtastic-node .mesh-peer-card .title {
  font-size: 13px;
  font-weight: 600;
}
.meshtastic-node .mesh-peer-card .metrics {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  font-size: 12px;
  color: #9aa3b2;
}
.meshtastic-node .mesh-peer-card .actions {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
}
.meshtastic-node .mesh-peer-card button {
  font-size: 12px;
  padding: 4px 6px;
}
.meshtastic-node .mesh-status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #ef4444;
}
.meshtastic-node .mesh-status-dot.ok {
  background: #5ee3a6;
}
.meshtastic-node .mesh-status-dot.warn {
  background: #ffd166;
  border-radius: 50%;
}
.meshtastic-settings {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.meshtastic-settings .mesh-settings-peers {
  display: flex;
  flex-direction: column;
  gap: 4px;
  max-height: 240px;
  overflow: auto;
  padding: 6px;
  background: rgba(15, 18, 32, 0.8);
  border: 1px solid #21264a;
  border-radius: 6px;
}
.meshtastic-settings .mesh-peer-toggle {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 4px 6px;
  border-radius: 6px;
  border: 1px solid #262b4d;
  background: #151a2d;
}
.meshtastic-settings .mesh-peer-toggle .info {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.meshtastic-settings .mesh-peer-toggle .info .name {
  font-weight: 600;
  font-size: 13px;
}
.meshtastic-settings .mesh-peer-toggle .info .meta {
  font-size: 11px;
  color: #9aa3b2;
}
.meshtastic-settings .mesh-peer-toggle .controls {
  display: flex;
  gap: 6px;
  align-items: center;
}
.meshtastic-settings .mesh-peer-toggle .controls button {
  font-size: 12px;
  padding: 3px 6px;
}
.meshtastic-settings .mesh-peer-toggle .badge {
  border-radius: 6px;
  padding: 2px 6px;
  font-size: 11px;
  background: #1f2a4e;
  color: #9aa3b2;
}
.meshtastic-node .mesh-spacer {
  height: 1px;
  background: rgba(255, 255, 255, 0.05);
}
`;

const PROTO_SRC = `
  syntax = "proto3";
  package meshtastic;

  enum LogRecord_Level {
    INFO = 0;
    NOTICE = 1;
    WARNING = 2;
    ERROR = 3;
    CRITICAL = 4;
    DEBUG = 5;
  }

  message LogRecord {
    fixed32 time = 1;
    LogRecord_Level level = 2;
    string filename = 3;
    uint32 line = 4;
    string function_name = 5;
    string message = 6;
    uint32 module = 7;
  }

  message Data {
    uint32 portnum = 1;
    uint32 want_response = 2;
    bytes payload = 3;
    fixed32 dest = 4;
    fixed32 source = 5;
    fixed32 request_id = 6;
    fixed32 reply_id = 7;
    fixed32 emoji = 8;
    optional uint32 bitfield = 9;
  }

  message MeshPacket {
    fixed32 from = 1;
    fixed32 to   = 2;
    uint32  channel = 3;
    oneof payload_variant {
      Data  decoded = 4;
      bytes encrypted = 5;
    }
    fixed32 id = 6;
    fixed32 rx_time = 7;
  }

  message Position {
    optional sfixed32 latitude_i  = 1;
    optional sfixed32 longitude_i = 2;
    optional int32    altitude    = 3;
    optional fixed32  time        = 4;
  }

  message DeviceMetrics {
    uint32 battery_level = 1;
    float  voltage = 2;
    float  channel_utilization = 3;
    float  air_util_tx = 4;
  }

  message User {
    string id = 1;
    string long_name = 2;
    string short_name = 3;
    bytes  macaddr = 4 [deprecated = true];
    int32  hw_model = 5;
    bool   is_licensed = 6;
    bytes  public_key = 8;
    optional bool is_unmessagable = 9;
  }

  message NodeInfo {
    uint32 num = 1;
    User   user = 2;
    Position position = 3;
    float  snr = 4;
    fixed32 last_heard = 5;
    DeviceMetrics device_metrics = 6;
    uint32 channel = 7;
    bool   via_mqtt = 8;
    optional uint32 hops_away = 9;
  }

  message MyNodeInfo {
    uint32 my_node_num = 1;
    uint32 reboot_count = 8;
    uint32 min_app_version = 11;
    bytes  device_id = 12;
    string pio_env = 13;
    uint32 nodedb_count = 15;
  }

  message Config {}

  message Heartbeat { uint32 nonce = 1; }

  message FromRadio {
    uint32 id = 1;
    oneof payload_variant {
      MeshPacket packet = 2;
      MyNodeInfo my_info = 3;
      NodeInfo   node_info = 4;
      Config     config = 5;
      uint32     config_complete_id = 7;
      bool       rebooted = 8;
    }
  }

  message ToRadio {
    oneof payload_variant {
      MeshPacket packet = 1;
      uint32     want_config_id = 3;
      bool       disconnect = 4;
      Heartbeat  heartbeat = 7;
    }
  }

  message Text { string text = 1; }
`;

const DEP_SCRIPTS = [
  { id: 'meshtastic-protobuf', src: 'https://cdn.jsdelivr.net/npm/protobufjs@7.2.6/dist/protobuf.min.js' },
  { id: 'meshtastic-pako', src: 'https://cdn.jsdelivr.net/npm/pako@2.1.0/dist/pako.min.js' },
  { id: 'meshtastic-leaflet', src: 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js' }
];

const DEP_STYLES = [
  { id: 'meshtastic-leaflet-css', href: 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css' }
];

const BROADCAST_NUM = 0xffffffff >>> 0;
const LOG_TAIL_KEEP = 256;
const ASCII_BUFFER_LIMIT = 20000;

const dependencyPromises = new Map();

function injectStylesOnce() {
  if (document.getElementById(STYLE_ID)) return;
  const style = document.createElement('style');
  style.id = STYLE_ID;
  style.textContent = STYLE_TEXT;
  document.head.appendChild(style);
}

function ensureExternalStyle({ id, href }) {
  if (document.getElementById(id)) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const link = document.createElement('link');
    link.id = id;
    link.rel = 'stylesheet';
    link.href = href;
    link.onload = () => resolve();
    link.onerror = (err) => reject(err);
    document.head.appendChild(link);
  });
}

function ensureExternalScript({ id, src }) {
  if (document.getElementById(id)) return Promise.resolve();
  if (dependencyPromises.has(id)) return dependencyPromises.get(id);
  const promise = new Promise((resolve, reject) => {
    const script = document.createElement('script');
    script.id = id;
    script.src = src;
    script.defer = true;
    script.crossOrigin = 'anonymous';
    script.onload = () => resolve();
    script.onerror = (err) => reject(err);
    document.head.appendChild(script);
  });
  dependencyPromises.set(id, promise);
  return promise;
}

function ensureDependencies() {
  return Promise.all([
    ...DEP_STYLES.map(ensureExternalStyle),
    ...DEP_SCRIPTS.map(ensureExternalScript)
  ]);
}

function createMeshtastic({ getNode, NodeStore, Router, log, setBadge }) {
  injectStylesOnce();

  const sessions = new Map();
  let Types = null;
  let protobufRoot = null;
  let refreshPortsHandler = null;
  let serialEventsBound = false;

  function setRefreshPortsHandler(fn) {
    refreshPortsHandler = typeof fn === 'function' ? fn : null;
  }

  async function ensureProtobuf() {
    if (Types) return;
    await ensureDependencies();
    if (!protobuf?.parse) throw new Error('protobuf runtime missing');
    const parsed = protobuf.parse(PROTO_SRC, { keepCase: true });
    protobufRoot = parsed.root;
    Types = {
      ToRadio: protobufRoot.lookupType('meshtastic.ToRadio'),
      FromRadio: protobufRoot.lookupType('meshtastic.FromRadio'),
      MeshPacket: protobufRoot.lookupType('meshtastic.MeshPacket'),
      Data: protobufRoot.lookupType('meshtastic.Data'),
      NodeInfo: protobufRoot.lookupType('meshtastic.NodeInfo'),
      MyNodeInfo: protobufRoot.lookupType('meshtastic.MyNodeInfo'),
      Position: protobufRoot.lookupType('meshtastic.Position'),
      DeviceMetrics: protobufRoot.lookupType('meshtastic.DeviceMetrics'),
      Text: protobufRoot.lookupType('meshtastic.Text')
    };
  }

  function stateDefaults(nodeId, session) {
    return {
      nodeId,
      session,
      port: null,
      reader: null,
      writer: null,
      readLoopAbort: null,
      reconnectTimer: null,
      reconnectBackoff: 1000,
      userRequestedDisconnect: false,
      allowed: false,
      ui: null,
      asciiDecoder: new TextDecoder('utf-8'),
      asciiLineBuffer: '',
      asciiParseBuffer: '',
      ansiCarry: '',
      logPending: '',
      lastLogDiv: null,
      nodes: new Map(),
      threads: new Map(),
      unread: new Map(),
      myNodeNum: null,
      myInfo: null,
      selfMetrics: null,
      selectedThread: 'public',
      activeView: 'chat',
      shareInterval: null,
      map: null,
      mapMarkers: new Map(),
      mapHasFit: false,
      binaryAcc: new Uint8Array(0),
      lastConfigNonce: 0,
      expectedNodes: 0,
      syncInProgress: false,
      seenMsgs: new Map(),
      dedupeList: [],
      peerTelemetry: new Map(),
      peerJsonMode: new Map(),
      lastPortInfo: null,
      heartbeatTimer: null,
      heartbeatNonce: 1,
      flushTimer: null,
      pendingConnect: null,
      connectInFlight: false,
      _mapRefreshTimer: null
    };
  }

  function ensureSession(nodeId) {
    let entry = sessions.get(nodeId);
    if (!entry) {
      entry = { nodeId, state: null, ready: false };
      entry.state = stateDefaults(nodeId, entry);
      sessions.set(nodeId, entry);
    }
    entry.state.nodeId = nodeId;
    entry.state.session = entry;
    return entry;
  }

  function getConfig(nodeId) {
    const rec = NodeStore.ensure(nodeId, 'Meshtastic');
    const cfg = rec?.config || {};
    return cfg;
  }

  function saveConfig(nodeId, patch) {
    const rec = NodeStore.ensure(nodeId, 'Meshtastic');
    const cfg = rec?.config || {};
    const next = { ...cfg, ...patch };
    NodeStore.saveCfg(nodeId, 'Meshtastic', next);
    return next;
  }

  function toBoolean(value, fallback = true) {
    if (typeof value === 'boolean') return value;
    if (typeof value === 'number') return value !== 0;
    if (typeof value === 'string') {
      const trimmed = value.trim().toLowerCase();
      if (!trimmed) return fallback;
      if (['true', '1', 'yes', 'on', 'y'].includes(trimmed)) return true;
      if (['false', '0', 'no', 'off', 'n'].includes(trimmed)) return false;
    }
    return fallback;
  }

  function escapeHtml(s) {
    if (s == null) return '';
    return String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c] || c));
  }

  function formatHex(num) {
    if (num == null) return '0x00000000';
    return '0x' + (num >>> 0).toString(16).padStart(8, '0');
  }

  function threadKey(num) {
    if (num == null) return 'public';
    if (num === BROADCAST_NUM) return 'public';
    return String(num >>> 0);
  }

  function makeDedupKey(srcDec, idHex, text) {
    return `${srcDec}|${idHex}|${text}`;
  }

  function rememberMsg(state, key) {
    state.seenMsgs.set(key, Date.now());
    state.dedupeList.push(key);
    if (state.dedupeList.length > 400) {
      const drop = state.dedupeList.splice(0, 200);
      drop.forEach((k) => state.seenMsgs.delete(k));
    }
  }

  function cleanAnsiStream(state, chunkText) {
    let s = state.ansiCarry + chunkText;
    state.ansiCarry = '';

    s = s.replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, '')
      .replace(/\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)/g, '')
      .replace(/\x1b[@-~]/g, '');

    const escIdx = s.lastIndexOf('\x1b');
    if (escIdx !== -1) {
      const tail = s.slice(escIdx);
      const looksStart = /^\x1b(?:\[|\]|[@-~]|$)/.test(tail);
      const completeCSI = /^\x1b\[[0-?]*[ -/]*[@-~]/.test(tail);
      const completeOSC = /^\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)/.test(tail);
      const single = /^\x1b[@-~]/.test(tail);
      if (looksStart && !(completeCSI || completeOSC || single)) {
        state.ansiCarry = tail;
        s = s.slice(0, escIdx);
      }
    }

    s = s.replace(/\b(\d{1,3})\s+(\d{1,3})m\b/g, '$1$2m')
      .replace(/\b(\d{1,3})\s+(\d{1,3})m\b/g, '$1$2m');

    s = s.replace(/(^|[^\w])\d{1,3}m(?=\w)/g, '$1')
      .replace(/(^|[^\w])\d{1,3}m(?!\S)/g, '$1');

    return s;
  }

  function appendAsciiBytes(state, bytes) {
    if (!bytes || !bytes.length) return;
    const raw = state.asciiDecoder.decode(bytes, { stream: true });
    if (!raw) return;
    const cleaned = cleanAnsiStream(state, raw).replace(/\r/g, '');
    if (!cleaned) return;

    state.asciiLineBuffer += cleaned;
    const parts = state.asciiLineBuffer.split('\n');
    state.asciiLineBuffer = parts.pop();
    for (const line of parts) {
      if (line.trim().length) logLine(state, line);
    }

    state.asciiParseBuffer += cleaned;
    if (state.asciiParseBuffer.length > ASCII_BUFFER_LIMIT) {
      state.asciiParseBuffer = state.asciiParseBuffer.slice(-Math.floor(ASCII_BUFFER_LIMIT * 0.6));
    }
    processAsciiBuffer(state);
  }

  function flushAscii(state) {
    const tail = state.asciiDecoder.decode();
    if (tail) {
      const cleaned = cleanAnsiStream(state, tail).replace(/\r/g, '');
      state.asciiLineBuffer += cleaned;
      state.asciiParseBuffer += cleaned;
    }
    const pendingLine = state.asciiLineBuffer;
    if (pendingLine.trim().length) logLine(state, pendingLine);
    state.asciiLineBuffer = '';
    if (state.asciiParseBuffer.length) processAsciiBuffer(state);
    state.ansiCarry = '';
    flushLog(state);
  }

  function logLine(state, line, cls = '') {
    if (!state.ui) return;
    const logEl = state.ui.log;
    if (!logEl) return;
    const RECORD_ANCHOR = /(?:INFO|DEBUG|WARN|ERROR)\s*\|\s*\d{1,2}:\d{2}:\d{2}\s+\d+\b/;
    const START_TOKEN_RX = new RegExp('^\\s*(?:\\d{1,3}\\s*m\\s*)*' + RECORD_ANCHOR.source);
    const isStart = START_TOKEN_RX.test(line) || cls === 'ok' || cls === 'warn' || cls === 'err';
    if (isStart && state.logPending) {
      console.log(state.logPending);
      state.logPending = '';
    }
    if (!state.lastLogDiv || isStart) {
      state.lastLogDiv = document.createElement('div');
      if (cls) state.lastLogDiv.classList.add(cls);
      state.lastLogDiv.textContent = line;
      logEl.appendChild(state.lastLogDiv);
    } else {
      state.lastLogDiv.textContent += ' ' + line;
    }
    state.logPending = state.logPending ? `${state.logPending} ${line}` : line;
    logEl.scrollTop = logEl.scrollHeight;
  }

  function flushLog(state) {
    if (state.logPending) {
      console.log(state.logPending);
      state.logPending = '';
    }
  }

  function processAsciiBuffer(state) {
    const compact = state.asciiParseBuffer.replace(/[ \t]+/g, ' ');
    const rx = /Received\s+text\s+msg\s+from=0x([0-9a-fA-F]{1,8})(?:[\s\S]*?)\bid=0x([0-9a-fA-F]+)(?:[\s\S]*?)\bmsg=([^\n\r]*?)(?=\r?\n(?:INFO|DEBUG|WARN|ERROR|\[Router\]|Show standard frames|Module ')|$)/gmi;
    let emitted = false;
    let m;
    while ((m = rx.exec(compact)) !== null) {
      if (rx.lastIndex === compact.length) break;
      const fromHex = m[1];
      const idHex = m[2];
      const text = (m[3] || '').trim();
      const srcDec = parseInt(fromHex, 16) >>> 0;
      const dstDec = state.myNodeNum != null ? state.myNodeNum : 0;
      const key = makeDedupKey(srcDec, idHex, text);
      if (!state.seenMsgs.has(key)) {
        rememberMsg(state, key);
        onInboundText(state, { srcDec, dstDec, text, idHex, via: 'ascii' });
      }
      emitted = true;
    }
    if (emitted) {
      state.asciiParseBuffer = state.asciiParseBuffer.slice(-LOG_TAIL_KEEP);
    }
  }

  function appendThread(state, num, message) {
    const key = threadKey(num);
    if (!state.threads.has(key)) state.threads.set(key, []);
    state.threads.get(key).push(message);
    if (state.threads.get(key).length > 200) {
      state.threads.get(key).splice(0, state.threads.get(key).length - 200);
    }
  }

  function setUnread(state, num, delta) {
    const key = threadKey(num);
    state.unread.set(key, Math.max(0, (state.unread.get(key) || 0) + delta));
  }

  function clearUnread(state, num) {
    state.unread.delete(threadKey(num));
  }

  function getUnread(state, num) {
    return state.unread.get(threadKey(num)) || 0;
  }

  function renderMessages(state, num) {
    if (!state.ui) return;
    const msgsEl = state.ui.messages;
    if (!msgsEl) return;
    msgsEl.innerHTML = '';
    const key = threadKey(num);
    const data = state.threads.get(key) || [];
    data.forEach((msg) => {
      const wrap = document.createElement('div');
      wrap.className = 'mesh-message ' + (msg.me ? 'me' : 'them');
      wrap.textContent = msg.display;
      msgsEl.appendChild(wrap);
    });
    msgsEl.scrollTop = msgsEl.scrollHeight;
  }

  function renderChatHeader(state, title) {
    if (!state.ui) return;
    const header = state.ui.chatHeader;
    if (header) header.textContent = `Chat with ${title}`;
  }

  function renderPeerChips(state) {
    if (!state.ui) return;
    const list = state.ui.peerBar;
    if (!list) return;
    const peers = Array.from(state.nodes.values()).sort((a, b) => (b.last_heard || 0) - (a.last_heard || 0));
    list.innerHTML = '';

    const publicChip = document.createElement('div');
    publicChip.className = 'mesh-peer-chip' + (state.selectedThread === 'public' ? ' active' : '');
    publicChip.dataset.peerKey = 'public';
    publicChip.innerHTML = `<span>Public (ch${getConfig(state.nodeId)?.channel ?? 0})</span>`;
    const pubUnread = getUnread(state, BROADCAST_NUM);
    if (pubUnread > 0) {
      const bubble = document.createElement('span');
      bubble.className = 'bubble';
      bubble.textContent = String(pubUnread);
      publicChip.appendChild(bubble);
    }
    list.appendChild(publicChip);

    peers.forEach((info) => {
      const key = String(info.num >>> 0);
      const chip = document.createElement('div');
      chip.className = 'mesh-peer-chip' + (state.selectedThread === key ? ' active' : '');
      chip.dataset.peerKey = key;
      const name = info.user?.long_name || info.user?.short_name || info.user?.id || `#${info.num}`;
      chip.innerHTML = `<span>${escapeHtml(name)}</span>`;
      const unread = getUnread(state, info.num);
      if (unread > 0) {
        const bubble = document.createElement('span');
        bubble.className = 'bubble';
        bubble.textContent = String(unread);
        chip.appendChild(bubble);
      }
      list.appendChild(chip);
    });
  }

  function updatePeerCards(state) {
    if (!state.ui) return;
    const cards = state.ui.peerCards;
    if (!cards) return;
    const cfg = getConfig(state.nodeId);
    const enabledPeers = cfg?.peers || {};
    cards.innerHTML = '';
    const entries = Array.from(state.nodes.values());
    const seen = new Set(entries.map((info) => String(info.num >>> 0)));
    for (const [peerKey, entry] of Object.entries(enabledPeers)) {
      if (!entry?.enabled) continue;
      if (seen.has(peerKey)) continue;
      const rawNum = Number(peerKey);
      entries.push({
        num: Number.isFinite(rawNum) ? rawNum >>> 0 : 0,
        user: { long_name: entry.label || `Peer ${peerKey}` },
        position: null,
        device_metrics: {},
        last_heard: 0,
        _placeholder: true,
        _peerKey: peerKey
      });
    }
    entries
      .sort((a, b) => (b.last_heard || 0) - (a.last_heard || 0))
      .forEach((info) => {
      const peerKey = info._peerKey || String(info.num >>> 0);
      const stored = enabledPeers[peerKey];
      if (!stored?.enabled) {
        if (info._placeholder && info._peerKey) {
          // placeholder corresponds to non-numeric key
        } else {
          return;
        }
      }
      const card = document.createElement('div');
      card.className = 'mesh-peer-card';
      const rawName = info.user?.long_name || info.user?.short_name || info.user?.id || `#${info.num}`;
      const displayName = stored?.label || rawName;
      let posStr = '—';
      const ll = positionToLatLon(info.position);
      if (ll) posStr = `${ll[0].toFixed(5)}, ${ll[1].toFixed(5)}`;
      const battery = info.device_metrics?.battery_level;
      const voltage = info.device_metrics?.voltage;
      const last = info.last_heard ? new Date(info.last_heard * 1000).toLocaleTimeString() : '—';
      card.innerHTML = `
        <div class="title">${escapeHtml(displayName)} <span style="font-weight:400;">#${info.num}</span></div>
        <div class="metrics">
          <span>${formatHex(info.num)}</span>
          <span>last: ${last}</span>
          ${battery != null ? `<span>batt: ${battery}%</span>` : ''}
          ${Number.isFinite(voltage) ? `<span>${voltage.toFixed(2)}V</span>` : ''}
          <span>pos: ${posStr}</span>
        </div>
      `;
      const actions = document.createElement('div');
      actions.className = 'actions';
      const toggle = document.createElement('button');
      toggle.textContent = peerJsonModeEnabled(cfg, peerKey) ? 'JSON output' : 'Raw output';
      toggle.addEventListener('click', () => {
        const current = peerJsonModeEnabled(getConfig(state.nodeId), peerKey);
        updatePeerJsonMode(state.nodeId, peerKey, !current);
        toggle.textContent = !current ? 'JSON output' : 'Raw output';
      });
      actions.appendChild(toggle);
      card.appendChild(actions);
      cards.appendChild(card);
    });
  }

  function peerJsonModeEnabled(cfg, peerKey) {
    if (!cfg) return true;
    const peer = cfg.peers?.[peerKey];
    if (peer && peer.json !== undefined) return toBoolean(peer.json, true);
    return toBoolean(cfg.defaultJson, true);
  }

  function updatePeerJsonMode(nodeId, peerKey, value) {
    const cfg = getConfig(nodeId);
    const peers = { ...(cfg.peers || {}) };
    peers[peerKey] = { ...(peers[peerKey] || {}), json: !!value, enabled: peers[peerKey]?.enabled ?? false };
    saveConfig(nodeId, { peers });
  }

  function updateSelectedThread(state, key) {
    state.selectedThread = key;
    renderPeerChips(state);
    if (state.ui?.sendButton) state.ui.sendButton.disabled = !state.writer;
    if (key === 'public') {
      clearUnread(state, BROADCAST_NUM);
      renderChatHeader(state, `Public (ch${getConfig(state.nodeId)?.channel ?? 0})`);
      renderMessages(state, BROADCAST_NUM);
      return;
    }
    const num = Number(key);
    clearUnread(state, num);
    const info = state.nodes.get(num);
    const title = info
      ? `${info.user?.long_name || info.user?.short_name || info.user?.id || `#${num}`} (#${num})`
      : `#${key}`;
    renderChatHeader(state, title);
    renderMessages(state, num);
  }

  function positionToLatLon(pos) {
    if (!pos) return null;
    if (!Number.isFinite(pos.latitude_i) || !Number.isFinite(pos.longitude_i)) return null;
    const lat = pos.latitude_i / 1e7;
    const lon = pos.longitude_i / 1e7;
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
    return [lat, lon];
  }

  function ensureMap(state) {
    if (!state.ui?.map) return;
    if (state.map) return;
    const L = window.L;
    if (!L) return;
    state.map = L.map(state.ui.map, {
      zoomControl: true,
      attributionControl: false,
      preferCanvas: true
    }).setView([0, 0], 2);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 18,
      attribution: '© OpenStreetMap'
    }).addTo(state.map);
    state.map.invalidateSize();
  }

  function refreshMap(state, { fit = false } = {}) {
    ensureMap(state);
    const L = window.L;
    if (!L || !state.map) return;
    state.nodes.forEach((info) => {
      if (!info || info._placeholder) return;
      const num = info.num;
      if (!Number.isFinite(num)) return;
      if (!info.position) return;
      if (!state.mapMarkers.has(num >>> 0)) {
        updateMap(state, num, info.position, info.last_heard);
      }
    });
    state.map.invalidateSize();
    if (!fit) return;
    const latLngs = [];
    state.mapMarkers.forEach((marker) => {
      if (!marker || typeof marker.getLatLng !== 'function') return;
      const point = marker.getLatLng();
      if (!point) return;
      if (!Number.isFinite(point.lat) || !Number.isFinite(point.lng)) return;
      latLngs.push(point);
    });
    if (latLngs.length) {
      const bounds = L.latLngBounds(latLngs);
      if (bounds.isValid()) {
        state.map.fitBounds(bounds, { padding: [32, 32], maxZoom: 14 });
        state.mapHasFit = true;
        return;
      }
    }
    state.mapHasFit = false;
    state.map.setView([0, 0], 2);
  }

  function updateMap(state, nodeNum, position, timeSec) {
    ensureMap(state);
    const L = window.L;
    if (!L || !state.map) return;
    const ll = positionToLatLon(position);
    if (!ll) return;
    const key = nodeNum >>> 0;
    let marker = state.mapMarkers.get(key);
    const info = state.nodes.get(key);
    const label = info ? (info.user?.long_name || info.user?.short_name || info.user?.id || `#${info.num}`) : `#${key}`;
    if (!marker) {
      marker = L.marker(ll);
      marker.addTo(state.map);
      state.mapMarkers.set(key, marker);
    } else {
      marker.setLatLng(ll);
    }
    const last = timeSec ? new Date(timeSec * 1000).toLocaleString() : '';
    marker.bindPopup(`<strong>${escapeHtml(label)}</strong><br>#${key}<br>${ll[0].toFixed(5)}, ${ll[1].toFixed(5)}<br>${last}`);
    if (state.activeView === 'map') {
      state._mapRefreshTimer && clearTimeout(state._mapRefreshTimer);
      state._mapRefreshTimer = setTimeout(() => refreshMap(state, { fit: false }), 120);
    }
  }

  function appendMessage(state, message) {
    const key = threadKey(message.to ?? BROADCAST_NUM);
    appendThread(state, message.to ?? BROADCAST_NUM, message);
    if (state.selectedThread === key) {
      renderMessages(state, message.to ?? BROADCAST_NUM);
    } else {
      setUnread(state, message.to ?? BROADCAST_NUM, 1);
      renderPeerChips(state);
    }
  }

  async function sendText(state, destNum, text) {
    if (!state.writer) throw new Error('Not connected');
    await ensureProtobuf();
    const encoder = new TextEncoder();
    const payloadBytes = encoder.encode(text);
    const isPublic = threadKey(destNum) === 'public';
    const dest = isPublic ? BROADCAST_NUM : (destNum >>> 0);
    const cfg = getConfig(state.nodeId);
    const channel = Number.isFinite(Number(cfg.channel)) ? Number(cfg.channel) : 0;
    const data = { portnum: 1, payload: payloadBytes, want_response: false, dest };
    const packet = { to: dest, channel, decoded: data };
    await writeToRadio(state, { packet });
    appendMessage(state, { me: true, display: text, ts: Date.now(), to: dest });
  }

  async function writeToRadio(state, obj) {
    if (!state.writer) throw new Error('Not connected');
    const payload = Types.ToRadio.encode(Types.ToRadio.create(obj)).finish();
    const header = new Uint8Array(4);
    header[0] = 0x94; header[1] = 0xc3;
    header[2] = (payload.length >>> 8) & 0xff;
    header[3] = payload.length & 0xff;
    const buf = new Uint8Array(4 + payload.length);
    buf.set(header, 0);
    buf.set(payload, 4);
    await state.writer.write(buf);
  }

  async function requestNodeDb(state) {
    if (!state.writer) throw new Error('Not connected');
    const nonce = (Math.random() * 0xffffffff) >>> 0;
    state.lastConfigNonce = nonce;
    state.syncInProgress = true;
    state.expectedNodes = 0;
    updateStatus(state, 'Syncing NodeDB…', 'ok');
    logLine(state, `Requesting NodeDB (nonce=${formatHex(nonce)})`, 'ok');
    await writeToRadio(state, { want_config_id: nonce });
  }

  function onInboundText(state, { srcDec, dstDec, text, idHex, via }) {
    if (!text) return;
    const destKey = dstDec === BROADCAST_NUM ? 'public' : threadKey(dstDec);
    const msg = {
      me: false,
      display: (destKey === 'public' ? `${formatSender(state, srcDec)}: ${text}` : text),
      from: srcDec,
      to: dstDec,
      ts: Date.now(),
      id: idHex,
      via
    };
    const key = threadKey(dstDec === BROADCAST_NUM ? srcDec : dstDec);
    const target = dstDec === BROADCAST_NUM ? BROADCAST_NUM : srcDec;
    appendThread(state, target, msg);
    if (state.selectedThread === threadKey(target)) {
      renderMessages(state, target);
    } else {
      setUnread(state, target, 1);
      renderPeerChips(state);
    }
    emitToGraph(state, msg);
  }

  function formatSender(state, num) {
    const info = state.nodes.get(num);
    if (!info) return `#${num}`;
    return info.user?.short_name || info.user?.long_name || info.user?.id || `#${num}`;
  }

  function emitToGraph(state, message) {
    const cfg = getConfig(state.nodeId);
    const publicName = cfg?.publicPortName || 'public';
    const selectedChannel = cfg?.channel ?? 0;
    const payload = buildMessagePayload(state, message, cfg, selectedChannel);
    if (message.to === BROADCAST_NUM || message.to === undefined || message.to === null) {
      Router.sendFrom(state.nodeId, publicName, payload);
      return;
    }
    const peerKey = String(message.from >>> 0);
    const peerPort = (cfg?.peers?.[peerKey]?.portName) || `peer-${peerKey}`;
    Router.sendFrom(state.nodeId, peerPort, payload);
  }

  function buildMessagePayload(state, message, cfg, channel) {
    const base = {
      from: formatSender(state, message.from ?? 0),
      fromNum: message.from ?? null,
      to: message.to ?? null,
      channel,
      text: message.display,
      timestamp: message.ts,
      via: message.via || 'radio'
    };
    const peerKey = message.from != null ? String(message.from >>> 0) : null;
    const wantJson = peerKey ? peerJsonModeEnabled(cfg, peerKey) : (cfg?.defaultJson ?? true);
    if (!wantJson && message.display) return message.display;
    const info = peerKey ? (state.nodes.get(message.from >>> 0) || null) : null;
    if (info) {
      base.peer = {
        num: info.num,
        hex: formatHex(info.num),
        long: info.user?.long_name || '',
        short: info.user?.short_name || '',
        id: info.user?.id || ''
      };
      if (info.device_metrics) {
        base.peer.metrics = {
          battery: info.device_metrics.battery_level ?? null,
          voltage: info.device_metrics.voltage ?? null,
          channelUtilization: info.device_metrics.channel_utilization ?? null,
          airUtilTx: info.device_metrics.air_util_tx ?? null
        };
      }
      const ll = positionToLatLon(info.position);
      if (ll) {
        base.peer.position = {
          lat: ll[0],
          lon: ll[1],
          altitude: Number.isFinite(info.position?.altitude) ? info.position.altitude : null
        };
      }
      base.peer.lastHeard = info.last_heard ? info.last_heard * 1000 : null;
    }
    return base;
  }

  function handlePeerTelemetry(state, nodeInfo) {
    const numRaw = nodeInfo?.num;
    const key = Number(numRaw);
    if (!Number.isFinite(key)) return;
    const dec = key >>> 0;
    const stored = JSON.parse(JSON.stringify(nodeInfo));
    state.nodes.set(dec, stored);
    const cfg = getConfig(state.nodeId);
    const peersCfg = { ...(cfg.peers || {}) };
    const peerKey = String(dec);
    const existing = peersCfg[peerKey];
    const label = nodeInfo.user?.long_name || nodeInfo.user?.short_name || nodeInfo.user?.id || `#${dec}`;
    let changed = false;
    if (existing) {
      const next = { ...existing };
      if (!next.portName) {
        next.portName = `peer-${peerKey}`;
        changed = true;
      }
      if (!next.label && label) {
        next.label = label;
        changed = true;
      }
      if (changed) {
        peersCfg[peerKey] = next;
        saveConfig(state.nodeId, { peers: peersCfg });
      }
    } else {
      peersCfg[peerKey] = {
        enabled: false,
        json: toBoolean(cfg.defaultJson, true),
        portName: `peer-${peerKey}`,
        label
      };
      saveConfig(state.nodeId, { peers: peersCfg });
    }
    updatePeerCards(state);
    renderPeerChips(state);
    updateMap(state, dec, nodeInfo.position, nodeInfo.last_heard);
    if (refreshPortsHandler) refreshPortsHandler(state.nodeId, 'peers');
  }

  async function startReadLoop(state) {
    if (!state.port?.readable) return;
    state.readLoopAbort = new AbortController();
    const signal = state.readLoopAbort.signal;
    const reader = state.port.readable.getReader();
    state.reader = reader;
    const HEADER0 = 0x94;
    const HEADER1 = 0xc3;
    const MAX_LEN = 4096;
    let acc = state.binaryAcc instanceof Uint8Array ? state.binaryAcc : new Uint8Array(0);
    await ensureProtobuf();
    try {
      while (true) {
        if (signal.aborted) break;
        let result;
        try {
          result = await reader.read();
        } catch (err) {
          if (signal.aborted) break;
          throw err;
        }
        const { value, done } = result;
        if (done) break;
        if (!value || !value.length) continue;
        const chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
        if (!acc.length) acc = chunk;
        else {
          const tmp = new Uint8Array(acc.length + chunk.length);
          tmp.set(acc, 0);
          tmp.set(chunk, acc.length);
          acc = tmp;
        }

        parse: while (acc.length >= 2) {
          let i = 0;
          while (i + 1 < acc.length && !(acc[i] === HEADER0 && acc[i + 1] === HEADER1)) i++;
          if (i + 3 >= acc.length) {
            if (i > 0) {
              appendAsciiBytes(state, acc.subarray(0, i));
              acc = acc.subarray(i);
            }
            break parse;
          }
          if (i > 0) {
            appendAsciiBytes(state, acc.subarray(0, i));
            acc = acc.subarray(i);
          }
          if (acc.length < 4) break parse;
          const len = (acc[2] << 8) | acc[3];
          if (len <= 0 || len > MAX_LEN) {
            appendAsciiBytes(state, acc.subarray(0, 2));
            acc = acc.subarray(2);
            continue;
          }
          if (acc.length < 4 + len) break parse;
          const frame = acc.subarray(4, 4 + len);
          acc = acc.subarray(4 + len);
          try {
            await ensureProtobuf();
            const msg = Types.FromRadio.decode(frame);
            handleFromRadioMessage(state, msg);
          } catch (err) {
            logLine(state, `protobuf decode error: ${err.message}`, 'err');
          }
        }
      }
    } catch (err) {
      if (!signal.aborted && !state.userRequestedDisconnect) {
        throw err;
      }
    } finally {
      state.binaryAcc = acc;
      try { reader.releaseLock(); } catch (_) { }
      state.reader = null;
      flushAscii(state);
      state.readLoopAbort = null;
    }
  }

  function handleFromRadioMessage(state, msg) {
    if (!msg) return;
    if (msg.my_info) {
      const info = Types.MyNodeInfo.toObject(msg.my_info, { defaults: true });
      state.myNodeNum = Number(info.my_node_num ?? state.myNodeNum ?? 0) >>> 0;
      state.myInfo = info;
      state.expectedNodes = Number(info.nodedb_count || 0);
      state.syncInProgress = true;
      updateSelfInfo(state);
      if (state.expectedNodes > 0) {
        updateStatus(state, `Syncing NodeDB… (${state.expectedNodes} nodes)`, 'ok');
      } else {
        updateStatus(state, 'Syncing NodeDB…', 'ok');
      }
      return;
    }
    if (msg.node_info) {
      const nodeInfo = Types.NodeInfo.toObject(msg.node_info, { defaults: true });
      handlePeerTelemetry(state, nodeInfo);
      if (state.syncInProgress) {
        const total = state.expectedNodes || state.nodes.size;
        updateStatus(state, `Synced ${state.nodes.size}/${total} nodes`, 'ok');
      }
      return;
    }
    if (msg.config_complete_id != null) {
      if (!state.lastConfigNonce || msg.config_complete_id === state.lastConfigNonce) {
        state.syncInProgress = false;
        updateStatus(state, 'NodeDB synced', 'ok');
        refreshMap(state, { fit: true });
      }
      return;
    }
    if (msg.packet) {
      handleMeshPacket(state, msg.packet);
      return;
    }
  }

  function handleMeshPacket(state, packet) {
    if (!packet) return;
    const from = packet.from >>> 0;
    const to = packet.to >>> 0;
    const channel = packet.channel >>> 0;
    if (packet.decoded) {
      const data = packet.decoded;
      if (data.portnum === 1) {
        const payloadBytes = data.payload instanceof Uint8Array
          ? data.payload
          : (Array.isArray(data.payload) ? new Uint8Array(data.payload) : new Uint8Array(0));
        let text = '';
        if (payloadBytes.length) {
          try {
            text = new TextDecoder().decode(payloadBytes);
          } catch (err) {
            text = `[decode error ${err.message}]`;
          }
        }
        const idHex = formatHex(packet.id || 0);
        const key = makeDedupKey(from, idHex, text);
        if (!state.seenMsgs.has(key)) {
          rememberMsg(state, key);
          onInboundText(state, { srcDec: from, dstDec: to, text, idHex, via: 'protobuf' });
        }
        return;
      }
      if (data.portnum === 3 && data.payload?.length) {
        try {
          const payloadBytes = data.payload instanceof Uint8Array
            ? data.payload
            : (Array.isArray(data.payload) ? new Uint8Array(data.payload) : new Uint8Array(0));
          if (!payloadBytes.length) return;
          const pos = Types.Position.decode(payloadBytes);
          applyPositionUpdate(state, from, pos, packet.rx_time | 0);
        } catch (err) {
          logLine(state, `position decode error: ${err.message}`, 'warn');
        }
        return;
      }
      if (data.portnum === 7 && data.payload?.length) {
        try {
          const payloadBytes = data.payload instanceof Uint8Array
            ? data.payload
            : (Array.isArray(data.payload) ? new Uint8Array(data.payload) : new Uint8Array(0));
          const inflated = payloadBytes.length ? pako.inflate(payloadBytes, { to: 'string' }) : '';
          const idHex = formatHex(packet.id || 0);
          const key = makeDedupKey(from, idHex, inflated);
          if (!state.seenMsgs.has(key)) {
            rememberMsg(state, key);
            onInboundText(state, { srcDec: from, dstDec: to, text: inflated, idHex, via: 'compressed' });
          }
        } catch (err) {
          logLine(state, `compressed payload error: ${err.message}`, 'warn');
        }
        return;
      }
    }
  }

  function applyPositionUpdate(state, num, pos, rxTime) {
    const existing = state.nodes.get(num >>> 0) || {};
    const merged = { ...existing, position: pos, last_heard: rxTime || existing.last_heard };
    state.nodes.set(num >>> 0, merged);
    updatePeerCards(state);
    updateMap(state, num, pos, rxTime);
  }

  function updateSelfInfo(state) {
    if (!state.ui?.selfInfo) return;
    const info = [];
    if (state.myNodeNum != null) info.push(`#${state.myNodeNum} (${formatHex(state.myNodeNum)})`);
    if (state.myInfo?.pio_env) info.push(state.myInfo.pio_env);
    if (state.myInfo?.nodedb_count != null) info.push(`db: ${state.myInfo.nodedb_count}`);
    state.ui.selfInfo.textContent = info.join(' • ');
  }

  async function startHeartbeat(state) {
    stopHeartbeat(state);
    if (!state.writer) return;
    state.heartbeatTimer = setInterval(async () => {
      try {
        const nonce = (state.heartbeatNonce++ >>> 0);
        await writeToRadio(state, { heartbeat: { nonce } });
      } catch (err) {
        logLine(state, `heartbeat error: ${err.message}`, 'warn');
      }
    }, 12000);
  }

  function stopHeartbeat(state) {
    if (state.heartbeatTimer) clearInterval(state.heartbeatTimer);
    state.heartbeatTimer = null;
  }

  async function connectSerial(state, port) {
    if (!port) throw new Error('No port selected');
    await ensureProtobuf();
    state.port = port;
    await port.open({ baudRate: 115200 });
    state.writer = port.writable?.getWriter?.();
    state.binaryAcc = new Uint8Array(0);
    state.lastConfigNonce = 0;
    state.expectedNodes = 0;
    state.syncInProgress = false;
    state.mapHasFit = false;
    state.asciiLineBuffer = '';
    state.asciiParseBuffer = '';
    state.ansiCarry = '';
    state.logPending = '';
    state.lastLogDiv = null;
    state.nodes.clear();
    state.mapMarkers.forEach((marker) => {
      try { marker.remove?.(); } catch (_) { /* ignore */ }
    });
    state.mapMarkers.clear();
    state.seenMsgs.clear();
    state.dedupeList.length = 0;
    if (state._mapRefreshTimer) {
      clearTimeout(state._mapRefreshTimer);
      state._mapRefreshTimer = null;
    }
    state.threads.clear();
    state.unread.clear();
    state.selectedThread = 'public';
    renderPeerChips(state);
    updatePeerCards(state);
    updateSelectedThread(state, 'public');
    updateStatus(state, 'Connected @115200', 'ok');
    if (state.ui) {
      if (state.ui.connectButton) state.ui.connectButton.disabled = true;
      if (state.ui.disconnectButton) state.ui.disconnectButton.disabled = false;
      if (state.ui.refreshButton) state.ui.refreshButton.disabled = false;
      if (state.ui.sendButton) state.ui.sendButton.disabled = false;
    }
    startReadLoop(state).catch(async (err) => {
      if (state.userRequestedDisconnect || state.readLoopAbort?.signal?.aborted) return;
      const detail = err?.message || String(err || 'read loop error');
      logLine(state, `read loop error: ${detail}`, 'err');
      await handleConnectionLost(state, 'read loop');
    });
  }

  async function disconnectSerial(state) {
    stopHeartbeat(state);
    stopLocationInterval(state);
    try {
      if (state.readLoopAbort) state.readLoopAbort.abort();
    } catch (_) { /* ignore */ }
    if (state.reader) {
      try { await state.reader.cancel(); } catch (_) { }
      try { state.reader.releaseLock?.(); } catch (_) { }
    }
    if (state.writer) {
      try { state.writer.releaseLock?.(); } catch (_) { }
    }
    if (state.port?.close) {
      try { await state.port.close(); } catch (_) { }
    }
    state.reader = null;
    state.writer = null;
    state.port = null;
    if (state._mapRefreshTimer) {
      clearTimeout(state._mapRefreshTimer);
      state._mapRefreshTimer = null;
    }
    state.binaryAcc = new Uint8Array(0);
    state.lastConfigNonce = 0;
    state.expectedNodes = 0;
    state.syncInProgress = false;
    state.asciiLineBuffer = '';
    state.asciiParseBuffer = '';
    state.ansiCarry = '';
    updateStatus(state, 'Disconnected', 'warn');
    if (state.ui) {
      if (state.ui.connectButton) state.ui.connectButton.disabled = !state.allowed;
      if (state.ui.disconnectButton) state.ui.disconnectButton.disabled = true;
      if (state.ui.refreshButton) state.ui.refreshButton.disabled = true;
      if (state.ui.sendButton) state.ui.sendButton.disabled = true;
      if (state.ui.shareLocStart) state.ui.shareLocStart.disabled = false;
      if (state.ui.shareLocStop) state.ui.shareLocStop.disabled = true;
    }
  }

  function updateStatus(state, text, status = '') {
    if (!state.ui?.statusText) return;
    state.ui.statusText.textContent = text;
    state.ui.statusDot.classList.remove('ok', 'warn', 'err');
    if (status) state.ui.statusDot.classList.add(status);
  }

  function resolveShareDestination(state) {
    if (!state) return BROADCAST_NUM;
    const mode = state.ui?.shareDest?.value || 'current';
    if (mode === 'public') return BROADCAST_NUM;
    if (state.selectedThread === 'public') return BROADCAST_NUM;
    const num = Number(state.selectedThread);
    if (!Number.isFinite(num)) return BROADCAST_NUM;
    return num >>> 0;
  }

  function buildUi(session) {
    const node = getNode(session.nodeId);
    if (!node?.el) return null;
    const wrap = node.el.querySelector('[data-mesh-root]');
    if (!wrap) return null;
    const ui = {
      root: wrap,
      statusText: wrap.querySelector('[data-mesh-status]'),
      statusDot: wrap.querySelector('[data-mesh-status-dot]'),
      portButton: wrap.querySelector('[data-mesh-port]'),
      connectButton: wrap.querySelector('[data-mesh-connect]'),
      refreshButton: wrap.querySelector('[data-mesh-refresh]'),
      disconnectButton: wrap.querySelector('[data-mesh-disconnect]'),
      nodesButton: wrap.querySelector('[data-mesh-nodes]'),
      log: wrap.querySelector('[data-mesh-log]'),
      peerBar: wrap.querySelector('[data-mesh-peer-bar]'),
      messages: wrap.querySelector('[data-mesh-messages]'),
      chatHeader: wrap.querySelector('[data-mesh-chat-header]'),
      map: wrap.querySelector('[data-mesh-map]'),
      logView: wrap.querySelector('[data-mesh-log]'),
      tabs: Array.from(wrap.querySelectorAll('[data-mesh-tab]')),
      views: new Map(Array.from(wrap.querySelectorAll('[data-mesh-view]')).map((el) => [el.dataset.meshView, el])),
      input: wrap.querySelector('[data-mesh-input]'),
      sendButton: wrap.querySelector('[data-mesh-send]'),
      shareUA: wrap.querySelector('[data-mesh-send-ua]'),
      shareLocOnce: wrap.querySelector('[data-mesh-loc-once]'),
      shareLocStart: wrap.querySelector('[data-mesh-loc-start]'),
      shareLocStop: wrap.querySelector('[data-mesh-loc-stop]'),
      shareIntervalInput: wrap.querySelector('[data-mesh-loc-interval]'),
      shareDest: wrap.querySelector('[data-mesh-share-dest]'),
      peerCards: wrap.querySelector('[data-mesh-peer-cards]'),
      selfInfo: wrap.querySelector('[data-mesh-self-info]')
    };
    return ui;
  }

  function attachUi(session) {
    const state = session.state;
    state.ui = buildUi(session);
    if (!state.ui) return;
    if (state.ui.connectButton) state.ui.connectButton.disabled = !state.allowed;
    if (state.ui.disconnectButton) state.ui.disconnectButton.disabled = true;
    if (state.ui.refreshButton) state.ui.refreshButton.disabled = true;
    if (state.ui.sendButton) state.ui.sendButton.disabled = true;
    if (state.ui.shareLocStop) state.ui.shareLocStop.disabled = true;
    if (state.ui.shareLocStart) state.ui.shareLocStart.disabled = false;
    if (state.ui.shareDest) state.ui.shareDest.value = state.ui.shareDest.value || 'current';
    state.ui.portButton?.addEventListener('click', async () => {
      if (!('serial' in navigator)) {
        setBadge?.('WebSerial unsupported', false);
        return;
      }
      try {
        const port = await navigator.serial.requestPort({});
        state.allowed = true;
        state.port = port;
        updateStatus(state, 'Port chosen', 'warn');
        state.ui.connectButton.disabled = false;
        try {
          const info = port.getInfo?.() || {};
          saveConfig(session.nodeId, { lastPortInfo: info });
        } catch (_) { }
      } catch (err) {
        if (err?.name !== 'NotFoundError') logLine(state, `requestPort: ${err.message}`, 'err');
      }
    });
    state.ui.connectButton?.addEventListener('click', async () => {
      try {
        state.ui.connectButton.disabled = true;
        state.ui.disconnectButton.disabled = false;
        state.ui.refreshButton.disabled = false;
        await connect(session);
      } catch (err) {
        logLine(state, `connect failed: ${err.message}`, 'err');
        state.ui.connectButton.disabled = !state.allowed;
      }
    });
    state.ui.refreshButton?.addEventListener('click', async () => {
      try { await requestNodeDb(state); } catch (err) { logLine(state, `refresh failed: ${err.message}`, 'err'); }
    });
    state.ui.disconnectButton?.addEventListener('click', async () => {
      state.userRequestedDisconnect = true;
      cancelReconnect(state);
      await disconnect(session);
    });
    state.ui.tabs?.forEach((tab) => {
      tab.addEventListener('click', () => {
        const view = tab.dataset.meshTab;
        state.activeView = view;
        state.ui.tabs.forEach((t) => t.classList.toggle('active', t === tab));
        state.ui.views?.forEach((el, key) => el.classList.toggle('active', key === view));
        if (view === 'map') {
          setTimeout(() => {
            refreshMap(state, { fit: true });
          }, 60);
        } else {
          state.mapHasFit = false;
        }
      });
    });
    state.ui.peerBar?.addEventListener('click', (ev) => {
      const chip = ev.target.closest('.mesh-peer-chip');
      if (!chip) return;
      const key = chip.dataset.peerKey;
      updateSelectedThread(state, key);
    });
    state.ui.sendButton?.addEventListener('click', async () => {
      const value = state.ui.input.value.trim();
      if (!value) return;
      const target = state.selectedThread === 'public' ? BROADCAST_NUM : Number(state.selectedThread);
      state.ui.input.value = '';
      try {
        await sendText(state, target, value);
      } catch (err) {
        logLine(state, `send failed: ${err.message}`, 'err');
      }
    });
    state.ui.input?.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter' && !ev.shiftKey) {
        ev.preventDefault();
        state.ui.sendButton?.click();
      }
    });
    state.ui.shareUA?.addEventListener('click', async () => {
      try {
        const target = resolveShareDestination(state);
        await sendText(state, target, buildBrowserInfo());
      } catch (err) {
        logLine(state, `UA send failed: ${err.message}`, 'err');
      }
    });
    state.ui.shareLocOnce?.addEventListener('click', async () => {
      try {
        const dest = resolveShareDestination(state);
        await sendLocationOnce(state, dest);
      } catch (err) {
        logLine(state, `Location send failed: ${err.message}`, 'err');
      }
    });
    state.ui.shareLocStart?.addEventListener('click', () => startLocationInterval(state));
    state.ui.shareLocStop?.addEventListener('click', () => stopLocationInterval(state));
    updateStatus(state, 'Select port', 'warn');
    renderPeerChips(state);
    renderMessages(state, BROADCAST_NUM);
    updatePeerCards(state);
  }

  function buildBrowserInfo() {
    const ua = navigator.userAgent || 'N/A';
    const plat = navigator.platform || 'N/A';
    const lang = navigator.language || 'N/A';
    const tz = Intl.DateTimeFormat().resolvedOptions().timeZone || 'N/A';
    const scr = window.screen ? `${window.screen.width}x${window.screen.height}` : 'N/A';
    const vp = `${window.innerWidth}x${window.innerHeight}`;
    return `BROWSER INFO
UA: ${ua}
Platform: ${plat}
Lang: ${lang}
TZ: ${tz}
Screen: ${scr}
Viewport: ${vp}`;
  }

  async function sendLocationOnce(state, destNum) {
    try {
      const gp = await getCurrentPosition();
      await sendPosition(state, destNum, gp.coords.latitude, gp.coords.longitude, gp.coords.altitude ?? null);
    } catch (err) {
      throw err;
    }
  }

  function startLocationInterval(state) {
    if (state.shareInterval) return;
    const secs = Math.max(5, Number(state.ui.shareIntervalInput?.value) || 60);
    if (state.ui.shareLocStart) state.ui.shareLocStart.disabled = true;
    if (state.ui.shareLocStop) state.ui.shareLocStop.disabled = false;
    state.shareInterval = setInterval(async () => {
      try {
        const dest = resolveShareDestination(state);
        const gp = await getCurrentPosition();
        await sendPosition(state, dest, gp.coords.latitude, gp.coords.longitude, gp.coords.altitude ?? null);
      } catch (err) {
        logLine(state, `Location interval error: ${err.message}`, 'warn');
      }
    }, secs * 1000);
    logLine(state, `Location sharing every ${secs}s started`, 'ok');
  }

  function stopLocationInterval(state) {
    if (state.shareInterval) clearInterval(state.shareInterval);
    state.shareInterval = null;
    if (state.ui.shareLocStart) state.ui.shareLocStart.disabled = false;
    if (state.ui.shareLocStop) state.ui.shareLocStop.disabled = true;
    logLine(state, 'Location sharing stopped', 'warn');
  }

  async function sendPosition(state, destNum, lat, lon, altitude = null) {
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) throw new Error('Invalid lat/lon');
    const pos = {
      latitude_i: Math.round(lat * 1e7),
      longitude_i: Math.round(lon * 1e7),
      time: Math.floor(Date.now() / 1000)
    };
    if (Number.isFinite(altitude)) pos.altitude = Math.round(altitude);
    const payload = Types.Position.encode(Types.Position.create(pos)).finish();
    const dest = threadKey(destNum) === 'public' ? BROADCAST_NUM : (destNum >>> 0);
    const cfg = getConfig(state.nodeId);
    const channel = Number.isFinite(Number(cfg.channel)) ? Number(cfg.channel) : 0;
    const data = { portnum: 3, payload, want_response: false, dest };
    const packet = { to: dest, channel, decoded: data };
    await writeToRadio(state, { packet });
    if (state.myNodeNum != null) applyPositionUpdate(state, state.myNodeNum >>> 0, pos, Math.floor(Date.now() / 1000));
    const text = `📍 Location: ${lat.toFixed(6)}, ${lon.toFixed(6)}${Number.isFinite(altitude) ? ` alt=${Math.round(altitude)}m` : ''}`;
    appendMessage(state, { me: true, display: text, to: dest });
  }

  function getCurrentPosition() {
    return new Promise((resolve, reject) => {
      if (!navigator.geolocation) return reject(new Error('Geolocation unavailable'));
      navigator.geolocation.getCurrentPosition(
        (pos) => resolve(pos),
        (err) => reject(err),
        { enableHighAccuracy: true, timeout: 10000, maximumAge: 0 }
      );
    });
  }

  function cancelReconnect(state) {
    if (state.reconnectTimer) clearTimeout(state.reconnectTimer);
    state.reconnectTimer = null;
  }

  function scheduleReconnect(state, reason = '') {
    if (state.userRequestedDisconnect) return;
    if (state.reconnectTimer) return;
    updateStatus(state, 'Reconnecting…', 'warn');
    logLine(state, `Scheduling reconnect ${reason ? `(${reason})` : ''} in ${state.reconnectBackoff}ms`, 'warn');
    state.reconnectTimer = setTimeout(async () => {
        state.reconnectTimer = null;
        try {
        await connect(state.session);
        logLine(state, 'Reconnected', 'ok');
        state.reconnectBackoff = 1000;
      } catch (err) {
        logLine(state, `Reconnect failed: ${err.message}`, 'err');
        state.reconnectBackoff = Math.min(15000, state.reconnectBackoff * 1.5);
        scheduleReconnect(state, 'after failure');
      }
    }, state.reconnectBackoff);
  }

  async function handleConnectionLost(state, reason) {
    cancelReconnect(state);
    if (state.userRequestedDisconnect) {
      updateStatus(state, 'Disconnected', 'warn');
      return;
    }
    logLine(state, `Connection lost${reason ? ` (${reason})` : ''}`, 'warn');
    flushAscii(state);
    try {
      await disconnectSerial(state);
    } catch (err) {
      logLine(state, `disconnect cleanup error: ${err.message}`, 'err');
    }
    scheduleReconnect(state, reason || 'connection lost');
  }

  async function connect(session) {
    const state = session.state;
    if (state.connectInFlight) return state.pendingConnect;
    state.connectInFlight = true;
    state.pendingConnect = (async () => {
      try {
        state.userRequestedDisconnect = false;
        if (!state.port) {
          const ports = await navigator.serial?.getPorts?.();
          if (ports && ports.length) {
            state.port = ports[0];
            state.allowed = true;
          }
        }
        if (!state.port) throw new Error('No serial port selected');
        await connectSerial(state, state.port);
        await requestNodeDb(state);
        await startHeartbeat(state);
      } finally {
        state.connectInFlight = false;
        state.pendingConnect = null;
      }
    })();
    return state.pendingConnect;
  }

  async function disconnect(session) {
    const state = session.state;
    state.userRequestedDisconnect = true;
    await disconnectSerial(state);
    state.userRequestedDisconnect = false;
  }

  function renderSettings(nodeId, container) {
    const session = ensureSession(nodeId);
    const state = session.state;
    const cfg = getConfig(nodeId);
    const knownPeers = Array.from(state.nodes.values());
    const peersState = cfg.peers || {};
    const peerRecords = knownPeers.slice();
    for (const [peerKey, info] of Object.entries(peersState)) {
      if (peerRecords.some((p) => String(p.num >>> 0) === peerKey)) continue;
      const rawNum = Number(peerKey);
      const num = Number.isFinite(rawNum) ? rawNum >>> 0 : 0;
      peerRecords.push({
        num,
        user: { long_name: info?.label || `Peer ${peerKey}` },
        position: null,
        device_metrics: {},
        last_heard: 0,
        _placeholder: true
      });
    }
    const peers = peerRecords.sort((a, b) => (b.last_heard || 0) - (a.last_heard || 0));
    const wrap = document.createElement('div');
    wrap.className = 'meshtastic-settings';
    const info = document.createElement('div');
    info.textContent = 'Select peers to expose as ports. Toggle JSON outputs per peer.';
    info.style.fontSize = '12px';
    info.style.color = '#9aa3b2';
    wrap.appendChild(info);
    const list = document.createElement('div');
    list.className = 'mesh-settings-peers';
    if (!peers.length) {
      const empty = document.createElement('div');
      empty.textContent = 'No peers discovered yet. Connect to your Meshtastic device.';
      empty.style.color = '#9aa3b2';
      empty.style.fontSize = '12px';
      list.appendChild(empty);
    } else {
      peers.forEach((peer) => {
        const key = String(peer.num >>> 0);
        const defaultName = peer.user?.long_name || peer.user?.short_name || peer.user?.id || `#${peer.num}`;
        const storedEntry = peersState[key];
        const configEntry = storedEntry
          ? { ...storedEntry }
          : {
              enabled: false,
              json: toBoolean(cfg.defaultJson, true),
              portName: `peer-${key}`,
              label: defaultName
            };
        const row = document.createElement('div');
        row.className = 'mesh-peer-toggle';
        const inf = document.createElement('div');
        inf.className = 'info';
        const name = document.createElement('div');
        name.className = 'name';
        name.textContent = configEntry.label || defaultName;
        const meta = document.createElement('div');
        meta.className = 'meta';
        const ll = positionToLatLon(peer.position);
        const pos = ll ? `${ll[0].toFixed(5)}, ${ll[1].toFixed(5)}` : '—';
        const battery = peer.device_metrics?.battery_level != null ? `${peer.device_metrics.battery_level}%` : '—';
      const idLabel = peer._placeholder ? `key ${key}` : `#${peer.num}`;
      const hexLabel = peer._placeholder ? '—' : formatHex(peer.num);
        meta.textContent = `${idLabel} • ${hexLabel} • batt ${battery} • pos ${pos}`;
        inf.appendChild(name);
        inf.appendChild(meta);
        row.appendChild(inf);
        const controls = document.createElement('div');
        controls.className = 'controls';
        const toggleBtn = document.createElement('button');
        toggleBtn.type = 'button';
        toggleBtn.textContent = configEntry.enabled ? 'Enabled' : 'Disabled';
        toggleBtn.classList.toggle('active', configEntry.enabled);
        toggleBtn.addEventListener('click', () => {
          const cfgNow = getConfig(nodeId);
          const peersNow = { ...(cfgNow.peers || {}) };
          const nextEnabled = !configEntry.enabled;
          const prior = peersNow[key] || {};
          const next = {
            ...prior,
            enabled: nextEnabled,
            json: peerJsonModeEnabled(cfgNow, key),
            portName: prior.portName || configEntry.portName || `peer-${key}`,
            label: prior.label || configEntry.label || defaultName
          };
          peersNow[key] = next;
          saveConfig(nodeId, { peers: peersNow });
          configEntry.enabled = nextEnabled;
          configEntry.portName = next.portName;
          configEntry.label = next.label;
          toggleBtn.textContent = nextEnabled ? 'Enabled' : 'Disabled';
          toggleBtn.classList.toggle('active', nextEnabled);
          badge.textContent = `port: ${next.portName}`;
          if (refreshPortsHandler) refreshPortsHandler(nodeId, 'settings-toggle');
          updatePeerCards(state);
        });
        controls.appendChild(toggleBtn);
        const jsonBtn = document.createElement('button');
        jsonBtn.type = 'button';
        const initialJson = peerJsonModeEnabled(cfg, key);
        jsonBtn.textContent = initialJson ? 'JSON' : 'Raw';
        jsonBtn.addEventListener('click', () => {
          const cfgNow = getConfig(nodeId);
          const peersNow = { ...(cfgNow.peers || {}) };
          const prior = peersNow[key] || {};
          const nextJson = !peerJsonModeEnabled(cfgNow, key);
          const next = {
            ...prior,
            enabled: prior.enabled ?? configEntry.enabled ?? false,
            json: nextJson,
            portName: prior.portName || configEntry.portName || `peer-${key}`,
            label: prior.label || configEntry.label || defaultName
          };
          peersNow[key] = next;
          saveConfig(nodeId, { peers: peersNow });
          configEntry.json = nextJson;
          configEntry.portName = next.portName;
          configEntry.label = next.label;
          jsonBtn.textContent = nextJson ? 'JSON' : 'Raw';
          badge.textContent = `port: ${next.portName}`;
          updatePeerCards(state);
        });
        controls.appendChild(jsonBtn);
        const badge = document.createElement('div');
        badge.className = 'badge';
        const badgePort = configEntry.portName || `peer-${key}`;
        badge.textContent = `port: ${badgePort}`;
        controls.appendChild(badge);
        row.appendChild(controls);
        list.appendChild(row);
      });
    }
    wrap.appendChild(list);
    container.appendChild(wrap);
  }

  function bindSerialEvents() {
    if (serialEventsBound) return;
    if (!navigator.serial?.addEventListener) return;
    navigator.serial.addEventListener('disconnect', (event) => {
      sessions.forEach((entry) => {
        const state = entry.state;
        if (state.port && event.target && state.port !== event.target) return;
        handleConnectionLost(state, 'serial disconnect');
      });
    });
    navigator.serial.addEventListener('connect', (event) => {
      sessions.forEach((entry) => {
        const state = entry.state;
        if (state.port && event.target && state.port !== event.target) return;
        if (!state.port && event.target) state.port = event.target;
        cancelReconnect(state);
        state.reconnectBackoff = 1000;
        scheduleReconnect(state, 'serial connect');
      });
    });
    serialEventsBound = true;
  }

  function ensureUiBuilt(nodeId) {
    const session = ensureSession(nodeId);
    session.session = session; // easier for references
    const node = getNode(nodeId);
    if (!node?.el) return;
    attachUi(session);
    bindSerialEvents();
    tryAutoConnect(session).catch((err) => {
      logLine(session.state, `Auto-connect failed: ${err.message}`, 'warn');
    });
  }

  async function tryAutoConnect(session) {
    const state = session.state;
    if (!navigator.serial?.getPorts) return;
    const cfg = getConfig(session.nodeId);
    const ports = await navigator.serial.getPorts();
    if (!ports || !ports.length) {
      state.allowed = false;
      if (state.ui?.connectButton) state.ui.connectButton.disabled = true;
      return;
    }
    state.allowed = true;
    const last = cfg?.lastPortInfo;
    let chosen = null;
    if (last && (last.usbVendorId || last.usbProductId)) {
      chosen = ports.find((port) => {
        const info = port.getInfo?.() || {};
        return (
          (last.usbVendorId == null || info.usbVendorId === last.usbVendorId) &&
          (last.usbProductId == null || info.usbProductId === last.usbProductId)
        );
      }) || null;
    }
    if (!chosen) chosen = ports[0];
    if (!chosen) return;
    state.port = chosen;
    if (state.ui?.connectButton) state.ui.connectButton.disabled = false;
    updateStatus(state, 'Port ready', 'warn');
    const autoConnect = toBoolean(cfg?.autoConnect, true);
    if (!autoConnect) return;
    try {
      await connect(session);
      logLine(state, 'Auto-connected to serial port', 'ok');
    } catch (err) {
      logLine(state, `Auto-connect failed: ${err.message}`, 'warn');
      if (state.ui?.connectButton) state.ui.connectButton.disabled = false;
      if (state.ui?.disconnectButton) state.ui.disconnectButton.disabled = true;
      if (state.ui?.refreshButton) state.ui.refreshButton.disabled = true;
      if (state.ui?.sendButton) state.ui.sendButton.disabled = true;
    }
  }

  return {
    init(nodeId) {
      ensureSession(nodeId);
      ensureUiBuilt(nodeId);
    },
    dispose(nodeId) {
      const entry = sessions.get(nodeId);
      if (!entry) return;
      stopLocationInterval(entry.state);
      stopHeartbeat(entry.state);
      sessions.delete(nodeId);
    },
    renderSettings,
    setRefreshPortsHandler,
    refresh(nodeId) {
      const entry = sessions.get(nodeId);
      if (!entry) return;
      updatePeerCards(entry.state);
      renderPeerChips(entry.state);
    },
    async sendPublic(nodeId, payload) {
      const entry = sessions.get(nodeId);
      if (!entry) throw new Error('Meshtastic node not initialized');
      if (!payload) return;
      await sendText(entry.state, BROADCAST_NUM, String(payload));
    },
    async sendPeer(nodeId, peerKey, payload) {
      const entry = sessions.get(nodeId);
      if (!entry) throw new Error('Meshtastic node not initialized');
      const num = Number(peerKey.replace(/^peer-/, ''));
      if (!Number.isFinite(num)) throw new Error('Invalid peer key');
      await sendText(entry.state, num, String(payload));
    }
  };
}

export { createMeshtastic };
