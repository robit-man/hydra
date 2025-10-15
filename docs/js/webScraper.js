import { ensureLocalNetworkAccess } from './localNetwork.js';

const TRUE_LIKE = new Set(['1', 'true', 'yes', 'on']);
const ACTION_ALIASES = {
  open: 'open',
  connect: 'open',
  start: 'open',
  close: 'close',
  stop: 'close',
  nav: 'nav',
  navigate: 'nav',
  go: 'nav',
  back: 'back',
  previous: 'back',
  forward: 'forward',
  next: 'forward',
  click: 'click',
  tap: 'click',
  type: 'type',
  input: 'type',
  scroll: 'scroll',
  scroll_up: 'scroll_up',
  scrollup: 'scroll_up',
  'scroll-up': 'scroll_up',
  up: 'scroll_up',
  scroll_down: 'scroll_down',
  scrolldown: 'scroll_down',
  'scroll-down': 'scroll_down',
  down: 'scroll_down',
  'scroll.point': 'scroll_point',
  scroll_point: 'scroll_point',
  scrollpoint: 'scroll_point',
  drag: 'drag',
  screenshot: 'screenshot',
  shot: 'screenshot',
  capture: 'screenshot',
  dom: 'dom',
  html: 'dom',
  events: 'events',
  subscribe: 'events'
};
const MAX_LOG_LINES = 80;

const noop = () => {};

const blobToBase64 = (blob) => new Promise((resolve, reject) => {
  const blobCtor = typeof Blob === 'undefined' ? null : Blob;
  if (!blobCtor || !(blob instanceof blobCtor)) {
    resolve('');
    return;
  }
  const reader = new FileReader();
  reader.onloadend = () => {
    const result = typeof reader.result === 'string' ? reader.result : '';
    const commaIdx = result.indexOf(',');
    resolve(commaIdx >= 0 ? result.slice(commaIdx + 1) : result);
  };
  reader.onerror = (err) => reject(err);
  try {
    reader.readAsDataURL(blob);
  } catch (err) {
    reject(err);
  }
});

function boolFromConfig(raw, def = false) {
  if (raw === undefined || raw === null) return def;
  if (typeof raw === 'boolean') return raw;
  if (typeof raw === 'number') return raw !== 0;
  const text = String(raw).trim().toLowerCase();
  if (!text) return def;
  if (TRUE_LIKE.has(text)) return true;
  if (['0', 'false', 'off', 'no'].includes(text)) return false;
  return def;
}

function normalizeBase(base) {
  const raw = String(base || '').trim();
  if (!raw) return '';
  return raw.replace(/\/+$/, '');
}

function asString(value) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string') return value;
  if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  if (typeof value === 'object' && 'text' in value) return asString(value.text);
  return String(value || '');
}

function asNumber(value, def = 0) {
  if (value === null || value === undefined) return def;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const str = String(value).trim();
  if (!str) return def;
  const num = Number(str);
  return Number.isFinite(num) ? num : def;
}

function asAction(payload) {
  if (payload === null || payload === undefined) return '';
  if (typeof payload === 'string') return ACTION_ALIASES[payload.trim().toLowerCase()] || payload.trim().toLowerCase();
  if (typeof payload === 'object') {
    const key = payload.action || payload.type || payload.kind || payload.mode;
    if (key) return asAction(String(key));
  }
  return '';
}

function serviceNameFromConfig(cfg) {
  const raw = (cfg.service || '').trim();
  return raw || 'web_scrape';
}

function createWebScraper({ getNode, NodeStore, Router, Net, setBadge = noop, log = noop }) {
  const stateMap = new Map();

  function ensureState(nodeId) {
    let state = stateMap.get(nodeId);
    const node = getNode(nodeId);
    if (!node || !node.el) return state || null;
    const el = node.el;
    const elements = {
      root: el.querySelector('[data-webscraper-root]'),
      connect: el.querySelector('[data-ws-connect]'),
      close: el.querySelector('[data-ws-close]'),
      nav: el.querySelector('[data-ws-nav]'),
      back: el.querySelector('[data-ws-back]'),
      forward: el.querySelector('[data-ws-forward]'),
      click: el.querySelector('[data-ws-click]'),
      type: el.querySelector('[data-ws-type]'),
      scroll: el.querySelector('[data-ws-scroll-btn]'),
      scrollUp: el.querySelector('[data-ws-scroll-up]'),
      scrollDown: el.querySelector('[data-ws-scroll-down]'),
      screenshot: el.querySelector('[data-ws-screenshot]'),
      dom: el.querySelector('[data-ws-dom]'),
      url: el.querySelector('[data-ws-url]'),
      selector: el.querySelector('[data-ws-selector]'),
      text: el.querySelector('[data-ws-text]'),
      amount: el.querySelector('[data-ws-scroll]'),
      status: el.querySelector('[data-ws-status]'),
      log: el.querySelector('[data-ws-log]'),
      sid: el.querySelector('[data-ws-sid]'),
      preview: el.querySelector('[data-ws-preview]'),
      frame: el.querySelector('[data-ws-frame]'),
      placeholder: el.querySelector('[data-ws-preview-placeholder]')
    };

    const keysToCheck = [
      'root',
      'connect',
      'close',
      'nav',
      'back',
      'forward',
      'click',
      'type',
      'scroll',
      'scrollUp',
      'scrollDown',
      'screenshot',
      'dom',
      'url',
      'selector',
      'text',
      'amount',
      'status',
      'log',
      'sid',
      'preview',
      'frame',
      'placeholder'
    ];

    if (!state) {
      state = {
        nodeId,
        elements,
        cleanup: [],
        sessionId: '',
        manualSid: '',
        previewUrl: '',
        autoScreenshot: false,
        autoCapture: false,
        captureTimer: null,
        capturePending: false,
        frameOutputMode: 'wrapped',
        lastWheelTs: 0,
        busyCount: 0,
        logLines: [],
        eventGen: 0,
        eventsCancel: null,
        inputs: {
          url: '',
          selector: '',
          text: '',
          amount: 600,
          sid: '',
          xy: null
        },
        lastFrameMeta: null,
        dragInfo: null,
        _bound: false
      };
      stateMap.set(nodeId, state);
    } else {
      const prevElements = state.elements || {};
      const elementsChanged = keysToCheck.some((key) => prevElements[key] !== elements[key]);
      if (elementsChanged) {
        if (Array.isArray(state.cleanup)) {
          for (const fn of state.cleanup) {
            try { fn(); } catch (_) { /* ignore */ }
          }
        }
        state.cleanup = [];
        state._bound = false;
      }
    }

    state.elements = elements;

    if (!state._bound && elements.root) {
      bindStateHandlers(state);
      state._bound = true;
    }

    return state;
  }

  function bindStateHandlers(state) {
    const nodeId = state.nodeId;
    const elements = state.elements || {};
    const cleanup = state.cleanup = Array.isArray(state.cleanup) ? state.cleanup : [];

    const bind = (elm, evt, handler) => {
      if (!elm || typeof elm.addEventListener !== 'function') return;
      elm.addEventListener(evt, handler);
      cleanup.push(() => {
        try { elm.removeEventListener(evt, handler); } catch (_) { /* ignore */ }
      });
    };

    bind(elements.connect, 'click', (e) => {
      e.preventDefault();
      openSession(nodeId);
    });
    bind(elements.close, 'click', (e) => {
      e.preventDefault();
      closeSession(nodeId);
    });
    bind(elements.nav, 'click', (e) => {
      e.preventDefault();
      performNavigate(nodeId);
    });
    bind(elements.back, 'click', (e) => {
      e.preventDefault();
      performBack(nodeId);
    });
    bind(elements.forward, 'click', (e) => {
      e.preventDefault();
      performForward(nodeId);
    });
    bind(elements.click, 'click', (e) => {
      e.preventDefault();
      performClick(nodeId);
    });
    bind(elements.type, 'click', (e) => {
      e.preventDefault();
      performType(nodeId);
    });
    bind(elements.scroll, 'click', (e) => {
      e.preventDefault();
      performScroll(nodeId);
    });
    bind(elements.scrollUp, 'click', (e) => {
      e.preventDefault();
      performScrollUp(nodeId);
    });
    bind(elements.scrollDown, 'click', (e) => {
      e.preventDefault();
      performScrollDown(nodeId);
    });
    bind(elements.screenshot, 'click', (e) => {
      e.preventDefault();
      captureScreenshot(nodeId);
    });
    bind(elements.dom, 'click', (e) => {
      e.preventDefault();
      fetchDom(nodeId);
    });
    bind(elements.url, 'change', (e) => {
      state.inputs.url = asString(e.target?.value || '').trim();
    });
    bind(elements.selector, 'change', (e) => {
      state.inputs.selector = asString(e.target?.value || '').trim();
    });
    bind(elements.text, 'change', (e) => {
      state.inputs.text = asString(e.target?.value || '');
    });
    bind(elements.amount, 'change', (e) => {
      state.inputs.amount = asNumber(e.target?.value, 600);
    });

    const finishPointer = (event, canceled = false) => {
      const info = state.dragInfo;
      const img = state.elements?.frame;
      state.dragInfo = null;
      if (img) {
        try { img.releasePointerCapture?.(event.pointerId); } catch (_) { /* ignore */ }
      }
      if (!info || (info.pointerId !== undefined && info.pointerId !== event.pointerId) || canceled) return;
      const rect = info.rect;
      const naturalW = info.naturalW || rect.width;
      const naturalH = info.naturalH || rect.height;
      const endX = event.clientX - rect.left;
      const endY = event.clientY - rect.top;
      const distance = Math.hypot(event.clientX - info.startClientX, event.clientY - info.startClientY);
      if (distance > 5) {
        performDrag(nodeId, {
          startX: Math.max(0, Math.round(info.x)),
          startY: Math.max(0, Math.round(info.y)),
          endX: Math.max(0, Math.round(endX)),
          endY: Math.max(0, Math.round(endY)),
          viewportW: Math.round(rect.width),
          viewportH: Math.round(rect.height),
          naturalW,
          naturalH
        });
        return;
      }
      performClickXY(nodeId, {
        x: Math.max(0, Math.round(info.x)),
        y: Math.max(0, Math.round(info.y)),
        viewportW: Math.round(rect.width),
        viewportH: Math.round(rect.height),
        naturalW,
        naturalH
      });
    };

    bind(elements.frame, 'pointerdown', (e) => {
      if (!state.lastFrameMeta) return;
      const img = state.elements?.frame;
      if (!img) return;
      const rect = img.getBoundingClientRect();
      if (!rect.width || !rect.height) return;
      e.preventDefault();
      const naturalW = img.naturalWidth || state.lastFrameMeta?.width || rect.width;
      const naturalH = img.naturalHeight || state.lastFrameMeta?.height || rect.height;
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;
      state.dragInfo = {
        pointerId: e.pointerId,
        startClientX: e.clientX,
        startClientY: e.clientY,
        x,
        y,
        rect,
        naturalW,
        naturalH,
        startTime: performance.now()
      };
      try { img.setPointerCapture?.(e.pointerId); } catch (_) { /* ignore */ }
    });

    bind(elements.frame, 'pointerup', (e) => finishPointer(e, false));
    bind(elements.frame, 'pointercancel', (e) => finishPointer(e, true));
    bind(elements.frame, 'pointerleave', (e) => {
      if (state.dragInfo) finishPointer(e, true);
    });

    if (elements.frame) {
      elements.frame.draggable = false;
      elements.frame.setAttribute('draggable', 'false');
      elements.frame.style.userSelect = 'none';
      elements.frame.style.webkitUserSelect = 'none';
      elements.frame.style.pointerEvents = 'auto';
      const dragBlock = (ev) => ev.preventDefault();
      elements.frame.addEventListener('dragstart', dragBlock);
      cleanup.push(() => {
        try { elements.frame.removeEventListener('dragstart', dragBlock); } catch (_) { /* ignore */ }
      });
      const wheelHandler = (e) => {
        if (!state.lastFrameMeta) return;
        e.preventDefault();
        const img = elements.frame;
        const rect = img.getBoundingClientRect();
        if (!rect.width || !rect.height) return;
        const naturalW = state.lastFrameMeta?.width || img.naturalWidth || rect.width;
        const naturalH = state.lastFrameMeta?.height || img.naturalHeight || rect.height;
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        const mode = e.deltaMode === 1 ? 16 : e.deltaMode === 2 ? 240 : 1;
        const deltaX = (e.deltaX || 0) * mode;
        const deltaY = (e.deltaY || 0) * mode;
        if (!deltaX && !deltaY) return;
        const now = performance.now();
        if (now - state.lastWheelTs < 12) return;
        state.lastWheelTs = now;
        performScrollPoint(nodeId, {
          x: Math.max(0, Math.round(x)),
          y: Math.max(0, Math.round(y)),
          deltaX,
          deltaY,
          viewportW: Math.round(rect.width),
          viewportH: Math.round(rect.height),
          naturalW,
          naturalH
        });
      };
      elements.frame.addEventListener('wheel', wheelHandler, { passive: false });
      cleanup.push(() => {
        try { elements.frame.removeEventListener('wheel', wheelHandler, { passive: false }); } catch (_) {
          try { elements.frame.removeEventListener('wheel', wheelHandler); } catch (_) { /* ignore */ }
        }
      });
    }

    if (elements.preview) {
      elements.preview.style.touchAction = 'none';
      elements.preview.style.userSelect = 'none';
    }
  }

  function updateSessionLabel(state) {
    const sidText = getActiveSid(state) || '(none)';
    if (state.elements.sid) state.elements.sid.textContent = sidText;
  }

  function setSessionId(nodeId, sid, { persist = true, announce = true } = {}) {
    const state = ensureState(nodeId);
    if (!state) return;
    state.sessionId = sid || '';
    if (persist) {
      updateConfig(nodeId, { lastSid: state.sessionId });
    }
    updateSessionLabel(state);
    refreshControls(nodeId);
    if (announce) {
      appendLog(nodeId, `session: ${state.sessionId || '(cleared)'}`);
    }
  }

  function setManualSid(nodeId, sid) {
    const state = ensureState(nodeId);
    if (!state) return;
    state.manualSid = sid || '';
    updateConfig(nodeId, { sid: state.manualSid });
    updateSessionLabel(state);
    subscribeEvents(nodeId);
  }

  function getActiveSid(state) {
    return state.sessionId || state.manualSid || state.inputs.sid || '';
  }

  function refreshControls(nodeId) {
    const state = ensureState(nodeId);
    if (!state) return;
    const sid = getActiveSid(state);
    const hasSession = Boolean(state.sessionId);
    const elements = state.elements;
    const disableIfNoSid = (elm) => {
      if (!elm) return;
      elm.disabled = !sid;
    };
    disableIfNoSid(elements.nav);
    disableIfNoSid(elements.back);
    disableIfNoSid(elements.forward);
    disableIfNoSid(elements.click);
    disableIfNoSid(elements.type);
    disableIfNoSid(elements.scroll);
    disableIfNoSid(elements.scrollUp);
    disableIfNoSid(elements.scrollDown);
    disableIfNoSid(elements.screenshot);
    disableIfNoSid(elements.dom);
    if (elements.close) elements.close.disabled = !hasSession;
  }

  function boolConfig(nodeId, key, fallback = false) {
    return boolFromConfig(config(nodeId)[key], fallback);
  }

  function scheduleAutoCapture(nodeId) {
    const state = ensureState(nodeId);
    if (!state) return;
    if (state.captureTimer) {
      clearInterval(state.captureTimer);
      state.captureTimer = null;
    }
    const cfg = config(nodeId);
    state.autoCapture = boolFromConfig(cfg.autoCapture, false);
    const fpsRaw = Number(cfg.frameRate || 0);
    const fps = Number.isFinite(fpsRaw) ? Math.max(0, fpsRaw) : 0;
    if (!state.autoCapture || fps <= 0) {
      state.capturePending = false;
      return;
    }
    const intervalMs = Math.max(1000 / fps, 100);
    state.captureTimer = setInterval(() => {
      if (state.capturePending) return;
      state.capturePending = true;
      captureScreenshot(nodeId, { silent: true }).finally(() => {
        const s = ensureState(nodeId);
        if (s) s.capturePending = false;
      });
    }, intervalMs);
  }

  function requestEnv(nodeId) {
    const cfg = config(nodeId);
    const base = normalizeBase(cfg.base) || 'http://127.0.0.1:8130';
    const relay = (cfg.relay || '').trim();
    const api = (cfg.api || '').trim();
    const service = serviceNameFromConfig(cfg);
    const viaNkn = Boolean(relay);
    return { cfg, base, relay, api, service, viaNkn };
  }

  async function openSession(nodeId) {
    const state = ensureState(nodeId);
    if (!state) return;
    const { base, relay, api, viaNkn, service } = requestEnv(nodeId);
    const headless = boolConfig(nodeId, 'headless', true);
    if (!viaNkn) {
      try {
        await ensureLocalNetworkAccess({ requireGesture: true });
      } catch (_) {
        // ignore permission rejection; fetch may still work
      }
    }
    const path = '/session/start';
    setBusy(state, true);
    setStatus(nodeId, 'Starting browser…', 'pending');
    try {
      const res = await Net.postJSON(base, path, { headless }, api, viaNkn, relay, 60000);
      const sid = res?.session_id || res?.sessionId || '';
      if (sid) {
        state.manualSid = '';
        updateConfig(nodeId, { sid: '', lastSid: sid });
      }
      setSessionId(nodeId, sid, { persist: true, announce: true });
      setStatus(nodeId, 'Browser ready', 'ok');
      appendLog(nodeId, res?.message || 'browser started');
      setBadge('Web scraper ready');
      subscribeEvents(nodeId);
    } catch (err) {
      const message = err?.message || String(err);
      setStatus(nodeId, `Start failed: ${message}`, 'err');
      appendLog(nodeId, `error: ${message}`, 'error');
      log(`[webscraper:${nodeId}] ${message}`);
    } finally {
      setBusy(state, false);
      refreshControls(nodeId);
    }
  }

  async function closeSession(nodeId) {
    const state = ensureState(nodeId);
    if (!state) return;
    const { base, relay, api, viaNkn } = requestEnv(nodeId);
    setBusy(state, true);
    try {
      await Net.postJSON(base, '/session/close', {}, api, viaNkn, relay, 30000);
      stopEvents(state);
      setSessionId(nodeId, '', { persist: true, announce: true });
      setStatus(nodeId, 'Browser closed', 'warn');
      appendLog(nodeId, 'browser closed');
    } catch (err) {
      const message = err?.message || String(err);
      setStatus(nodeId, `Close failed: ${message}`, 'err');
      appendLog(nodeId, `error: ${message}`, 'error');
    } finally {
      setBusy(state, false);
      refreshControls(nodeId);
    }
  }

  async function performNavigate(nodeId, urlOverride) {
    const state = ensureState(nodeId);
    if (!state) return;
    const sid = getActiveSid(state);
    if (!sid) {
      setStatus(nodeId, 'No session id', 'warn');
      return;
    }
    const url = urlOverride || state.inputs.url || asString(state.elements.url?.value || '').trim();
    if (!url) {
      setStatus(nodeId, 'Enter a URL first', 'warn');
      return;
    }
    const { base, relay, api, viaNkn } = requestEnv(nodeId);
    const body = { url, sid };
    setBusy(state, true);
    setStatus(nodeId, `Navigating to ${url}`, 'pending');
    try {
      const res = await Net.postJSON(base, '/navigate', body, api, viaNkn, relay, 45000);
      appendLog(nodeId, res?.msg || `navigate ${url}`);
      setStatus(nodeId, 'Navigation done', 'ok');
      if (state.autoScreenshot) await captureScreenshot(nodeId, { silent: true });
    } catch (err) {
      const message = err?.message || String(err);
      setStatus(nodeId, `Navigate failed: ${message}`, 'err');
      appendLog(nodeId, `error: ${message}`, 'error');
    } finally {
      setBusy(state, false);
    }
  }

  async function performClick(nodeId, selectorOverride) {
    const state = ensureState(nodeId);
    if (!state) return;
    const sid = getActiveSid(state);
    if (!sid) {
      setStatus(nodeId, 'No session id', 'warn');
      return;
    }
    const selector = selectorOverride || state.inputs.selector || asString(state.elements.selector?.value || '').trim();
    if (!selector) {
      setStatus(nodeId, 'Selector required', 'warn');
      return;
    }
    const { base, relay, api, viaNkn } = requestEnv(nodeId);
    setBusy(state, true);
    setStatus(nodeId, `Click ${selector}`, 'pending');
    try {
      const res = await Net.postJSON(base, '/click', { selector, sid }, api, viaNkn, relay, 30000);
      appendLog(nodeId, res?.msg || `click ${selector}`);
      setStatus(nodeId, 'Click ok', 'ok');
      if (state.autoScreenshot) await captureScreenshot(nodeId, { silent: true });
    } catch (err) {
      const message = err?.message || String(err);
      setStatus(nodeId, `Click failed: ${message}`, 'err');
      appendLog(nodeId, `error: ${message}`, 'error');
    } finally {
      setBusy(state, false);
    }
  }

  async function performDrag(nodeId, coords) {
    const state = ensureState(nodeId);
    if (!state) return;
    const sid = getActiveSid(state);
    if (!sid) {
      setStatus(nodeId, 'No session id', 'warn');
      return;
    }
    const { base, relay, api, viaNkn } = requestEnv(nodeId);
    const body = {
      sid,
      startX: coords.startX,
      startY: coords.startY,
      endX: coords.endX,
      endY: coords.endY,
      viewportW: coords.viewportW,
      viewportH: coords.viewportH,
      naturalW: coords.naturalW,
      naturalH: coords.naturalH
    };
    if ((!body.viewportW || !body.viewportH) && state.lastFrameMeta) {
      body.viewportW = body.viewportW || state.lastFrameMeta.width || 0;
      body.viewportH = body.viewportH || state.lastFrameMeta.height || 0;
    }
    if ((!body.naturalW || !body.naturalH) && state.lastFrameMeta) {
      body.naturalW = body.naturalW || state.lastFrameMeta.width || 0;
      body.naturalH = body.naturalH || state.lastFrameMeta.height || 0;
    }
    if (!body.viewportW || !body.viewportH) {
      appendLog(nodeId, 'drag skipped: missing viewport size', 'warn');
      return;
    }
    setBusy(state, true);
    setStatus(nodeId, 'Drag…', 'pending');
    try {
      const res = await Net.postJSON(base, '/drag', body, api, viaNkn, relay, 45000);
      appendLog(nodeId, res?.message || 'drag completed');
      setStatus(nodeId, res?.message || 'Drag ok', 'ok');
      if (state.autoScreenshot) await captureScreenshot(nodeId, { silent: true });
    } catch (err) {
      const message = err?.message || String(err);
      setStatus(nodeId, `Drag failed: ${message}`, 'err');
      appendLog(nodeId, `error: ${message}`, 'error');
    } finally {
      setBusy(state, false);
    }
  }

  async function performBack(nodeId) {
    const state = ensureState(nodeId);
    if (!state) return;
    const sid = getActiveSid(state);
    if (!sid) {
      setStatus(nodeId, 'No session id', 'warn');
      return;
    }
    const { base, relay, api, viaNkn } = requestEnv(nodeId);
    if (!viaNkn) {
      try {
        await ensureLocalNetworkAccess({ requireGesture: false });
      } catch (_) {
        // ignore
      }
    }
    setBusy(state, true);
    setStatus(nodeId, 'Going back…', 'pending');
    try {
      const res = await Net.postJSON(base, '/history/back', { sid }, api, viaNkn, relay, 30000);
      appendLog(nodeId, res?.message || 'navigated back');
      setStatus(nodeId, 'Went back', 'ok');
      if (state.autoScreenshot) await captureScreenshot(nodeId, { silent: true });
    } catch (err) {
      const message = err?.message || String(err);
      setStatus(nodeId, `Back failed: ${message}`, 'err');
      appendLog(nodeId, `error: ${message}`, 'error');
    } finally {
      setBusy(state, false);
    }
  }

  async function performForward(nodeId) {
    const state = ensureState(nodeId);
    if (!state) return;
    const sid = getActiveSid(state);
    if (!sid) {
      setStatus(nodeId, 'No session id', 'warn');
      return;
    }
    const { base, relay, api, viaNkn } = requestEnv(nodeId);
    if (!viaNkn) {
      try {
        await ensureLocalNetworkAccess({ requireGesture: false });
      } catch (_) {
        // ignore
      }
    }
    setBusy(state, true);
    setStatus(nodeId, 'Going forward…', 'pending');
    try {
      const res = await Net.postJSON(base, '/history/forward', { sid }, api, viaNkn, relay, 30000);
      appendLog(nodeId, res?.message || 'navigated forward');
      setStatus(nodeId, 'Went forward', 'ok');
      if (state.autoScreenshot) await captureScreenshot(nodeId, { silent: true });
    } catch (err) {
      const message = err?.message || String(err);
      setStatus(nodeId, `Forward failed: ${message}`, 'err');
      appendLog(nodeId, `error: ${message}`, 'error');
    } finally {
      setBusy(state, false);
    }
  }

  async function performScrollUp(nodeId, amountOverride) {
    const state = ensureState(nodeId);
    if (!state) return;
    const sid = getActiveSid(state);
    if (!sid) {
      setStatus(nodeId, 'No session id', 'warn');
      return;
    }
    const amount = Math.abs(Number.isFinite(amountOverride) ? amountOverride : state.inputs.amount || 600);
    const { base, relay, api, viaNkn } = requestEnv(nodeId);
    setBusy(state, true);
    setStatus(nodeId, `Scroll up ${amount}`, 'pending');
    try {
      const res = await Net.postJSON(base, '/scroll/up', { sid, amount }, api, viaNkn, relay, 30000);
      appendLog(nodeId, res?.message || `scroll up ${amount}`);
      setStatus(nodeId, 'Scroll up done', 'ok');
      if (state.autoScreenshot) await captureScreenshot(nodeId, { silent: true });
    } catch (err) {
      const message = err?.message || String(err);
      setStatus(nodeId, `Scroll up failed: ${message}`, 'err');
      appendLog(nodeId, `error: ${message}`, 'error');
    } finally {
      setBusy(state, false);
    }
  }

  async function performScrollDown(nodeId, amountOverride) {
    const state = ensureState(nodeId);
    if (!state) return;
    const sid = getActiveSid(state);
    if (!sid) {
      setStatus(nodeId, 'No session id', 'warn');
      return;
    }
    const amount = Math.abs(Number.isFinite(amountOverride) ? amountOverride : state.inputs.amount || 600);
    const { base, relay, api, viaNkn } = requestEnv(nodeId);
    setBusy(state, true);
    setStatus(nodeId, `Scroll down ${amount}`, 'pending');
    try {
      const res = await Net.postJSON(base, '/scroll/down', { sid, amount }, api, viaNkn, relay, 30000);
      appendLog(nodeId, res?.message || `scroll down ${amount}`);
      setStatus(nodeId, 'Scroll down done', 'ok');
      if (state.autoScreenshot) await captureScreenshot(nodeId, { silent: true });
    } catch (err) {
      const message = err?.message || String(err);
      setStatus(nodeId, `Scroll down failed: ${message}`, 'err');
      appendLog(nodeId, `error: ${message}`, 'error');
    } finally {
      setBusy(state, false);
    }
  }

  async function performScrollPoint(nodeId, payload) {
    const state = ensureState(nodeId);
    if (!state) return;
    const sid = getActiveSid(state);
    if (!sid) return;
    const { base, relay, api, viaNkn } = requestEnv(nodeId);
    const body = {
      sid,
      x: payload.x,
      y: payload.y,
      deltaX: payload.deltaX,
      deltaY: payload.deltaY,
      viewportW: payload.viewportW,
      viewportH: payload.viewportH,
      naturalW: payload.naturalW,
      naturalH: payload.naturalH
    };
    try {
      await Net.postJSON(base, '/scroll/point', body, api, viaNkn, relay, 20000);
    } catch (err) {
      const message = err?.message || String(err);
      appendLog(nodeId, `scroll point failed: ${message}`, 'warn');
    }
  }

  async function performType(nodeId, selectorOverride, textOverride) {
    const state = ensureState(nodeId);
    if (!state) return;
    const sid = getActiveSid(state);
    if (!sid) {
      setStatus(nodeId, 'No session id', 'warn');
      return;
    }
    const selector = selectorOverride || state.inputs.selector || asString(state.elements.selector?.value || '').trim();
    const text = textOverride !== undefined ? textOverride : state.inputs.text || asString(state.elements.text?.value || '');
    if (!selector) {
      setStatus(nodeId, 'Selector required', 'warn');
      return;
    }
    const { base, relay, api, viaNkn } = requestEnv(nodeId);
    setBusy(state, true);
    setStatus(nodeId, `Type into ${selector}`, 'pending');
    try {
      const res = await Net.postJSON(base, '/type', { selector, text, sid }, api, viaNkn, relay, 45000);
      appendLog(nodeId, res?.msg || `type ${selector}`);
      setStatus(nodeId, 'Type ok', 'ok');
      if (state.autoScreenshot) await captureScreenshot(nodeId, { silent: true });
    } catch (err) {
      const message = err?.message || String(err);
      setStatus(nodeId, `Type failed: ${message}`, 'err');
      appendLog(nodeId, `error: ${message}`, 'error');
    } finally {
      setBusy(state, false);
    }
  }

  async function performScroll(nodeId, amountOverride) {
    const state = ensureState(nodeId);
    if (!state) return;
    const sid = getActiveSid(state);
    if (!sid) {
      setStatus(nodeId, 'No session id', 'warn');
      return;
    }
    const amount = Number.isFinite(amountOverride) ? amountOverride : state.inputs.amount;
    const { base, relay, api, viaNkn } = requestEnv(nodeId);
    setBusy(state, true);
    setStatus(nodeId, `Scroll ${amount}`, 'pending');
    try {
      const res = await Net.postJSON(base, '/scroll', { amount, sid }, api, viaNkn, relay, 30000);
      appendLog(nodeId, res?.msg || `scroll ${amount}`);
      setStatus(nodeId, 'Scroll ok', 'ok');
      if (state.autoScreenshot) await captureScreenshot(nodeId, { silent: true });
    } catch (err) {
      const message = err?.message || String(err);
      setStatus(nodeId, `Scroll failed: ${message}`, 'err');
      appendLog(nodeId, `error: ${message}`, 'error');
    } finally {
      setBusy(state, false);
    }
  }

  async function performClickXY(nodeId, payload) {
    const state = ensureState(nodeId);
    if (!state) return;
    const sid = getActiveSid(state);
    if (!sid) {
      setStatus(nodeId, 'No session id', 'warn');
      return;
    }
    const body = { ...payload, sid };
    const { base, relay, api, viaNkn } = requestEnv(nodeId);
    setBusy(state, true);
    setStatus(nodeId, 'Click (xy)', 'pending');
    try {
      const res = await Net.postJSON(base, '/click_xy', body, api, viaNkn, relay, 45000);
      appendLog(nodeId, res?.message || 'click_xy');
      setStatus(nodeId, 'Click ok', 'ok');
      if (state.autoScreenshot) await captureScreenshot(nodeId, { silent: true });
    } catch (err) {
      const message = err?.message || String(err);
      setStatus(nodeId, `Click xy failed: ${message}`, 'err');
      appendLog(nodeId, `error: ${message}`, 'error');
    } finally {
      setBusy(state, false);
    }
  }

  async function fetchDom(nodeId) {
    const state = ensureState(nodeId);
    if (!state) return;
    const sid = getActiveSid(state);
    if (!sid) {
      setStatus(nodeId, 'No session id', 'warn');
      return;
    }
    const { base, relay, api, viaNkn } = requestEnv(nodeId);
    const path = `/dom?sid=${encodeURIComponent(sid)}`;
    setBusy(state, true);
    setStatus(nodeId, 'Fetching DOM…', 'pending');
    try {
      const res = await Net.getJSON(base, path, api, viaNkn, relay);
      const dom = res?.dom || '';
      Router.sendFrom(nodeId, 'dom', { nodeId, sid, dom, length: dom.length, ts: Date.now() });
      updateConfig(nodeId, { lastDom: dom });
      appendLog(nodeId, `dom length ${dom.length}`);
      setStatus(nodeId, 'DOM captured', 'ok');
    } catch (err) {
      const message = err?.message || String(err);
      setStatus(nodeId, `DOM failed: ${message}`, 'err');
      appendLog(nodeId, `error: ${message}`, 'error');
    } finally {
      setBusy(state, false);
    }
  }

  async function captureScreenshot(nodeId, { silent = false } = {}) {
    const state = ensureState(nodeId);
    if (!state) return;
    const sid = getActiveSid(state);
    if (!sid) {
      if (markPending) state.capturePending = false;
      if (!silent) setStatus(nodeId, 'No session id', 'warn');
      return;
    }
    const { base, relay, api, viaNkn } = requestEnv(nodeId);
    const path = `/screenshot?sid=${encodeURIComponent(sid)}`;
    if (!viaNkn) {
      try {
        await ensureLocalNetworkAccess({ requireGesture: false });
      } catch (_) {
        // ignore
      }
    }
    const markPending = silent && !state.capturePending;
    if (markPending) state.capturePending = true;
    if (!silent) {
      setBusy(state, true);
      setStatus(nodeId, 'Capturing screenshot…', 'pending');
    }
    try {
      const res = await Net.getJSON(base, path, api, viaNkn, relay);
      const file = res?.file || '';
      const width = Number(res?.width || 0);
      const height = Number(res?.height || 0);
      const b64 = typeof res?.b64 === 'string' ? res.b64 : '';
      const mime = res?.mime || 'image/png';
      if (file) {
        await loadFrame(nodeId, file, { width, height, mime }, b64);
        if (!silent) appendLog(nodeId, `frame ${file}`);
      }
      if (!silent) setStatus(nodeId, 'Screenshot ready', 'ok');
    } catch (err) {
      const message = err?.message || String(err);
      if (!silent) {
        setStatus(nodeId, `Screenshot failed: ${message}`, 'err');
        appendLog(nodeId, `error: ${message}`, 'error');
      }
    } finally {
      if (!silent) {
        setBusy(state, false);
      }
      if (markPending) state.capturePending = false;
    }
  }

  async function loadFrame(nodeId, file, meta = {}, inlineB64 = '') {
    const state = ensureState(nodeId);
    if (!state) return;
    const prevMeta = state.lastFrameMeta;
    const { base, relay, api, viaNkn } = requestEnv(nodeId);
    let imageUrl = '';
    const mime = meta?.mime || 'image/png';
    let rawData = inlineB64;
    let blob = null;
    if (inlineB64) {
      imageUrl = `data:${mime};base64,${inlineB64}`;
      if (state.previewUrl && state.previewUrl.startsWith('blob:')) {
        try { URL.revokeObjectURL(state.previewUrl); } catch (_) { /* ignore */ }
      }
    } else {
      const url = buildFileUrl(base, file);
      if (!url) return;
      if (!viaNkn) {
        try {
          await ensureLocalNetworkAccess({ requireGesture: false });
        } catch (_) {
          // ignore
        }
      }
      try {
        blob = await Net.fetchBlob(url, viaNkn, relay, api);
      } catch (err) {
        appendLog(nodeId, `frame fetch failed: ${err?.message || err}`, 'error');
        return;
      }
      if (!blob) return;
      if (state.previewUrl && state.previewUrl.startsWith('blob:')) {
        try { URL.revokeObjectURL(state.previewUrl); } catch (_) { /* ignore */ }
      }
      imageUrl = URL.createObjectURL(blob);
      try {
        rawData = await blobToBase64(blob);
      } catch (err) {
        rawData = '';
      }
    }
    state.previewUrl = imageUrl;
    if (state.elements.frame) {
      state.elements.frame.src = imageUrl;
      state.elements.frame.classList.add('active');
    }
    if (state.elements.placeholder) {
      state.elements.placeholder.classList.add('hidden');
    }
    const activeSid = getActiveSid(state);
    const payload = {
      nodeId,
      sid: activeSid,
      file,
      width: meta.width || (prevMeta?.width ?? 0),
      height: meta.height || (prevMeta?.height ?? 0),
      blobUrl: imageUrl,
      mime,
      b64: rawData,
      ts: Date.now()
    };
    state.lastFrameMeta = payload;
    const cfg = NodeStore.ensure(nodeId, 'WebScraper').config || {};
    const frameMode = (cfg.frameOutputMode || state.frameOutputMode || 'wrapped').toLowerCase() === 'raw' ? 'raw' : 'wrapped';
    if (frameMode === 'raw') {
      if (rawData) Router.sendFrom(nodeId, 'frame', rawData);
      else Router.sendFrom(nodeId, 'frame', payload);
    } else {
      Router.sendFrom(nodeId, 'frame', payload);
    }
    if (rawData) {
      Router.sendFrom(nodeId, 'rawFrame', rawData);
    }
    updateConfig(nodeId, { lastFrame: file });
  }

  function buildFileUrl(base, file) {
    if (!file) return '';
    if (/^https?:\/\//i.test(file)) return file;
    const prefix = normalizeBase(base);
    if (!prefix) return '';
    if (file.startsWith('/')) return `${prefix}${file}`;
    return `${prefix}/${file}`;
  }

  function stopEvents(state) {
    state.eventGen += 1;
    if (state.eventsCancel) {
      try { state.eventsCancel(); } catch (_) { /* ignore */ }
    }
    state.eventsCancel = null;
  }

  function createSseParser(nodeId, localGen) {
    let dataLines = [];
    return {
      push(line) {
        if (stateMap.get(nodeId)?.eventGen !== localGen) return;
        const trimmed = (line || '').replace(/\r$/, '');
        if (trimmed === '') {
          if (!dataLines.length) return;
          const payload = dataLines.join('\n');
          dataLines = [];
          if (!payload) return;
          try {
            const evt = JSON.parse(payload);
            handleEvent(nodeId, evt);
          } catch (err) {
            appendLog(nodeId, `event parse failed: ${err?.message || err}`, 'warn');
          }
          return;
        }
        if (trimmed.startsWith('data:')) {
          dataLines.push(trimmed.slice(5).trim());
        }
      }
    };
  }

  function handleEvent(nodeId, evt) {
    const state = ensureState(nodeId);
    if (!state || !evt || typeof evt !== 'object') return;
    const type = (evt.type || '').toLowerCase();
    if (type === 'status') {
      const msg = evt.msg || evt.message || '';
      setStatus(nodeId, msg, evt.level || 'info');
      appendLog(nodeId, msg);
    } else if (type === 'frame') {
      loadFrame(nodeId, evt.file || '', { width: evt.width, height: evt.height, mime: evt.mime }, typeof evt.b64 === 'string' ? evt.b64 : '');
    } else if (type === 'dom') {
      appendLog(nodeId, `dom event (${evt.chars || 0} chars)`);
    } else if (type) {
      appendLog(nodeId, `${type}: ${JSON.stringify(evt)}`);
    }
  }

  function subscribeEvents(nodeId) {
    const state = ensureState(nodeId);
    if (!state) return;
    const sid = getActiveSid(state);
    if (!sid) {
      stopEvents(state);
      return;
    }
    const { base, relay, api, viaNkn, service } = requestEnv(nodeId);
    stopEvents(state);
    const localGen = ++state.eventGen;
    const parser = createSseParser(nodeId, localGen);
    if (viaNkn) {
      const headers = Net.auth({ Accept: 'text/event-stream', 'X-Relay-Stream': 'lines' }, api);
      const req = {
        service,
        path: `/events?sid=${encodeURIComponent(sid)}`,
        method: 'GET',
        headers,
        stream: 'lines',
        timeout_ms: 300000
      };
      let cancelled = false;
      const promise = Net.nknStream(req, relay, {
        onLine: (entry) => {
          if (cancelled) return;
          const line = typeof entry === 'string' ? entry : entry?.line || '';
          parser.push(line);
        },
        onEnd: () => {
          if (cancelled) return;
          setTimeout(() => {
            if (stateMap.get(nodeId)?.eventGen === localGen) subscribeEvents(nodeId);
          }, 500);
        },
        onError: (err) => {
          if (cancelled) return;
          appendLog(nodeId, `events error: ${err?.message || err}`, 'warn');
          setTimeout(() => {
            if (stateMap.get(nodeId)?.eventGen === localGen) subscribeEvents(nodeId);
          }, 1500);
        }
      }, 300000);
      promise.catch((err) => {
        appendLog(nodeId, `events stream failed: ${err?.message || err}`, 'warn');
      });
      state.eventsCancel = () => {
        cancelled = true;
      };
    } else {
      const controller = new AbortController();
      const headers = Net.auth({ Accept: 'text/event-stream' }, api);
      const url = `${normalizeBase(base)}/events?sid=${encodeURIComponent(sid)}`;
      const run = async () => {
        try {
          await ensureLocalNetworkAccess({ requireGesture: false });
        } catch (_) {
          // ignore permission failures
        }
        try {
          const res = await fetch(url, { headers, signal: controller.signal });
          if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
          const reader = res.body?.getReader();
          if (!reader) throw new Error('no response body');
          const decoder = new TextDecoder();
          let buffer = '';
          while (true) {
            const { value, done } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            let idx = buffer.indexOf('\n');
            while (idx >= 0) {
              const line = buffer.slice(0, idx);
              buffer = buffer.slice(idx + 1);
              parser.push(line);
              idx = buffer.indexOf('\n');
            }
          }
          parser.push('');
        } catch (err) {
          if (controller.signal.aborted) return;
          appendLog(nodeId, `events error: ${err?.message || err}`, 'warn');
        } finally {
          if (!controller.signal.aborted && stateMap.get(nodeId)?.eventGen === localGen) {
            setTimeout(() => subscribeEvents(nodeId), 1500);
          }
        }
      };
      run().catch(() => {});
      state.eventsCancel = () => controller.abort();
    }
  }

  function hydrateState(nodeId) {
    const state = ensureState(nodeId);
    if (!state) return;
    const cfg = config(nodeId);
    state.inputs.url = cfg.lastUrl || '';
    state.inputs.selector = cfg.lastSelector || '';
    state.inputs.text = cfg.lastText || '';
    state.inputs.amount = Number(cfg.lastAmount || 600);
    state.inputs.sid = cfg.sid || '';
    state.autoScreenshot = boolFromConfig(cfg.autoScreenshot, false);
    state.autoCapture = boolFromConfig(cfg.autoCapture, false);
    state.frameOutputMode = (cfg.frameOutputMode || 'wrapped').toLowerCase() === 'raw' ? 'raw' : 'wrapped';

    if (state.elements.url && state.inputs.url) state.elements.url.value = state.inputs.url;
    if (state.elements.selector && state.inputs.selector) state.elements.selector.value = state.inputs.selector;
    if (state.elements.text && state.inputs.text) state.elements.text.value = state.inputs.text;
    if (state.elements.amount) state.elements.amount.value = String(state.inputs.amount);

    if (cfg.lastFrame && !state.previewUrl) {
      loadFrame(nodeId, cfg.lastFrame, {});
    }
    if (cfg.lastSid) setSessionId(nodeId, cfg.lastSid, { persist: false, announce: false });
    if (cfg.sid) {
      state.manualSid = cfg.sid;
      updateSessionLabel(state);
    }
    refreshControls(nodeId);
    scheduleAutoCapture(nodeId);
  }

  function init(nodeId) {
    hydrateState(nodeId);
    subscribeEvents(nodeId);
  }

  function refresh(nodeId) {
    const state = ensureState(nodeId);
    if (!state) return;
    const cfg = config(nodeId);
    state.autoScreenshot = boolFromConfig(cfg.autoScreenshot, false);
    state.autoCapture = boolFromConfig(cfg.autoCapture, false);
    state.frameOutputMode = (cfg.frameOutputMode || 'wrapped').toLowerCase() === 'raw' ? 'raw' : 'wrapped';
    const overrideSid = (cfg.sid || '').trim();
    if (overrideSid !== state.manualSid) {
      state.manualSid = overrideSid;
      updateSessionLabel(state);
      subscribeEvents(nodeId);
    }
    refreshControls(nodeId);
    scheduleAutoCapture(nodeId);
  }

  function onInput(nodeId, portName, payload) {
    const state = ensureState(nodeId);
    if (!state) return;
    switch (portName) {
      case 'url':
        state.inputs.url = asString(payload).trim();
        if (state.elements.url && state.inputs.url) state.elements.url.value = state.inputs.url;
        updateConfig(nodeId, { lastUrl: state.inputs.url });
        break;
      case 'selector':
        state.inputs.selector = asString(payload).trim();
        if (state.elements.selector && state.inputs.selector) state.elements.selector.value = state.inputs.selector;
        updateConfig(nodeId, { lastSelector: state.inputs.selector });
        break;
      case 'text':
        state.inputs.text = asString(payload);
        if (state.elements.text) state.elements.text.value = state.inputs.text;
        updateConfig(nodeId, { lastText: state.inputs.text });
        break;
      case 'amount':
        state.inputs.amount = asNumber(payload, state.inputs.amount || 600);
        if (state.elements.amount) state.elements.amount.value = String(state.inputs.amount);
        updateConfig(nodeId, { lastAmount: state.inputs.amount });
        break;
      case 'sid':
        state.inputs.sid = asString(payload).trim();
        setManualSid(nodeId, state.inputs.sid);
        break;
      case 'xy':
        if (payload && typeof payload === 'object' && 'x' in payload && 'y' in payload) {
          performClickXY(nodeId, {
            x: asNumber(payload.x),
            y: asNumber(payload.y),
            viewportW: asNumber(payload.viewportW || payload.viewportw || payload.viewport_width || 0),
            viewportH: asNumber(payload.viewportH || payload.viewporth || payload.viewport_height || 0),
            naturalW: asNumber(payload.naturalW || payload.naturalWidth || payload.width || 0),
            naturalH: asNumber(payload.naturalH || payload.naturalHeight || payload.height || 0)
          });
        }
        break;
      case 'action':
        handleAction(nodeId, payload);
        break;
      default:
        break;
    }
  }

  function handleAction(nodeId, payload) {
    const action = asAction(payload);
    if (!action) return;
    switch (action) {
      case 'open':
        openSession(nodeId);
        break;
      case 'close':
        closeSession(nodeId);
        break;
      case 'nav':
        performNavigate(nodeId, payload?.url || payload?.target);
        break;
      case 'back':
        performBack(nodeId);
        break;
      case 'forward':
        performForward(nodeId);
        break;
      case 'click':
        performClick(nodeId, payload?.selector);
        break;
      case 'type':
        performType(nodeId, payload?.selector, payload?.text);
        break;
      case 'scroll':
        performScroll(nodeId, payload?.amount);
        break;
      case 'scroll_up':
        performScrollUp(nodeId, payload?.amount);
        break;
      case 'scroll_down':
        performScrollDown(nodeId, payload?.amount);
        break;
      case 'drag':
        if (payload && typeof payload === 'object') {
          const startX = asNumber(payload.startX ?? payload.start_x ?? payload.fromX ?? payload.from_x, NaN);
          const startY = asNumber(payload.startY ?? payload.start_y ?? payload.fromY ?? payload.from_y, NaN);
          const endX = asNumber(payload.endX ?? payload.end_x ?? payload.toX ?? payload.to_x, NaN);
          const endY = asNumber(payload.endY ?? payload.end_y ?? payload.toY ?? payload.to_y, NaN);
          if (!Number.isFinite(startX) || !Number.isFinite(startY) || !Number.isFinite(endX) || !Number.isFinite(endY)) {
            appendLog(nodeId, 'drag action missing coordinates', 'warn');
            break;
          }
          performDrag(nodeId, {
            startX,
            startY,
            endX,
            endY,
            viewportW: asNumber(payload.viewportW ?? payload.viewport_w ?? payload.width ?? 0),
            viewportH: asNumber(payload.viewportH ?? payload.viewport_h ?? payload.height ?? 0),
            naturalW: asNumber(payload.naturalW ?? payload.natural_w ?? payload.naturalWidth ?? payload.width ?? 0),
            naturalH: asNumber(payload.naturalH ?? payload.natural_h ?? payload.naturalHeight ?? payload.height ?? 0)
          });
        }
        break;
      case 'screenshot':
        captureScreenshot(nodeId);
        break;
      case 'dom':
        fetchDom(nodeId);
        break;
      case 'events':
        subscribeEvents(nodeId);
        break;
      default:
        appendLog(nodeId, `unknown action ${action}`, 'warn');
        break;
    }
  }

  return {
    init,
    dispose,
    refresh,
    onInput
  };
}

export { createWebScraper };
