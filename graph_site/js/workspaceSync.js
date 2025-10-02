import { qs } from './utils.js';

function createWorkspaceSync({
  Graph,
  Net,
  CFG,
  saveCFG,
  setBadge,
  log,
  updateTransportButton
}) {
  const state = {
    shareTarget: null,
    incomingOffer: null,
    listenerAttached: false,
    lastShareParam: null,
    qrInitialized: false,
    allowRequest: false
  };

  const els = {
    shareModal: null,
    shareMessage: null,
    shareYes: null,
    shareRequest: null,
    shareNo: null,
    shareClose: null,
    shareBackdrop: null,

    receiveModal: null,
    receiveMessage: null,
    receiveYes: null,
    receiveNo: null,
    receiveClose: null,
    receiveBackdrop: null,

    qrButton: null,
    qrModal: null,
    qrBackdrop: null,
    qrClose: null,
    qrCanvas: null,
    qrHelp: null,
    qrLink: null
  };

  function assignElements() {
    els.shareModal = qs('#syncShareModal');
    els.shareMessage = qs('#syncShareMessage');
    els.shareYes = qs('#syncShareYes');
    els.shareRequest = qs('#syncShareRequest');
    els.shareNo = qs('#syncShareNo');
    els.shareClose = qs('#syncShareClose');
    els.shareBackdrop = qs('#syncShareBackdrop');
    if (els.shareRequest) {
      els.shareRequest.classList.add('hidden');
      els.shareRequest.disabled = true;
    }

    els.receiveModal = qs('#syncReceiveModal');
    els.receiveMessage = qs('#syncReceiveMessage');
    els.receiveYes = qs('#syncReceiveYes');
    els.receiveNo = qs('#syncReceiveNo');
    els.receiveClose = qs('#syncReceiveClose');
    els.receiveBackdrop = qs('#syncReceiveBackdrop');

    els.qrButton = qs('#syncQrButton');
    els.qrModal = qs('#syncQrModal');
    els.qrBackdrop = qs('#syncQrBackdrop');
    els.qrClose = qs('#syncQrClose');
    els.qrCanvas = qs('#syncQrCanvas');
    els.qrHelp = qs('#syncQrHelp');
    els.qrLink = qs('#syncQrLink');
  }

  function stripGraphPrefix(addr) {
    if (!addr) return '';
    return String(addr).replace(/^graph\./i, '');
  }

  function ensureGraphPrefix(addr) {
    const raw = stripGraphPrefix(addr);
    if (!raw) return '';
    if (/^[a-z0-9]+\./i.test(String(addr))) return String(addr);
    return `graph.${raw}`;
  }

  const qrReady = new Promise((resolve) => {
    const done = () => window.QRCode && typeof window.QRCode.toCanvas === 'function';
    if (done()) return resolve();
    const iv = setInterval(() => {
    if (done()) {
        clearInterval(iv);
        resolve();
      }
    }, 50);
    setTimeout(() => {
    if (done()) {
        clearInterval(iv);
        resolve();
      }
    }, 0);
  });

  async function ensureQrReady() {
    await qrReady;
    if (!window.QRCode || typeof window.QRCode.toCanvas !== 'function') {
      throw new Error('QR code library unavailable');
    }
  }

  function showModal(modal) {
    if (!modal) return;
    modal.classList.remove('hidden');
    modal.setAttribute('aria-hidden', 'false');
  }

  function hideModal(modal) {
    if (!modal) return;
    modal.classList.add('hidden');
    modal.setAttribute('aria-hidden', 'true');
  }

  function formatAddress(addr) {
    const raw = stripGraphPrefix(addr);
    if (!raw) return '(unknown)';
    if (raw.length <= 16) return raw;
    return `${raw.slice(0, 8)}…${raw.slice(-6)}`;
  }

  function sanitizeHex(value) {
    if (typeof value !== 'string') return null;
    const trimmed = value.trim().replace(/^\?+/, '');
    if (!trimmed) return null;
    const candidate = trimmed.includes('=') ? trimmed.split('=').pop() : trimmed;
    if (!candidate) return null;
    const normalized = candidate.trim();
    if (/^[0-9a-fA-F]{32,}$/.test(normalized)) return normalized.toLowerCase();
    return null;
  }

  function consumeSearchParam() {
    try {
      const url = new URL(window.location.href);
      let value = url.searchParams.get('sync');
      if (!value) {
        const raw = window.location.search;
        if (raw && raw.length > 1) value = raw.slice(1).split('&')[0];
      }
      const hex = sanitizeHex(value);
      if (hex) {
        state.lastShareParam = hex;
        url.searchParams.delete('sync');
        const newSearch = url.searchParams.toString();
        const newUrl = url.pathname + (newSearch ? `?${newSearch}` : '') + (url.hash || '');
        try {
          window.history.replaceState({}, document.title, newUrl);
        } catch (err) {
          // ignore history failures
        }
      }
      return hex;
    } catch (err) {
      return null;
    }
  }

  function ensureNknTransport() {
    if (CFG.transport === 'nkn') return;
    CFG.transport = 'nkn';
    try {
      saveCFG();
    } catch (err) {
      // ignore storage issues
    }
    if (typeof updateTransportButton === 'function') {
      try {
        updateTransportButton();
      } catch (err) {
        // ignore UI update failures
      }
    }
    setBadge('Switching to NKN for workspace sync…');
    try {
      Net.ensureNkn();
    } catch (err) {
      // ignore
    }
  }

  function waitForNknReady(timeoutMs = 15000) {
    return new Promise((resolve, reject) => {
      const start = Date.now();

      const check = () => {
        if (Net.nkn && Net.nkn.client && Net.nkn.ready) {
          if (!state.listenerAttached) attachListener(Net.nkn.client);
          return resolve(Net.nkn.client);
        }
        if (Date.now() - start > timeoutMs) {
          return reject(new Error('NKN connection timed out'));
        }
        setTimeout(check, 250);
      };

      try {
        Net.ensureNkn();
      } catch (err) {
        // ensureNkn may early exit when transport is http
      }
      check();
    });
  }

  async function ensureNknReady() {
    ensureNknTransport();
    try {
      const client = await waitForNknReady(20000);
      return client;
    } catch (err) {
      setBadge(`NKN unavailable: ${err?.message || err}`, false);
      throw err;
    }
  }

  function attachListener(client) {
    if (!client || state.listenerAttached) return;
    try {
      client.on('message', handleIncomingMessage);
      state.listenerAttached = true;
    } catch (err) {
      log(`[sync] failed to attach listener: ${err?.message || err}`);
    }
  }

  function parseIncoming(raw, payload) {
    let data = payload;
    if (raw && typeof raw === 'object' && raw.payload !== undefined) {
      data = raw.payload;
    }
    try {
      const text = data && data.toString ? data.toString() : String(data);
      return JSON.parse(text);
    } catch (err) {
      return null;
    }
  }

  function handleIncomingMessage(a, b) {
    const msg = parseIncoming(a, b);
    if (!msg) return;
    const type = msg.type || msg.event;
    if (type !== 'workspace-sync' && type !== 'workspace.sync.offer') return;

    const action = msg.action || (msg.event === 'workspace.sync.offer' ? 'offer' : undefined);
    const from = stripGraphPrefix(msg.from || msg.sender || (a && a.src) || '');

    if (action === 'request') {
      if (!from) return;
      promptShare(from, { incoming: true });
      return;
    }

    if (action !== 'offer') return;

    const workspace = msg.workspace || (msg.payload && msg.payload.workspace);
    if (!workspace || typeof workspace !== 'object') return;

    state.incomingOffer = {
      from,
      workspace,
      graphId: msg.graphId || null,
      ts: msg.ts || Date.now()
    };
    presentIncomingOffer();
  }

  function presentIncomingOffer() {
    if (!state.incomingOffer || !els.receiveModal) return;
    const { from } = state.incomingOffer;
    if (els.receiveMessage) {
      els.receiveMessage.textContent = `${formatAddress(from)} wishes to share their workspace with you. Accept?`;
    }
    showModal(els.receiveModal);
  }

  async function generateQr() {
    const client = await ensureNknReady();
    const addressFull = Net.nkn.addr || client.addr || '';
    if (!addressFull) {
      setBadge('Unable to determine NKN address', false);
      return;
    }
    await ensureQrReady().catch((err) => {
      setBadge(err?.message || 'QR unavailable', false);
      throw err;
    });

    const origin = window.location.origin;
    const path = window.location.pathname;
    const base = `${origin}${path}`;
    const addressParam = stripGraphPrefix(addressFull);
    const url = `${base}?sync=${addressParam}`;
    if (els.qrLink) els.qrLink.textContent = url;

    if (els.qrCanvas) {
      try {
        window.QRCode.toCanvas(els.qrCanvas, url, { width: 280 });
      } catch (err) {
        if (els.qrHelp) els.qrHelp.textContent = `QR generation failed: ${err?.message || err}`;
      }
    }
    showModal(els.qrModal);
  }

  function closeQr() {
    hideModal(els.qrModal);
  }

  function closeShareModal() {
    hideModal(els.shareModal);
    state.shareTarget = null;
    state.allowRequest = false;
    if (els.shareRequest) {
      els.shareRequest.classList.remove('hidden');
      els.shareRequest.disabled = false;
    }
  }

  function closeReceiveModal() {
    hideModal(els.receiveModal);
    state.incomingOffer = null;
  }

  function promptShare(targetHex, { incoming = false } = {}) {
    if (!targetHex || !els.shareModal) return;
    const normalized = stripGraphPrefix(targetHex);
    if (!normalized) {
      setBadge('Invalid sync target', false);
      return;
    }
    state.shareTarget = normalized;
    state.allowRequest = !incoming;
    if (els.shareMessage) {
      els.shareMessage.textContent = incoming
        ? `${formatAddress(normalized)} requested your workspace. Share now?`
        : `How would you like to sync with ${formatAddress(normalized)}?`;
    }
    if (els.shareRequest) {
      if (state.allowRequest) {
        els.shareRequest.classList.remove('hidden');
        els.shareRequest.disabled = false;
      } else {
        els.shareRequest.classList.add('hidden');
        els.shareRequest.disabled = true;
      }
    }
    showModal(els.shareModal);
  }

  async function sendWorkspaceToTarget() {
    const targetRaw = stripGraphPrefix(state.shareTarget);
    if (!targetRaw) {
      setBadge('No sync target', false);
      closeShareModal();
      return;
    }
    try {
      const client = await ensureNknReady();
      const from = stripGraphPrefix(Net.nkn.addr || client.addr || '');
      const targetFull = ensureGraphPrefix(targetRaw);
      if (!targetFull) throw new Error('Invalid target address');
      const snapshot = Graph.exportWorkspace();
      if (!snapshot || typeof snapshot !== 'object') {
        setBadge('Nothing to share', false);
        return;
      }
      const payload = {
        type: 'workspace-sync',
        action: 'offer',
        from,
        graphId: CFG.graphId || null,
        workspace: snapshot,
        ts: Date.now()
      };
      const json = JSON.stringify(payload);
      await client.send(targetFull, json, { noReply: true, maxHoldingSeconds: 120 });
      setBadge(`Workspace sent to ${formatAddress(targetRaw)}`);
    } catch (err) {
      setBadge(`Share failed: ${err?.message || err}`, false);
      log(`[sync] share error: ${err?.stack || err}`);
    } finally {
      closeShareModal();
    }
  }

  async function requestWorkspaceFromTarget() {
    const targetRaw = stripGraphPrefix(state.shareTarget);
    if (!targetRaw) {
      setBadge('No sync target', false);
      closeShareModal();
      return;
    }
    try {
      const client = await ensureNknReady();
      const from = stripGraphPrefix(Net.nkn.addr || client.addr || '');
      const targetFull = ensureGraphPrefix(targetRaw);
      if (!targetFull) throw new Error('Invalid target address');
      const payload = {
        type: 'workspace-sync',
        action: 'request',
        from,
        graphId: CFG.graphId || null,
        ts: Date.now()
      };
      await client.send(targetFull, JSON.stringify(payload), { noReply: true, maxHoldingSeconds: 120 });
      setBadge(`Requested workspace from ${formatAddress(targetRaw)}`);
    } catch (err) {
      setBadge(`Request failed: ${err?.message || err}`, false);
      log(`[sync] request error: ${err?.stack || err}`);
    } finally {
      closeShareModal();
    }
  }

  function acceptIncomingWorkspace() {
    if (!state.incomingOffer) {
      closeReceiveModal();
      return;
    }
    const { from, workspace } = state.incomingOffer;
    const ok = Graph.importWorkspace(workspace, { silent: true });
    if (ok) {
      setBadge(`Workspace imported from ${formatAddress(from)}`);
    }
    closeReceiveModal();
  }

  function declineIncomingWorkspace() {
    if (state.incomingOffer) {
      setBadge(`Declined workspace from ${formatAddress(state.incomingOffer.from)}`, false);
    }
    closeReceiveModal();
  }

  function wireEvents() {
    if (els.qrButton && !els.qrButton._syncBound) {
      els.qrButton.addEventListener('click', (e) => {
        e.preventDefault();
        generateQr().catch(() => {});
      });
      els.qrButton._syncBound = true;
    }
    [
      [els.qrClose, closeQr],
      [els.qrBackdrop, closeQr]
    ].forEach(([el, handler]) => {
      if (el && !el._syncBound) {
        el.addEventListener('click', (e) => {
          e.preventDefault();
          handler();
        });
        el._syncBound = true;
      }
    });

    if (els.shareYes && !els.shareYes._syncBound) {
      els.shareYes.addEventListener('click', (e) => {
        e.preventDefault();
        sendWorkspaceToTarget();
      });
      els.shareYes._syncBound = true;
    }
    if (els.shareRequest && !els.shareRequest._syncBound) {
      els.shareRequest.addEventListener('click', (e) => {
        e.preventDefault();
        requestWorkspaceFromTarget();
      });
      els.shareRequest._syncBound = true;
    }
    [
      [els.shareNo, () => {
        if (state.shareTarget) setBadge(`Declined share for ${formatAddress(state.shareTarget)}`, false);
        closeShareModal();
      }],
      [els.shareClose, closeShareModal],
      [els.shareBackdrop, closeShareModal]
    ].forEach(([el, handler]) => {
      if (el && !el._syncBound) {
        el.addEventListener('click', (e) => {
          e.preventDefault();
          handler();
        });
        el._syncBound = true;
      }
    });

    if (els.receiveYes && !els.receiveYes._syncBound) {
      els.receiveYes.addEventListener('click', (e) => {
        e.preventDefault();
        acceptIncomingWorkspace();
      });
      els.receiveYes._syncBound = true;
    }
    [
      [els.receiveNo, declineIncomingWorkspace],
      [els.receiveClose, declineIncomingWorkspace],
      [els.receiveBackdrop, declineIncomingWorkspace]
    ].forEach(([el, handler]) => {
      if (el && !el._syncBound) {
        el.addEventListener('click', (e) => {
          e.preventDefault();
          handler();
        });
        el._syncBound = true;
      }
    });
  }

  function init() {
    assignElements();
    wireEvents();

    const searchHex = consumeSearchParam();
    if (searchHex) {
      ensureNknTransport();
      setBadge(`Sync target detected: ${formatAddress(searchHex)}`);
      promptShare(searchHex);
    }

    // If already in NKN mode, attach listener immediately
    if (CFG.transport === 'nkn' && Net.nkn && Net.nkn.client) {
      attachListener(Net.nkn.client);
    }
  }

  return {
    init
  };
}

export { createWorkspaceSync };
