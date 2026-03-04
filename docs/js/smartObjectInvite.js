/**
 * Smart Object Invite System
 * Generates QR codes for NoClipBridge targets and exposes invite ingress links.
 */

const PUBKEY_RE = /^[0-9a-f]{64}$/i;
const TRUE_VALUES = new Set(['1', 'true', 'yes', 'y', 'on']);

const normalizePub = (value) => {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return '';
  const normalized = raw
    .replace(/^nkn:\/\//, '')
    .replace(/^noclip\./, '')
    .replace(/^hydra\./, '');
  return PUBKEY_RE.test(normalized) ? normalized : '';
};

const cleanField = (value, maxLen = 160) => {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const max = Math.max(1, Math.floor(Number(maxLen) || 160));
  return raw.slice(0, max);
};

const parseBoolean = (value, fallback = false) => {
  if (value === true || value === false) return value;
  const text = String(value || '').trim().toLowerCase();
  if (!text) return !!fallback;
  if (TRUE_VALUES.has(text)) return true;
  if (text === '0' || text === 'false' || text === 'no' || text === 'n' || text === 'off') return false;
  return !!fallback;
};

export function initSmartObjectInvite({ NodeStore, Net, setBadge }) {
  const state = {
    modal: null,
    nodeId: '',
    lastUrl: ''
  };

  const query = (selector) => state.modal?.querySelector(selector) || null;

  const listNoClipBridgeNodes = () => {
    const nodes = [];
    const seen = new Set();
    const els = Array.from(document.querySelectorAll('.node[data-id]'));
    els.forEach((el) => {
      const nodeId = String(el?.dataset?.id || '').trim();
      if (!nodeId || seen.has(nodeId)) return;
      const rec = NodeStore?.load?.(nodeId);
      if (!rec || rec.type !== 'NoClipBridge') return;
      nodes.push(nodeId);
      seen.add(nodeId);
    });
    return nodes;
  };

  const readBridgeConfig = (nodeId) => {
    const id = String(nodeId || '').trim();
    if (!id) return null;
    const rec = NodeStore?.load?.(id);
    if (!rec || rec.type !== 'NoClipBridge') return null;
    const cfg = rec.config && typeof rec.config === 'object' ? rec.config : {};
    const interop = cfg.interopTarget && typeof cfg.interopTarget === 'object' ? cfg.interopTarget : {};
    const targetPub = normalizePub(cfg.targetPub || cfg.targetAddr || '');
    return {
      nodeId: id,
      targetPub,
      sessionId: cleanField(cfg.sessionId || interop.sessionId || '', 128),
      objectUuid: cleanField(cfg.objectUuid || cfg.objectId || interop.objectUuid || '', 160),
      overlayId: cleanField(cfg.overlayId || interop.overlayId || '', 128),
      itemId: cleanField(cfg.itemId || interop.itemId || '', 160),
      layerId: cleanField(cfg.layerId || interop.layerId || '', 128)
    };
  };

  const readHydraPub = () => {
    const addr = String(Net?.nkn?.addr || Net?.nkn?.client?.addr || '').trim();
    if (!addr) return '';
    return normalizePub(addr);
  };

  const buildInviteUrl = ({ nodeId, network, autoSync }) => {
    const bridge = readBridgeConfig(nodeId);
    if (!bridge) {
      throw new Error('NoClipBridge node not found');
    }
    if (!bridge.targetPub) {
      throw new Error('No target peer configured on this bridge node');
    }
    const targetNetwork = String(network || 'noclip').trim().toLowerCase() === 'hydra' ? 'hydra' : 'noclip';
    const baseUrl = targetNetwork === 'hydra'
      ? 'https://hydras.nexus/'
      : 'https://noclip.nexus/';
    const inviteUrl = new URL(baseUrl);

    // Primary parser contract (Hydra invite ingress path)
    inviteUrl.searchParams.set('noclip', `noclip.${bridge.targetPub}`);
    inviteUrl.searchParams.set('bridgeNodeId', bridge.nodeId);
    inviteUrl.searchParams.set('autoSync', autoSync ? 'true' : 'false');
    if (bridge.sessionId) inviteUrl.searchParams.set('sessionId', bridge.sessionId);
    if (bridge.objectUuid) inviteUrl.searchParams.set('objectUuid', bridge.objectUuid);
    if (bridge.overlayId) inviteUrl.searchParams.set('overlayId', bridge.overlayId);
    if (bridge.itemId) inviteUrl.searchParams.set('itemId', bridge.itemId);
    if (bridge.layerId) inviteUrl.searchParams.set('layerId', bridge.layerId);

    // Backward-compat metadata for older scanner flows.
    const hydraPub = readHydraPub();
    if (hydraPub) inviteUrl.searchParams.set('hydra', `hydra.${hydraPub}`);
    inviteUrl.searchParams.set('node', bridge.nodeId);
    inviteUrl.searchParams.set('kind', 'smart-object');

    return inviteUrl.toString();
  };

  const ensureModal = () => {
    if (state.modal) return state.modal;
    const modal = document.createElement('div');
    modal.id = 'smartInviteModal';
    modal.className = 'modal hidden';
    modal.setAttribute('aria-hidden', 'true');
    modal.innerHTML = `
      <div class="modal-backdrop" data-smart-invite-backdrop></div>
      <div class="modal-panel" style="max-width:560px;">
        <div class="modal-head">
          <div class="modal-title">Smart Object Invite</div>
          <button type="button" class="ghost" data-smart-invite-close>✕</button>
        </div>
        <div class="help">Generate invite QR for the selected NoClipBridge target.</div>
        <div class="row" style="margin-top:10px;gap:8px;align-items:center;">
          <label style="min-width:84px;">Bridge Node</label>
          <input data-smart-invite-node readonly style="flex:1;" />
        </div>
        <div class="row" style="margin-top:8px;gap:8px;align-items:center;">
          <label style="min-width:84px;">Target Peer</label>
          <input data-smart-invite-target readonly style="flex:1;" />
        </div>
        <div class="row" style="margin-top:8px;gap:8px;align-items:center;">
          <label style="min-width:84px;">Network</label>
          <select data-smart-invite-network style="flex:1;">
            <option value="noclip">NoClip</option>
            <option value="hydra">Hydra</option>
          </select>
        </div>
        <div class="row" style="margin-top:8px;gap:8px;align-items:center;">
          <label style="min-width:84px;">Auto Sync</label>
          <select data-smart-invite-autosync style="flex:1;">
            <option value="true">true</option>
            <option value="false">false</option>
          </select>
        </div>
        <div class="row" style="margin-top:10px;gap:8px;">
          <button type="button" class="secondary" data-smart-invite-generate>Generate QR</button>
          <button type="button" class="ghost" data-smart-invite-copy>Copy URL</button>
        </div>
        <canvas data-smart-invite-qr style="margin-top:10px;max-width:100%;border-radius:8px;background:#0f1118;"></canvas>
        <div class="code" data-smart-invite-url style="margin-top:10px;max-height:110px;overflow:auto;word-break:break-all;">(no invite generated)</div>
      </div>
    `;
    document.body.appendChild(modal);
    state.modal = modal;

    const hideModal = () => {
      modal.classList.add('hidden');
      modal.setAttribute('aria-hidden', 'true');
    };

    query('[data-smart-invite-close]')?.addEventListener('click', (e) => {
      e.preventDefault();
      hideModal();
    });
    query('[data-smart-invite-backdrop]')?.addEventListener('click', (e) => {
      e.preventDefault();
      hideModal();
    });
    query('[data-smart-invite-generate]')?.addEventListener('click', (e) => {
      e.preventDefault();
      void generateQR();
    });
    query('[data-smart-invite-copy]')?.addEventListener('click', async (e) => {
      e.preventDefault();
      const url = String(state.lastUrl || '').trim();
      if (!url) {
        setBadge?.('Generate invite URL first', false);
        return;
      }
      try {
        await navigator.clipboard.writeText(url);
        setBadge?.('Invite URL copied');
      } catch (_) {
        setBadge?.('Clipboard unavailable', false);
      }
    });

    return modal;
  };

  const syncModalFields = () => {
    const bridge = readBridgeConfig(state.nodeId);
    const nodeField = query('[data-smart-invite-node]');
    const targetField = query('[data-smart-invite-target]');
    const urlField = query('[data-smart-invite-url]');
    if (nodeField) nodeField.value = state.nodeId || '';
    if (targetField) targetField.value = bridge?.targetPub ? `noclip.${bridge.targetPub}` : '(none)';
    if (urlField && !state.lastUrl) {
      urlField.textContent = '(no invite generated)';
    }
  };

  const showModal = (nodeId = '') => {
    ensureModal();
    const requestedNodeId = String(nodeId || '').trim();
    if (requestedNodeId) {
      state.nodeId = requestedNodeId;
    } else if (!state.nodeId) {
      state.nodeId = listNoClipBridgeNodes()[0] || '';
    }
    if (!state.nodeId) {
      setBadge?.('No NoClipBridge node available', false);
      return;
    }
    state.lastUrl = '';
    syncModalFields();
    state.modal.classList.remove('hidden');
    state.modal.setAttribute('aria-hidden', 'false');
  };

  const hideModal = () => {
    if (!state.modal) return;
    state.modal.classList.add('hidden');
    state.modal.setAttribute('aria-hidden', 'true');
  };

  const generateQR = async () => {
    ensureModal();
    if (!state.nodeId) {
      setBadge?.('No NoClipBridge node selected', false);
      return;
    }
    const network = String(query('[data-smart-invite-network]')?.value || 'noclip').trim().toLowerCase();
    const autoSync = parseBoolean(query('[data-smart-invite-autosync]')?.value, true);
    let inviteUrl = '';
    try {
      inviteUrl = buildInviteUrl({
        nodeId: state.nodeId,
        network,
        autoSync
      });
    } catch (err) {
      setBadge?.(err?.message || 'Failed to generate invite URL', false);
      return;
    }

    state.lastUrl = inviteUrl;
    const urlField = query('[data-smart-invite-url]');
    const canvas = query('[data-smart-invite-qr]');
    if (urlField) urlField.textContent = inviteUrl;

    if (!canvas) {
      setBadge?.('QR canvas unavailable', false);
      return;
    }
    if (!window.QRCode || typeof window.QRCode.toCanvas !== 'function') {
      setBadge?.('QR library unavailable; URL generated only', false);
      return;
    }
    try {
      await window.QRCode.toCanvas(canvas, inviteUrl, {
        width: 300,
        margin: 2,
        color: {
          dark: '#5ee3a6',
          light: '#0f1118'
        }
      });
      setBadge?.('Smart object invite QR generated');
    } catch (err) {
      setBadge?.(`QR generation failed: ${err?.message || err}`, false);
    }
  };

  const handleOpenRequest = (event) => {
    const detail = event?.detail && typeof event.detail === 'object' ? event.detail : {};
    const nodeId = cleanField(detail.nodeId || detail.bridgeNodeId || '', 96);
    showModal(nodeId);
  };

  if (typeof window !== 'undefined') {
    window.addEventListener('hydra-noclip-smart-invite-open', handleOpenRequest);
  }

  return {
    showModal,
    hideModal,
    generateQR,
    addInviteButton: () => {}
  };
}
