/**
 * Smart Object Invite System
 * Generates QR codes for connecting Hydra nodes to NoClip Smart Objects
 */

export function initSmartObjectInvite({ NodeStore, Net, setBadge }) {
  const state = {
    modal: null,
    initialized: false
  };

  /**
   * Create invite modal HTML
   */
  function createModal() {
    if (state.modal) return state.modal;

    const modal = document.createElement('div');
    modal.id = 'smart-invite-modal';
    modal.className = 'modal-overlay';
    modal.style.display = 'none';

    modal.innerHTML = `
      <div class="modal-content" style="max-width: 500px;">
        <div class="modal-header">
          <h3>Smart Object Invite</h3>
          <button class="modal-close" id="smart-invite-close">&times;</button>
        </div>
        <div class="modal-body">
          <p style="margin-bottom: 16px; color: var(--muted);">
            Generate a QR code to connect this node to a NoClip Smart Object
          </p>

          <div class="form-group">
            <label>Node ID:</label>
            <input type="text" id="smart-invite-node-id" readonly style="background: rgba(255,255,255,0.05);">
          </div>

          <div class="form-group">
            <label>Target Network:</label>
            <select id="smart-invite-network">
              <option value="noclip">NoClip (noclip.nexus)</option>
              <option value="hydra">Hydra (hydras.nexus)</option>
            </select>
          </div>

          <div class="form-group">
            <button class="btn btn-primary" id="smart-invite-generate">
              Generate QR Code
            </button>
          </div>

          <div id="smart-invite-qr-container" style="display: none; text-align: center; margin-top: 20px;">
            <canvas id="smart-invite-qr-canvas"></canvas>
            <p id="smart-invite-url" style="
              font-family: monospace;
              font-size: 12px;
              color: var(--accent);
              word-break: break-all;
              margin-top: 12px;
              padding: 8px;
              background: rgba(0,0,0,0.3);
              border-radius: 4px;
            "></p>
            <p style="font-size: 12px; color: var(--muted); margin-top: 8px;">
              Scan with NoClip Smart Object modal
            </p>
          </div>
        </div>
      </div>
    `;

    document.body.appendChild(modal);
    state.modal = modal;

    _bindEvents();

    return modal;
  }

  /**
   * Bind event handlers
   */
  function _bindEvents() {
    const modal = state.modal;
    if (!modal) return;

    // Close button
    const closeBtn = modal.querySelector('#smart-invite-close');
    closeBtn.addEventListener('click', hideModal);

    // Click outside to close
    modal.addEventListener('click', (e) => {
      if (e.target === modal) {
        hideModal();
      }
    });

    // Generate button
    const generateBtn = modal.querySelector('#smart-invite-generate');
    generateBtn.addEventListener('click', generateQR);
  }

  /**
   * Show invite modal for a specific node
   */
  function showModal(nodeId) {
    const modal = createModal();
    const nodeIdInput = modal.querySelector('#smart-invite-node-id');
    nodeIdInput.value = nodeId;

    // Reset QR display
    const qrContainer = modal.querySelector('#smart-invite-qr-container');
    qrContainer.style.display = 'none';

    modal.style.display = 'flex';
  }

  /**
   * Hide modal
   */
  function hideModal() {
    if (state.modal) {
      state.modal.style.display = 'none';
    }
  }

  /**
   * Generate QR code
   */
  async function generateQR() {
    const modal = state.modal;
    if (!modal) return;

    try {
      const nodeIdInput = modal.querySelector('#smart-invite-node-id');
      const networkSelect = modal.querySelector('#smart-invite-network');
      const nodeId = nodeIdInput.value;
      const network = networkSelect.value;

      if (!nodeId) {
        setBadge?.('No node ID specified', false);
        return;
      }

      // Get Hydra address
      const client = Net?.nkn?.client;
      const hydraAddr = Net?.nkn?.addr || client?.addr || '';

      if (!hydraAddr) {
        setBadge?.('Hydra address not available', false);
        return;
      }

      // Extract pub key (remove hydra. prefix if present)
      const hydraPub = hydraAddr.replace(/^hydra\./, '');

      // Generate URL
      const baseUrl = network === 'noclip'
        ? 'https://noclip.nexus/'
        : 'https://hydras.nexus/';

      const url = `${baseUrl}?hydra=hydra.${hydraPub}&node=${encodeURIComponent(nodeId)}`;

      // Check if QRCode library is available
      if (typeof window.QRCode === 'undefined' || typeof window.QRCode.toCanvas !== 'function') {
        setBadge?.('QR Code library not loaded', false);
        // Show URL anyway
        const urlDisplay = modal.querySelector('#smart-invite-url');
        urlDisplay.textContent = url;
        const qrContainer = modal.querySelector('#smart-invite-qr-container');
        qrContainer.style.display = 'block';
        return;
      }

      // Generate QR code
      const canvas = modal.querySelector('#smart-invite-qr-canvas');
      const urlDisplay = modal.querySelector('#smart-invite-url');

      window.QRCode.toCanvas(canvas, url, {
        width: 280,
        margin: 2,
        color: {
          dark: '#5ee3a6',
          light: '#0f1118'
        }
      }, (error) => {
        if (error) {
          console.error('[SmartInvite] QR generation error:', error);
          setBadge?.('QR generation failed', false);
          urlDisplay.textContent = url;
        } else {
          urlDisplay.textContent = url;
          const qrContainer = modal.querySelector('#smart-invite-qr-container');
          qrContainer.style.display = 'block';
          setBadge?.('QR code generated', true);
        }
      });

    } catch (err) {
      console.error('[SmartInvite] Error:', err);
      setBadge?.('Failed to generate invite', false);
    }
  }

  /**
   * Add invite button to NoClipBridge nodes
   */
  function addInviteButton(nodeId) {
    // Wait for node element to exist
    requestAnimationFrame(() => {
      const nodeEl = document.querySelector(`[data-node-id="${nodeId}"]`);
      if (!nodeEl) return;

      // Check if button already exists
      if (nodeEl.querySelector('.smart-invite-btn')) return;

      // Find node body or controls area
      const nodeBody = nodeEl.querySelector('.node-body') || nodeEl.querySelector('.node-content');
      if (!nodeBody) return;

      // Create invite button
      const btn = document.createElement('button');
      btn.className = 'btn btn-sm smart-invite-btn';
      btn.textContent = '📱 Smart Object Invite';
      btn.title = 'Generate QR code to connect to NoClip Smart Object';
      btn.style.cssText = `
        width: 100%;
        margin-top: 8px;
        background: linear-gradient(135deg, #5ee3a6, #a8ff60);
        color: #000;
        border: none;
        padding: 6px 12px;
        border-radius: 4px;
        cursor: pointer;
        font-size: 12px;
        font-weight: 600;
      `;

      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        showModal(nodeId);
      });

      nodeBody.appendChild(btn);
    });
  }

  // Auto-add button when NoClipBridge nodes are created/refreshed
  if (typeof window !== 'undefined') {
    // Hook into node creation
    const originalEnsure = NodeStore?.ensure;
    if (originalEnsure) {
      NodeStore.ensure = function(nodeId, type, ...args) {
        const result = originalEnsure.call(this, nodeId, type, ...args);
        if (type === 'NoClipBridge') {
          setTimeout(() => addInviteButton(nodeId), 100);
        }
        return result;
      };
    }
  }

  state.initialized = true;

  return {
    showModal,
    hideModal,
    generateQR,
    addInviteButton
  };
}
