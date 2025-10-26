import { qs } from './utils.js';

/**
 * NoClipBridgeSync
 * Handles sync requests from NoClip Smart Objects
 * Shows modal for approval and auto-creates NoClipBridge nodes
 */

function createNoClipBridgeSync({
  Graph,
  PeerDiscovery,
  setBadge,
  log
}) {
  const state = {
    pendingRequests: new Map(), // noclipPub -> request data
    approvedConnections: new Map(), // noclipPub -> bridgeNodeId
    modal: null
  };

  /**
   * Initialize - attach listener to peer discovery
   */
  function init() {
    if (!PeerDiscovery) {
      console.warn('[NoClipBridgeSync] PeerDiscovery not available');
      return;
    }

    // Listen for incoming messages from Hydra discovery (NATS)
    PeerDiscovery.on('dm', (event) => {
      const { from, msg } = event;
      if (!msg || !msg.type) return;

      if (msg.type === 'noclip-bridge-sync-request') {
        handleSyncRequest(from, msg);
      } else if (msg.type === 'noclip-bridge-sync-accepted') {
        handleSyncAccepted(from, msg);
      } else if (msg.type === 'noclip-bridge-sync-rejected') {
        handleSyncRejected(from, msg);
      }
    });

    console.log('[NoClipBridgeSync] Initialized');
  }

  /**
   * Handle incoming sync request from NoClip
   */
  function handleSyncRequest(from, msg) {
    console.log('[NoClipBridgeSync] Received sync request from:', from, msg);

    const noclipPub = msg.from || from;
    const noclipAddr = msg.noclipAddr || `noclip.${noclipPub}`;
    const objectId = msg.objectId;
    const objectLabel = msg.objectConfig?.label || 'Smart Object';

    // Store pending request
    state.pendingRequests.set(noclipPub, {
      noclipPub,
      noclipAddr,
      objectId,
      objectLabel,
      position: msg.objectConfig?.position,
      receivedAt: Date.now()
    });

    // Show approval modal
    showSyncRequestModal(noclipPub, noclipAddr, objectLabel);

    // Also show toast notification
    setBadge?.(`NoClip sync request from ${noclipPub.slice(0, 8)}...`);
  }

  /**
   * Show modal to approve/reject sync request
   */
  function showSyncRequestModal(noclipPub, noclipAddr, objectLabel) {
    // Create modal if it doesn't exist
    if (!state.modal) {
      createModal();
    }

    const modal = state.modal;
    const message = modal.querySelector('.noclip-sync-message');
    const approveBtn = modal.querySelector('.noclip-sync-approve');
    const rejectBtn = modal.querySelector('.noclip-sync-reject');

    // Update message
    if (message) {
      message.innerHTML = `
        <p><strong>NoClip Bridge Sync Request</strong></p>
        <p>From: <code>${noclipAddr}</code></p>
        <p>Smart Object: <strong>${objectLabel}</strong></p>
        <p>Accept this request to create a NoClipBridge node and connect to this NoClip scene?</p>
      `;
    }

    // Bind approve button
    const onApprove = () => {
      approveSyncRequest(noclipPub);
      hideModal();
    };

    // Bind reject button
    const onReject = () => {
      rejectSyncRequest(noclipPub);
      hideModal();
    };

    // Remove old listeners and add new ones
    if (approveBtn) {
      const newApproveBtn = approveBtn.cloneNode(true);
      approveBtn.parentNode.replaceChild(newApproveBtn, approveBtn);
      newApproveBtn.addEventListener('click', onApprove);
    }

    if (rejectBtn) {
      const newRejectBtn = rejectBtn.cloneNode(true);
      rejectBtn.parentNode.replaceChild(newRejectBtn, rejectBtn);
      newRejectBtn.addEventListener('click', onReject);
    }

    // Show modal
    modal.classList.remove('hidden');
    modal.setAttribute('aria-hidden', 'false');
  }

  /**
   * Create the modal DOM element
   */
  function createModal() {
    const existingModal = document.getElementById('noclipBridgeSyncModal');
    if (existingModal) {
      state.modal = existingModal;
      return;
    }

    const modal = document.createElement('div');
    modal.id = 'noclipBridgeSyncModal';
    modal.className = 'modal hidden';
    modal.setAttribute('aria-hidden', 'true');
    modal.setAttribute('role', 'dialog');

    modal.innerHTML = `
      <div class="modal-backdrop noclip-sync-backdrop"></div>
      <div class="modal-content">
        <div class="modal-header">
          <h2>NoClip Bridge Sync Request</h2>
          <button class="modal-close noclip-sync-close" aria-label="Close">&times;</button>
        </div>
        <div class="modal-body noclip-sync-message">
          <p>Loading...</p>
        </div>
        <div class="modal-footer">
          <button class="btn btn-secondary noclip-sync-reject">Reject</button>
          <button class="btn btn-primary noclip-sync-approve">Approve & Create Bridge</button>
        </div>
      </div>
    `;

    document.body.appendChild(modal);
    state.modal = modal;

    // Bind close button
    const closeBtn = modal.querySelector('.noclip-sync-close');
    const backdrop = modal.querySelector('.noclip-sync-backdrop');

    if (closeBtn) {
      closeBtn.addEventListener('click', hideModal);
    }

    if (backdrop) {
      backdrop.addEventListener('click', hideModal);
    }
  }

  /**
   * Hide the modal
   */
  function hideModal() {
    if (state.modal) {
      state.modal.classList.add('hidden');
      state.modal.setAttribute('aria-hidden', 'true');
    }
  }

  /**
   * Approve sync request - create NoClipBridge node and send confirmation
   */
  async function approveSyncRequest(noclipPub) {
    const request = state.pendingRequests.get(noclipPub);
    if (!request) {
      console.error('[NoClipBridgeSync] No pending request found for', noclipPub);
      return;
    }

    try {
      // Create NoClipBridge node
      const nodeId = `noclip-bridge-${noclipPub.slice(0, 8)}`;
      const position = { x: Math.random() * 400 - 200, y: Math.random() * 300 - 150 };

      const bridgeNode = await Graph.createNode({
        id: nodeId,
        type: 'NoClipBridge',
        position,
        config: {
          targetPub: noclipPub,
          targetAddr: request.noclipAddr,
          autoConnect: 'true'
        }
      });

      console.log('[NoClipBridgeSync] Created NoClipBridge node:', nodeId);

      // Store approved connection
      state.approvedConnections.set(noclipPub, nodeId);

      // Send approval response back to NoClip
      if (PeerDiscovery) {
        await PeerDiscovery.dm(noclipPub, {
          type: 'noclip-bridge-sync-accepted',
          bridgeNodeId: nodeId,
          timestamp: Date.now()
        });
      }

      setBadge?.(`✓ NoClip bridge created: ${nodeId}`);
      log?.(`[noclip-sync] Created bridge node ${nodeId} for ${request.noclipAddr}`);

      // Remove from pending
      state.pendingRequests.delete(noclipPub);

    } catch (err) {
      console.error('[NoClipBridgeSync] Failed to approve sync:', err);
      setBadge?.(`✗ Failed to create NoClip bridge: ${err.message}`);

      // Send rejection instead
      rejectSyncRequest(noclipPub, err.message);
    }
  }

  /**
   * Reject sync request
   */
  async function rejectSyncRequest(noclipPub, reason = 'User rejected') {
    const request = state.pendingRequests.get(noclipPub);
    if (!request) return;

    try {
      // Send rejection response
      if (PeerDiscovery) {
        await PeerDiscovery.dm(noclipPub, {
          type: 'noclip-bridge-sync-rejected',
          reason,
          timestamp: Date.now()
        });
      }

      log?.(`[noclip-sync] Rejected sync request from ${request.noclipAddr}: ${reason}`);
    } catch (err) {
      console.error('[NoClipBridgeSync] Failed to send rejection:', err);
    }

    // Remove from pending
    state.pendingRequests.delete(noclipPub);
  }

  /**
   * Handle sync accepted response (from NoClip acknowledging our approval)
   */
  function handleSyncAccepted(from, msg) {
    console.log('[NoClipBridgeSync] Sync accepted by NoClip:', from);
    setBadge?.(`✓ NoClip ${from.slice(0, 8)}... acknowledged bridge connection`);
  }

  /**
   * Handle sync rejected response
   */
  function handleSyncRejected(from, msg) {
    console.log('[NoClipBridgeSync] Sync rejected by NoClip:', from, msg.reason);
    setBadge?.(`✗ NoClip ${from.slice(0, 8)}... rejected: ${msg.reason || 'Unknown'}`);
  }

  /**
   * Get approved bridge node ID for a NoClip peer
   */
  function getBridgeNodeId(noclipPub) {
    return state.approvedConnections.get(noclipPub);
  }

  return {
    init,
    handleSyncRequest,
    approveSyncRequest,
    rejectSyncRequest,
    getBridgeNodeId
  };
}

export { createNoClipBridgeSync };
