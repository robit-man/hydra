import { qs } from './utils.js';

/**
 * NoClipBridgeSync
 * Handles sync requests from NoClip Smart Objects
 * Shows modal for approval and auto-creates NoClipBridge nodes
 */

function createNoClipBridgeSync({
  Graph,
  PeerDiscovery,
  Net,
  CFG,
  setBadge,
  log
}) {
  const state = {
    pendingRequests: new Map(), // noclipPub -> request data
    approvedConnections: new Map(), // noclipPub -> bridgeNodeId
    sessions: new Map(), // sessionId -> session record
    modal: null,
    bridgeAdapter: null
  };
  const SESSION_STORAGE_KEY = 'hydra.noclip.sessions.v1';
  let sessionsLoaded = false;

  const nowMs = () => Date.now();

  const normalizeHex64 = (value) => {
    if (typeof value !== 'string') return '';
    const match = value.match(/([0-9a-f]{64})$/i);
    return match ? match[1].toLowerCase() : '';
  };

  const notifyBridge = (session) => {
    if (!session) return;
    try {
      state.bridgeAdapter?.onSessionUpdate?.(session);
    } catch (err) {
      console.warn('[NoClipBridgeSync] Bridge adapter notification failed:', err);
    }
  };

  const ensureSessionsLoaded = () => {
    if (sessionsLoaded) return;
    sessionsLoaded = true;
    if (typeof localStorage === 'undefined') return;
    try {
      const raw = localStorage.getItem(SESSION_STORAGE_KEY);
      if (!raw) return;
      const arr = JSON.parse(raw);
      if (!Array.isArray(arr)) return;
      arr.forEach((entry) => {
        const normalized = normalizeSession(entry);
        if (normalized) state.sessions.set(normalized.sessionId, normalized);
      });
      if (state.bridgeAdapter?.onSessionUpdate) {
        state.sessions.forEach((session) => notifyBridge(session));
      }
    } catch (err) {
      console.warn('[NoClipBridgeSync] Failed to load sessions:', err);
    }
  };

  const persistSessions = () => {
    if (typeof localStorage === 'undefined') return;
    try {
      const list = Array.from(state.sessions.values());
      localStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(list));
    } catch (err) {
      console.warn('[NoClipBridgeSync] Failed to persist sessions:', err);
    }
  };

  const generateSessionId = () => `sess-${nowMs().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

  const getHydraIdentity = () => {
    const identity = {
      addr: '',
      pub: '',
      graphId: typeof CFG?.graphId === 'string' ? CFG.graphId : ''
    };
    try {
      const nknAddr = Net?.nkn?.addr || Net?.nkn?.client?.addr || '';
      identity.addr = typeof nknAddr === 'string' ? nknAddr : '';
      const explicitPub = Net?.nkn?.client?.getPublicKey?.();
      const inferredPub = normalizeHex64(identity.addr);
      identity.pub = normalizeHex64(explicitPub) || inferredPub;
    } catch (err) {
      console.warn('[NoClipBridgeSync] Failed to derive Hydra identity:', err);
    }
    return identity;
  };

  const normalizeSession = (input) => {
    if (!input || typeof input !== 'object') return null;
    const sessionId = typeof input.sessionId === 'string' ? input.sessionId.trim() : '';
    const objectUuid = typeof input.objectUuid === 'string' ? input.objectUuid.trim() : '';
    const noclipPub = normalizeHex64(input.noclipPub || input.noclipAddr || '');
    if (!sessionId || !objectUuid || !noclipPub) return null;
    const stamp = nowMs();
    const position = input.position && typeof input.position === 'object' ? { ...input.position } : null;
    const geo = input.geo && typeof input.geo === 'object' ? { ...input.geo } : null;
    return {
      sessionId,
      objectUuid,
      objectLabel: typeof input.objectLabel === 'string' ? input.objectLabel : '',
      noclipPub,
      noclipAddr: typeof input.noclipAddr === 'string' ? input.noclipAddr : `noclip.${noclipPub}`,
      hydraBridgeNodeId: typeof input.hydraBridgeNodeId === 'string' ? input.hydraBridgeNodeId : '',
      hydraGraphId: typeof input.hydraGraphId === 'string' ? input.hydraGraphId : '',
      hydraPub: normalizeHex64(input.hydraPub),
      hydraAddr: typeof input.hydraAddr === 'string' ? input.hydraAddr : '',
      bridgeNodeId: typeof input.bridgeNodeId === 'string' ? input.bridgeNodeId : '',
      status: typeof input.status === 'string' ? input.status : 'pending-handshake',
      createdAt: Number.isFinite(input.createdAt) ? input.createdAt : stamp,
      updatedAt: Number.isFinite(input.updatedAt) ? input.updatedAt : stamp,
      ...(position ? { position } : {}),
      ...(geo ? { geo } : {})
    };
  };

  const upsertSession = (payload) => {
    const normalized = normalizeSession(payload);
    if (!normalized) return null;
    const existing = state.sessions.get(normalized.sessionId) || {};
    const merged = {
      ...existing,
      ...normalized,
      updatedAt: nowMs()
    };
    state.sessions.set(merged.sessionId, merged);
    persistSessions();
    notifyBridge(merged);
    return merged;
  };

  const findSessionsByNoclipPub = (pub) => {
    const normalized = normalizeHex64(pub);
    if (!normalized) return [];
    return Array.from(state.sessions.values()).filter((session) => session.noclipPub === normalized);
  };

  const findSessionById = (sessionId) => {
    if (!sessionId) return null;
    return state.sessions.get(sessionId) || null;
  };

  const findSessionsByNode = (nodeId) => {
    if (!nodeId) return [];
    return Array.from(state.sessions.values()).filter((session) => session.hydraBridgeNodeId === nodeId || session.bridgeNodeId === nodeId);
  };

  const updateSessionStatus = ({ sessionId, noclipPub, hydraBridgeNodeId, status, rejectionReason, lastHandshakeAt }) => {
    ensureSessionsLoaded();
    const targets = [];
    if (sessionId) {
      const direct = findSessionById(sessionId);
      if (direct) targets.push(direct);
    }
    if (!targets.length && noclipPub) {
      targets.push(...findSessionsByNoclipPub(noclipPub));
    }
    if (!targets.length && hydraBridgeNodeId) {
      targets.push(...findSessionsByNode(hydraBridgeNodeId));
    }
    if (!targets.length) return 0;
    targets.forEach((session) => {
      const next = {
        ...session
      };
      if (status) next.status = status;
      if (rejectionReason !== undefined) next.rejectionReason = rejectionReason;
      if (hydraBridgeNodeId) next.hydraBridgeNodeId = hydraBridgeNodeId;
      if (lastHandshakeAt) next.lastHandshakeAt = lastHandshakeAt;
      upsertSession(next);
    });
    return targets.length;
  };

  /**
   * Initialize - register message handler
   * Note: This needs to be called with the actual discovery instance once it's created
   */
  function init(discoveryInstance = null) {
    ensureSessionsLoaded();
    if (discoveryInstance) {
      // Direct attachment if discovery instance is provided
      discoveryInstance.on('dm', handleDiscoveryDm);
      console.log('[NoClipBridgeSync] Attached to discovery instance');
    } else {
      // Store handler for later attachment
      console.log('[NoClipBridgeSync] Handler registered, waiting for discovery');
    }
  }

  /**
   * Handle discovery DM messages
   */
  function handleDiscoveryDm(event) {
    const { from, msg } = event || {};
    if (!msg || !msg.type) return;

    if (msg.type === 'noclip-bridge-sync-request') {
      handleSyncRequest(from, msg);
    } else if (msg.type === 'noclip-bridge-sync-accepted') {
      handleSyncAccepted(from, msg);
    } else if (msg.type === 'noclip-bridge-sync-rejected') {
      handleSyncRejected(from, msg);
    }
  }

  /**
   * Attach to an existing discovery instance
   */
  function attachToDiscovery(discoveryInstance) {
    if (!discoveryInstance) return;
    discoveryInstance.on('dm', handleDiscoveryDm);
    console.log('[NoClipBridgeSync] Attached to discovery');
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
      position: msg.objectConfig?.position && typeof msg.objectConfig.position === 'object'
        ? { ...msg.objectConfig.position }
        : null,
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
    ensureSessionsLoaded();
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

      const hydraIdentity = getHydraIdentity();
      const positionData = request.objectConfig?.position && typeof request.objectConfig.position === 'object'
        ? { ...request.objectConfig.position }
        : null;
      let geoData = null;
      if (positionData && Number.isFinite(positionData.lat) && Number.isFinite(positionData.lon)) {
        geoData = {
          lat: positionData.lat,
          lon: positionData.lon
        };
        if (Number.isFinite(positionData.ground)) geoData.ground = positionData.ground;
        if (Number.isFinite(positionData.alt)) geoData.alt = positionData.alt;
        const geohash = positionData.gh || positionData.geohash;
        if (typeof geohash === 'string') geoData.gh = geohash;
      }
      const session = upsertSession({
        sessionId: generateSessionId(),
        objectUuid: request.objectId,
        objectLabel: request.objectLabel,
        noclipPub,
        noclipAddr: request.noclipAddr,
        hydraBridgeNodeId: nodeId,
        hydraGraphId: hydraIdentity.graphId,
        hydraPub: hydraIdentity.pub,
        hydraAddr: hydraIdentity.addr,
        bridgeNodeId: nodeId,
        status: 'pending-handshake',
        position: positionData,
        geo: geoData
      });

      // Send approval response back to NoClip
      if (PeerDiscovery && PeerDiscovery.sendDm) {
        await PeerDiscovery.sendDm(noclipPub, {
          type: 'noclip-bridge-sync-accepted',
          bridgeNodeId: nodeId,
          session,
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
      if (PeerDiscovery && PeerDiscovery.sendDm) {
        await PeerDiscovery.sendDm(noclipPub, {
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
    ensureSessionsLoaded();
    const payload = msg?.session;
    let updatedCount = 0;
    if (payload && payload.sessionId) {
      const normalized = {
        ...payload,
        noclipPub: payload.noclipPub || from,
        status: payload.status || 'acknowledged'
      };
      upsertSession(normalized);
      updatedCount = 1;
    } else {
      updatedCount = updateSessionStatus({
        noclipPub: from,
        status: 'acknowledged'
      });
    }
    console.log('[NoClipBridgeSync] Sync accepted by NoClip:', from);
    const suffix = updatedCount ? '' : ' (session pending)';
    setBadge?.(`✓ NoClip ${from.slice(0, 8)}... acknowledged bridge connection${suffix}`);
  }

  /**
   * Handle sync rejected response
   */
  function handleSyncRejected(from, msg) {
    ensureSessionsLoaded();
    const reason = msg?.reason || 'Unknown';
    updateSessionStatus({
      sessionId: msg?.session?.sessionId,
      noclipPub: from,
      status: 'rejected',
      rejectionReason: reason
    });
    console.log('[NoClipBridgeSync] Sync rejected by NoClip:', from, msg.reason);
    setBadge?.(`✗ NoClip ${from.slice(0, 8)}... rejected: ${reason}`);
  }

  /**
   * Get approved bridge node ID for a NoClip peer
   */
  function getBridgeNodeId(noclipPub) {
    return state.approvedConnections.get(noclipPub);
  }

  function registerBridgeAdapter(adapter) {
    state.bridgeAdapter = adapter || null;
    if (state.bridgeAdapter?.onSessionUpdate) {
      ensureSessionsLoaded();
      state.sessions.forEach((session) => notifyBridge(session));
    }
  }

  return {
    init,
    attachToDiscovery,
    handleSyncRequest,
    approveSyncRequest,
    rejectSyncRequest,
    getBridgeNodeId,
    registerBridgeAdapter,
    updateSessionStatus,
    state // Expose state for debugging
  };
}

export { createNoClipBridgeSync };
