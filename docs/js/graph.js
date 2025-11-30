import { LS, qs, qsa, setBadge, log } from './utils.js';
import { createHistory } from './graph/history.js';
import {
  deepClone,
  isEditableTarget,
  cloneWireForHistory,
  sanitizeHistoryNode,
  convertBooleanSelectsIn
} from './graph/utils.js';
import { GraphTypes as graphTypes } from './graph/types.js';
import { createAudioHelpers } from './graph/audio.js';
import { createWorkspaceManager } from './graph/workspace.js';
import { createFaceViewer } from './faceViewer.js';

function createGraph({
  Router,
  NodeStore,
  LLM,
  TTS,
  ASR,
  NknDM,
  Media,
  Orientation,
  Location,
  FileTransfer,
  Payments,
  MCP,
  Meshtastic,
  WebSerial,
  WebScraper,
  Vision,
  NoClip,
  NoClipBridgeSync,
  Net,
  CFG,
  saveCFG,
  openQrScanner,
  closeQrScanner,
  registerQrResultHandler,
  updateTransportButton
}) {
  const WS = {
    el: null,
    svg: null,
    svgLayer: null,
    root: null,
    canvas: null,
    nodes: new Map(),
    wires: [],
    portSel: null,
    drag: null,
    view: { x: 0, y: 0, scale: 1 },
    _redrawReq: false,
    selectedNodeId: null,
    selectedNodes: new Set(),
    clipboard: null,
    lastPointer: null
  };

  const Tooltip = (() => {
    let root = null;
    let current = null;
    const margin = 10;

    const reposition = () => position();
    const dismiss = () => hide();

    function ensureRoot() {
      if (root) return root;
      root = document.createElement('div');
      root.className = 'graph-tooltip';
      root.dataset.visible = 'false';
      root.style.visibility = 'hidden';
      document.body.appendChild(root);
      window.addEventListener('scroll', reposition, true);
      window.addEventListener('resize', reposition);
      window.addEventListener('blur', dismiss);
      document.addEventListener('pointerdown', dismiss, true);
      document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') dismiss();
      });
      return root;
    }

    function hide(target) {
      if (target && target !== current) return;
      if (!root) {
        current = null;
        return;
      }
      root.dataset.visible = 'false';
      root.style.visibility = 'hidden';
      root.style.transform = 'translate3d(-9999px,-9999px,0)';
      root.dataset.position = '';
      root.textContent = '';
      current = null;
    }

    function position() {
      if (!current) return;
      const tooltip = ensureRoot();
      const text = current.dataset?.tooltip;
      if (!text) {
        hide();
        return;
      }
      const rect = current.getBoundingClientRect();
      if (!rect || (rect.width === 0 && rect.height === 0)) {
        hide();
        return;
      }
      const viewportW = window.innerWidth || document.documentElement.clientWidth || 0;
      const viewportH = window.innerHeight || document.documentElement.clientHeight || 0;
      if (rect.bottom < 0 || rect.top > viewportH || rect.right < 0 || rect.left > viewportW) {
        hide();
        return;
      }
      tooltip.style.visibility = 'hidden';
      tooltip.dataset.visible = 'true';
      tooltip.style.transform = 'translate3d(-9999px,-9999px,0)';
      const tooltipRect = tooltip.getBoundingClientRect();
      let top = rect.bottom + margin;
      let position = 'bottom';
      if (top + tooltipRect.height > viewportH - 8) {
        const candidate = rect.top - tooltipRect.height - margin;
        if (candidate >= 8) {
          top = candidate;
          position = 'top';
        } else {
          top = Math.max(8, rect.bottom + margin);
        }
      }
      let left = rect.left + (rect.width / 2) - (tooltipRect.width / 2);
      left = Math.max(8, Math.min(left, viewportW - tooltipRect.width - 8));
      tooltip.dataset.position = position;
      tooltip.style.transform = `translate3d(${Math.round(left)}px, ${Math.round(top)}px, 0)`;
      tooltip.style.visibility = 'visible';
    }

    function show(el) {
      const text = el?.dataset?.tooltip;
      if (!text) {
        hide();
        return;
      }
      const tooltip = ensureRoot();
      current = el;
      tooltip.textContent = text;
      tooltip.dataset.visible = 'true';
      tooltip.style.visibility = 'hidden';
      requestAnimationFrame(() => {
        if (current !== el) return;
        position();
      });
    }

    function refresh(el) {
      if (current === el) show(el);
    }

    return { show, hide, refresh, position };
  })();

  const LOG_COLLAPSED_KEY = 'graph.logCollapsed';
  const History = createHistory({ LS });
  const audio = createAudioHelpers();
  const FaceViewer = Vision ? createFaceViewer({ Vision }) : null;
  const {
    downloadWorkspaceFile,
    openGraphImportDialog,
    saveWorkspaceAsInteractive,
    setFlowSaveHandler: setWorkspaceFlowSaveHandler,
    getFlowSaveHandler
  } = createWorkspaceManager({
    setBadge,
    deepClone,
    exportWorkspaceSnapshot,
    importWorkspaceSnapshot
  });

  function setFlowSaveHandler(fn) {
    setWorkspaceFlowSaveHandler(fn);
  }

  const ensureSelectionSet = () => {
    if (!WS.selectedNodes) WS.selectedNodes = new Set();
  };

  function updateSelectionClass(nodeId, selected) {
    const node = WS.nodes.get(nodeId);
    if (!node?.el) return;
    node.el.classList.toggle('selected', Boolean(selected));
  }

  function isNodeSelected(nodeId) {
    ensureSelectionSet();
    return WS.selectedNodes.has(nodeId);
  }

  function setSelectedNode(nodeId, { focus = false, additive = false, toggle = false } = {}) {
    ensureSelectionSet();
    if (nodeId == null) {
      if (!WS.selectedNodes.size) return;
      WS.selectedNodes.forEach((id) => updateSelectionClass(id, false));
      WS.selectedNodes.clear();
      WS.selectedNodeId = null;
      return;
    }
    const already = WS.selectedNodes.has(nodeId);
    if (toggle) {
      if (already) {
        WS.selectedNodes.delete(nodeId);
        updateSelectionClass(nodeId, false);
        if (WS.selectedNodeId === nodeId) {
          WS.selectedNodeId = WS.selectedNodes.size ? Array.from(WS.selectedNodes).slice(-1)[0] : null;
        }
        return;
      }
      WS.selectedNodes.add(nodeId);
      updateSelectionClass(nodeId, true);
      WS.selectedNodeId = nodeId;
    } else if (additive) {
      if (!already) {
        WS.selectedNodes.add(nodeId);
        updateSelectionClass(nodeId, true);
      }
      WS.selectedNodeId = nodeId;
    } else {
      if (already && WS.selectedNodes.size === 1) {
        WS.selectedNodeId = nodeId;
        if (focus) WS.nodes.get(nodeId)?.el?.focus?.({ preventScroll: true });
        return;
      }
      WS.selectedNodes.forEach((id) => {
        if (id !== nodeId) updateSelectionClass(id, false);
      });
      WS.selectedNodes.clear();
      WS.selectedNodes.add(nodeId);
      updateSelectionClass(nodeId, true);
      WS.selectedNodeId = nodeId;
    }
    if (!WS.selectedNodes.size) {
      WS.selectedNodeId = null;
    } else if (!WS.selectedNodes.has(WS.selectedNodeId)) {
      WS.selectedNodeId = Array.from(WS.selectedNodes).slice(-1)[0];
    }
    if (focus && WS.selectedNodeId) {
      WS.nodes.get(WS.selectedNodeId)?.el?.focus?.({ preventScroll: true });
    }
  }

  function deselectNode(nodeId) {
    ensureSelectionSet();
    if (!WS.selectedNodes.delete(nodeId)) return;
    updateSelectionClass(nodeId, false);
    if (WS.selectedNodeId === nodeId) {
      WS.selectedNodeId = WS.selectedNodes.size ? Array.from(WS.selectedNodes).slice(-1)[0] : null;
    }
  }

  function clearSelectedNode() {
    setSelectedNode(null);
  }

  function selectAllNodes() {
    ensureSelectionSet();
    if (!WS.nodes.size) {
      clearSelectedNode();
      return;
    }
    WS.selectedNodes.clear();
    WS.nodes.forEach((n, id) => {
      if (!n?.el) return;
      WS.selectedNodes.add(id);
      n.el.classList.add('selected');
    });
    WS.selectedNodeId = Array.from(WS.selectedNodes)[WS.selectedNodes.size - 1] || null;
    if (WS.selectedNodeId) {
      WS.nodes.get(WS.selectedNodeId)?.el?.focus?.({ preventScroll: true });
    }
  }

  function updatePointerLocation(e) {
    if (!e) return;
    WS.lastPointer = { clientX: e.clientX, clientY: e.clientY };
  }

  function workspacePointFromPointer() {
    if (!WS.lastPointer) return currentViewCenter();
    const { clientX, clientY } = WS.lastPointer;
    if (!Number.isFinite(clientX) || !Number.isFinite(clientY)) return currentViewCenter();
    return clientToWorkspace(clientX, clientY);
  }

  const NODE_MIN_WIDTH = 230;
  const NODE_MIN_HEIGHT = 120;
  const NODE_MAX_WIDTH = 512;
  const NODE_MAX_HEIGHT = 1024;
  const GRID_SIZE = 14;
  const WIRE_ACTIVE_MS = 750;
  let zoomRefreshRaf = 0;

  const currentScale = () => {
    const s = Number(WS.view?.scale);
    return Number.isFinite(s) && s > 0 ? s : 1;
  };

  const clampSize = (value, min, max) => {
    const numeric = Number.isFinite(value) ? value : min;
    return Math.min(Math.max(numeric, min), max);
  };

  function historySnapshotNode(nodeId) {
    const node = WS.nodes.get(nodeId);
    if (!node) return null;
    const width = Math.round(node.el?.offsetWidth || node.w || 0);
    const height = Math.round(node.el?.offsetHeight || node.h || 0);
    node.w = width;
    node.h = height;
    const rec = NodeStore.load(nodeId);
    const config = rec?.config ? deepClone(rec.config) : {};
    return {
      id: node.id,
      type: node.type,
      x: Number.isFinite(node.x) ? node.x : 0,
      y: Number.isFinite(node.y) ? node.y : 0,
      w: width,
      h: height,
      sizeLocked: Boolean(node.sizeLocked),
      config
    };
  }

  function restoreNodeFromHistory(snapshot) {
    if (!snapshot || typeof snapshot !== 'object') return null;
    const options = {
      id: snapshot.id,
      config: snapshot.config,
      sizeLocked: Boolean(snapshot.sizeLocked),
      width: snapshot.w,
      height: snapshot.h
    };
    const node = addNode(snapshot.type, snapshot.x, snapshot.y, options);
    if (node) {
      node.x = snapshot.x;
      node.y = snapshot.y;
      if (node.el) {
        node.el.style.left = `${snapshot.x}px`;
        node.el.style.top = `${snapshot.y}px`;
      }
    }
    return node;
  }

  function removeWireByEndpoints(fromNodeId, fromPort, toNodeId, toPort, { skipHistory = false, suppressBadge = false } = {}) {
    const wire = WS.wires.find(
      (w) =>
        w.from.node === fromNodeId &&
        w.from.port === fromPort &&
        w.to.node === toNodeId &&
        w.to.port === toPort
    );
    if (!wire) return false;
    const payload = cloneWireForHistory(wire);
    detachWire(wire);
    flushWireMutations();
    if (!skipHistory && !History.isSilent() && payload) {
      History.push({ type: 'wire.remove', wire: payload });
    }
    if (!suppressBadge) setBadge('Wire removed');
    return true;
  }

  function applyHistoryUndo(entry) {
    if (!entry || typeof entry !== 'object') return;
    switch (entry.type) {
      case 'node.add': {
        removeNode(entry.node?.id);
        setBadge('Undo node add');
        break;
      }
      case 'node.remove': {
        const restored = restoreNodeFromHistory(entry.node);
        if (restored && Array.isArray(entry.wires)) {
          entry.wires.forEach((wire) => {
            if (!wire) return;
            const fromNodeId = wire.from?.node;
            const toNodeId = wire.to?.node;
            if (!WS.nodes.has(fromNodeId) || !WS.nodes.has(toNodeId)) return;
            addLink(fromNodeId, wire.from.port, toNodeId, wire.to.port);
          });
        }
        setBadge('Undo node removal');
        break;
      }
      case 'wire.add': {
        const wire = entry.wire;
        if (!wire) break;
        removeWireByEndpoints(wire.from.node, wire.from.port, wire.to.node, wire.to.port, { skipHistory: true, suppressBadge: true });
        setBadge('Undo wire add');
        break;
      }
      case 'wire.remove': {
        const wire = entry.wire;
        if (!wire) break;
        if (WS.nodes.has(wire.from.node) && WS.nodes.has(wire.to.node)) {
          addLink(wire.from.node, wire.from.port, wire.to.node, wire.to.port);
        }
        setBadge('Undo wire removal');
        break;
      }
      case 'node.config': {
        applyNodeConfigSnapshot(entry.nodeId, entry.nodeType, entry.before, { quiet: true });
        setBadge('Undo node settings');
        break;
      }
      default:
        break;
    }
  }

  function applyHistoryRedo(entry) {
    if (!entry || typeof entry !== 'object') return;
    switch (entry.type) {
      case 'node.add': {
        restoreNodeFromHistory(entry.node);
        setBadge('Redo node add');
        break;
      }
      case 'node.remove': {
        removeNode(entry.node?.id);
        setBadge('Redo node removal');
        break;
      }
      case 'wire.add': {
        const wire = entry.wire;
        if (!wire) break;
        if (WS.nodes.has(wire.from.node) && WS.nodes.has(wire.to.node)) {
          addLink(wire.from.node, wire.from.port, wire.to.node, wire.to.port);
        }
        setBadge('Redo wire add');
        break;
      }
      case 'wire.remove': {
        const wire = entry.wire;
        if (!wire) break;
        removeWireByEndpoints(wire.from.node, wire.from.port, wire.to.node, wire.to.port, { skipHistory: true, suppressBadge: true });
        setBadge('Redo wire removal');
        break;
      }
      case 'node.config': {
        applyNodeConfigSnapshot(entry.nodeId, entry.nodeType, entry.after, { quiet: true });
        setBadge('Redo node settings');
        break;
      }
      default:
        break;
    }
  }

  History.setHandlers({ onUndo: applyHistoryUndo, onRedo: applyHistoryRedo });

  function ensureSvgLayer() {
    if (!WS.svg) return;
    if (!WS.svgLayer || WS.svgLayer.parentNode !== WS.svg) {
      WS.svgLayer = document.createElementNS('http://www.w3.org/2000/svg', 'g');
      WS.svg.appendChild(WS.svgLayer);
    }
    // keep SVG wires layer in sync with current view
    WS.svgLayer.setAttribute('transform', `translate(${WS.view.x},${WS.view.y}) scale(${WS.view.scale})`);
  }


  function requestRedraw() {
    if (WS._redrawReq) return;
    WS._redrawReq = true;
    requestAnimationFrame(() => {
      WS._redrawReq = false;
      drawAllLinks();
    });
    //console.log('redraw requested')
  }

  function scheduleZoomRefresh() {
    if (zoomRefreshRaf) return;
    zoomRefreshRaf = requestAnimationFrame(() => {
      zoomRefreshRaf = 0;
      refreshNodeResolution();
    });
  }

  function broadcastViewportChange(reason = 'zoom') {
    const payload = { x: WS.view.x, y: WS.view.y, scale: WS.view.scale, reason };
    WS.nodes.forEach((node) => {
      node.refreshDimensions?.(false, reason);
      node.el?.dispatchEvent(new CustomEvent('workspace:viewport', { detail: payload }));
      // let feature modules repaint if they expose hooks
      if (node.type === 'TTS') TTS?.refreshUI?.(node.id);
      if (node.type === 'ASR') ASR?.refreshUI?.(node.id);
      Media?.onViewport?.(node.id, payload);
    });
  }


  function applyViewTransform() {
    const t = `translate(${WS.view.x}px, ${WS.view.y}px) scale(${WS.view.scale})`;
    if (WS.canvas) WS.canvas.style.transform = t;
    if (WS.svgLayer) WS.svgLayer.setAttribute('transform', `translate(${WS.view.x},${WS.view.y}) scale(${WS.view.scale})`);
    syncWorkspaceBackground();       // maintains grid vars
    broadcastViewportChange('zoom'); // tell every node to recompute + repaint
    scheduleZoomRefresh();           // bump canvas backing stores next frame
    requestRedraw();                 // re-path the wires immediately
  }


  function syncWorkspaceBackground() {
    if (!WS.el) return;
    WS.el.style.setProperty('--grid-scale', WS.view.scale.toFixed(4));
    WS.el.style.setProperty('--grid-offset-x', `${WS.view.x}px`);
    WS.el.style.setProperty('--grid-offset-y', `${WS.view.y}px`);
    //console.log('synced workspace background')
    refreshNodeResolution();
  }
function refreshNodeResolution(force = false) {
  const dpr = (typeof window !== 'undefined' && window.devicePixelRatio) ? window.devicePixelRatio : 1;
  const scale = currentScale();

  WS.nodes.forEach((node) => {
    const root = node.el;
    if (!root) return;

    // DO NOT call node.refreshDimensions() here; it can cause width jumps.
    root.querySelectorAll('canvas').forEach((cv) => {
      // getBoundingClientRect() already includes CSS transforms (zoom)
      const r = cv.getBoundingClientRect();
      const targetW = Math.max(1, Math.round(r.width * dpr));
      const targetH = Math.max(1, Math.round(r.height * dpr));

      if (force || cv.width !== targetW || cv.height !== targetH) {
        cv.width = targetW;
        cv.height = targetH;

        // let node-specific drawers repaint if they want
        if (typeof node.onCanvasResize === 'function') {
          node.onCanvasResize(cv, { dpr, scale, width: targetW, height: targetH });
        }
        try {
          cv.dispatchEvent(new CustomEvent('canvas:resized', {
            bubbles: false,
            detail: { dpr, scale, width: targetW, height: targetH }
          }));
        } catch (_) { /* no-op */ }
      }
    });

    if (typeof node.onResolutionChange === 'function') {
      node.onResolutionChange({ dpr, scale });
    }
  });

  requestRedraw();
}


  function clientToWorkspace(cx, cy) {
    const rect = WS.root?.getBoundingClientRect?.() || document.body.getBoundingClientRect();
    const x = (cx - rect.left - WS.view.x) / WS.view.scale;
    const y = (cy - rect.top - WS.view.y) / WS.view.scale;
    return { x, y };
  }

  function currentViewCenter() {
    const rect = WS.root?.getBoundingClientRect?.() || document.body.getBoundingClientRect();
    return clientToWorkspace(rect.left + rect.width / 2, rect.top + rect.height / 2);
  }

  function positionNodeCentered(node, point, { save = true } = {}) {
    if (!node?.el || !point) return;
    const width = node.el.offsetWidth || node.w || 0;
    const height = node.el.offsetHeight || node.h || 0;
    const centeredX = Math.round(point.x - width / 2);
    const centeredY = Math.round(point.y - height / 2);
    node.x = centeredX;
    node.y = centeredY;
    node.el.style.left = `${centeredX}px`;
    node.el.style.top = `${centeredY}px`;
    if (save) saveGraph();
    requestRedraw();
  }

  function snapshotNode(nodeId) {
    const node = WS.nodes.get(nodeId);
    if (!node) return null;
    const rec = NodeStore.load(nodeId);
    const config = rec?.config ? deepClone(rec.config) : {};
    const width = Number.isFinite(node.w) ? node.w : Math.round(node.el?.offsetWidth || 0);
    const height = Number.isFinite(node.h) ? node.h : Math.round(node.el?.offsetHeight || 0);
    return {
      sourceId: nodeId,
      type: node.type,
      config,
      sizeLocked: Boolean(node.sizeLocked),
      w: width,
      h: height
    };
  }

  function duplicateNodeFromSnapshot(snapshot, point, { select = false } = {}) {
    if (!snapshot || typeof snapshot !== 'object') return null;
    const dropPoint = point || currentViewCenter();
    const width = Number.isFinite(snapshot.w) ? snapshot.w : undefined;
    const height = Number.isFinite(snapshot.h) ? snapshot.h : undefined;
    const node = addNode(snapshot.type, Math.round(dropPoint.x), Math.round(dropPoint.y), {
      config: snapshot.config,
      sizeLocked: snapshot.sizeLocked,
      width,
      height,
      select
    });
    if (node) {
      positionNodeCentered(node, dropPoint);
    }
    return node;
  }

  function duplicateNode(nodeId, point, { select = true, badge = true } = {}) {
    const snapshot = snapshotNode(nodeId);
    if (!snapshot) return null;
    WS.clipboard = snapshot;
    const targetPoint = point || currentViewCenter();
    const node = duplicateNodeFromSnapshot(snapshot, targetPoint, { select });
    if (node && badge) setBadge('Node duplicated');
    if (node && select) setSelectedNode(node.id, { focus: true });
    return node;
  }

  function uid() {
    return 'n' + Math.random().toString(36).slice(2, 8);
  }

  const relayStates = new Map();
  const RELAY_STATE_CLASSES = new Set(['ok', 'warn', 'err']);
  const DEFAULT_PENDING_MESSAGE = 'Awaiting connection';

  const MODEL_PREFETCHERS = {
    ASR: (id) => ASR?.ensureDefaults?.(id),
    LLM: (id) => LLM?.ensureDefaults?.(id),
    TTS: (id) => TTS?.ensureDefaults?.(id)
  };
  const pendingModelPrefetch = new Set();
  const MODEL_PROVIDERS = { LLM, TTS, ASR };
  let settingsModelSubscription = null;
  const LLM_BASE_INPUTS = new Set(['prompt', 'image', 'system']);
  const LLM_CAPABILITY_PORT_SPECS = [
    {
      name: 'tools',
      label: 'Tools',
      matches: (cap) => /tool|function/.test(cap)
    }
  ];
  const WASM_TYPES = new Set(['ASR', 'TTS']);
  const isTruthy = (value) => {
    if (value === true || value === false) return value;
    if (typeof value === 'string') return value.trim().toLowerCase() === 'true';
    return Boolean(value);
  };
  const isWasmNode = (type, cfg) => {
    if (!WASM_TYPES.has(type)) return false;
    return isTruthy(cfg?.wasm);
  };

  function initImageInputNode(node) {
    if (!node?.el || node.type !== 'ImageInput') return;
    const dropArea = node.el.querySelector('[data-image-drop]');
    const preview = node.el.querySelector('[data-image-preview]');
    const status = node.el.querySelector('[data-image-status]');
    if (!dropArea || dropArea._imageInitialized) return;
    dropArea._imageInitialized = true;

    const fileInput = dropArea.querySelector('input[type="file"]');

    const rec = NodeStore.ensure(node.id, 'ImageInput');
    const cfg = rec?.config || {};

    const renderImage = (dataUrl, mime, info) => {
      if (preview) {
        preview.src = dataUrl;
        preview.classList.remove('hidden');
      }
      if (status) {
        const parts = [];
        if (mime) parts.push(mime);
        if (info?.width && info?.height) parts.push(`${info.width}×${info.height}`);
        status.textContent = parts.length ? parts.join(' • ') : 'Image ready';
      }
    };

    if (cfg.image) {
      renderImage(cfg.image, cfg.mime, { width: cfg.width, height: cfg.height });
    }

    const sendPayload = (dataUrl, file) => {
      if (!dataUrl) return;
      const base64 = dataUrl.includes(',') ? dataUrl.split(',')[1] : dataUrl;
      const img = new Image();
      img.onload = () => {
        const payload = {
          nodeId: node.id,
          from: node.id,
          kind: 'image',
          mime: (file && file.type) || 'image/png',
          dataUrl,
          b64: base64,
          image: base64,
          width: img.width,
          height: img.height,
          ts: Date.now(),
          route: 'image'
        };
        NodeStore.update(node.id, {
          type: 'ImageInput',
          image: dataUrl,
          b64: base64,
          mime: payload.mime,
          width: img.width,
          height: img.height,
          updatedAt: Date.now()
        });
        renderImage(dataUrl, payload.mime, img);
        Router.sendFrom(node.id, 'image', payload);
      };
      img.onerror = () => {
        setBadge('Unable to read image', false);
      };
      img.src = dataUrl;
    };

    const handleFiles = (files) => {
      if (!files || !files.length) return;
      const file = files[0];
      if (!file.type.startsWith('image/')) {
        setBadge('Unsupported file type', false);
        return;
      }
      const reader = new FileReader();
      reader.onload = () => {
        const dataUrl = reader.result;
        sendPayload(dataUrl, file);
      };
      reader.onerror = () => {
        setBadge('Failed to read image', false);
      };
      reader.readAsDataURL(file);
    };

    dropArea.addEventListener('click', () => {
      fileInput?.click();
    });

    if (fileInput) {
      fileInput.addEventListener('change', (e) => {
        handleFiles(e.target.files);
        fileInput.value = '';
      });
    }

    dropArea.addEventListener('dragover', (e) => {
      e.preventDefault();
      dropArea.classList.add('dragover');
    });
    dropArea.addEventListener('dragleave', () => {
      dropArea.classList.remove('dragover');
    });
    dropArea.addEventListener('drop', (e) => {
      e.preventDefault();
      dropArea.classList.remove('dragover');
      handleFiles(e.dataTransfer?.files);
    });

    dropArea.addEventListener('paste', (e) => {
      const items = e.clipboardData?.items;
      if (!items) return;
      for (const item of items) {
        if (item.type && item.type.startsWith('image/')) {
          const file = item.getAsFile();
          if (file) {
            handleFiles([file]);
            e.preventDefault();
            break;
          }
        }
      }
    });
  }

  function initFaceLandmarksNode(node) {
    if (!node?.el || node.type !== 'FaceLandmarks') return;
    const body = node.el.querySelector('.body');
    if (!body) return;
    if (body.querySelector('[data-face-viewer]')) return;

    const viewerWrap = document.createElement('div');
    viewerWrap.className = 'face-viewer';
    viewerWrap.dataset.faceViewer = 'true';

    const status = document.createElement('div');
    status.className = 'face-viewer-status';
    status.textContent = 'Initializing 3D face…';
    viewerWrap.appendChild(status);

    const ports = body.querySelector('.ports');
    if (ports && ports.parentElement === body) {
      body.insertBefore(viewerWrap, ports.nextSibling);
    } else {
      body.appendChild(viewerWrap);
    }

    if (FaceViewer && typeof FaceViewer.init === 'function') {
      FaceViewer.init(node.id, viewerWrap, status);
    } else if (status) {
      status.textContent = '3D viewer unavailable';
    }
  }

  function initPaymentsNode(node) {
    if (!node?.el || node.type !== 'Payments') return;
    const body = node.el.querySelector('.body');
    if (!body) return;
    if (body.querySelector('[data-payment-panel]')) return;
    const panel = document.createElement('div');
    panel.className = 'payment-panel';
    panel.dataset.paymentPanel = 'true';
    body.appendChild(panel);
    Payments.mount?.(node.id, panel);
    Payments.init?.(node.id);
  }

  function scheduleModelPrefetch(nodeId, nodeType, delay = 200) {
    const run = MODEL_PREFETCHERS[nodeType];
    if (typeof run !== 'function') return;
    const rec = NodeStore.ensure(nodeId, nodeType);
    const cfg = rec?.config || {};
    if (isWasmNode(nodeType, cfg)) return;
    const baseRaw = cfg.base;
    const base = typeof baseRaw === 'string'
      ? baseRaw.trim()
      : String(baseRaw ?? '').trim();
    if (!base) return;
    const modelValue = cfg.model;
    const hasModel = typeof modelValue === 'string'
      ? modelValue.trim().length > 0
      : Boolean(modelValue);
    if (hasModel) return;
    const key = `${nodeType}:${nodeId}`;
    if (pendingModelPrefetch.has(key)) return;
    pendingModelPrefetch.add(key);
    const exec = () => {
      Promise.resolve(run(nodeId))
        .then(() => {
          if (nodeType === 'LLM') refreshLlmControls(nodeId);
        })
        .catch(() => { })
        .finally(() => pendingModelPrefetch.delete(key));
    };
    if (delay > 0) setTimeout(exec, delay);
    else exec();
  }

  function initLlmControls(node) {
    if (!node || node.type !== 'LLM') return;
    const body = node.el?.querySelector('.body');
    if (!body) return;
    if (node.llmControls?.container?.isConnected) return;

    const container = document.createElement('div');
    container.dataset.llmControls = 'true';
    container.className = 'llm-controls';

    const capLine = document.createElement('div');
    capLine.dataset.llmCapabilities = 'true';
    capLine.className = 'llm-capabilities';
    container.appendChild(capLine);

    const thinkWrap = document.createElement('label');
    thinkWrap.dataset.llmThink = 'true';
    thinkWrap.className = 'llm-think hidden';
    const thinkInput = document.createElement('input');
    thinkInput.type = 'checkbox';
    thinkInput.dataset.llmThinkInput = 'true';
    thinkWrap.appendChild(thinkInput);
    thinkWrap.appendChild(document.createTextNode(' Think'));
    container.appendChild(thinkWrap);

    const imageNote = document.createElement('div');
    imageNote.dataset.llmImageNote = 'true';
    imageNote.className = 'llm-image-note hidden';
    imageNote.textContent = 'Image input available via image port.';
    container.appendChild(imageNote);

    body.insertBefore(container, body.firstChild || null);

    thinkInput.addEventListener('change', () => {
      NodeStore.update(node.id, { type: 'LLM', think: !!thinkInput.checked });
      refreshLlmControls(node.id);
    });

    node.llmControls = {
      container,
      capLine,
      thinkWrap,
      thinkInput,
      imageNote
    };
  }

  function refreshLlmControls(nodeId) {
    const node = WS.nodes.get(nodeId);
    if (!node || node.type !== 'LLM') return;
    initLlmControls(node);
    const controls = node.llmControls;
    if (!controls) return;
    const rec = NodeStore.ensure(nodeId, 'LLM');
    const cfg = rec?.config || {};
    const capabilities = Array.isArray(cfg.capabilities)
      ? cfg.capabilities.map((c) => String(c).toLowerCase())
      : [];
    const displayCaps = Array.isArray(cfg.capabilities) && cfg.capabilities.length
      ? cfg.capabilities.join(', ')
      : '—';
    if (controls.capLine) controls.capLine.textContent = `Capabilities: ${displayCaps}`;

    const hasThinking = capabilities.some((cap) => cap === 'thinking' || cap === 'think' || cap === 'reasoning');
    if (controls.thinkWrap) controls.thinkWrap.classList.toggle('hidden', !hasThinking);
    if (controls.thinkInput && hasThinking) {
      const current = !!cfg.think;
      if (controls.thinkInput.checked !== current) controls.thinkInput.checked = current;
    }

    const hasImages = capabilities.some((cap) => cap === 'images' || cap === 'image' || cap === 'vision');
    if (controls.imageNote) controls.imageNote.classList.toggle('hidden', !hasImages);
    const imagePort = node.portEls?.['in:image'];
    if (imagePort) {
      imagePort.style.display = hasImages ? '' : 'none';
    }
    if (!hasImages) removeWiresAt(nodeId, 'in', 'image');

    ensureLlmCapabilityPorts(nodeId, capabilities);
  }

  function handleInputPayload(node, portName, payload) {
    if (!node) return;
    if (node.type === 'LogicGate') {
      return handleLogicGateInput(node.id, portName, payload);
    }
    if (node.type === 'FaceLandmarks') {
      return Vision?.Face?.onInput?.(node.id, portName, payload);
    }
    if (node.type === 'PoseLandmarks') {
      if (portName === 'media' || portName === 'image') return Vision?.Pose?.onInput?.(node.id, portName, payload);
      return;
    }
    if (node.type === 'Meshtastic') {
      if (portName === 'public') return Meshtastic.sendPublic?.(node.id, payload);
      if (portName.startsWith('peer-')) return Meshtastic.sendPeer?.(node.id, portName, payload);
      if (portName === 'auto') return Meshtastic.sendAuto?.(node.id, payload);
      return;
    }
    if (node.type === 'LLM') {
      if (portName === 'prompt') {
        pullLlmSystemInput(node.id);
        const pendingImages = pullLlmImageInput(node.id);
        if (pendingImages.length && typeof LLM.onImage === 'function') {
          LLM.onImage(node.id, pendingImages);
        }
        return LLM.onPrompt(node.id, payload);
      }
      if (portName === 'image') return LLM.onImage?.(node.id, payload);
      if (portName === 'system') return LLM.onSystem(node.id, payload);
      if (portName === 'tools') return LLM.onTools?.(node.id, payload);
      return;
    }
    if (node.type === 'ASR') {
      if (portName === 'mute') return ASR.onMute?.(node.id, payload);
      if (portName === 'audio') return ASR.onAudio?.(node.id, payload);
      return;
    }
    if (node.type === 'TTS') {
      if (portName === 'text') return TTS.onText(node.id, payload);
      if (portName === 'mute') return TTS.onMute?.(node.id, payload);
      return;
    }
    if (node.type === 'NknDM') {
      if (portName === 'text') return NknDM.onText(node.id, payload);
      if (portName === 'packet') return NknDM.onPacket(node.id, payload);
      return;
    }
    if (node.type === 'NoClipBridge') {
      if (portName === 'pose') return NoClip?.onPose?.(node.id, payload);
      if (portName === 'resource') return NoClip?.onResource?.(node.id, payload);
      if (portName === 'command') return NoClip?.onCommand?.(node.id, payload);
      if (portName === 'audioOutput') return NoClip?.onAudioOutput?.(node.id, payload);
      return;
    }
    if (node.type === 'WebSerial') {
      if (portName === 'send') return WebSerial.onSend(node.id, payload);
      return;
    }
    if (node.type === 'FileTransfer') {
      if (portName === 'incoming') return FileTransfer.onIncoming(node.id, payload);
      if (portName === 'file') return FileTransfer.onFilePayload(node.id, payload);
      return;
    }
    if (node.type === 'MCP') {
      if (portName === 'query') return MCP.onQuery(node.id, payload);
      if (portName === 'tool') return MCP.onTool(node.id, payload);
      if (portName === 'refresh') return MCP.onRefresh(node.id, payload);
      return;
    }
    if (node.type === 'Payments') {
      return Payments.onInput?.(node.id, portName, payload);
    }

    if (node.type === 'WebScraper') {
      return WebScraper.onInput?.(node.id, portName, payload);
    }

    if (node.type === 'MediaStream') {
      if (portName === 'media') return Media.onInput(node.id, payload);
      return;
    }
    if (node.type === 'TextDisplay') {
      if (portName === 'text') return handleTextDisplayInput(node.id, payload);
      return;
    }
    if (node.type === 'TextInput') {
      return handleTextInputIngress(node.id, portName, payload);
    }
    if (node.type === 'Template') {
      if (portName === 'trigger') return handleTemplateTrigger(node.id, payload);
      return;
    }
  }

  function unregisterInputPort(node, portName) {
    const baseKey = `${node.id}:in:${portName}`;
    Router.ports.delete(baseKey);
    const fromTypeMatch = /^([a-z]+):([^:]+):(.+)$/.exec(baseKey);
    if (fromTypeMatch) {
      Router.ports.delete(`${fromTypeMatch[2]}:in:${fromTypeMatch[3]}`);
    }
    const typeGuess = node.type?.toLowerCase?.();
    if (typeGuess) Router.ports.delete(`${typeGuess}:${node.id}:${portName}`);
  }

  const PORT_DISCONNECT_HINT = 'Alt-click to disconnect';

  function formatPortTitle(label, tooltip) {
    const hint = PORT_DISCONNECT_HINT;
    const tip = typeof tooltip === 'string' ? tooltip.trim() : '';
    if (tip) return `${tip}\n\n${hint}`;
    return `${label} (${hint})`;
  }

  function applyPortTooltip(el, label, tooltip) {
    if (!el) return;
    const labelText = formatPortTitle(label, tooltip);
    if (labelText) {
      el.setAttribute('aria-label', labelText);
      el.setAttribute('title', labelText);
    } else {
      el.removeAttribute('aria-label');
      el.removeAttribute('title');
    }
    const baseTip = typeof tooltip === 'string' ? tooltip.trim() : '';
    const includeDefault = tooltip !== null && tooltip !== false;
    const tip = baseTip ? `${baseTip}\n\n${PORT_DISCONNECT_HINT}` : (includeDefault ? PORT_DISCONNECT_HINT : '');
    if (tip) {
      el.dataset.tooltip = tip;
      if (!el._tooltipBound) {
        const show = () => Tooltip.show(el);
        const hide = () => Tooltip.hide(el);
        el.addEventListener('mouseenter', show);
        el.addEventListener('pointerenter', show);
        el.addEventListener('mouseleave', hide);
        el.addEventListener('pointerleave', hide);
        el.addEventListener('focus', show);
        el.addEventListener('blur', hide);
        el.addEventListener('pointerdown', hide);
        el._tooltipHandlers = { show, hide };
        el._tooltipBound = true;
      }
      Tooltip.refresh(el);
    } else {
      delete el.dataset.tooltip;
      Tooltip.hide(el);
    }
  }

  function createInputPort(node, container, portName, label = portName, tooltip = '', portType = null) {
    if (!node || !container) return null;
    node.portEls = node.portEls || {};
    const key = `in:${portName}`;
    const existing = node.portEls[key];
    if (existing && existing.isConnected) {
      const labelEl = existing.querySelector('span:last-child');
      if (labelEl) labelEl.textContent = label;
      applyPortTooltip(existing, label, tooltip);
      return existing;
    }
    const portEl = document.createElement('div');
    portEl.className = 'wp-port in';
    portEl.dataset.port = portName;
    if (portType) portEl.dataset.portType = portType;
    applyPortTooltip(portEl, label, tooltip);

    // Audio ports get special styling
    const dotClass = portType === 'audio' ? 'dot audio-port' : 'dot';
    portEl.innerHTML = `<span class="${dotClass}"></span><span>${label}</span>`;
    node.portEls[key] = portEl;

    portEl.addEventListener('click', (ev) => {
      if (ev.altKey || ev.metaKey || ev.ctrlKey) {
        removeWiresAt(node.id, 'in', portName);
        return;
      }
      onPortClick(node.id, 'in', portName, portEl);
    });

    portEl.addEventListener('pointerdown', (ev) => {
      if (ev.pointerType === 'touch') ev.preventDefault();
      const wires = connectedWires(node.id, 'in', portName);
      if (wires.length) {
        const w = wires[wires.length - 1];
        w.path?.setAttribute('stroke-dasharray', '6 4');
        const move = (e) => {
          if (e.pointerType === 'touch') e.preventDefault();
          if (WS.drag) WS.drag.lastClient = { x: e.clientX, y: e.clientY };
          const pt = clientToWorkspace(e.clientX, e.clientY);
          drawRetarget(w, 'to', pt.x, pt.y);
          if (WS.drag) updateDropHover(WS.drag.expected);
        };
        const up = (e) => {
          setDropHover(null);
          finishAnyDrag(e.clientX, e.clientY);
        };
        WS.drag = {
          kind: 'retarget',
          wireId: w.id,
          grabSide: 'to',
          path: w.path,
          pointerId: ev.pointerId,
          expected: 'out',
          lastClient: { x: ev.clientX, y: ev.clientY },
          _cleanup: () => window.removeEventListener('pointermove', move)
        };
        window.addEventListener('pointermove', move, { passive: false });
        window.addEventListener('pointerup', up, { once: true, passive: false });
        updateDropHover('out');
        return;
      }

      const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      path.setAttribute('fill', 'none');
      path.setAttribute('stroke', 'rgba(255,255,255,.6)');
      path.setAttribute('stroke-width', '2');
      path.setAttribute('stroke-linecap', 'round');
      path.setAttribute('opacity', '0.9');
      path.setAttribute('stroke-dasharray', '6 4');
      path.setAttribute('vector-effect', 'non-scaling-stroke');
      path.setAttribute('pointer-events', 'none');
      WS.svgLayer.appendChild(path);

      const move = (e) => {
        if (e.pointerType === 'touch') e.preventDefault();
        if (WS.drag) WS.drag.lastClient = { x: e.clientX, y: e.clientY };
        const pt = clientToWorkspace(e.clientX, e.clientY);
        drawTempFromPort({ nodeId: node.id, side: 'in', portName }, pt.x, pt.y);
        updateDropHover(WS.drag?.expected);
      };
      const up = (e) => {
        setDropHover(null);
        finishAnyDrag(e.clientX, e.clientY);
      };
      WS.drag = {
        kind: 'newFromInput',
        toNodeId: node.id,
        toPort: portName,
        path,
        pointerId: ev.pointerId,
        expected: 'out',
        lastClient: { x: ev.clientX, y: ev.clientY },
        _cleanup: () => window.removeEventListener('pointermove', move)
      };
      window.addEventListener('pointermove', move, { passive: false });
      window.addEventListener('pointerup', up, { once: true, passive: false });
      const pt = clientToWorkspace(ev.clientX, ev.clientY);
      drawTempFromPort({ nodeId: node.id, side: 'in', portName }, pt.x, pt.y);
      updateDropHover('out');
    });

    container.appendChild(portEl);
    Router.register(`${node.id}:in:${portName}`, portEl, (payload) => handleInputPayload(node, portName, payload));
    return portEl;
  }

  function createOutputPort(node, container, portName, label = portName, tooltip = '', portType = null) {
    if (!node || !container) return null;
    node.portEls = node.portEls || {};
    const key = `out:${portName}`;
    const existing = node.portEls[key];
    if (existing && existing.isConnected) {
      const labelEl = existing.querySelector('span:first-child');
      if (labelEl) labelEl.textContent = label;
      applyPortTooltip(existing, label, tooltip);
      return existing;
    }
    const portEl = document.createElement('div');
    portEl.className = 'wp-port out';
    portEl.dataset.port = portName;
    if (portType) portEl.dataset.portType = portType;
    applyPortTooltip(portEl, label, tooltip);

    // Audio ports get special styling
    const dotClass = portType === 'audio' ? 'dot audio-port' : 'dot';
    portEl.innerHTML = `<span>${label}</span><span class="${dotClass}"></span>`;
    node.portEls[key] = portEl;

    portEl.addEventListener('click', (ev) => {
      if (ev.altKey || ev.metaKey || ev.ctrlKey) {
        removeWiresAt(node.id, 'out', portName);
        return;
      }
      onPortClick(node.id, 'out', portName, portEl);
    });

    portEl.addEventListener('pointerdown', (ev) => {
      if (ev.pointerType === 'touch') ev.preventDefault();
      const wires = connectedWires(node.id, 'out', portName);
      if (wires.length) {
        const w = wires[wires.length - 1];
        w.path?.setAttribute('stroke-dasharray', '6 4');
        const move = (e) => {
          if (e.pointerType === 'touch') e.preventDefault();
          if (WS.drag) WS.drag.lastClient = { x: e.clientX, y: e.clientY };
          const pt = clientToWorkspace(e.clientX, e.clientY);
          drawRetarget(w, 'from', pt.x, pt.y);
          if (WS.drag) updateDropHover(WS.drag.expected);
        };
        const up = (e) => {
          setDropHover(null);
          finishAnyDrag(e.clientX, e.clientY);
        };
        WS.drag = {
          kind: 'retarget',
          wireId: w.id,
          grabSide: 'from',
          path: w.path,
          pointerId: ev.pointerId,
          expected: 'in',
          lastClient: { x: ev.clientX, y: ev.clientY },
          _cleanup: () => window.removeEventListener('pointermove', move)
        };
        window.addEventListener('pointermove', move, { passive: false });
        window.addEventListener('pointerup', up, { once: true, passive: false });
        updateDropHover('in');
        return;
      }

      const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      path.setAttribute('fill', 'none');
      path.setAttribute('stroke', 'rgba(255,255,255,.6)');
      path.setAttribute('stroke-width', '2');
      path.setAttribute('stroke-linecap', 'round');
      path.setAttribute('opacity', '0.9');
      path.setAttribute('stroke-dasharray', '6 4');
      path.setAttribute('vector-effect', 'non-scaling-stroke');
      path.setAttribute('pointer-events', 'none');
      WS.svgLayer.appendChild(path);

      const move = (e) => {
        if (e.pointerType === 'touch') e.preventDefault();
        if (WS.drag) WS.drag.lastClient = { x: e.clientX, y: e.clientY };
        const pt = clientToWorkspace(e.clientX, e.clientY);
        drawTempFromPort({ nodeId: node.id, side: 'out', portName }, pt.x, pt.y);
        updateDropHover(WS.drag?.expected);
      };
      const up = (e) => {
        setDropHover(null);
        finishAnyDrag(e.clientX, e.clientY);
      };
      WS.drag = {
        kind: 'new',
        fromNodeId: node.id,
        fromPort: portName,
        path,
        pointerId: ev.pointerId,
        expected: 'in',
        lastClient: { x: ev.clientX, y: ev.clientY },
        _cleanup: () => window.removeEventListener('pointermove', move)
      };
      window.addEventListener('pointermove', move, { passive: false });
      window.addEventListener('pointerup', up, { once: true, passive: false });
      const pt = clientToWorkspace(ev.clientX, ev.clientY);
      drawTempFromPort({ nodeId: node.id, side: 'out', portName }, pt.x, pt.y);
      updateDropHover('in');
    });

    container.appendChild(portEl);
    return portEl;
  }

  function ensureLlmCapabilityPorts(nodeId, capsOverride) {
    const node = WS.nodes.get(nodeId);
    if (!node || node.type !== 'LLM') return;
    const left = node.el?.querySelector('.side.left');
    if (!left) return;
    const rec = NodeStore.ensure(nodeId, 'LLM');
    const cfgCaps = capsOverride || rec?.config?.capabilities || [];
    const normalized = Array.isArray(cfgCaps)
      ? cfgCaps.map((c) => String(c).toLowerCase())
      : [];

    const desired = new Map();
    for (const spec of LLM_CAPABILITY_PORT_SPECS) {
      if (normalized.some((cap) => spec.matches(cap))) {
        desired.set(spec.name, spec.label);
      }
    }

    const existingEls = Array.from(left.querySelectorAll('.wp-port.in'));
    const existingNames = new Set(existingEls.map((el) => el.dataset.port));

    for (const [portName, label] of desired) {
      if (!existingNames.has(portName)) {
        createInputPort(node, left, portName, label);
      }
    }

    for (const el of existingEls) {
      const name = el.dataset.port;
      if (!name) continue;
      if (LLM_BASE_INPUTS.has(name)) continue;
      if (desired.has(name)) continue;
      removeWiresAt(nodeId, 'in', name);
      unregisterInputPort(node, name);
      el.remove();
      delete node.portEls[`in:${name}`];
    }
  }

  function normalizeRelayState(state) {
    const value = String(state || '').toLowerCase();
    return RELAY_STATE_CLASSES.has(value) ? value : 'warn';
  }

  function getTypeInfo(type) {
    return typeof type === 'string' && GraphTypes ? GraphTypes[type] : null;
  }

  function getRelayValueFromConfig(cfg, info) {
    if (!info || !info.relayKey) return '';
    if (isWasmNode(info?.title || info?.type || '', cfg) || isWasmNode(info?.nodeType || '', cfg)) return '';
    if (isWasmNode('ASR', cfg) || isWasmNode('TTS', cfg)) return '';
    const mode = String(cfg?.endpointMode || 'auto').toLowerCase();
    if (mode === 'local') return '';
    const raw = cfg ? cfg[info.relayKey] : '';
    return typeof raw === 'string' ? raw.trim() : '';
  }

  function getNodeRelayState(nodeId) {
    return relayStates.get(nodeId) || null;
  }

  function clearNodeRelayState(nodeId) {
    relayStates.delete(nodeId);
  }

  function ensurePendingRelayState(nodeId, message = DEFAULT_PENDING_MESSAGE) {
    if (!relayStates.has(nodeId)) {
      relayStates.set(nodeId, { state: 'warn', message, at: Date.now() });
    }
  }

  function setNodeRelayState(nodeId, stateOrDetail, message) {
    const node = WS.nodes.get(nodeId);
    if (!node) return;
    const info = getTypeInfo(node.type);
    if (!info?.supportsNkn) return;
    let state = 'warn';
    let msg = '';
    if (typeof stateOrDetail === 'string') {
      state = stateOrDetail;
      msg = typeof message === 'string' ? message : '';
    } else if (stateOrDetail && typeof stateOrDetail === 'object') {
      state = stateOrDetail.state ?? stateOrDetail.status ?? 'warn';
      msg = stateOrDetail.message ?? stateOrDetail.reason ?? stateOrDetail.detail ?? '';
    }
    state = normalizeRelayState(state);
    relayStates.set(nodeId, { state, message: msg, at: Date.now() });
    refreshNodeTransport(nodeId);
  }

  function parseNknAddress(rawText) {
    if (rawText == null) return '';
    let text = String(rawText).trim();
    if (!text) return '';
    try {
      const url = new URL(text);
      const params = url.searchParams;
      let candidate = '';
      const preferredKeys = ['nkn', 'address', 'addr', 'target'];
      for (const key of preferredKeys) {
        const value = params.get(key);
        if (value) {
          candidate = value;
          break;
        }
      }
      if (!candidate && params.size === 1) {
        candidate = Array.from(params.values())[0] || '';
      }
      if (!candidate && url.search && url.search.length > 1) {
        candidate = url.search.slice(1);
      }
      if (!candidate && url.hash && url.hash.length > 1) {
        candidate = url.hash.slice(1);
      }
      if (!candidate && url.pathname && url.pathname !== '/') {
        candidate = url.pathname.replace(/^\/+/, '');
      }
      if (candidate) text = candidate;
    } catch (err) {
      // not a URL, keep original text
    }
    try {
      text = decodeURIComponent(text);
    } catch (err) {
      // ignore decode issues
    }
    text = text.trim().replace(/^[?#/]+/, '').replace(/[?#/]+$/, '');
    if (!text) return '';
    if (text.includes('?')) text = text.split('?')[0];
    if (text.includes('&')) text = text.split('&')[0];
    if (text.includes('=')) text = text.split('=')[1] || text;
    text = text.replace(/\s+/g, '');
    if (!/^[A-Za-z0-9][A-Za-z0-9._:~-]{2,}$/.test(text)) return '';
    return text;
  }

  function applyRelayValue(nodeId, nodeType, info, value) {
    if (!info?.supportsNkn) return;
    const key = info.relayKey || 'relay';
    if (key === 'relay') {
      NodeStore.setRelay(nodeId, nodeType, value);
    } else {
      NodeStore.update(nodeId, { type: nodeType, [key]: value });
    }
  }

  function handleRelayScan(nodeId, nodeType, rawText, options = {}) {
    const { skipIfSet = false, pendingMessage = DEFAULT_PENDING_MESSAGE, badgeOnSave = 'NKN address saved', badgeOnNoChange = 'NKN address already set', badgeOnInvalid = 'Invalid NKN address' } = options;
    const node = WS.nodes.get(nodeId);
    if (!node) return false;
    const info = getTypeInfo(nodeType || node.type);
    if (!info?.supportsNkn) return false;
    const parsed = parseNknAddress(rawText);
    if (!parsed) {
      setBadge(badgeOnInvalid, false);
      return false;
    }
    const rec = NodeStore.ensure(nodeId, node.type);
    const cfg = rec.config || {};
    const current = getRelayValueFromConfig(cfg, info);
    if (current) {
      if (skipIfSet) {
        refreshNodeTransport(nodeId);
        return false;
      }
      if (current === parsed) {
        ensurePendingRelayState(nodeId, pendingMessage);
        refreshNodeTransport(nodeId);
        setBadge(badgeOnNoChange);
        return false;
      }
    }
    applyRelayValue(nodeId, node.type, info, parsed);
    scheduleModelPrefetch(node.id, node.type, 0);
    ensurePendingRelayState(nodeId, pendingMessage);
    saveGraph();
    if (info.relayKey === 'relay') updateTransportButton();
    setBadge(badgeOnSave);
    refreshNodeTransport(nodeId);
    return true;
  }

  function refreshNodeTransport(nodeId) {
    const node = WS.nodes.get(nodeId);
    if (!node) return;
    const info = getTypeInfo(node.type);
    if (!info?.supportsNkn) return;
    const btn = node.el?.querySelector('.node-transport');
    if (!btn) return;
    const cfg = NodeStore.ensure(nodeId, node.type)?.config || {};
    const wasmMode = isWasmNode(node.type, cfg);
    btn.classList.toggle('hidden', wasmMode);
    if (wasmMode) {
      btn.setAttribute('aria-hidden', 'true');
      btn.classList.remove('active');
      btn.removeAttribute('data-relay-state');
      btn.innerHTML = '';
      return;
    }
    btn.removeAttribute('aria-hidden');
    btn.innerHTML = '';
    btn.classList.remove('active');
    btn.removeAttribute('data-relay-state');
    const dot = document.createElement('span');
    dot.className = 'dot';
    const label = document.createElement('span');
    label.className = 'label';
    label.textContent = 'NKN';
    if (node.type === 'MediaStream') {
      const targets = Media.getTargets(node.id);
      const peerMeta = Media.getPeerMeta(node.id);
      const activeTargets = new Set(Media.getActiveTargets(node.id));
      let accepted = 0;
      let pending = 0;
      let viewing = 0;
      for (const addr of targets) {
        const meta = peerMeta[addr] || {};
        const handshakeStatus = meta.handshake?.status || 'idle';
        if (handshakeStatus === 'accepted') {
          accepted += 1;
          if (meta.viewing) viewing += 1;
        } else if (handshakeStatus === 'pending') {
          pending += 1;
        }
      }
      let stateClass = 'warn';
      if (!targets.length) stateClass = 'warn';
      else if (accepted) stateClass = 'ok';
      else if (!pending) stateClass = 'err';
      dot.classList.add(stateClass === 'ok' ? 'ok' : stateClass === 'err' ? 'err' : 'warn');
      btn.classList.toggle('active', targets.length > 0);
      btn.dataset.relayState = stateClass;
      const summary = [];
      summary.push(`Targets: ${targets.length}`);
      if (accepted) summary.push(`Accepted: ${accepted}`);
      if (pending) summary.push(`Pending: ${pending}`);
      if (viewing) summary.push(`Viewing: ${viewing}`);
      if (activeTargets.size) summary.push(`Active: ${activeTargets.size}`);
      btn.title = summary.join('\n');
      btn.append(dot, label);
      return;
    }
    const rec = NodeStore.ensure(node.id, node.type);
    const cfgr = rec.config || {};
    const relay = getRelayValueFromConfig(cfgr, info);
    if (relay) {
      btn.classList.add('active');
      let entry = relayStates.get(node.id);
      if (!entry) {
        ensurePendingRelayState(node.id);
        entry = relayStates.get(node.id);
      }
      let stateClass = entry?.state || 'warn';
      stateClass = normalizeRelayState(stateClass);
      let detailMessage = entry?.message || '';
      if (CFG.transport !== 'nkn') {
        stateClass = 'warn';
        if (!detailMessage) detailMessage = 'HTTP transport active';
      } else if (!Net?.nkn?.ready) {
        stateClass = 'warn';
        if (!detailMessage) detailMessage = 'NKN transport connecting';
      }
      dot.classList.add(stateClass);
      btn.dataset.relayState = stateClass;
      const titleLines = [relay];
      if (detailMessage) titleLines.push(detailMessage);
      btn.title = titleLines.join('\n');
    } else {
      clearNodeRelayState(node.id);
      dot.classList.add('err');
      btn.title = 'Scan a relay QR code';
    }
    btn.append(dot, label);
  }

  function refreshAllNodeTransport() {
    WS.nodes.forEach((n) => refreshNodeTransport(n.id));
  }

  registerQrResultHandler(({ text }) => {
    if (!text) return;
    const form = qs('#settingsForm');
    const nodeId = form?.dataset?.nodeId;
    if (!nodeId) return;
    try {
      handleRelayScan(nodeId, WS.nodes.get(nodeId)?.type, text, {
        skipIfSet: true,
        badgeOnSave: 'NKN address saved',
        badgeOnNoChange: 'NKN address unchanged',
        badgeOnInvalid: 'Invalid NKN QR payload'
      });
      refreshNodeTransport(nodeId);
    } catch (err) {
      log('[qr] ' + (err?.message || err));
    }
  });

  function makeNodeEl(node) {
    const TYPES = GraphTypes;
    const t = TYPES[node.type];
    const el = document.createElement('div');
    el.className = 'node';
    el.style.maxWidth = `${NODE_MAX_WIDTH}px`;
    el.style.maxHeight = `${NODE_MAX_HEIGHT}px`;
    el.style.left = `${node.x || 60}px`;
    el.style.top = `${node.y || 60}px`;
    const initialWidth = Number.isFinite(node.w)
      ? clampSize(node.w, NODE_MIN_WIDTH, NODE_MAX_WIDTH)
      : null;
    const initialHeight = Number.isFinite(node.h)
      ? clampSize(node.h, NODE_MIN_HEIGHT, NODE_MAX_HEIGHT)
      : null;
    if (initialWidth !== null) node.w = initialWidth;
    if (initialHeight !== null) node.h = initialHeight;
    el.dataset.id = node.id;
    el.tabIndex = 0;
    el.addEventListener('pointerdown', (evt) => {
      if (evt.button !== undefined && evt.button !== 0) return;
      const toggle = evt.ctrlKey || evt.metaKey;
      const additive = !toggle && evt.shiftKey;
      if (toggle) {
        setSelectedNode(node.id, { toggle: true, focus: true });
      } else if (additive) {
        setSelectedNode(node.id, { additive: true, focus: true });
      } else if (isNodeSelected(node.id) && WS.selectedNodes.size > 1) {
        setSelectedNode(node.id, { additive: true, focus: true });
      } else {
        setSelectedNode(node.id, { focus: true });
      }
    });
    el.addEventListener('focus', () => {
      const multi = WS.selectedNodes?.size > 1;
      if (multi && isNodeSelected(node.id)) {
        node.el.focus?.({ preventScroll: true });
        return;
      }
      setSelectedNode(node.id, { focus: true });
    });

    node.sizeLocked = Boolean(node.sizeLocked);
    let headEl = null;
    let bodyEl = null;
    let frameEl = null;

    const ensureNodeRefs = () => {
      if (!frameEl) frameEl = el.querySelector('.node__frame');
      if (!headEl) headEl = el.querySelector('.head');
      if (!bodyEl) bodyEl = el.querySelector('.body');
    };

    const syncFrameConstraints = () => {
      ensureNodeRefs();
      if (!frameEl) return;
      frameEl.style.maxWidth = `${NODE_MAX_WIDTH}px`;
      frameEl.style.maxHeight = `${NODE_MAX_HEIGHT}px`;
      frameEl.style.minWidth = `${NODE_MIN_WIDTH}px`;
      frameEl.style.minHeight = 'min-content';
    };

    const syncFrameDimensions = () => {
      ensureNodeRefs();
      if (!frameEl) return;
      if (el.style.width) frameEl.style.width = el.style.width;
      else frameEl.style.removeProperty('width');
      if (el.style.height) frameEl.style.height = el.style.height;
      else frameEl.style.removeProperty('height');
    };

    const applyNodeWidth = (widthPx) => {
      if (!Number.isFinite(widthPx)) return node.w;
      const clamped = clampSize(widthPx, NODE_MIN_WIDTH, NODE_MAX_WIDTH);
      const widthStyle = `${clamped}px`;
      if (el.style.width !== widthStyle) el.style.width = widthStyle;
      ensureNodeRefs();
      if (frameEl && frameEl.style.width !== widthStyle) frameEl.style.width = widthStyle;
      node.w = clamped;
      return clamped;
    };

    const applyNodeHeight = (heightPx) => {
      if (!Number.isFinite(heightPx)) return node.h;
      const clamped = clampSize(heightPx, NODE_MIN_HEIGHT, NODE_MAX_HEIGHT);
      const heightStyle = `${clamped}px`;
      if (el.style.height !== heightStyle) el.style.height = heightStyle;
      ensureNodeRefs();
      if (frameEl && frameEl.style.height !== heightStyle) frameEl.style.height = heightStyle;
      node.h = clamped;
      return clamped;
    };

    const updateBodyOverflow = () => {
      ensureNodeRefs();
      if (!bodyEl) return;
      bodyEl.style.overflow = (node.sizeLocked || node._resizing) ? 'auto' : '';
    };

    const setSizeLock = (locked) => {
      node.sizeLocked = Boolean(locked);
      if (node.sizeLocked) el.dataset.sizeLock = 'manual';
      else delete el.dataset.sizeLock;
      updateBodyOverflow();
    };

    const transportMarkup = t.supportsNkn
      ? '<button type="button" class="node-transport" title="Toggle NKN relay">NKN</button>'
      : '';
    const wasmToggleMarkup = (node.type === 'ASR' || node.type === 'TTS')
      ? '<button class="gear wasmToggle" title="Toggle WASM / remote">WASM</button>'
      : '';

    el.innerHTML = `
      <div class="node__frame">
      <div class="head">
        <div class="titleRow"><div class="title">${t.title}</div>${transportMarkup}</div>
        <div class="row" style="gap:6px;">
          <button class="gear" title="Settings">⚙</button>
          <button class="info-btn docBtn" title="Open documentation" data-tooltip="Open documentation">
            <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" aria-hidden="true">
              <path d="M12 14.5V17.5M12 11.5H12.01M13 3H8.2C7.0799 3 6.51984 3 6.09202 3.21799C5.71569 3.40973 5.40973 3.71569 5.21799 4.09202C5 4.51984 5 5.0799 5 6.2V17.8C5 18.9201 5 19.4802 5.21799 19.908C5.40973 20.2843 5.71569 20.5903 6.09202 20.782C6.51984 21 7.0799 21 8.2 21H15.8C16.9201 21 17.4802 21 17.908 20.782C18.2843 20.5903 18.5903 20.2843 18.782 19.908C19 19.4802 19 18.9201 19 17.8V9M13 3L19 9M13 3V7.4C13 7.96005 13 8.24008 13.109 8.45399C13.2049 8.64215 13.3578 8.79513 13.546 8.89101C13.7599 9 14.0399 9 14.6 9H19" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
          </button>
          ${node.type === 'ASR' ? `<button class="gear asrPlay" title="Start/Stop">▶</button>` : ''}
          ${node.type === 'MediaStream' ? `<button class="gear mediaToggle" title="Start/Stop">▶</button>` : ''}
          ${node.type === 'Orientation' ? `<button class="gear orientationToggle" title="Start/Stop">▶</button>` : ''}
          ${node.type === 'Location' ? `<button class="gear locationToggle" title="Start/Stop">▶</button>` : ''}
          ${wasmToggleMarkup}
          <button class="gear" title="Remove">🗑</button>
        </div>
      </div>
      <div class="body">
        <div class="ports">
          <div class="side left"></div>
          <div class="side right"></div>
        </div>
        ${node.type === 'ASR' ? `
          <canvas data-asr-vis style="pointer-events:auto;width:100%;height:56px;background:rgba(0,0,0,.25);border-radius:4px"></canvas>
          <div class="asr-partial-row" style="pointer-events:auto;display:flex;align-items:center;justify-content:space-between;gap:10px;">
            <div class="muted" data-asr-partial-flag data-state="idle">Waiting</div>
            <div class="asr-rms" data-asr-rms data-state="idle">0.000</div>
          </div>
          <div class="bubble" data-asr-partial style="min-height:28px"></div>
          <div class="muted" style="pointer-events:auto;">Finals</div>
          <div class="code" data-asr-final style="min-height:60px;max-height:360px"></div>
        ` : ''}
        ${node.type === 'TTS' ? `
          <div class="row" style="pointer-events:auto;align-items:center;justify-content:space-between;gap:12px;">
              <input type="range" min="0" max="1" step="0.01" data-tts-volume class="slider" style="width:100%;">
          </div>
          <canvas data-tts-vis style="margin-top:4px;width:100%;height:56px;background:rgba(0,0,0,.25);border-radius:4px"></canvas>
          <audio data-tts-audio controls style="pointer-events:auto;display:none;"></audio>
        ` : ''}
        ${node.type === 'LLM' ? `
          <div class="muted" style="pointer-events:auto;">Output</div>
          <div class="code" data-llm-out style="min-height:60px;max-height:360px;overflow:auto;white-space:pre-wrap"></div>
        ` : ''}
        ${node.type === 'MCP' ? `
          <div class="muted" style="pointer-events:auto;">Status</div>
          <div class="bubble" data-mcp-status>(idle)</div>
          <div class="muted" style="pointer-events:auto;">Server</div>
          <div class="code" data-mcp-server style="min-height:48px;max-height:360px;overflow:auto;"></div>
          <div class="muted" style="pointer-events:auto;">Resources</div>
          <div class="code" data-mcp-resources style="min-height:48px;max-height:360px;overflow:auto;"></div>
          <div class="muted" style="pointer-events:auto;">Last Context</div>
          <div class="code" data-mcp-context style="min-height:60px;max-height:160px;overflow:auto;white-space:pre-wrap"></div>
        ` : ''}
        ${node.type === 'MediaStream' ? `
          <div class="muted" style="pointer-events:auto;">Local Preview</div>
          <div class="media-preview-wrap" data-media-local-wrap>
            <video class="media-preview" data-media-local autoplay playsinline muted></video>
            <button type="button" class="ghost media-flash hidden" data-media-flash title="Toggle flashlight">🔦</button>
          </div>
          <div class="tiny" data-media-local-hint style="margin-top:4px;">Tap preview to switch cameras</div>
          <div class="muted" style="margin-top:10px;">Send Targets</div>
          <div class="media-targets">
            <div class="media-target-entry">
              <input data-media-target-input type="text" placeholder="hydra.peer" autocapitalize="none" autocomplete="off" spellcheck="false" />
              <button type="button" class="ghost" data-media-target-add title="Add target">＋</button>
            </div>
            <div class="media-target-chips" data-media-target-chips></div>
          </div>
          <div class="muted" style="margin-top:10px;">Remote Feeds</div>
          <div class="media-remote-grid" data-media-remote-grid>
            <div class="tiny media-remote-empty" data-media-remote-empty>(no remote streams)</div>
          </div>
          <div class="muted" data-media-status style="pointer-events:auto;">Ready</div>
        ` : ''}
        ${node.type === 'WebScraper' ? `
          <div class="wscraper" data-webscraper-root>
            <div class="wscraper-top">
              <div class="wscraper-buttons">
                <button type="button" class="ghost" data-ws-connect>Connect</button>
                <button type="button" class="ghost" data-ws-close disabled>Close</button>
                <button type="button" class="ghost" data-ws-screenshot>Screenshot</button>
                <button type="button" class="ghost" data-ws-dom>DOM</button>
              </div>
              <div class="wscraper-session">
                <span class="muted">SID</span>
                <span class="code" data-ws-sid>(none)</span>
              </div>
            </div>
            <div class="wscraper-field">
              <label class="muted">URL</label>
              <div class="wscraper-input-row">
                <input type="text" data-ws-url placeholder="https://example.com" autocapitalize="none" autocomplete="off" spellcheck="false" />
                <button type="button" class="ghost" data-ws-nav>Go</button>
              </div>
            </div>
            <div class="wscraper-buttons secondary">
              <button type="button" class="ghost" data-ws-back>◀ Back</button>
              <button type="button" class="ghost" data-ws-forward>Forward ▶</button>
            </div>
            <div class="wscraper-field-grid">
              <label>
                Selector
                <input type="text" data-ws-selector placeholder="#submit" autocapitalize="none" autocomplete="off" spellcheck="false" />
              </label>
              <label>
                Text
                <input type="text" data-ws-text placeholder="hello" />
              </label>
              <label>
                Scroll
                <input type="number" data-ws-scroll value="600" />
              </label>
            </div>
            <div class="wscraper-buttons secondary">
              <button type="button" class="ghost" data-ws-click>Click</button>
              <button type="button" class="ghost" data-ws-type>Type</button>
              <button type="button" class="ghost" data-ws-enter>Enter</button>
              <button type="button" class="ghost" data-ws-scroll-btn>Scroll</button>
              <button type="button" class="ghost" data-ws-scroll-up>Scroll Up</button>
              <button type="button" class="ghost" data-ws-scroll-down>Scroll Down</button>
            </div>
            <div class="wscraper-preview" data-ws-preview>
              <div class="wscraper-preview-placeholder" data-ws-preview-placeholder>(no frame)</div>
              <img data-ws-frame alt="Screenshot preview" />
            </div>
            <div class="tiny" data-ws-status>(idle)</div>
            <div class="code wscraper-log" data-ws-log>(log)</div>
          </div>
        ` : ''}
        ${node.type === 'Meshtastic' ? `
          <div class="meshtastic-node" data-mesh-root>
            <div class="mesh-header">
              <div class="mesh-main-controls">
                <button type="button" data-mesh-port>Choose Port…</button>
                <button type="button" data-mesh-connect disabled>Connect</button>
                <button type="button" data-mesh-refresh disabled>Refresh Nodes</button>
                <button type="button" data-mesh-disconnect disabled>Disconnect</button>
              </div>
              <div class="mesh-status-line">
                <span class="mesh-status-dot" data-mesh-status-dot></span>
                <span data-mesh-status>Not connected</span>
                <span class="mesh-self" data-mesh-self-info></span>
              </div>
            </div>
            <div class="mesh-tabs">
              <div class="mesh-tab active" data-mesh-tab="chat">Chat</div>
              <div class="mesh-tab" data-mesh-tab="map">Map</div>
              <div class="mesh-tab" data-mesh-tab="log">Logs</div>
            </div>
            <div class="mesh-views">
              <div class="mesh-view active" data-mesh-view="chat">
                <div class="mesh-chat-header" data-mesh-chat-header>Public broadcast</div>
                <input type="text" class="mesh-peer-filter" data-mesh-peer-filter placeholder="Filter peers…" autocapitalize="none" autocomplete="off" spellcheck="false">
                <div class="mesh-peer-bar" data-mesh-peer-bar></div>
                <div class="mesh-messages" data-mesh-messages></div>
                <div class="mesh-sharebar">
                  <div class="mesh-sharebar-group">
                    <button type="button" data-mesh-send-ua>Send Browser Info</button>
                  </div>
                  <div class="mesh-sharebar-group">
                    <button type="button" data-mesh-loc-once>Send Location Once</button>
                    <label>Every (s)
                      <input type="number" min="5" value="60" data-mesh-loc-interval>
                    </label>
                    <button type="button" data-mesh-loc-start>Start</button>
                    <button type="button" data-mesh-loc-stop disabled>Stop</button>
                  </div>
                  <div class="mesh-sharebar-group">
                    <label>Destination
                      <select data-mesh-share-dest>
                        <option value="current" selected>Current thread</option>
                        <option value="public">Public</option>
                      </select>
                    </label>
                  </div>
                </div>
                <div class="mesh-composer">
                  <input type="text" data-mesh-input placeholder="Type message…">
                  <button type="button" data-mesh-send disabled>Send</button>
                </div>
                <div class="mesh-peer-cards" data-mesh-peer-cards></div>
              </div>
              <div class="mesh-view" data-mesh-view="map">
                <div class="mesh-map" data-mesh-map></div>
              </div>
              <div class="mesh-view" data-mesh-view="log">
                <div class="mesh-log" data-mesh-log></div>
              </div>
            </div>
          </div>
        ` : ''}
        ${node.type === 'NoClipBridge' ? `
          <div class="noclip-bridge-node" data-noclip-root style="pointer-events:auto;">
            <div class="muted" style="pointer-events:auto;margin-bottom:6px;">
              Hydra Address: <code data-noclip-self style="font-size:11px;color:var(--accent);">—</code>
            </div>
            <div class="muted" style="pointer-events:auto;margin-bottom:6px;">
              Discovery Room: <code data-noclip-room style="font-size:11px;color:var(--accent);">—</code>
            </div>
            <div class="muted" style="pointer-events:auto;margin-bottom:6px;">Discovered NoClip Peers</div>
            <div style="display:flex;gap:6px;margin-bottom:8px;">
              <select data-noclip-peer-select style="flex:1;pointer-events:auto;" autocomplete="off">
                <option value="">-- Select NoClip Peer --</option>
              </select>
              <button type="button" data-noclip-peer-refresh title="Refresh peers">🔄</button>
              <button type="button" data-noclip-peer-sync title="Send sync request" aria-label="Send sync request">🤝</button>
            </div>
            <div class="muted" style="pointer-events:auto;margin-top:6px;">Pending Sync Requests</div>
            <div data-noclip-sync-list style="pointer-events:auto;margin-bottom:10px;font-size:12px;line-height:1.4;color:var(--muted);border:1px solid rgba(255,255,255,0.08);border-radius:4px;padding:6px;max-height:140px;overflow:auto;">
              <div data-empty>None pending</div>
            </div>
            <div class="muted" style="pointer-events:auto;margin-top:6px;">Session Status</div>
            <div data-noclip-session-status style="pointer-events:auto;margin-bottom:6px;font-size:12px;line-height:1.4;color:var(--muted);">
              No active sessions
            </div>
            <div data-noclip-session-list style="pointer-events:auto;margin-bottom:10px;font-size:12px;line-height:1.5;color:var(--muted);max-height:140px;overflow:auto;border:1px solid rgba(255,255,255,0.08);border-radius:4px;padding:6px;">
              <div data-empty>Sessions will appear here after approval.</div>
            </div>
            <div class="muted" style="pointer-events:auto;margin-top:12px;">Connection Log</div>
            <div class="code noclip-bridge-log" data-noclip-log style="min-height:100px;max-height:200px;overflow:auto;font-size:11px;line-height:1.4;"></div>
          </div>
        ` : ''}
        ${node.type === 'WebSerial' ? `
          <div class="webserial-node" data-webserial-root style="pointer-events:auto;">
            <div class="mesh-header" style="pointer-events:auto;">
            <div class="mesh-main-controls" style="pointer-events:auto;gap:6px;flex-wrap:wrap;display:flex;">
                <button type="button" data-webserial-choose>Choose Port…</button>
                <button type="button" data-webserial-connect disabled>Connect</button>
                <button type="button" data-webserial-disconnect disabled>Disconnect</button>
                <button type="button" data-webserial-clear>Clear Log</button>
              </div>
              <div class="mesh-status-line" style="pointer-events:auto;">
                <span class="mesh-status-dot" data-webserial-status-dot></span>
                <span data-webserial-status>Disconnected</span>
              </div>
            </div>
            <div class="webserial-baud" style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
              <label style="display:flex;align-items:center;gap:6px;">
                Baud
                <select data-webserial-baud>
                  <option value="9600">9600</option>
                  <option value="14400">14400</option>
                  <option value="19200">19200</option>
                  <option value="28800">28800</option>
                  <option value="38400">38400</option>
                  <option value="57600">57600</option>
                  <option value="74880">74880</option>
                  <option value="115200" selected>115200</option>
                  <option value="230400">230400</option>
                  <option value="460800">460800</option>
                  <option value="921600">921600</option>
                  <option value="custom">Custom…</option>
                </select>
              </label>
              <input type="number" data-webserial-baud-custom placeholder="Custom" style="width:120px;">
              <button type="button" data-webserial-baud-apply>Set</button>
            </div>
            <div class="webserial-log" data-webserial-log></div>
            <div class="mesh-composer">
              <input type="text" data-webserial-send-input placeholder="Send data…" autocomplete="off" spellcheck="false">
              <button type="button" data-webserial-send-button disabled>Send</button>
            </div>
          </div>
        ` : ''}
        ${node.type === 'Orientation' ? `
          <div class="muted" style="pointer-events:auto;">Orientation</div>
          <div class="code" data-orientation-status style="min-height:28px">Idle</div>
        ` : ''}
        ${node.type === 'Location' ? `
          <div class="muted" style="pointer-events:auto;">Location</div>
          <div class="code" data-location-status style="min-height:28px">Idle</div>
        ` : ''}
        ${node.type === 'NknDM' ? `
          <div class="muted" style="pointer-events:auto;">Local Address</div>
          <div class="dm-address-row">
            <div class="bubble" data-nkndm-local style="min-height:24px">(offline)</div>
            <button type="button" class="ghost" data-nkndm-copy>Copy</button>
            <span class="dm-indicator offline" data-nkndm-indicator title="Offline"></span>
          </div>
          <div class="row" style="pointer-events:auto;align-items:center;gap:8px;flex-wrap:wrap;">
            <button type="button" class="ghost" data-nkndm-autochunk>Auto chunk: off</button>
            <span class="tiny" data-nkndm-chunk-note>Chunk Size</span>
          </div>
          <div class="muted" style="pointer-events:auto;">Peer</div>
          <div class="dm-peer-row">
            <div class="bubble" data-nkndm-peer style="min-height:24px">(none)</div>
            <div class="dm-peer-actions hidden" data-nkndm-actions>
              <button type="button" class="ghost" data-nkndm-approve title="Trust peer">✔</button>
              <button type="button" class="ghost" data-nkndm-revoke title="Remove trust">✖</button>
            </div>
          </div>
          <div class="muted" style="pointer-events:auto;">Log</div>
          <div class="code" data-nkndm-log style="min-height:60px;max-height:360px"></div>
        ` : ''}
        ${node.type === 'TextInput' ? `
          <div class="text-input-area" style="pointer-events:auto;">
            <textarea data-textinput-field placeholder="Type a message…"></textarea>
            <div class="text-input-actions">
              <button type="button" class="secondary" data-textinput-send>Send</button>
            </div>
          </div>
          <div class="text-input-preview" data-textinput-preview-wrap>
            <div class="text-input-preview-header">
              <span data-textinput-preview-label>Draft preview</span>
              <button type="button" class="ghost tiny" data-textinput-preview-copy disabled>Copy</button>
            </div>
            <pre class="text-input-preview-body" data-textinput-preview-body>(empty)</pre>
          </div>
        ` : ''}
        ${node.type === 'TextDisplay' ? `
          <div class="muted" style="pointer-events:auto;">Latest Text</div>
          <div class="text-display-wrap" data-textdisplay-wrap style="position:relative;margin-top:4px;">
            <button type="button" class="ghost" data-textdisplay-copy title="Copy" style="position:absolute;top:4px;right:4px;padding:2px 8px;line-height:1.2;">Copy</button>
            <div class="bubble" data-textdisplay-content style="min-height:48px;padding-right:48px;white-space:pre-wrap;word-break:break-word;"></div>
          </div>
        ` : ''}
        ${node.type === 'Template' ? `
          <div class="template-editor" style="pointer-events:auto;">
            <textarea data-template-editor placeholder="Hello {name}, welcome to {place}."></textarea>
            <div class="template-preview" data-template-preview></div>
          </div>
        ` : ''}
        ${node.type === 'LogicGate' ? `
          <div class="muted" style="pointer-events:auto;">Rules</div>
          <div class="logic-gate-list" data-logic-gate></div>
        ` : ''}
        ${node.type === 'FileTransfer' ? `
          <div class="file-transfer">
            <div class="file-transfer-send">
              <div class="file-drop" data-ft-drop>
                <input type="file" data-ft-input hidden>
                <span data-ft-drop-label>Drop file or click to select</span>
              </div>
              <div class="row" style="margin-top:8px;gap:8px;flex-wrap:wrap;">
                <input type="text" data-ft-route class="input" placeholder="Route (optional)" style="flex:1 1 160px;">
                <input type="password" data-ft-pass class="input" placeholder="Passphrase (optional)" autocomplete="new-password" style="flex:1 1 160px;">
              </div>
              <div class="row" style="margin-top:8px;gap:8px;">
                <button type="button" class="secondary" data-ft-send disabled>Send</button>
                <button type="button" class="ghost" data-ft-cancel hidden>Cancel</button>
              </div>
              <div class="file-progress hidden" data-ft-progress>
                <div class="file-progress-bar" data-ft-progress-bar></div>
                <div class="tiny" data-ft-progress-text>0%</div>
              </div>
            </div>
            <div class="file-transfer-receive">
              <div class="tiny" data-ft-status>Waiting for file…</div>
              <div class="file-transfer-info" data-ft-info></div>
              <div class="file-progress hidden" data-ft-rprogress>
                <div class="file-progress-bar" data-ft-rprogress-bar></div>
                <div class="tiny" data-ft-rprogress-text>0%</div>
              </div>
              <div class="tiny" data-ft-stats></div>
              <div class="row" style="margin-top:8px;gap:8px;">
                <button type="button" class="ghost" data-ft-save disabled>Save File</button>
                <button type="button" class="ghost" data-ft-clear disabled>Clear</button>
              </div>
            </div>
          </div>
        ` : ''}
        ${node.type === 'ImageInput' ? `
          <div class="image-input" style="pointer-events:auto;">
            <div class="image-input-drop" data-image-drop>
              <input type="file" accept="image/*" hidden />
              <div class="image-input-instructions">Drop image, paste, or click to upload</div>
            </div>
            <img data-image-preview class="image-input-preview hidden" alt="Selected image" />
            <div class="muted" data-image-status>Awaiting image</div>
          </div>
        ` : ''}
      </div>
      </div>
    `;

    ensureNodeRefs();
    setSizeLock(node.sizeLocked);
    syncFrameConstraints();
    if (initialWidth !== null) applyNodeWidth(initialWidth);
    if (initialHeight !== null) applyNodeHeight(initialHeight);
    syncFrameDimensions();

    const computeMinDimensions = () => {
      ensureNodeRefs();
      // IMPORTANT: layout metrics are already in CSS pixels, do NOT scale by view
      const headHeight = headEl ? headEl.offsetHeight : 0;
      const bodyHeight = bodyEl ? bodyEl.scrollHeight : 0;
      const baseMinHeight = Math.max(NODE_MIN_HEIGHT, Math.ceil(headHeight + bodyHeight));

      const headWidth = headEl ? headEl.scrollWidth : 0;
      const bodyWidth = bodyEl ? bodyEl.scrollWidth : 0;
      const baseMinWidth = Math.max(NODE_MIN_WIDTH, Math.ceil(Math.max(headWidth, bodyWidth)));

      return {
        minHeight: Math.min(baseMinHeight, NODE_MAX_HEIGHT),
        minWidth: Math.min(baseMinWidth, NODE_MAX_WIDTH)
      };
    };

    const getMeasuredWidth = () => {
      const direct = Number.parseFloat(el.style.width);
      if (Number.isFinite(direct)) return direct;
      const rect = el.getBoundingClientRect();
      return rect.width / currentScale();
    };

    const getMeasuredHeight = () => {
      const direct = Number.parseFloat(el.style.height);
      if (Number.isFinite(direct)) return direct;
      const rect = el.getBoundingClientRect();
      return rect.height / currentScale();
    };
    const applyMinDimensions = ({ save = false, reason = 'auto' } = {}) => {
      const dims = computeMinDimensions();
      const locked = Boolean(node.sizeLocked);

      el.style.maxWidth = `${NODE_MAX_WIDTH}px`;
      el.style.maxHeight = `${NODE_MAX_HEIGHT}px`;
      el.style.minWidth = `${NODE_MIN_WIDTH}px`;
      el.style.minHeight = 'min-content';
      syncFrameConstraints();
      updateBodyOverflow();

      const currentW = getMeasuredWidth();
      const currentH = getMeasuredHeight();

      // Do NOT enforce mins on zoom; only when unlocked or non-zoom reasons
      const allowEnforceMins = !locked && reason !== 'zoom';

      const targetW = allowEnforceMins
        ? clampSize(Math.max(currentW, dims.minWidth), NODE_MIN_WIDTH, NODE_MAX_WIDTH)
        : clampSize(currentW, NODE_MIN_WIDTH, NODE_MAX_WIDTH);

      const targetH = allowEnforceMins
        ? clampSize(Math.max(currentH, dims.minHeight), NODE_MIN_HEIGHT, NODE_MAX_HEIGHT)
        : clampSize(currentH, NODE_MIN_HEIGHT, NODE_MAX_HEIGHT);

      let adjusted = false;
      if (targetW !== currentW) {
        applyNodeWidth(targetW);
        adjusted = true;
      } else {
        node.w = targetW;
      }
      if (targetH !== currentH) {
        applyNodeHeight(targetH);
        adjusted = true;
      } else {
        node.h = targetH;
      }

      syncFrameDimensions();

      if (adjusted) requestRedraw();
      if (save && (!locked || adjusted)) saveGraph();

      return { minWidth: dims.minWidth, minHeight: dims.minHeight };
    };


    node.refreshDimensions = (save = false, reason = 'auto') => applyMinDimensions({ save, reason });


    const scheduleAutoGrow = (() => {
      let raf = 0;
      let pendingSave = false;
      return (shouldSave = false) => {
        pendingSave = pendingSave || shouldSave;
        if (raf) return;
        raf = requestAnimationFrame(() => {
          raf = 0;
          const save = pendingSave && !node.sizeLocked;
          pendingSave = false;
          if (!node._resizing) applyMinDimensions({ save });
          requestRedraw();
        });
      };
    })();

    const transportBtn = el.querySelector('.node-transport');
    if (transportBtn && t.supportsNkn) {
      transportBtn.addEventListener('click', (ev) => {
        ev.stopPropagation();
        if (node.type === 'MediaStream') {
          openQrScanner(null, (txt) => {
            if (!txt) return;
            const { added, address } = Media.addTarget(node.id, txt, { auto: true, handshake: true });
            if (!address) {
              setBadge('Invalid NKN address', false);
              return;
            }
            if (added) setBadge(`Added media peer ${Media.formatAddress(address)}`);
            else setBadge('Target already added');
            refreshNodeTransport(node.id);
          });
          return;
        }
        const rec = NodeStore.ensure(node.id, node.type);
        const cfg = rec.config || {};
        const info = t;
        const currentRelay = getRelayValueFromConfig(cfg, info);
        if (!currentRelay) {
          openQrScanner(null, (txt) => {
            if (!txt) return;
            handleRelayScan(node.id, node.type, txt, { pendingMessage: DEFAULT_PENDING_MESSAGE });
          });
        } else {
          setBadge(info.relayKey === 'relay' ? 'NKN relay already configured' : 'NKN address already set');
        }
      });
      refreshNodeTransport(node.id);
    }

    const resizeHandle = document.createElement('div');
    resizeHandle.setAttribute('data-resize', '');
    resizeHandle.title = 'Resize';
    resizeHandle.className = 'resizeHandle';
    resizeHandle.style.cssText = [
      'position:absolute',
      'right:4px',
      'bottom:4px',
      'width:12px',
      'height:12px',
      'border-bottom-right-radius:10px',
      'cursor:se-resize',
      'border-right:2px solid rgba(255,255,255,0.6)',
      'border-bottom:2px solid rgba(255,255,255,0.6)',
      'box-sizing:border-box',
      'z-index:2'
    ].join(';');
    el.appendChild(resizeHandle);

    let resizeState = null;
    resizeHandle.addEventListener('pointerdown', (e) => {
      e.stopPropagation();
      ensureNodeRefs();
      node._resizing = true;
      const scale = currentScale();
      resizeState = {
        id: e.pointerId,
        startX: e.clientX,
        startY: e.clientY,
        startW: clampSize(getMeasuredWidth(), NODE_MIN_WIDTH, NODE_MAX_WIDTH),
        startH: clampSize(getMeasuredHeight(), NODE_MIN_HEIGHT, NODE_MAX_HEIGHT),
        minW: NODE_MIN_WIDTH,
        minH: NODE_MIN_HEIGHT,
        maxW: NODE_MAX_WIDTH,
        maxH: NODE_MAX_HEIGHT,
        scale
      };
      resizeHandle.setPointerCapture(e.pointerId);
      document.body.style.cursor = 'se-resize';
      updateBodyOverflow();
    });

    resizeHandle.addEventListener('pointermove', (e) => {
      if (!resizeState) return;
      const dx = e.clientX - resizeState.startX;
      const dy = e.clientY - resizeState.startY;
      const minW = resizeState.minW;
      const minH = resizeState.minH;
      const scale = resizeState.scale || currentScale();
      const rawW = resizeState.startW + dx / scale;
      const rawH = resizeState.startH + dy / scale;
      const snappedW = clampSize(Math.round(rawW / GRID_SIZE) * GRID_SIZE, minW, resizeState.maxW);
      const snappedH = clampSize(Math.round(rawH / GRID_SIZE) * GRID_SIZE, minH, resizeState.maxH);
      const prevW = node.w ?? getMeasuredWidth();
      const prevH = node.h ?? getMeasuredHeight();
      let changed = false;
      if (snappedW !== prevW) {
        applyNodeWidth(snappedW);
        changed = true;
      }
      if (snappedH !== prevH) {
        applyNodeHeight(snappedH);
        changed = true;
      }
      if (changed) {
        audio.playResizeBlip();
        requestRedraw();
      }
    });

    const endResize = () => {
      if (!resizeState) return;
      resizeState = null;
      document.body.style.cursor = '';
      const widthNow = getMeasuredWidth();
      const heightNow = getMeasuredHeight();
      const snappedW = clampSize(Math.round(widthNow / GRID_SIZE) * GRID_SIZE, NODE_MIN_WIDTH, NODE_MAX_WIDTH);
      const snappedH = clampSize(Math.round(heightNow / GRID_SIZE) * GRID_SIZE, NODE_MIN_HEIGHT, NODE_MAX_HEIGHT);
      applyNodeWidth(snappedW);
      applyNodeHeight(snappedH);
      setSizeLock(true);
      node._resizing = false;
      applyMinDimensions({ save: false });
      saveGraph();
      requestRedraw();
    };
    resizeHandle.addEventListener('pointerup', endResize);
    resizeHandle.addEventListener('pointercancel', endResize);

    const left = el.querySelector('.side.left');
    const right = el.querySelector('.side.right');
    node.portEls = node.portEls || {};

    if (node.type === 'LLM') {
      initLlmControls(node);
    }

    if (node.type === 'TextInput') {
      initTextInputNode(node);
    }

    if (node.type === 'TextDisplay') {
      initTextDisplayNode(node);
    }

    if (node.type === 'NknDM') {
      initNknDmNode(node);
    }

    if (node.type === 'NoClipBridge') {
      initNoClipNode(node);
    }

    if (node.type === 'ImageInput') {
      initImageInputNode(node);
    }

    for (const p of (t.inputs || [])) {
      createInputPort(node, left, p.name, p.label || p.name, p.tooltip, p.type);
    }

    for (const p of (t.outputs || [])) {
      createOutputPort(node, right, p.name, p.label || p.name, p.tooltip, p.type);
    }

    if (node.type === 'Meshtastic') {
      initMeshtasticNode(node);
    }

    if (node.type === 'WebSerial') {
      WebSerial.init(node.id);
    }

    if (node.type === 'Template') {
      setupTemplateNode(node, left);
    }

    if (node.type === 'LogicGate') {
      setupLogicGateNode(node, left);
    }

    const head = el.querySelector('.head');
    const body = el.querySelector('.body');
    let drag = null;

    const startNodeDrag = (sourceEl, e) => {
      if (e.button !== undefined && e.button !== 0) return;
      if (!isNodeSelected(node.id)) setSelectedNode(node.id, { focus: true });
      const start = clientToWorkspace(e.clientX, e.clientY);
      const ids = WS.selectedNodes && WS.selectedNodes.size
        ? Array.from(WS.selectedNodes)
        : [node.id];
      const nodes = ids
        .map((id) => {
          const target = WS.nodes.get(id);
          if (!target?.el) return null;
          return {
            id,
            node: target,
            el: target.el,
            dx: start.x - (target.x || 0),
            dy: start.y - (target.y || 0)
          };
        })
        .filter(Boolean);
      if (!nodes.length) return;
      drag = {
        pointerId: e.pointerId,
        nodes,
        moved: false
      };
      sourceEl.setPointerCapture?.(e.pointerId);
    };

    const handlePointerMove = (e) => {
      if (!drag || e.pointerId !== drag.pointerId) return;
      const p = clientToWorkspace(e.clientX, e.clientY);
      let changed = false;
      drag.nodes.forEach((entry) => {
        const target = entry.node;
        const targetEl = entry.el;
        const prevX = target.x || 0;
        const prevY = target.y || 0;
        const gx = Math.round((p.x - entry.dx) / GRID_SIZE) * GRID_SIZE;
        const gy = Math.round((p.y - entry.dy) / GRID_SIZE) * GRID_SIZE;
        if (gx !== prevX || gy !== prevY) {
          targetEl.style.left = `${gx}px`;
          targetEl.style.top = `${gy}px`;
          target.x = gx;
          target.y = gy;
          changed = true;
        }
      });
      if (changed) {
        drag.moved = true;
        audio.playMoveBlip();
        requestRedraw();
      }
    };

    const handlePointerUp = (e) => {
      if (!drag || e.pointerId !== drag.pointerId) return;
      const shouldSave = drag.moved;
      drag = null;
      if (shouldSave) saveGraph();
    };

    const bindDragSurface = (surfaceEl, guard) => {
      if (!surfaceEl) return;
      surfaceEl.addEventListener('pointerdown', (e) => {
        if (guard && guard(e) === false) return;
        startNodeDrag(surfaceEl, e);
      });
      surfaceEl.addEventListener('pointermove', handlePointerMove);
      surfaceEl.addEventListener('pointerup', handlePointerUp);
      surfaceEl.addEventListener('pointercancel', handlePointerUp);
    };

    bindDragSurface(head, (e) => !e.target.closest('button'));
    bindDragSurface(body, (e) => e.target === body);

    const gearButtons = el.querySelectorAll('.gear');
    const btnGear = gearButtons[0];
    const btnDel = gearButtons[gearButtons.length - 1];
    btnGear.addEventListener('click', () => openSettings(node.id));
    const btnDoc = el.querySelector('.docBtn');
    if (btnDoc) {
      const openDoc = (e) => {
        e?.preventDefault?.();
        e?.stopPropagation?.();
        openDocs(node.type);
      };
      btnDoc.addEventListener('click', openDoc);
      btnDoc.addEventListener('pointerdown', (e) => e.stopPropagation());
    }
    const btnASR = el.querySelector('.asrPlay');
    if (btnASR) {
      const glyph = () => {
        btnASR.textContent = (ASR.running && ASR.ownerId === node.id) ? '■' : '▶';
      };
      glyph();
      btnASR.addEventListener('click', async () => {
        try {
          if (ASR.ownerId === node.id && (ASR.running || ASR._startingSid)) await ASR.stop();
          else await ASR.start(node.id);
        } catch (err) {
          setBadge(`ASR error: ${err?.message || err}`, false);
        } finally {
          setTimeout(glyph, 0);
        }
      });
    }
    const btnWasm = el.querySelector('.wasmToggle');
    if (btnWasm) {
      const applyWasmUi = () => {
        const cfg = NodeStore.ensure(node.id, node.type)?.config || {};
        const enabled = isWasmNode(node.type, cfg);
        btnWasm.classList.toggle('active', enabled);
        btnWasm.textContent = enabled ? 'W' : 'N';
        btnWasm.title = enabled ? 'Browser (WASM) mode' : 'Remote service mode';
        refreshNodeTransport(node.id);
      };
      applyWasmUi();
      btnWasm.addEventListener('click', (e) => {
        e.preventDefault();
        const rec = NodeStore.ensure(node.id, node.type);
        const cfg = rec?.config || {};
        const next = !isWasmNode(node.type, cfg);
        const defaults = NodeStore.defaultsByType?.[node.type] || {};
        const patch = { type: node.type, wasm: next };
        if (next) {
          ['wasmWhisperModel', 'wasmPiperModelUrl', 'wasmPiperConfigUrl', 'wasmSpeakerId', 'wasmThreads'].forEach((key) => {
            if (cfg[key] === undefined && defaults[key] !== undefined) patch[key] = defaults[key];
          });
        }
        const updated = NodeStore.update(node.id, patch);
        runNodeConfigSideEffects(node, updated, { quiet: false });
        applyWasmUi();
        saveGraph();
        openSettings(node.id);
      });
    }
    const btnMedia = el.querySelector('.mediaToggle');
    if (btnMedia) {
      const syncGlyph = () => {
        btnMedia.textContent = Media.isRunning(node.id) ? '■' : '▶';
      };
      syncGlyph();
      btnMedia.addEventListener('click', (e) => {
        e.preventDefault();
        Media.toggle(node.id);
        setTimeout(syncGlyph, 100);
      });
    }
    const btnOrientation = el.querySelector('.orientationToggle');
    if (btnOrientation) {
      const glyph = () => {
        btnOrientation.textContent = Orientation.isRunning(node.id) ? '■' : '▶';
      };
      glyph();
      btnOrientation.addEventListener('click', (e) => {
        e.preventDefault();
        Orientation.toggle(node.id);
        setTimeout(glyph, 100);
      });
    }
    const btnLocation = el.querySelector('.locationToggle');
    if (btnLocation) {
      const glyph = () => {
        btnLocation.textContent = Location.isRunning(node.id) ? '■' : '▶';
      };
      glyph();
      btnLocation.addEventListener('click', (e) => {
        e.preventDefault();
        Location.toggle(node.id);
        setTimeout(glyph, 100);
      });
    }
    btnDel.addEventListener('click', () => removeNode(node.id));
    if ('ResizeObserver' in window) {
      const ro = new ResizeObserver(() => {
        scheduleAutoGrow(false);
        requestRedraw();
      });
      ro.observe(el);
      node._ro = ro;
    }

    if ('MutationObserver' in window) {
      const body = el.querySelector('.body');
      if (body) {
        const mo = new MutationObserver(() => scheduleAutoGrow(true));
        mo.observe(body, { childList: true, subtree: true, characterData: true });
        node._mo = mo;
      }
    }

    if (node.type === 'LLM') {
      refreshLlmControls(node.id);
    }

    requestAnimationFrame(() => scheduleAutoGrow(false));
    return el;
  }

  function onPortClick(nodeId, side, portName, el) {
    if (!WS.portSel) {
      if (side !== 'out') {
        setBadge('Pick an output first', false);
        return;
      }
      WS.portSel = { nodeId, side, portName, el };
      el.classList.add('sel');
      return;
    }
    if (side !== 'in') {
      setBadge('Connect to an input port', false);
      return;
    }
    if (nodeId === WS.portSel.nodeId) {
      setBadge('Cannot self-link', false);
      return;
    }
    addLink(WS.portSel.nodeId, WS.portSel.portName, nodeId, portName);
    WS.portSel.el.classList.remove('sel');
    WS.portSel = null;
  }

  function connectedWires(nodeId, side, portName) {
    return WS.wires.filter((w) =>
      (side === 'out' && w.from.node === nodeId && w.from.port === portName) ||
      (side === 'in' && w.to.node === nodeId && w.to.port === portName)
    );
  }

  function addLink(fromNodeId, fromPort, toNodeId, toPort) {
    if (WS.wires.find((w) => w.from.node === fromNodeId && w.from.port === fromPort && w.to.node === toNodeId && w.to.port === toPort)) return;
    const w = { id: uid(), from: { node: fromNodeId, port: fromPort }, to: { node: toNodeId, port: toPort }, path: null };
    WS.wires.push(w);
    const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    path.setAttribute('fill', 'none');
    path.setAttribute('stroke', 'rgba(255,255,255,.7)');
    path.setAttribute('stroke-width', '2');
    path.setAttribute('stroke-linecap', 'round');
    path.setAttribute('opacity', '0.95');
    path.setAttribute('vector-effect', 'non-scaling-stroke');
    path.dataset.id = w.id;
    path.classList.add('wire');
    path.style.cursor = 'grab';
    const kill = (e) => {
      removeWireById(w.id);
      e.stopPropagation();
    };
    path.addEventListener('click', (e) => {
      if (e.altKey || e.metaKey || e.ctrlKey) kill(e);
    });
    path.addEventListener('dblclick', kill);
    path.addEventListener('pointerdown', (ev) => {
      if (ev.button && ev.button !== 0) return;
      if (ev.altKey || ev.metaKey || ev.ctrlKey) return;
      const wire = WS.wires.find((wireItem) => wireItem.id === w.id);
      if (!wire) return;
      if (ev.pointerType === 'touch') ev.preventDefault();
      const pointer = clientToWorkspace(ev.clientX, ev.clientY);
      const fromCenter = portCenter(wire.from.node, 'out', wire.from.port);
      const toCenter = portCenter(wire.to.node, 'in', wire.to.port);
      const distFrom = Math.hypot(pointer.x - fromCenter.x, pointer.y - fromCenter.y);
      const distTo = Math.hypot(pointer.x - toCenter.x, pointer.y - toCenter.y);
      const grabSide = distTo < distFrom ? 'to' : 'from';
      const expected = grabSide === 'from' ? 'in' : 'out';
      if (wire.path) {
        wire.path.setAttribute('stroke-dasharray', '6 4');
        wire.path.dataset.prevCursor = wire.path.style.cursor || '';
        wire.path.style.cursor = 'grabbing';
      }
      const move = (e) => {
        if (e.pointerType === 'touch') e.preventDefault();
        const pt = clientToWorkspace(e.clientX, e.clientY);
        drawRetarget(wire, grabSide, pt.x, pt.y);
        if (WS.drag) WS.drag.lastClient = { x: e.clientX, y: e.clientY };
      };
      const up = (e) => {
        setDropHover(null);
        finishAnyDrag(e.clientX, e.clientY);
      };
      WS.drag = {
        kind: 'retarget',
        wireId: wire.id,
        grabSide,
        path: wire.path,
        pointerId: ev.pointerId,
        expected,
        lastClient: { x: ev.clientX, y: ev.clientY },
        _cleanup: () => window.removeEventListener('pointermove', move)
      };
      drawRetarget(wire, grabSide, pointer.x, pointer.y);
      window.addEventListener('pointermove', move, { passive: false });
      window.addEventListener('pointerup', up, { once: true, passive: false });
      updateDropHover(expected);
    });
    WS.svgLayer.appendChild(path);
    w.path = path;

    const fk = `${fromNodeId}:out:${fromPort}`;
    const tk = `${toNodeId}:in:${toPort}`;
    if (!Router.wires.find((x) => x.from === fk && x.to === tk)) {
      Router.wires.push({ from: fk, to: tk });
      CFG.wires = Router.wires.slice();
      saveCFG();
      Router.render();
      log(`wired ${fk} → ${tk}`);
    }

    const targetNode = WS.nodes.get(toNodeId);
    if (targetNode?.type === 'LLM' && toPort === 'system') {
      pullLlmSystemInput(toNodeId);
    }

    if (!History.isSilent()) {
      const historyWire = cloneWireForHistory(w);
      if (historyWire) History.push({ type: 'wire.add', wire: historyWire });
    }

    requestRedraw();
    saveGraph();
  }

  function setDropHover(el) {
    if (WS.drag?.dropHover === el) return;
    if (WS.drag?.dropHover) WS.drag.dropHover.classList.remove('drop-hover');
    if (el) el.classList.add('drop-hover');
    if (WS.drag) WS.drag.dropHover = el || null;
  }

  function updateDropHover(expected) {
    if (!WS.drag || !WS.drag.lastClient || !expected) {
      setDropHover(null);
      return;
    }
    const el = document.elementFromPoint(WS.drag.lastClient.x, WS.drag.lastClient.y);
    const selector = expected === 'in' ? '.wp-port.in' : '.wp-port.out';
    const target = el && el.closest?.(selector);
    setDropHover(target || null);
  }

  function clearTempLink() {
    if (WS.drag?.kind && WS.drag.path) {
      if (WS.drag.kind === 'retarget') {
        WS.drag.path.setAttribute('stroke-dasharray', '');
        const prev = WS.drag.path.dataset.prevCursor;
        if (prev !== undefined) WS.drag.path.style.cursor = prev || 'grab';
        delete WS.drag.path.dataset.prevCursor;
      }
      else WS.drag.path.remove();
    }
  }

  function drawTempFromPort(from, mx, my) {
    if (!WS.drag?.path) return;
    const a = portCenter(from.nodeId, from.side, from.portName);
    const b = { x: mx, y: my };
    const dx = Math.max(40, Math.abs(b.x - a.x) * 0.5);
    WS.drag.path.setAttribute('d', `M ${a.x},${a.y} C ${a.x + dx},${a.y} ${b.x - dx},${b.y} ${b.x},${b.y}`);
    if (WS.drag?.expected) updateDropHover(WS.drag.expected);
  }

  function drawRetarget(wire, grabSide, mx, my) {
    if (!wire?.path) return;
    const a = grabSide === 'from'
      ? { x: mx, y: my }
      : portCenter(wire.from.node, 'out', wire.from.port);
    const b = grabSide === 'to'
      ? { x: mx, y: my }
      : portCenter(wire.to.node, 'in', wire.to.port);
    const dx = Math.max(40, Math.abs(b.x - a.x) * 0.5);
    wire.path.setAttribute('d', `M ${a.x},${a.y} C ${a.x + dx},${a.y} ${b.x - dx},${b.y} ${b.x},${b.y}`);
    if (WS.drag?.expected) updateDropHover(WS.drag.expected);
  }

  function finishAnyDrag(cx, cy) {
    try {
      WS.drag?._cleanup && WS.drag._cleanup();
    } catch (err) {
      // ignore
    }
    const el = document.elementFromPoint(cx, cy);
    setDropHover(null);

    if (WS.drag?.kind === 'new') {
      const target = el && el.closest?.('.wp-port.in');
      if (target) {
        const toNode = target.closest('.node')?.dataset?.id;
        const toPort = target.dataset.port;
        if (toNode && toPort && toNode !== WS.drag.fromNodeId) addLink(WS.drag.fromNodeId, WS.drag.fromPort, toNode, toPort);
        else setBadge('Invalid drop target', false);
      }
      clearTempLink();
      WS.drag = null;
      return;
    }

    if (WS.drag?.kind === 'newFromInput') {
      const target = el && el.closest?.('.wp-port.out');
      if (target) {
        const fromNode = target.closest('.node')?.dataset?.id;
        const fromPort = target.dataset.port;
        if (fromNode && fromPort && fromNode !== WS.drag.toNodeId) addLink(fromNode, fromPort, WS.drag.toNodeId, WS.drag.toPort);
        else setBadge('Invalid drop target', false);
      }
      clearTempLink();
      WS.drag = null;
      return;
    }

    if (WS.drag?.kind === 'retarget') {
      const wire = WS.wires.find((w) => w.id === WS.drag.wireId);
      if (!wire) {
        clearTempLink();
        WS.drag = null;
        return;
      }
      const need = WS.drag.grabSide === 'from' ? '.wp-port.in' : '.wp-port.out';
      const target = el && el.closest?.(need);
      wire.path?.setAttribute('stroke-dasharray', '');
      if (!target) {
        removeWireById(wire.id);
        clearTempLink();
        WS.drag = null;
        return;
      }
      const hitNodeId = target.closest('.node')?.dataset?.id;
      const hitPort = target.dataset?.port;
      if (!hitNodeId || !hitPort) {
        clearTempLink();
        WS.drag = null;
        return;
      }
      if (WS.drag.grabSide === 'from') {
        if (!target.classList.contains('in')) {
          setBadge('Drop on an input port', false);
          clearTempLink();
          requestRedraw();
          WS.drag = null;
          return;
        }
        if (hitNodeId === wire.from.node) {
          setBadge('Cannot self-link', false);
          clearTempLink();
          requestRedraw();
          WS.drag = null;
          return;
        }
        wire.to = { node: hitNodeId, port: hitPort };
      } else {
        if (!target.classList.contains('out')) {
          setBadge('Drop on an output port', false);
          clearTempLink();
          requestRedraw();
          WS.drag = null;
          return;
        }
        if (hitNodeId === wire.to.node) {
          setBadge('Cannot self-link', false);
          clearTempLink();
          requestRedraw();
          WS.drag = null;
          return;
        }
        wire.from = { node: hitNodeId, port: hitPort };
      }
      syncRouterFromWS();
      saveGraph();
      requestRedraw();
      clearTempLink();
      setBadge('Wire reconnected');
      WS.drag = null;
      return;
    }

    clearTempLink();
    WS.drag = null;
  }

  function portCenter(nodeId, side, portName) {
    const n = WS.nodes.get(nodeId);
    if (!n) return { x: 0, y: 0 };
    const dot = n.el.querySelector(`.wp-port.${side}[data-port="${CSS.escape(portName)}"] .dot`);
    if (!dot) return { x: 0, y: 0 };
    const rect = dot.getBoundingClientRect();
    return clientToWorkspace(rect.left + rect.width / 2, rect.top + rect.height / 2);
  }

  function drawLink(w) {
    const a = portCenter(w.from.node, 'out', w.from.port);
    const b = portCenter(w.to.node, 'in', w.to.port);
    const dx = Math.max(40, Math.abs(b.x - a.x) * 0.5);
    const d = `M ${a.x},${a.y} C ${a.x + dx},${a.y} ${b.x - dx},${b.y} ${b.x},${b.y}`;
    w.path.setAttribute('d', d);
  }

  function parseWireKey(key) {
    if (!key || typeof key !== 'string') return null;
    const parts = key.split(':');
    if (parts.length < 3) return null;
    return {
      node: parts[0],
      dir: parts[1],
      port: parts.slice(2).join(':')
    };
  }

  function setWireActiveState(wire) {
    if (!wire?.path) return;
    wire.path.classList.add('wire-active');
    if (wire._activeTimer) clearTimeout(wire._activeTimer);
    wire._activeTimer = setTimeout(() => {
      if (wire._activeTimer) {
        clearTimeout(wire._activeTimer);
        wire._activeTimer = null;
      }
      wire.path?.classList.remove('wire-active');
    }, WIRE_ACTIVE_MS);
  }

  function markWireActive(fromKey, toKey) {
    const from = parseWireKey(fromKey);
    const to = parseWireKey(toKey);
    if (!from || !to) return;
    for (const wire of WS.wires) {
      if (wire.from.node === from.node && wire.from.port === from.port && wire.to.node === to.node && wire.to.port === to.port) {
        setWireActiveState(wire);
      }
    }
  }

  Router.onSend = (from, to) => markWireActive(from, to);

  function drawAllLinks() {
    for (const w of WS.wires) drawLink(w);
  }

  function detachWire(wire) {
    if (!wire) return false;
    if (wire._activeTimer) {
      clearTimeout(wire._activeTimer);
      wire._activeTimer = null;
    }
    wire.path?.remove();
    WS.wires = WS.wires.filter((x) => x !== wire);
    return true;
  }

  function flushWireMutations() {
    syncRouterFromWS();
    saveGraph();
    requestRedraw();
  }

  function removeWireById(id) {
    const w = WS.wires.find((x) => x.id === id);
    if (!w) return;
    const payload = cloneWireForHistory(w);
    detachWire(w);
    flushWireMutations();
    if (!History.isSilent() && payload) {
      History.push({ type: 'wire.remove', wire: payload });
    }
    setBadge('Wire removed');
  }

  function removeWiresAt(nodeId, side, portName) {
    const rm = WS.wires.filter((w) =>
      (side === 'out' && w.from.node === nodeId && w.from.port === portName) ||
      (side === 'in' && w.to.node === nodeId && w.to.port === portName)
    );
    if (!rm.length) return;
    const payloads = History.isSilent() ? [] : rm.map((w) => cloneWireForHistory(w)).filter(Boolean);
    rm.forEach((w) => detachWire(w));
    flushWireMutations();
    if (!History.isSilent() && payloads.length) {
      payloads.forEach((wire) => History.push({ type: 'wire.remove', wire }));
    }
  }

  function syncRouterFromWS() {
    const newWires = WS.wires.map((w) => ({ from: `${w.from.node}:out:${w.from.port}`, to: `${w.to.node}:in:${w.to.port}` }));
    Router.wires = newWires;
    CFG.wires = newWires.slice();
    saveCFG();
    Router.render();
  }

  function saveGraph() {
    const data = {
      nodes: Array.from(WS.nodes.values()).map((n) => {
        const width = Math.round(n.el?.offsetWidth || n.w || 0);
        const height = Math.round(n.el?.offsetHeight || n.h || 0);
        n.w = width;
        n.h = height;
        return {
          id: n.id,
          type: n.type,
          x: n.x,
          y: n.y,
          w: width,
          h: height,
          sizeLocked: Boolean(n.sizeLocked)
        };
      }),
      links: WS.wires.map((w) => ({ from: w.from, to: w.to })),
      nodeConfigs: {},
      viewport: {
        x: Math.round(WS.view.x),
        y: Math.round(WS.view.y),
        scale: Number(WS.view.scale.toFixed(4))
      },
      transport: 'nkn'
    };
    for (const n of WS.nodes.values()) {
      const rec = NodeStore.load(n.id);
      if (rec) data.nodeConfigs[n.id] = rec;
    }
    LS.set('graph.workspace', data);
  }

  function loadGraph() {
    const performLoad = () => {
      const data = LS.get('graph.workspace', null);
      WS.canvas.innerHTML = '';
      WS.svg.innerHTML = '';
      ensureSvgLayer();
      // reapply current transform after recreating the <g> layer
      applyViewTransform();

      WS.nodes.clear();
      WS.wires = [];
      clearSelectedNode();
      WS.clipboard = null;
      if (!data) {
        const a = addNode('ASR', 90, 200);
        const l = addNode('LLM', 380, 180);
        const t = addNode('TTS', 680, 200);
        addLink(a.id, 'final', l.id, 'prompt');
        addLink(l.id, 'final', t.id, 'text');
        requestRedraw();
        saveGraph();
        return;
      }
      if (data.nodeConfigs) {
        for (const [id, obj] of Object.entries(data.nodeConfigs)) {
          if (!obj || !obj.type || !obj.config) continue;
          const defaults = NodeStore?.defaultsByType?.[obj.type] || {};
          const existing = NodeStore.load(id) || {};
          const mergedCfg = deepClone({ ...defaults, ...(existing.config || {}), ...obj.config });
          NodeStore.saveObj(id, { id, type: obj.type, config: mergedCfg });
        }
      }
      if (data.viewport && typeof data.viewport === 'object') {
        const vx = Number(data.viewport.x);
        const vy = Number(data.viewport.y);
        const vs = Number(data.viewport.scale);
        if (Number.isFinite(vx)) WS.view.x = vx;
        if (Number.isFinite(vy)) WS.view.y = vy;
        if (Number.isFinite(vs) && vs > 0.1 && vs < 8) WS.view.scale = vs;
        applyViewTransform();
      }

      if (CFG.transport !== 'nkn') {
        CFG.transport = 'nkn';
        saveCFG();
      }
      Net.ensureNkn();
      updateTransportButton();

      for (const n of (data.nodes || [])) {
        const node = {
          id: n.id,
          type: n.type,
          x: n.x,
          y: n.y,
          w: n.w,
          h: n.h,
          sizeLocked: Boolean(n.sizeLocked)
        };
        NodeStore.ensure(node.id, node.type);
        node.el = makeNodeEl(node);
        WS.canvas.appendChild(node.el);
        WS.nodes.set(node.id, node);
        scheduleModelPrefetch(node.id, node.type, 400);
        if (node.type === 'TTS') requestAnimationFrame(() => TTS.refreshUI(node.id));
        if (node.type === 'TextInput') requestAnimationFrame(() => initTextInputNode(node));
        if (node.type === 'TextDisplay') requestAnimationFrame(() => initTextDisplayNode(node));
        if (node.type === 'Template') requestAnimationFrame(() => {
          const leftSide = node.el?.querySelector('.side.left');
          setupTemplateNode(node, leftSide);
          pullTemplateInputs(node.id);
        });
        if (node.type === 'Meshtastic') requestAnimationFrame(() => initMeshtasticNode(node));
        if (node.type === 'LogicGate') requestAnimationFrame(() => {
          const leftSide = node.el?.querySelector('.side.left');
          setupLogicGateNode(node, leftSide);
        });
        if (node.type === 'NknDM') requestAnimationFrame(() => initNknDmNode(node));
        if (node.type === 'FaceLandmarks') requestAnimationFrame(() => {
          initFaceLandmarksNode(node);
          Vision?.Face?.init?.(node.id);
        });
        if (node.type === 'PoseLandmarks') requestAnimationFrame(() => Vision?.Pose?.init?.(node.id));
        if (node.type === 'MCP') requestAnimationFrame(() => MCP.init(node.id));
      if (node.type === 'MediaStream') requestAnimationFrame(() => Media.init(node.id));
      if (node.type === 'Orientation') requestAnimationFrame(() => Orientation.init(node.id));
      if (node.type === 'Location') requestAnimationFrame(() => Location.init(node.id));
      if (node.type === 'FileTransfer') requestAnimationFrame(() => FileTransfer.init(node.id));
      if (node.type === 'Payments') requestAnimationFrame(() => initPaymentsNode(node));
    }
    for (const node of WS.nodes.values()) {
      const rec = NodeStore.ensure(node.id, node.type);
      const cfg = rec?.config || {};
      runNodeConfigSideEffects(node, cfg, { quiet: true });
    }
    for (const l of (data.links || [])) addLink(l.from.node, l.from.port, l.to.node, l.to.port);
    requestRedraw();
  };
    History.silence(performLoad);
  }

  function exportWorkspaceSnapshot() {
    const data = LS.get('graph.workspace', null);
    if (!data || typeof data !== 'object') {
      return { nodes: [], links: [], nodeConfigs: {} };
    }
    try {
      return JSON.parse(JSON.stringify(data));
    } catch (err) {
      return { nodes: [], links: [], nodeConfigs: {} };
    }
  }

  function importWorkspaceSnapshot(snapshot, { silent = false, source = '', badgeText } = {}) {
    if (!snapshot || typeof snapshot !== 'object') {
      if (!silent) setBadge('Workspace data invalid', false);
      return false;
    }
    History.clear();
    try {
      LS.set('graph.workspace', snapshot);
    } catch (err) {
      // ignore storage failures, we'll still try to render
    }
    try {
      loadGraph();
    } catch (err) {
      if (!silent) setBadge(`Workspace load failed: ${err?.message || err}`, false);
      return false;
    }
    if (!silent) {
      if (badgeText) setBadge(badgeText);
      else if (source) setBadge(`Workspace loaded from ${source}`);
      else setBadge('Workspace loaded');
    }
    return true;
  }

  async function discoverASRModels(base, api, useNkn, relay) {
    const b = (base || '').replace(/\/+$/, '');
    if (!b) return [];
    try {
      const j = await Net.getJSON(b, '/models', api, useNkn, relay);
      const arr = Array.isArray(j?.models) ? j.models : (Array.isArray(j) ? j : []);
      const names = arr
        .map((m) => (m && (m.name || m.id || m)) ?? '')
        .filter(Boolean)
        .map(String);
      return Array.from(new Set(names));
    } catch (err) {
      return [];
    }
  }

  async function discoverLLMModels(base, api, useNkn, relay) {
    const out = [];
    const b = (base || '').replace(/\/+$/, '');
    if (!b) return out;
    try {
      const j = await Net.getJSON(b, '/api/tags', api, useNkn, relay);
      if (j && Array.isArray(j.models)) {
        for (const m of j.models) if (m && m.name) out.push(String(m.name));
      }
    } catch (err) {
      // ignore
    }
    try {
      const j = await Net.getJSON(b, '/v1/models', api, useNkn, relay);
      const arr = Array.isArray(j?.data) ? j.data : (Array.isArray(j) ? j : []);
      for (const m of arr) if (m && (m.id || m.name)) out.push(String(m.id || m.name));
    } catch (err) {
      // ignore
    }
    try {
      const j = await Net.getJSON(b, '/models', api, useNkn, relay);
      if (Array.isArray(j)) {
        for (const m of j) out.push(String(m.id || m.name || m));
      } else if (Array.isArray(j?.data)) {
        for (const m of j.data) out.push(String(m.id || m.name));
      }
    } catch (err) {
      // ignore
    }
    return Array.from(new Set(out.filter(Boolean))).sort((a, b) => a.localeCompare(b));
  }

  async function discoverTTSModels(base, api, useNkn, relay) {
    const b = (base || '').replace(/\/+$/, '');
    if (!b) return [];
    try {
      const j = await Net.getJSON(b, '/models', api, useNkn, relay);
      let arr = [];
      if (Array.isArray(j?.models)) arr = j.models;
      else if (Array.isArray(j?.data)) arr = j.data;
      else if (Array.isArray(j)) arr = j;
      const names = arr
        .map((m) => (m && (m.name || m.id || m)) ?? '')
        .filter(Boolean)
        .map(String);
      return Array.from(new Set(names)).sort((a, b) => a.localeCompare(b));
    } catch (err) {
      return [];
    }
  }

  const GraphTypes = graphTypes;
  const EXTRA_DOCS = [
    { type: 'INDEX', title: 'Routing & Data Guide', url: 'docs/INDEX.md', insertable: false }
  ];
  const DOC_ENTRIES = [
    ...EXTRA_DOCS,
    ...Object.keys(GraphTypes).map((type) => ({
      type,
      title: GraphTypes[type]?.title || type,
      url: `docs/${type}.md`,
      insertable: true
    }))
  ];
  const docsCache = new Map();
  let docsActiveType = DOC_ENTRIES[0]?.type || null;
  const docEls = {
    modal: qs('#docsModal'),
    sidebar: qs('#docsSidebar'),
    content: qs('#docsContent'),
    search: qs('#docsSearch'),
    close: qs('#docsClose'),
    backdrop: qs('#docsBackdrop'),
    insert: qs('#docsInsertBtn')
  };

  const markdownToHtml = (text) => {
    try {
      if (window.marked?.parse) return window.marked.parse(text);
    } catch (err) {
      // ignore parse issues
    }
    const safe = String(text || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
    return `<pre class="code" style="white-space:pre-wrap;">${safe}</pre>`;
  };

  async function fetchDocText(type) {
    if (docsCache.has(type)) return docsCache.get(type);
    const entry = DOC_ENTRIES.find((d) => d.type === type);
    if (!entry) throw new Error(`Missing doc for ${type}`);
    const res = await fetch(entry.url);
    if (!res.ok) throw new Error(`Doc load failed (${res.status})`);
    const text = await res.text();
    docsCache.set(type, text);
    return text;
  }

  function renderDocsSidebar(filter = '') {
    if (!docEls.sidebar) return;
    const q = filter.trim().toLowerCase();
    docEls.sidebar.innerHTML = '';
    const entries = DOC_ENTRIES.filter((e) => {
      if (!q) return true;
      return e.title.toLowerCase().includes(q) || e.type.toLowerCase().includes(q);
    });
    const fragment = document.createDocumentFragment();
    entries.forEach((entry) => {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.textContent = `${entry.title} (${entry.type})`;
      if (entry.type === docsActiveType) btn.classList.add('active');
      btn.addEventListener('click', () => openDocs(entry.type));
      fragment.appendChild(btn);
    });
    if (!entries.length) {
      const empty = document.createElement('div');
      empty.className = 'muted';
      empty.style.padding = '6px 8px';
      empty.textContent = 'No matches';
      fragment.appendChild(empty);
    }
    docEls.sidebar.appendChild(fragment);
  }

  async function renderDocContent(type) {
    if (!docEls.content) return;
    const entry = DOC_ENTRIES.find((d) => d.type === type);
    if (!entry) {
      docEls.content.innerHTML = '<p class="muted">No documentation available.</p>';
      return;
    }
    docsActiveType = entry.type;
    renderDocsSidebar(docEls.search?.value || '');
    docEls.content.innerHTML = '<p class="muted">Loading…</p>';
    try {
      const text = await fetchDocText(type);
      const html = markdownToHtml(text);
      docEls.content.innerHTML = html;
    } catch (err) {
      docEls.content.innerHTML = `<p class="muted">Failed to load documentation: ${err?.message || err}</p>`;
    }
    if (docEls.insert) {
      const allowInsert = entry.insertable !== false;
      docEls.insert.dataset.nodeType = allowInsert ? entry.type : '';
      docEls.insert.textContent = allowInsert ? `Insert ${entry.title} node` : 'Insert node';
      docEls.insert.classList.toggle('hidden', !allowInsert);
      docEls.insert.disabled = !allowInsert;
    }
  }

  function closeDocs() {
    if (!docEls.modal) return;
    docEls.modal.classList.add('hidden');
    docEls.modal.setAttribute('aria-hidden', 'true');
  }

  function bindDocsModal() {
    if (!docEls.modal || docEls.modal._docsBound) return;
    docEls.modal._docsBound = true;
    docEls.close?.addEventListener('click', (e) => {
      e.preventDefault();
      closeDocs();
    });
    docEls.backdrop?.addEventListener('click', closeDocs);
    if (docEls.search) {
      docEls.search.addEventListener('input', () => renderDocsSidebar(docEls.search.value));
    }
    if (docEls.insert) {
      docEls.insert.addEventListener('click', (e) => {
        e.preventDefault();
        const type = docEls.insert.dataset.nodeType;
        if (!type) return;
        const center = currentViewCenter();
        const node = addNode(type, Math.round(center.x), Math.round(center.y), { select: true });
        if (node) {
          positionNodeCentered(node, center);
          saveGraph();
          setBadge(`Inserted ${type}`);
        }
      });
    }
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && !docEls.modal.classList.contains('hidden')) {
        closeDocs();
      }
    });
  }

  function openDocs(type) {
    bindDocsModal();
    if (!docEls.modal) return;
    let chosen = type || docsActiveType || DOC_ENTRIES[0]?.type;
    if (chosen && !DOC_ENTRIES.some((d) => d.type === chosen) && DOC_ENTRIES.length) {
      chosen = DOC_ENTRIES[0].type;
    }
    if (docEls.search) docEls.search.value = '';
    renderDocsSidebar('');
    renderDocContent(chosen);
    docEls.modal.classList.remove('hidden');
    docEls.modal.setAttribute('aria-hidden', 'false');
  }

  const TEMPLATE_VAR_RE = /\{([a-zA-Z0-9_]+)\}/g;

  function extractPayloadText(payload) {
    if (payload == null) return '';
    if (typeof payload === 'string') return payload;
    if (typeof payload === 'number') return String(payload);
    if (typeof payload === 'boolean') return payload ? 'true' : 'false';
    if (typeof payload !== 'object') return String(payload);
    if (payload.text != null) return String(payload.text);
    if (payload.value != null) return String(payload.value);
    if (payload.content != null) return String(payload.content);
    if (payload.data != null) return String(payload.data);
    try {
      return JSON.stringify(payload);
    } catch (err) {
      return String(payload);
    }
  }

  function extractTemplateVariables(template) {
    const names = new Set();
    String(template || '').replace(TEMPLATE_VAR_RE, (_, name) => {
      if (name) names.add(name);
      return '';
    });
    return Array.from(names);
  }

  // Support inline numeric transforms in templates: any `[ ... ]` block is treated
  // as a lightweight expression where `{var}` placeholders are replaced with the
  // numeric value of the corresponding variable (if possible). This lets authors
  // write templates such as `Battery: [0.1 * {millivolts} / 20] %`.
  const TEMPLATE_EXPR_RE = /\[([^[\]]+)\]/g;
  const NUMERIC_FRAGMENT_RE = /[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?/;
  const SAFE_EXPR_RE = /^[0-9+\-*/().\s%Ee]+$/;

  function extractNumericValue(raw) {
    if (raw == null) return Number.NaN;
    if (typeof raw === 'number') return Number.isFinite(raw) ? raw : Number.NaN;
    if (typeof raw === 'boolean') return raw ? 1 : 0;
    const match = String(raw).match(NUMERIC_FRAGMENT_RE);
    if (!match) return Number.NaN;
    const num = Number(match[0]);
    return Number.isFinite(num) ? num : Number.NaN;
  }

  function evaluateTemplateExpression(body, vars) {
    const trimmed = body.trim();
    if (!trimmed) return '';
    const rawValues = [];
    let nonNumeric = false;
    const substituted = trimmed.replace(TEMPLATE_VAR_RE, (_, name) => {
      const value = vars[name];
      rawValues.push(value != null ? String(value) : '');
      const num = extractNumericValue(value);
      if (!Number.isFinite(num)) {
        nonNumeric = true;
        return '0';
      }
      return String(num);
    });
    if (nonNumeric) {
      if (rawValues.length === 1) return rawValues[0];
      return rawValues.filter((v) => v.length).join(' ') || '';
    }
    if (!SAFE_EXPR_RE.test(substituted)) return trimmed;
    let result;
    try {
      // eslint-disable-next-line no-new-func
      result = Function(`"use strict"; return (${substituted});`)();
    } catch (_) {
      return trimmed;
    }
    if (typeof result === 'number' && Number.isFinite(result)) {
      const rounded = Math.abs(result) < 1e6 ? Number(result.toFixed(6)) : result;
      return String(rounded);
    }
    return trimmed;
  }

  function renderTemplateString(template, variables) {
    const vars = variables || {};
    const templateText = String(template || '');
    const withMath = templateText.replace(TEMPLATE_EXPR_RE, (_, exprBody) => evaluateTemplateExpression(exprBody, vars));
    return withMath.replace(TEMPLATE_VAR_RE, (_, name) => {
      const value = vars[name];
      return value != null ? String(value) : '';
    });
  }

  function updateTemplatePreview(node, cfg, renderedText) {
    const preview = node.el?.querySelector('[data-template-preview]');
    if (!preview) return;
    const text = renderedText ?? renderTemplateString(cfg?.template, cfg?.variables);
    preview.textContent = text || '';
  }

  function emitTemplate(nodeId, cfg) {
    const node = WS.nodes.get(nodeId);
    if (!node) return;
    const config = cfg || NodeStore.ensure(nodeId, 'Template').config || {};
    const text = renderTemplateString(config.template, config.variables);
    updateTemplatePreview(node, config, text);
    Router.sendFrom(nodeId, 'text', {
      nodeId,
      type: 'text',
      text,
      template: config.template,
      variables: config.variables
    });
  }

  function handleTemplateTrigger(nodeId) {
    const updated = pullTemplateInputs(nodeId);
    emitTemplate(nodeId, updated);
  }

  function handleTemplateVariable(nodeId, varName, payload) {
    const node = WS.nodes.get(nodeId);
    if (!node) return;
    const incoming = extractPayloadText(payload);
    const current = NodeStore.ensure(nodeId, 'Template').config || {};
    const vars = { ...(current.variables || {}) };
    vars[varName] = incoming;
    const updated = NodeStore.update(nodeId, { type: 'Template', variables: vars });
    updateTemplatePreview(node, updated);
    emitTemplate(nodeId, updated);
  }

  function removeTemplatePort(node, portName, portEl) {
    removeWiresAt(node.id, 'in', portName);
    if (portEl) {
      for (const [key, value] of Array.from(Router.ports.entries())) {
        if (value && value.el === portEl) {
          Router.ports.delete(key);
        }
      }
      portEl.remove();
    }
  }

  function createTemplateVariablePort(node, leftContainer, varName) {
    if (!leftContainer) return null;
    const portEl = document.createElement('div');
    portEl.className = 'wp-port in';
    portEl.dataset.port = varName;
    portEl.dataset.templateVar = varName;
    applyPortTooltip(portEl, varName, '');
    portEl.innerHTML = `<span class="dot"></span><span>${varName}</span>`;

    portEl.addEventListener('click', (ev) => {
      if (ev.altKey || ev.metaKey || ev.ctrlKey) {
        removeTemplatePort(node, varName, portEl);
        return;
      }
      onPortClick(node.id, 'in', varName, portEl);
    });

    portEl.addEventListener('pointerdown', (ev) => {
      if (ev.pointerType === 'touch') ev.preventDefault();
      const wires = connectedWires(node.id, 'in', varName);
      if (wires.length) {
        const w = wires[wires.length - 1];
        w.path?.setAttribute('stroke-dasharray', '6 4');
        const move = (e) => {
          if (e.pointerType === 'touch') e.preventDefault();
          if (WS.drag) WS.drag.lastClient = { x: e.clientX, y: e.clientY };
          const pt = clientToWorkspace(e.clientX, e.clientY);
          drawRetarget(w, 'to', pt.x, pt.y);
          if (WS.drag) updateDropHover(WS.drag.expected);
        };
        const up = (e) => {
          setDropHover(null);
          finishAnyDrag(e.clientX, e.clientY);
        };
        WS.drag = {
          kind: 'retarget',
          wireId: w.id,
          grabSide: 'to',
          path: w.path,
          pointerId: ev.pointerId,
          expected: 'out',
          lastClient: { x: ev.clientX, y: ev.clientY },
          _cleanup: () => window.removeEventListener('pointermove', move)
        };
        window.addEventListener('pointermove', move, { passive: false });
        window.addEventListener('pointerup', up, { once: true, passive: false });
        updateDropHover('out');
        return;
      }

      const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      path.setAttribute('fill', 'none');
      path.setAttribute('stroke', 'rgba(255,255,255,.6)');
      path.setAttribute('stroke-width', '2');
      path.setAttribute('stroke-linecap', 'round');
      path.setAttribute('opacity', '0.9');
      path.setAttribute('stroke-dasharray', '6 4');
      path.setAttribute('vector-effect', 'non-scaling-stroke');
      path.setAttribute('pointer-events', 'none');
      WS.svgLayer.appendChild(path);

      const move = (e) => {
        if (e.pointerType === 'touch') e.preventDefault();
        if (WS.drag) WS.drag.lastClient = { x: e.clientX, y: e.clientY };
        const pt = clientToWorkspace(e.clientX, e.clientY);
        drawTempFromPort({ nodeId: node.id, side: 'in', portName: varName }, pt.x, pt.y);
        updateDropHover(WS.drag?.expected);
      };
      const up = (e) => {
        setDropHover(null);
        finishAnyDrag(e.clientX, e.clientY);
      };
      WS.drag = {
        kind: 'newFromInput',
        toNodeId: node.id,
        toPort: varName,
        path,
        pointerId: ev.pointerId,
        expected: 'out',
        lastClient: { x: ev.clientX, y: ev.clientY },
        _cleanup: () => window.removeEventListener('pointermove', move)
      };
      window.addEventListener('pointermove', move, { passive: false });
      window.addEventListener('pointerup', up, { once: true, passive: false });
      const pt = clientToWorkspace(ev.clientX, ev.clientY);
      drawTempFromPort({ nodeId: node.id, side: 'in', portName: varName }, pt.x, pt.y);
      updateDropHover('out');
    });

    leftContainer.appendChild(portEl);
    Router.register(`${node.id}:in:${varName}`, portEl, (payload) => handleTemplateVariable(node.id, varName, payload));
    return portEl;
  }

  function rebuildTemplateVariablePorts(node, leftContainer, cfg) {
    if (!leftContainer) return;
    node._templateVarPorts = node._templateVarPorts || new Map();
    const ports = node._templateVarPorts;
    const desiredVars = new Set(Object.keys((cfg && cfg.variables) || {}));

    for (const [varName, portEl] of Array.from(ports.entries())) {
      if (!desiredVars.has(varName)) {
        removeTemplatePort(node, varName, portEl);
        ports.delete(varName);
      }
    }

    desiredVars.forEach((varName) => {
      if (!ports.has(varName)) {
        const elPort = createTemplateVariablePort(node, leftContainer, varName);
        ports.set(varName, elPort);
      }
    });

    requestRedraw();
  }

  function setupTemplateNode(node, leftContainer) {
    const textarea = node.el?.querySelector('[data-template-editor]');
    const config = NodeStore.ensure(node.id, 'Template').config || {};
    const ensureVars = () => {
      const fresh = NodeStore.ensure(node.id, 'Template').config || {};
      const existingVars = { ...(fresh.variables || {}) };
      const names = extractTemplateVariables(fresh.template || '');
      let changed = false;
      const nextVars = {};
      names.forEach((name) => {
        if (Object.prototype.hasOwnProperty.call(existingVars, name)) {
          nextVars[name] = existingVars[name];
        } else {
          nextVars[name] = '';
        }
        if (!(name in existingVars)) changed = true;
      });
      for (const key of Object.keys(existingVars)) {
        if (!names.includes(key)) {
          changed = true;
        }
      }
      if (changed) {
        return NodeStore.update(node.id, { type: 'Template', variables: nextVars });
      }
      return fresh;
    };

    if (textarea) {
      if (!node._templateReady) {
        textarea.addEventListener('input', () => {
          const templateText = textarea.value || '';
          const existing = NodeStore.ensure(node.id, 'Template').config || {};
          const vars = { ...(existing.variables || {}) };
          const names = extractTemplateVariables(templateText);
          const nextVars = {};
          names.forEach((name) => {
            nextVars[name] = Object.prototype.hasOwnProperty.call(vars, name) ? vars[name] : '';
          });
          const updated = NodeStore.update(node.id, { type: 'Template', template: templateText, variables: nextVars });
          rebuildTemplateVariablePorts(node, leftContainer, updated);
          updateTemplatePreview(node, updated);
        });
        node._templateReady = true;
      }
      textarea.value = config.template || '';
    }

    const updatedCfg = ensureVars();
    rebuildTemplateVariablePorts(node, leftContainer, updatedCfg);
    updateTemplatePreview(node, updatedCfg);
  }

  function refreshMeshtasticPorts(nodeId) {
    const node = WS.nodes.get(nodeId);
    if (!node || node.type !== 'Meshtastic') return;
    const cfg = NodeStore.ensure(nodeId, 'Meshtastic').config || {};
    const peers = cfg.peers || {};
    const left = node.el?.querySelector('.side.left');
    const right = node.el?.querySelector('.side.right');
    if (!left || !right) return;

    const channel = Number.isFinite(Number(cfg.channel)) ? Number(cfg.channel) : 0;
    const pubLabel = `Public ch${channel}`;
    const publicIn = node.portEls?.['in:public'];
    if (publicIn) {
      const span = publicIn.querySelector('span:last-child');
      if (span) span.textContent = pubLabel;
      applyPortTooltip(publicIn, pubLabel, '');
    }
    const publicOut = node.portEls?.['out:public'];
    if (publicOut) {
      const span = publicOut.querySelector('span:first-child');
      if (span) span.textContent = pubLabel;
      applyPortTooltip(publicOut, pubLabel, '');
    }

    const desired = new Map();
    const nextPeers = { ...peers };
    let changed = false;
    for (const [key, info] of Object.entries(peers)) {
      if (!info || !info.enabled) continue;
      const portName = info.portName || `peer-${key}`;
      if (info.portName !== portName) {
        nextPeers[key] = { ...info, portName };
        changed = true;
      }
      const label = info.label || `Peer #${key}`;
      desired.set(portName, { peerKey: key, label });
    }
    if (changed) {
      NodeStore.update(nodeId, { type: 'Meshtastic', peers: nextPeers });
    }

    node._meshtasticPorts = node._meshtasticPorts || new Set();
    const active = node._meshtasticPorts;
    const keep = new Set();

    desired.forEach((meta, portName) => {
      const inLabel = `To ${meta.label}`;
      const outLabel = meta.label;
      const inEl = createInputPort(node, left, portName, inLabel);
      if (inEl) {
        const span = inEl.querySelector('span:last-child');
        if (span) span.textContent = inLabel;
        applyPortTooltip(inEl, inLabel, '');
      }
      const outEl = createOutputPort(node, right, portName, outLabel);
      if (outEl) {
        const span = outEl.querySelector('span:first-child');
        if (span) span.textContent = outLabel;
        applyPortTooltip(outEl, outLabel, '');
      }
      active.add(portName);
      keep.add(portName);
    });

    const autoMeta = Meshtastic.getAutoTarget?.(nodeId) || {};
    const autoLabel = autoMeta.label ? `Auto ${autoMeta.label}` : 'Auto (no target)';
    const autoInLabel = autoMeta.label ? `Auto → ${autoMeta.label}` : 'Auto (no target)';
    const autoIn = createInputPort(node, left, 'auto', autoInLabel);
    if (autoIn) {
      const span = autoIn.querySelector('span:last-child');
      if (span) span.textContent = autoInLabel;
      applyPortTooltip(autoIn, autoInLabel, '');
    }
    const autoOut = createOutputPort(node, right, 'auto', autoLabel);
    if (autoOut) {
      const span = autoOut.querySelector('span:first-child');
      if (span) span.textContent = autoLabel;
      applyPortTooltip(autoOut, autoLabel, '');
    }
    active.add('auto');
    keep.add('auto');

    for (const portName of Array.from(active)) {
      if (keep.has(portName)) continue;
      unregisterInputPort(node, portName);
      const inEl = node.portEls?.[`in:${portName}`];
      if (inEl?.remove) inEl.remove();
      if (node.portEls) delete node.portEls[`in:${portName}`];
      removeOutputPort(node, portName);
      active.delete(portName);
    }

    Meshtastic.refresh?.(nodeId);
  }

  function initMeshtasticNode(node) {
    if (!node || node.type !== 'Meshtastic') return;
    refreshMeshtasticPorts(node.id);
    Meshtastic.init?.(node.id);
  }

  function pullTemplateInputs(nodeId) {
    const node = WS.nodes.get(nodeId);
    if (!node) return NodeStore.ensure(nodeId, 'Template').config || {};
    let config = NodeStore.ensure(nodeId, 'Template').config || {};
    const vars = { ...(config.variables || {}) };
    let changed = false;

    const names = Object.keys(vars);
    names.forEach((name) => {
      const wires = WS.wires.filter((w) => w.to.node === nodeId && w.to.port === name);
      for (const wire of wires) {
        const srcNode = WS.nodes.get(wire.from.node);
        if (!srcNode) continue;
        if (srcNode.type === 'TextInput') {
          const srcCfg = NodeStore.ensure(srcNode.id, 'TextInput').config || {};
          const candidate = srcCfg.text || srcCfg.lastSent || '';
          if (candidate !== vars[name]) {
            vars[name] = candidate;
            changed = true;
          }
          break;
        } else if (srcNode.type === 'Template') {
          const srcCfg = NodeStore.ensure(srcNode.id, 'Template').config || {};
          const candidate = renderTemplateString(srcCfg.template, srcCfg.variables);
          if (candidate !== vars[name]) {
            vars[name] = candidate;
            changed = true;
          }
          break;
        }
      }
    });

    if (changed) {
      config = NodeStore.update(nodeId, { type: 'Template', variables: vars });
      updateTemplatePreview(node, config);
    }
    return config;
  }

  const LOGIC_OPERATOR_OPTIONS = [
    { value: 'truthy', label: 'Truthy', needsValue: false },
    { value: 'falsy', label: 'Falsy', needsValue: false },
    { value: 'exists', label: 'Exists', needsValue: false },
    { value: 'notExists', label: 'Not Exists', needsValue: false },
    { value: 'equals', label: 'Equals', needsValue: true },
    { value: 'notEquals', label: 'Not Equals', needsValue: true },
    { value: 'contains', label: 'Contains', needsValue: true },
    { value: 'greaterThan', label: 'Greater Than', needsValue: true },
    { value: 'lessThan', label: 'Less Than', needsValue: true },
    { value: 'greaterOrEqual', label: '≥', needsValue: true },
    { value: 'lessOrEqual', label: '≤', needsValue: true },
    { value: 'matches', label: 'Matches (regex)', needsValue: true }
  ];
  const LOGIC_OPERATORS_WITH_VALUE = new Set(LOGIC_OPERATOR_OPTIONS.filter((o) => o.needsValue).map((o) => o.value));
  const LOGIC_OPERATOR_LABELS = new Map(LOGIC_OPERATOR_OPTIONS.map((o) => [o.value, o.label]));
  const LOGIC_PASS_MODES = ['message', 'value', 'boolean'];

  function logicRuleId() {
    return `lg-${uid().slice(1)}`;
  }

  function sanitizeLogicRules(rules) {
    const list = Array.isArray(rules) ? rules : [];
    const usedInputs = new Set();
    const sanitized = [];
    list.forEach((raw, idx) => {
      if (!raw || typeof raw !== 'object') return;
      const copy = { ...raw };
      let input = String(copy.input || '').trim();
      if (!input) input = `input${idx + 1}`;
      let unique = input;
      let counter = 2;
      while (usedInputs.has(unique)) {
        unique = `${input}-${counter++}`;
      }
      if (unique !== input) copy.input = unique;
      usedInputs.add(unique);

      const id = String(copy.id || '').trim();
      copy.id = id || logicRuleId();
      copy.label = String(copy.label || unique);
      copy.description = typeof copy.description === 'string' ? copy.description : '';
      copy.path = typeof copy.path === 'string' ? copy.path.trim() : '';

      copy.operator = LOGIC_OPERATOR_LABELS.has(copy.operator)
        ? copy.operator
        : 'truthy';

      copy.compareValue = copy.compareValue != null ? String(copy.compareValue) : '';
      if (copy.outputTrue === undefined || copy.outputTrue === null) copy.outputTrue = `${unique}:true`;
      if (copy.outputFalse === undefined || copy.outputFalse === null) copy.outputFalse = `${unique}:false`;
      copy.outputTrue = String(copy.outputTrue).trim();
      copy.outputFalse = String(copy.outputFalse).trim();

      copy.trueMode = LOGIC_PASS_MODES.includes(copy.trueMode) ? copy.trueMode : 'message';
      copy.falseMode = LOGIC_PASS_MODES.includes(copy.falseMode) ? copy.falseMode : 'message';

      sanitized.push(copy);
    });
    return sanitized;
  }

  function ensureLogicGateConfig(nodeId) {
    const rec = NodeStore.ensure(nodeId, 'LogicGate');
    const cfg = rec.config || {};
    const sanitized = sanitizeLogicRules(cfg.rules);
    const existing = Array.isArray(cfg.rules) ? cfg.rules : [];
    if (JSON.stringify(existing) !== JSON.stringify(sanitized)) {
      NodeStore.update(nodeId, { type: 'LogicGate', rules: sanitized });
      const refreshed = NodeStore.ensure(nodeId, 'LogicGate');
      return refreshed.config || { rules: sanitized };
    }
    return { ...cfg, rules: sanitized };
  }

  function normalizeLogicPath(path) {
    if (!path) return '';
    return String(path)
      .replace(/\[(\w+)\]/g, '.$1')
      .split('.')
      .map((part) => part.trim())
      .filter(Boolean)
      .join('.');
  }

  function resolveLogicValue(payload, path) {
    const normalized = normalizeLogicPath(path);
    if (!normalized) return payload;
    const segments = normalized.split('.');
    let current = payload;
    for (const segment of segments) {
      if (current == null) return undefined;
      current = current[segment];
    }
    return current;
  }

  function parseLogicCompareValue(raw) {
    if (raw === undefined) return undefined;
    if (raw === null) return null;
    const str = String(raw).trim();
    if (!str.length) return '';
    if (str === 'true') return true;
    if (str === 'false') return false;
    if (str === 'null') return null;
    if (str === 'undefined') return undefined;
    if (/^-?\d+(?:\.\d+)?$/.test(str)) return Number(str);
    if ((str.startsWith('{') && str.endsWith('}')) || (str.startsWith('[') && str.endsWith(']'))) {
      try {
        return JSON.parse(str);
      } catch (_) {
        return str;
      }
    }
    return str;
  }

  function logicEquals(a, b) {
    if (a === b) return true;
    if (a == null || b == null) return a == null && b == null;
    if (typeof a === 'number' && typeof b === 'number') return Number.isFinite(a) && Number.isFinite(b) && a === b;
    if (typeof a === 'boolean' && typeof b === 'boolean') return a === b;
    if (typeof a === 'object' && typeof b === 'object') {
      try {
        return JSON.stringify(a) === JSON.stringify(b);
      } catch (_) {
        return false;
      }
    }
    return String(a) === String(b);
  }

  function asNumber(value) {
    const num = Number(value);
    return Number.isFinite(num) ? num : NaN;
  }

  function toRegex(value) {
    if (value instanceof RegExp) return value;
    if (typeof value !== 'string') return null;
    const match = value.match(/^\/(.*)\/(\w*)$/);
    if (!match) return null;
    try {
      return new RegExp(match[1], match[2]);
    } catch (_) {
      return null;
    }
  }

  function evaluateLogicRule(rule, value) {
    const op = rule.operator || 'truthy';
    const compare = parseLogicCompareValue(rule.compareValue);
    switch (op) {
      case 'truthy':
        return !!value;
      case 'falsy':
        return !value;
      case 'exists':
        return value !== undefined && value !== null;
      case 'notExists':
        return value === undefined || value === null;
      case 'equals':
        return logicEquals(value, compare);
      case 'notEquals':
        return !logicEquals(value, compare);
      case 'contains': {
        if (Array.isArray(value)) {
          return value.some((entry) => logicEquals(entry, compare));
        }
        if (typeof value === 'string') {
          return String(value).includes(String(compare));
        }
        if (value && typeof value === 'object' && typeof compare === 'string') {
          return Object.prototype.hasOwnProperty.call(value, compare);
        }
        return false;
      }
      case 'greaterThan': {
        const a = asNumber(value);
        const b = asNumber(compare);
        if (Number.isNaN(a) || Number.isNaN(b)) return false;
        return a > b;
      }
      case 'lessThan': {
        const a = asNumber(value);
        const b = asNumber(compare);
        if (Number.isNaN(a) || Number.isNaN(b)) return false;
        return a < b;
      }
      case 'greaterOrEqual': {
        const a = asNumber(value);
        const b = asNumber(compare);
        if (Number.isNaN(a) || Number.isNaN(b)) return false;
        return a >= b;
      }
      case 'lessOrEqual': {
        const a = asNumber(value);
        const b = asNumber(compare);
        if (Number.isNaN(a) || Number.isNaN(b)) return false;
        return a <= b;
      }
      case 'matches': {
        const regex = toRegex(compare);
        if (regex) return regex.test(String(value ?? ''));
        if (typeof compare === 'string') return String(value ?? '') === compare;
        return false;
      }
      default:
        return !!value;
    }
  }

  function determineLogicOutputPayload(rule, result, originalPayload, extractedValue) {
    const mode = result ? rule.trueMode : rule.falseMode;
    if (mode === 'value') return extractedValue;
    if (mode === 'boolean') return result;
    return originalPayload;
  }

  function formatLogicValue(value) {
    if (value === undefined) return 'undefined';
    if (value === null) return 'null';
    if (typeof value === 'boolean') return value ? 'true' : 'false';
    if (typeof value === 'number') return Number.isFinite(value) ? String(value) : 'NaN';
    if (typeof value === 'string') {
      return value.length > 64 ? `${value.slice(0, 61)}…` : value || '""';
    }
    try {
      const text = JSON.stringify(value);
      return text.length > 64 ? `${text.slice(0, 61)}…` : text;
    } catch (_) {
      return '[Object]';
    }
  }

  function refreshLogicGatePreview(node, rules) {
    if (!node?.el) return;
    const container = node.el.querySelector('[data-logic-gate]');
    if (!container) return;
    container.innerHTML = '';
    if (!rules || !rules.length) {
      const empty = document.createElement('div');
      empty.className = 'logic-gate-empty muted';
      empty.textContent = 'No rules configured';
      container.appendChild(empty);
      return;
    }
    const state = node._logicState || new Map();
    rules.forEach((rule) => {
      const row = document.createElement('div');
      row.className = 'logic-gate-row';
      const snapshot = state.get(rule.id);
      const status = snapshot ? (snapshot.result ? 'true' : 'false') : 'pending';
      row.dataset.state = status;

      const name = document.createElement('span');
      name.className = 'logic-gate-name';
      name.textContent = rule.label || rule.input;
      row.appendChild(name);

      const op = document.createElement('span');
      op.className = 'logic-gate-operator';
      const opLabel = LOGIC_OPERATOR_LABELS.get(rule.operator) || rule.operator;
      op.textContent = opLabel;
      row.appendChild(op);

      const path = document.createElement('span');
      path.className = 'logic-gate-path';
      path.textContent = rule.path ? rule.path : '(payload)';
      row.appendChild(path);

      const val = document.createElement('span');
      val.className = 'logic-gate-value';
      val.textContent = snapshot ? formatLogicValue(snapshot.value) : '—';
      row.appendChild(val);

      container.appendChild(row);
    });
  }

  function removeLogicInputPort(node, portName) {
    if (!node) return;
    removeWiresAt(node.id, 'in', portName);
    unregisterInputPort(node, portName);
    const key = `in:${portName}`;
    const el = node.portEls?.[key];
    if (el?.remove) el.remove();
    if (node.portEls) delete node.portEls[key];
  }

  function removeOutputPort(node, portName) {
    if (!node) return;
    removeWiresAt(node.id, 'out', portName);
    const key = `out:${portName}`;
    const el = node.portEls?.[key];
    if (el?.remove) el.remove();
    if (node.portEls) delete node.portEls[key];
  }

  function removeLogicOutputPort(node, portName) {
    if (!node) return;
    removeWiresAt(node.id, 'out', portName);
    const key = `out:${portName}`;
    const el = node.portEls?.[key];
    if (el?.remove) el.remove();
    if (node.portEls) delete node.portEls[key];
  }

  function ensureLogicOutputPort(node, container, portName, variant = 'mixed') {
    if (!node || !container || !portName) return null;
    node.portEls = node.portEls || {};
    node._logicOutputs = node._logicOutputs || new Map();
    const key = `out:${portName}`;
    const existing = node._logicOutputs.get(portName);
    if (existing && existing.el?.isConnected) {
      existing.variant = variant;
      existing.el.dataset.logicVariant = variant;
      const labelEl = existing.el.querySelector('span');
      if (labelEl) labelEl.textContent = portName;
      return existing.el;
    }

    const portEl = document.createElement('div');
    portEl.className = 'wp-port out';
    portEl.dataset.port = portName;
    portEl.dataset.logicVariant = variant;
    applyPortTooltip(portEl, portName, '');
    portEl.innerHTML = `<span>${portName}</span><span class="dot"></span>`;

    portEl.addEventListener('click', (ev) => {
      if (ev.altKey || ev.metaKey || ev.ctrlKey) {
        removeWiresAt(node.id, 'out', portName);
        return;
      }
      onPortClick(node.id, 'out', portName, portEl);
    });

    portEl.addEventListener('pointerdown', (ev) => {
      if (ev.pointerType === 'touch') ev.preventDefault();
      const wires = connectedWires(node.id, 'out', portName);
      if (wires.length) {
        const w = wires[wires.length - 1];
        w.path?.setAttribute('stroke-dasharray', '6 4');
        const move = (e) => {
          if (e.pointerType === 'touch') e.preventDefault();
          if (WS.drag) WS.drag.lastClient = { x: e.clientX, y: e.clientY };
          const pt = clientToWorkspace(e.clientX, e.clientY);
          drawRetarget(w, 'from', pt.x, pt.y);
          if (WS.drag) updateDropHover(WS.drag.expected);
        };
        const up = (e) => {
          setDropHover(null);
          finishAnyDrag(e.clientX, e.clientY);
        };
        WS.drag = {
          kind: 'retarget',
          wireId: w.id,
          grabSide: 'from',
          path: w.path,
          pointerId: ev.pointerId,
          expected: 'in',
          lastClient: { x: ev.clientX, y: ev.clientY },
          _cleanup: () => window.removeEventListener('pointermove', move)
        };
        window.addEventListener('pointermove', move, { passive: false });
        window.addEventListener('pointerup', up, { once: true, passive: false });
        updateDropHover('in');
        return;
      }

      const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      path.setAttribute('fill', 'none');
      path.setAttribute('stroke', 'rgba(255,255,255,.7)');
      path.setAttribute('stroke-width', '2');
      path.setAttribute('stroke-linecap', 'round');
      path.setAttribute('opacity', '0.9');
      path.setAttribute('stroke-dasharray', '6 4');
      path.setAttribute('vector-effect', 'non-scaling-stroke');
      path.setAttribute('pointer-events', 'none');
      WS.svgLayer.appendChild(path);

      const move = (e) => {
        if (e.pointerType === 'touch') e.preventDefault();
        if (WS.drag) WS.drag.lastClient = { x: e.clientX, y: e.clientY };
        const pt = clientToWorkspace(e.clientX, e.clientY);
        drawTempFromPort({ nodeId: node.id, side: 'out', portName }, pt.x, pt.y);
        updateDropHover(WS.drag?.expected);
      };
      const up = (e) => {
        setDropHover(null);
        finishAnyDrag(e.clientX, e.clientY);
      };
      WS.drag = {
        kind: 'new',
        fromNodeId: node.id,
        fromPort: portName,
        path,
        pointerId: ev.pointerId,
        expected: 'in',
        lastClient: { x: ev.clientX, y: ev.clientY },
        _cleanup: () => window.removeEventListener('pointermove', move)
      };
      window.addEventListener('pointermove', move, { passive: false });
      window.addEventListener('pointerup', up, { once: true, passive: false });
      const pt = clientToWorkspace(ev.clientX, ev.clientY);
      drawTempFromPort({ nodeId: node.id, side: 'out', portName }, pt.x, pt.y);
      updateDropHover('in');
    });

    container.appendChild(portEl);
    node.portEls[key] = portEl;
    node._logicOutputs.set(portName, { el: portEl, variant });
    return portEl;
  }

  function setupLogicGateNode(node, leftContainer) {
    if (!node || node.type !== 'LogicGate') return;
    const left = leftContainer || node.el?.querySelector('.side.left');
    const right = node.el?.querySelector('.side.right');
    const cfg = ensureLogicGateConfig(node.id);
    const rules = cfg.rules || [];

    node._logicInputs = node._logicInputs || new Map();
    node._logicOutputs = node._logicOutputs || new Map();
    node._logicState = node._logicState || new Map();

    const desiredInputs = new Set(rules.map((r) => r.input));

    for (const [portName, info] of Array.from(node._logicInputs.entries())) {
      if (!desiredInputs.has(portName)) {
        removeLogicInputPort(node, portName);
        node._logicInputs.delete(portName);
      }
    }

    rules.forEach((rule) => {
      if (!rule.input) return;
      let entry = node._logicInputs.get(rule.input);
      const label = rule.label || rule.input;
      if (!entry) {
        const portEl = createInputPort(node, left, rule.input, label);
        if (portEl) {
          portEl.dataset.logicRuleId = rule.id;
          node._logicInputs.set(rule.input, { el: portEl, ruleId: rule.id });
        }
      } else {
        const portEl = entry.el;
        if (portEl) {
          portEl.dataset.logicRuleId = rule.id;
          const labelEl = portEl.querySelector('span:last-child');
          if (labelEl) labelEl.textContent = label;
          applyPortTooltip(portEl, label, '');
        }
        entry.ruleId = rule.id;
      }
    });

    const desiredOutputs = new Map();
    rules.forEach((rule) => {
      const trueName = rule.outputTrue?.trim();
      const falseName = rule.outputFalse?.trim();
      if (trueName) {
        const existing = desiredOutputs.get(trueName);
        desiredOutputs.set(trueName, existing && existing.variant !== 'true' ? { variant: 'mixed' } : { variant: 'true' });
      }
      if (falseName) {
        const existing = desiredOutputs.get(falseName);
        desiredOutputs.set(falseName, existing && existing.variant !== 'false' ? { variant: 'mixed' } : { variant: 'false' });
      }
    });

    for (const [portName] of Array.from(node._logicOutputs.entries())) {
      if (!desiredOutputs.has(portName)) {
        removeLogicOutputPort(node, portName);
        node._logicOutputs.delete(portName);
      }
    }

    desiredOutputs.forEach((meta, portName) => {
      ensureLogicOutputPort(node, right, portName, meta.variant);
    });

    refreshLogicGatePreview(node, rules);
  }

  function handleLogicGateInput(nodeId, portName, payload) {
    const node = WS.nodes.get(nodeId);
    if (!node || node.type !== 'LogicGate') return;
    const cfg = ensureLogicGateConfig(nodeId);
    const rules = cfg.rules || [];
    const rule = rules.find((r) => r.input === portName);
    if (!rule) return;
    const extracted = resolveLogicValue(payload, rule.path);
    const result = evaluateLogicRule(rule, extracted);
    const targetPort = result ? rule.outputTrue : rule.outputFalse;
    if (targetPort) {
      const outPayload = determineLogicOutputPayload(rule, result, payload, extracted);
      Router.sendFrom(nodeId, targetPort, outPayload);
    }
    node._logicState = node._logicState || new Map();
    node._logicState.set(rule.id, {
      result,
      value: extracted,
      emittedPort: targetPort,
      ts: Date.now()
    });
    refreshLogicGatePreview(node, rules);
  }

  function createLogicRulesEditor(node, field, cfg) {
    const wrap = document.createElement('div');
    wrap.className = 'logic-rules-editor';

    const hidden = document.createElement('input');
    hidden.type = 'hidden';
    hidden.name = field.key;
    wrap.appendChild(hidden);

    const list = document.createElement('div');
    list.className = 'logic-rules-items';
    wrap.appendChild(list);

    const note = document.createElement('div');
    note.className = 'logic-rules-note muted';
    note.textContent = 'Use dot paths (foo.bar) to inspect incoming payloads.';
    wrap.appendChild(note);

    const controls = document.createElement('div');
    controls.className = 'logic-rules-controls';
    wrap.appendChild(controls);

    const addBtn = document.createElement('button');
    addBtn.type = 'button';
    addBtn.className = 'secondary';
    addBtn.textContent = 'Add rule';
    controls.appendChild(addBtn);

    let rules = sanitizeLogicRules((cfg?.rules || []).map((rule) => ({ ...rule })));

    const syncHidden = () => {
      hidden.value = JSON.stringify(sanitizeLogicRules(rules));
    };

    const removeRule = (id) => {
      rules = rules.filter((rule) => rule.id !== id);
      render();
    };

    const buildSelect = (options, current) => {
      const sel = document.createElement('select');
      options.forEach((opt) => {
        const option = document.createElement('option');
        option.value = opt.value;
        option.textContent = opt.label;
        if (opt.value === current) option.selected = true;
        sel.appendChild(option);
      });
      return sel;
    };

    const render = () => {
      rules = sanitizeLogicRules(rules).map((rule) => ({ ...rule }));
      list.innerHTML = '';
      if (!rules.length) {
        const empty = document.createElement('div');
        empty.className = 'logic-rules-empty muted';
        empty.textContent = 'No rules defined.';
        list.appendChild(empty);
        syncHidden();
        return;
      }
      rules.forEach((rule, index) => {
        const card = document.createElement('div');
        card.className = 'logic-rule-card';
        card.dataset.ruleId = rule.id;

        const header = document.createElement('div');
        header.className = 'logic-rule-header';
        const title = document.createElement('span');
        title.className = 'logic-rule-title';
        title.textContent = rule.label || rule.input || `Rule ${index + 1}`;
        header.appendChild(title);

        const removeBtn = document.createElement('button');
        removeBtn.type = 'button';
        removeBtn.className = 'ghost';
        removeBtn.textContent = 'Remove';
        removeBtn.addEventListener('click', (e) => {
          e.preventDefault();
          removeRule(rule.id);
        });
        header.appendChild(removeBtn);
        card.appendChild(header);

        const makeField = (labelText, control) => {
          const row = document.createElement('label');
          row.className = 'logic-field';
          const span = document.createElement('span');
          span.textContent = labelText;
          row.appendChild(span);
          row.appendChild(control);
          return row;
        };

        const labelInput = document.createElement('input');
        labelInput.type = 'text';
        labelInput.value = rule.label || '';
        labelInput.placeholder = `Rule ${index + 1}`;
        labelInput.addEventListener('input', () => {
          rule.label = labelInput.value;
          title.textContent = rule.label || rule.input || `Rule ${index + 1}`;
          syncHidden();
        });
        card.appendChild(makeField('Label', labelInput));

        const inputName = document.createElement('input');
        inputName.type = 'text';
        inputName.value = rule.input || '';
        inputName.placeholder = 'inputName';
        inputName.addEventListener('input', () => {
          rule.input = inputName.value;
          syncHidden();
        });
        card.appendChild(makeField('Input port', inputName));

        const pathInput = document.createElement('input');
        pathInput.type = 'text';
        pathInput.value = rule.path || '';
        pathInput.placeholder = 'payload.data';
        pathInput.addEventListener('input', () => {
          rule.path = pathInput.value;
          syncHidden();
        });
        card.appendChild(makeField('Payload path', pathInput));

        const opSelect = buildSelect(LOGIC_OPERATOR_OPTIONS.map((o) => ({ value: o.value, label: o.label })), rule.operator);
        const operatorRow = makeField('Operator', opSelect);
        card.appendChild(operatorRow);

        const compareInput = document.createElement('input');
        compareInput.type = 'text';
        compareInput.value = rule.compareValue || '';
        compareInput.placeholder = 'Value';
        compareInput.addEventListener('input', () => {
          rule.compareValue = compareInput.value;
          syncHidden();
        });
        const compareRow = makeField('Compare value', compareInput);
        compareRow.dataset.role = 'compare';
        card.appendChild(compareRow);

        const outputsWrap = document.createElement('div');
        outputsWrap.className = 'logic-output-grid';

        const trueOutput = document.createElement('input');
        trueOutput.type = 'text';
        trueOutput.value = rule.outputTrue || '';
        trueOutput.placeholder = 'true port';
        trueOutput.addEventListener('input', () => {
          rule.outputTrue = trueOutput.value;
          syncHidden();
        });
        outputsWrap.appendChild(makeField('True output', trueOutput));

        const trueMode = buildSelect([
          { value: 'message', label: 'Whole message' },
          { value: 'value', label: 'Extracted value' },
          { value: 'boolean', label: 'Boolean' }
        ], rule.trueMode || 'message');
        trueMode.addEventListener('change', () => {
          rule.trueMode = trueMode.value;
          syncHidden();
        });
        outputsWrap.appendChild(makeField('When true send', trueMode));

        const falseOutput = document.createElement('input');
        falseOutput.type = 'text';
        falseOutput.value = rule.outputFalse || '';
        falseOutput.placeholder = 'false port';
        falseOutput.addEventListener('input', () => {
          rule.outputFalse = falseOutput.value;
          syncHidden();
        });
        outputsWrap.appendChild(makeField('False output', falseOutput));

        const falseMode = buildSelect([
          { value: 'message', label: 'Whole message' },
          { value: 'value', label: 'Extracted value' },
          { value: 'boolean', label: 'Boolean' }
        ], rule.falseMode || 'message');
        falseMode.addEventListener('change', () => {
          rule.falseMode = falseMode.value;
          syncHidden();
        });
        outputsWrap.appendChild(makeField('When false send', falseMode));

        card.appendChild(outputsWrap);

        const description = document.createElement('textarea');
        description.rows = 2;
        description.placeholder = 'Optional notes';
        description.value = rule.description || '';
        description.addEventListener('input', () => {
          rule.description = description.value;
          syncHidden();
        });
        card.appendChild(makeField('Notes', description));

        const toggleCompare = () => {
          const needs = LOGIC_OPERATORS_WITH_VALUE.has(opSelect.value);
          compareRow.classList.toggle('hidden', !needs);
        };
        opSelect.addEventListener('change', () => {
          rule.operator = opSelect.value;
          toggleCompare();
          syncHidden();
        });
        toggleCompare();

        list.appendChild(card);
      });
      syncHidden();
    };

    addBtn.addEventListener('click', (e) => {
      e.preventDefault();
      const idx = rules.length + 1;
      rules.push({
        id: logicRuleId(),
        label: `Rule ${idx}`,
        input: `input${idx}`,
        path: '',
        operator: 'truthy',
        compareValue: '',
        outputTrue: undefined,
        outputFalse: undefined,
        trueMode: 'message',
        falseMode: 'message',
        description: ''
      });
      render();
    });

    render();
    return wrap;
  }

  function createFieldMapEditor(field, cfg) {
    const wrap = document.createElement('div');
    wrap.className = 'field-map-editor';

    const hidden = document.createElement('input');
    hidden.type = 'hidden';
    hidden.name = field.key;
    wrap.appendChild(hidden);

    const list = document.createElement('div');
    list.className = 'field-map-list';
    wrap.appendChild(list);

    const controlsRow = document.createElement('div');
    controlsRow.className = 'field-map-controls';
    wrap.appendChild(controlsRow);

    const addBtn = document.createElement('button');
    addBtn.type = 'button';
    addBtn.className = 'ghost field-map-add';
    addBtn.textContent = 'Add Field';
    controlsRow.appendChild(addBtn);

    if (field.note) {
      const note = document.createElement('div');
      note.className = 'field-map-note tiny muted';
      note.textContent = field.note;
      wrap.appendChild(note);
    }

    const MODE_CONFIG = [
      { value: 'literal', label: 'Literal value', requiresValue: true, control: 'input', placeholder: 'value' },
      { value: 'text', label: 'Current text', requiresValue: false },
      { value: 'incoming', label: 'Incoming text', requiresValue: false },
      { value: 'raw', label: 'Incoming payload', requiresValue: false },
      { value: 'action', label: 'Action value', requiresValue: false },
      { value: 'json', label: 'JSON object', requiresValue: true, control: 'textarea', placeholder: '{"key":"value"}' },
      { value: 'number', label: 'Number', requiresValue: true, control: 'input', inputType: 'number', placeholder: '0' },
      { value: 'boolean', label: 'Boolean', requiresValue: true, control: 'select', options: ['true', 'false'] },
      { value: 'nodeId', label: 'Node ID', requiresValue: false },
      { value: 'timestamp', label: 'Timestamp', requiresValue: true, control: 'select', options: ['iso', 'ms'] },
      { value: 'template', label: 'Template', requiresValue: true, control: 'textarea', placeholder: 'Hello {{text}}' },
      { value: 'type', label: 'Type value', requiresValue: false }
    ];

    const modeMap = new Map(MODE_CONFIG.map((m) => [m.value, m]));

    const normalizeEntries = (entries) => {
      if (!Array.isArray(entries)) return [];
      return entries.map((entry) => {
        const normalized = entry && typeof entry === 'object' ? entry : {};
        const mode = modeMap.has(normalized.mode) ? normalized.mode : 'literal';
        return {
          key: typeof normalized.key === 'string' ? normalized.key : '',
          mode,
          value: normalized.value !== undefined && normalized.value !== null ? String(normalized.value) : ''
        };
      });
    };

    let rows = normalizeEntries(cfg?.[field.key] ?? field.def ?? []);

    const updateHidden = () => {
      hidden.value = JSON.stringify(rows);
    };

    const moveEntry = (index, delta) => {
      const nextIndex = index + delta;
      if (nextIndex < 0 || nextIndex >= rows.length) return;
      const [entry] = rows.splice(index, 1);
      rows.splice(nextIndex, 0, entry);
      render();
    };

    const removeEntry = (index) => {
      rows.splice(index, 1);
      render();
    };

    const renderEmpty = () => {
      const empty = document.createElement('div');
      empty.className = 'field-map-empty tiny muted';
      empty.textContent = 'No additional fields configured.';
      list.appendChild(empty);
    };

    const renderRow = (entry, index) => {
      const row = document.createElement('div');
      row.className = 'field-map-row';

      const keyInput = document.createElement('input');
      keyInput.type = 'text';
      keyInput.placeholder = 'key';
      keyInput.value = entry.key;
      keyInput.addEventListener('input', () => {
        rows[index].key = keyInput.value;
        updateHidden();
      });
      row.appendChild(keyInput);

      const modeSelect = document.createElement('select');
      modeSelect.className = 'field-map-mode';
      MODE_CONFIG.forEach((mode) => {
        const opt = document.createElement('option');
        opt.value = mode.value;
        opt.textContent = mode.label;
        modeSelect.appendChild(opt);
      });
      modeSelect.value = entry.mode;
      modeSelect.addEventListener('change', () => {
        rows[index].mode = modeSelect.value;
        const info = modeMap.get(modeSelect.value);
        if (info && !info.requiresValue) rows[index].value = '';
        render();
      });
      row.appendChild(modeSelect);

      const renderValueControl = () => {
        const info = modeMap.get(rows[index].mode) || modeMap.get('literal');
        const container = document.createElement('div');
        container.className = 'field-map-value';
        if (!info.requiresValue) {
          const placeholder = document.createElement('div');
          placeholder.className = 'field-map-value-placeholder tiny muted';
          placeholder.textContent = 'auto';
          container.appendChild(placeholder);
          return container;
        }

        if (info.control === 'textarea') {
          const area = document.createElement('textarea');
          area.placeholder = info.placeholder || '';
          area.value = rows[index].value ?? '';
          area.rows = rows[index].mode === 'json' ? 3 : 2;
          area.addEventListener('input', () => {
            rows[index].value = area.value;
            updateHidden();
          });
          container.appendChild(area);
          return container;
        }

        if (info.control === 'select') {
          const sel = document.createElement('select');
          (info.options || []).forEach((optVal) => {
            const opt = document.createElement('option');
            opt.value = String(optVal);
            opt.textContent = String(optVal).toUpperCase();
            sel.appendChild(opt);
          });
          const current = rows[index].value && info.options.includes(rows[index].value)
            ? rows[index].value
            : info.options[0];
          sel.value = current;
          rows[index].value = current;
          sel.addEventListener('change', () => {
            rows[index].value = sel.value;
            updateHidden();
          });
          container.appendChild(sel);
          return container;
        }

        const input = document.createElement('input');
        input.type = info.inputType || 'text';
        input.placeholder = info.placeholder || '';
        input.value = rows[index].value ?? '';
        input.addEventListener('input', () => {
          rows[index].value = input.value;
          updateHidden();
        });
        container.appendChild(input);
        return container;
      };

      const valueContainer = renderValueControl();
      row.appendChild(valueContainer);

      const actions = document.createElement('div');
      actions.className = 'field-map-actions';

      const upBtn = document.createElement('button');
      upBtn.type = 'button';
      upBtn.className = 'ghost tiny';
      upBtn.textContent = '↑';
      upBtn.disabled = index === 0;
      upBtn.addEventListener('click', () => moveEntry(index, -1));
      actions.appendChild(upBtn);

      const downBtn = document.createElement('button');
      downBtn.type = 'button';
      downBtn.className = 'ghost tiny';
      downBtn.textContent = '↓';
      downBtn.disabled = index === rows.length - 1;
      downBtn.addEventListener('click', () => moveEntry(index, 1));
      actions.appendChild(downBtn);

      const removeBtn = document.createElement('button');
      removeBtn.type = 'button';
      removeBtn.className = 'ghost tiny field-map-remove';
      removeBtn.textContent = '✕';
      removeBtn.addEventListener('click', () => removeEntry(index));
      actions.appendChild(removeBtn);

      row.appendChild(actions);
      list.appendChild(row);
    };

    const render = () => {
      list.innerHTML = '';
      if (!rows.length) {
        renderEmpty();
      } else {
        rows.forEach((entry, idx) => renderRow(entry, idx));
      }
      updateHidden();
    };

    addBtn.addEventListener('click', (e) => {
      e.preventDefault();
      rows.push({ key: '', mode: 'literal', value: '' });
      render();
    });

    render();
    return wrap;
  }

  function createFilterListEditor(field, cfg) {
    const wrap = document.createElement('div');
    wrap.className = 'filter-token-editor';

    const hidden = document.createElement('input');
    hidden.type = 'hidden';
    hidden.name = field.key;
    wrap.appendChild(hidden);

    const inputRow = document.createElement('div');
    inputRow.className = 'filter-token-input-row';
    wrap.appendChild(inputRow);

    const input = document.createElement('input');
    input.type = 'text';
    input.placeholder = field.placeholder || 'Add filter';
    input.autocomplete = 'off';
    input.spellcheck = false;
    inputRow.appendChild(input);

    const addBtn = document.createElement('button');
    addBtn.type = 'button';
    addBtn.className = 'ghost filter-token-add';
    addBtn.textContent = 'Add';
    inputRow.appendChild(addBtn);

    const note = document.createElement('div');
    note.className = 'filter-token-note tiny muted';
    note.textContent = field.note || 'Filters are stripped before synthesis.';
    wrap.appendChild(note);

    const list = document.createElement('div');
    list.className = 'filter-token-list';
    wrap.appendChild(list);

    const normalizeValue = (value) => {
      let text = String(value ?? '');
      try {
        text = text.normalize('NFKC');
      } catch (err) {
        // ignore
      }
      return text.trim();
    };

    const normalizeTokens = (items) => {
      const out = [];
      const seen = new Set();
      if (!Array.isArray(items)) return out;
      items.forEach((value) => {
        const normalized = normalizeValue(value);
        if (!normalized) return;
        if (seen.has(normalized)) return;
        seen.add(normalized);
        out.push(normalized);
      });
      return out;
    };

    let tokens = normalizeTokens(
      Array.isArray(cfg?.[field.key]) ? cfg[field.key] : (Array.isArray(field.def) ? field.def : [])
    );

    const render = () => {
      tokens = normalizeTokens(tokens);
      hidden.value = JSON.stringify(tokens);
      list.innerHTML = '';
      if (!tokens.length) {
        const empty = document.createElement('div');
        empty.className = 'filter-token-empty tiny muted';
        empty.textContent = 'No filters configured.';
        list.appendChild(empty);
        return;
      }
      tokens.forEach((token, index) => {
        const chip = document.createElement('span');
        chip.className = 'filter-token-chip';

        const labelSpan = document.createElement('span');
        labelSpan.className = 'filter-token-label';
        labelSpan.textContent = token;
        chip.appendChild(labelSpan);

        const removeBtn = document.createElement('button');
        removeBtn.type = 'button';
        removeBtn.className = 'filter-token-remove';
        removeBtn.textContent = 'x';
        removeBtn.addEventListener('click', (e) => {
          e.preventDefault();
          tokens = tokens.filter((_, idx) => idx !== index);
          render();
        });
        chip.appendChild(removeBtn);

        list.appendChild(chip);
      });
    };

    const addToken = (value) => {
      const normalized = normalizeValue(value);
      if (!normalized) return;
      if (tokens.includes(normalized)) return;
      tokens = [...tokens, normalized];
      render();
    };

    addBtn.addEventListener('click', (e) => {
      e.preventDefault();
      addToken(input.value);
      input.value = '';
      input.focus();
    });

    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        addToken(input.value);
        input.value = '';
        input.focus();
      }
    });

    render();
    return wrap;
  }

  function getTextInputState(node) {
    if (!node) return null;
    if (!node._textInputState) {
      node._textInputState = {
        lastIncomingPayload: null,
        lastIncomingText: '',
        previewText: '',
        previewStatus: 'draft',
        previewOrigin: 'manual',
        previewCopyValue: ''
      };
    }
    return node._textInputState;
  }

  function textInputBool(value, def = false) {
    if (value === undefined || value === null) return def;
    if (typeof value === 'boolean') return value;
    if (typeof value === 'number') return value !== 0;
    if (typeof value === 'string') {
      const normalized = value.trim().toLowerCase();
      if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
      if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    }
    return def;
  }

  function sanitizeTextInputFields(entries) {
    if (!Array.isArray(entries)) return [];
    return entries.map((entry) => {
      const normalized = entry && typeof entry === 'object' ? entry : {};
      return {
        key: typeof normalized.key === 'string' ? normalized.key : '',
        mode: typeof normalized.mode === 'string' ? normalized.mode : 'literal',
        value: normalized.value !== undefined && normalized.value !== null ? String(normalized.value) : ''
      };
    });
  }

  function normalizeTextInputConfig(rawCfg = {}) {
    const defaults = NodeStore.defaultsByType?.TextInput || {};
    const base = deepClone(defaults);
    const merged = { ...base, ...deepClone(rawCfg) };
    merged.includeNodeId = textInputBool(merged.includeNodeId, true);
    merged.includeText = textInputBool(merged.includeText, true);
    merged.autoSendIncoming = textInputBool(merged.autoSendIncoming, true);
    merged.customFields = sanitizeTextInputFields(merged.customFields);
    merged.emitActionKey = typeof merged.emitActionKey === 'string' ? merged.emitActionKey : '(none)';
    merged.actionValue = typeof merged.actionValue === 'string' ? merged.actionValue : 'type';
    merged.outputMode = typeof merged.outputMode === 'string' ? merged.outputMode : 'object';
    merged.previewMode = merged.previewMode === 'compact' ? 'compact' : 'pretty';
    merged.nodeIdKey = typeof merged.nodeIdKey === 'string' ? merged.nodeIdKey : 'nodeId';
    merged.typeKey = typeof merged.typeKey === 'string' ? merged.typeKey : 'type';
    merged.typeValue = merged.typeValue !== undefined && merged.typeValue !== null ? merged.typeValue : 'text';
    merged.typeBackupKey = typeof merged.typeBackupKey === 'string' ? merged.typeBackupKey : 'messageType';
    merged.textKey = typeof merged.textKey === 'string' ? merged.textKey : 'text';
    merged.incomingMode = ['replace', 'append', 'ignore'].includes(merged.incomingMode) ? merged.incomingMode : 'replace';
    return merged;
  }

  function formatTextInputPreview(payload, cfg) {
    if (payload === null || payload === undefined) return '(empty)';
    if (typeof payload === 'string') return payload || '(empty)';
    try {
      if (cfg.previewMode === 'compact') return JSON.stringify(payload);
      return JSON.stringify(payload, null, 2);
    } catch (err) {
      try {
        return String(payload);
      } catch (_) {
        return '(unserializable)';
      }
    }
  }

  function textInputCopyValue(payload, cfg) {
    if (payload === null || payload === undefined) return '';
    if (typeof payload === 'string') return payload;
    try {
      if (cfg.previewMode === 'compact') return JSON.stringify(payload);
      return JSON.stringify(payload, null, 2);
    } catch (_) {
      try {
        return String(payload);
      } catch (err) {
        return '';
      }
    }
  }

  function evaluateTextInputField(entry, context) {
    const mode = entry.mode || 'literal';
    const value = entry.value ?? '';
    switch (mode) {
      case 'literal':
        return value;
      case 'text':
        return context.text;
      case 'incoming':
        return context.incomingText;
      case 'raw':
        return context.incomingPayload !== undefined ? context.incomingPayload : context.incomingText;
      case 'action':
        if (!context.actionKey || context.actionKey === '(none)') return undefined;
        return context.actionValue;
      case 'json':
        try {
          return JSON.parse(value || '{}');
        } catch (err) {
          return undefined;
        }
      case 'number': {
        const num = Number(value);
        return Number.isFinite(num) ? num : undefined;
      }
      case 'boolean': {
        const normalized = String(value).trim().toLowerCase();
        if (['true', '1', 'yes', 'on'].includes(normalized)) return true;
        if (['false', '0', 'no', 'off'].includes(normalized)) return false;
        return undefined;
      }
      case 'nodeId':
        return context.nodeId;
      case 'timestamp':
        return value === 'ms' ? context.timestampMs : context.timestampIso;
      case 'template': {
        const template = value || '';
        return template.replace(/\{\{\s*(\w+)\s*\}\}/g, (_, token) => {
          if (token === 'text') return context.text;
          if (token === 'incoming') return context.incomingText;
          if (token === 'action') return context.actionValue;
          if (token === 'nodeId') return context.nodeId;
          if (token === 'type') return context.typeValue;
          if (token === 'timestamp') return context.timestampIso;
          if (token === 'ms') return String(context.timestampMs);
          return '';
        });
      }
      case 'type':
        return context.typeValue;
      default:
        return value;
    }
  }

  function composeTextInputPayload(node, cfg, state, {
    text = '',
    incomingPayload,
    incomingText,
    origin = 'manual',
    forPreview = false
  } = {}) {
    if (!node) return null;
    const nodeId = node.id;
    const normalized = normalizeTextInputConfig(cfg);
    const trimmedText = typeof text === 'string' ? text : '';
    const resolvedIncomingPayload = incomingPayload !== undefined ? incomingPayload : state?.lastIncomingPayload;
    const resolvedIncomingText = incomingText !== undefined
      ? incomingText
      : (state?.lastIncomingText ?? '');
    const actionKey = (normalized.emitActionKey || '(none)').trim();
    const actionValue = (normalized.actionValue || '').trim() || 'type';
    const timestamp = Date.now();
    const context = {
      nodeId,
      text: trimmedText,
      incomingText: resolvedIncomingText || '',
      incomingPayload: resolvedIncomingPayload,
      actionKey,
      actionValue,
      typeValue: normalized.typeValue,
      timestampMs: timestamp,
      timestampIso: new Date(timestamp).toISOString()
    };

    let payload;
    if (normalized.outputMode === 'text') {
      if (!trimmedText && !forPreview) return null;
      payload = trimmedText;
    } else {
      const obj = {};
      let typeKeyUsed = '';
      if (normalized.includeNodeId) {
        const nodeKeyRaw = typeof normalized.nodeIdKey === 'string' ? normalized.nodeIdKey.trim() : 'nodeId';
        if (nodeKeyRaw) obj[nodeKeyRaw] = nodeId;
      }
      const typeKeyRaw = typeof normalized.typeKey === 'string' ? normalized.typeKey.trim() : 'type';
      if (typeKeyRaw) {
        typeKeyUsed = typeKeyRaw;
        obj[typeKeyRaw] = normalized.typeValue;
      }
      const textKeyRaw = typeof normalized.textKey === 'string' ? normalized.textKey.trim() : 'text';
      if (normalized.includeText && textKeyRaw) {
        obj[textKeyRaw] = trimmedText;
      }
      const fields = sanitizeTextInputFields(normalized.customFields);
      fields.forEach((entry) => {
        const key = (entry.key || '').trim();
        if (!key) return;
        const value = evaluateTextInputField(entry, context);
        if (value === undefined) return;
        obj[key] = value;
      });
      if (actionKey && actionKey !== '(none)') {
        obj[actionKey] = actionValue;
        const backupKeyRaw = typeof normalized.typeBackupKey === 'string' ? normalized.typeBackupKey.trim() : 'messageType';
        if (typeKeyUsed && actionKey === typeKeyUsed && backupKeyRaw && !obj[backupKeyRaw]) {
          obj[backupKeyRaw] = normalized.typeValue;
        }
      }
      payload = obj;
    }

    const preview = formatTextInputPreview(payload, normalized);
    const copyValue = textInputCopyValue(payload, normalized);
    return {
      payload,
      preview,
      copyValue,
      actionKey,
      actionValue,
      typeValue: normalized.typeValue,
      text: trimmedText,
      origin,
      incomingText: resolvedIncomingText
    };
  }

  function setTextInputPreview(node, previewText, { status = 'draft', origin = 'manual', copyValue } = {}) {
    const state = getTextInputState(node);
    if (!state) return;
    state.previewText = previewText || '';
    state.previewStatus = status;
    state.previewOrigin = origin;
    if (copyValue !== undefined) state.previewCopyValue = copyValue;
    else state.previewCopyValue = previewText || '';
    const labelEl = state.previewLabel;
    const bodyEl = state.previewBody;
    const copyBtn = state.previewCopy;
    if (labelEl) {
      if (status === 'emitted') {
        labelEl.textContent = origin === 'incoming' ? 'Last emitted payload (incoming)' : 'Last emitted payload';
      } else {
        labelEl.textContent = origin === 'incoming' ? 'Incoming preview' : 'Draft preview';
      }
    }
    if (bodyEl) {
      bodyEl.textContent = (previewText && previewText.length) ? previewText : '(empty)';
      bodyEl.dataset.status = status;
      bodyEl.dataset.origin = origin;
    }
    if (copyBtn) {
      const copyText = state.previewCopyValue;
      copyBtn.disabled = !(copyText && String(copyText).length);
    }
  }

  function refreshTextInputDraft(node) {
    if (!node) return;
    const state = getTextInputState(node);
    const cfg = NodeStore.ensure(node.id, 'TextInput').config || {};
    const normalized = normalizeTextInputConfig(cfg);
    const textarea = state?.textarea;
    const draftText = textarea ? textarea.value.trim() : (cfg.text || '');
    const result = composeTextInputPayload(node, normalized, state, {
      text: draftText,
      origin: 'manual',
      forPreview: true
    });
    if (result) setTextInputPreview(node, result.preview, { status: 'draft', origin: 'manual', copyValue: result.copyValue });
    else setTextInputPreview(node, '', { status: 'draft', origin: 'manual', copyValue: '' });
  }

  function handleTextInputIngress(nodeId, portName, payload) {
    if (portName !== 'incoming' && portName !== 'text') return;
    const node = WS.nodes.get(nodeId);
    if (!node) return;
    const state = getTextInputState(node);
    state.lastIncomingPayload = payload;
    const incomingText = extractPayloadText(payload);
    state.lastIncomingText = incomingText;
    const cfgObj = NodeStore.ensure(nodeId, 'TextInput').config || {};
    const normalized = normalizeTextInputConfig(cfgObj);
    const textarea = state.textarea;
    const currentText = textarea ? textarea.value : (cfgObj.text || '');
    let nextText = currentText;
    if (normalized.incomingMode === 'replace') {
      nextText = incomingText;
    } else if (normalized.incomingMode === 'append') {
      const separator = currentText && !currentText.endsWith('\n') ? '\n' : '';
      nextText = currentText ? `${currentText}${separator}${incomingText}` : incomingText;
    }

    if (normalized.incomingMode !== 'ignore') {
      NodeStore.update(nodeId, { type: 'TextInput', text: nextText, lastIncomingText: incomingText });
      if (state.setValue) state.setValue(nextText);
    } else {
      NodeStore.update(nodeId, { type: 'TextInput', lastIncomingText: incomingText });
    }

    const result = composeTextInputPayload(node, normalized, state, {
      text: nextText.trim(),
      incomingPayload: payload,
      incomingText,
      origin: 'incoming'
    });
    if (!result) {
      setTextInputPreview(node, '', { status: 'draft', origin: 'incoming', copyValue: '' });
      return;
    }

    if (normalized.autoSendIncoming) {
      Router.sendFrom(nodeId, 'text', result.payload);
      NodeStore.update(nodeId, {
        type: 'TextInput',
        lastSent: nextText.trim(),
        lastPreview: result.preview,
        lastPreviewCopy: result.copyValue,
        lastPreviewOrigin: 'incoming'
      });
      setTextInputPreview(node, result.preview, { status: 'emitted', origin: 'incoming', copyValue: result.copyValue });
    } else {
      setTextInputPreview(node, result.preview, { status: 'draft', origin: 'incoming', copyValue: result.copyValue });
    }
  }

  function initTextInputNode(node) {
    const textarea = node.el?.querySelector('[data-textinput-field]');
    const sendBtn = node.el?.querySelector('[data-textinput-send]');
    const previewLabel = node.el?.querySelector('[data-textinput-preview-label]');
    const previewBody = node.el?.querySelector('[data-textinput-preview-body]');
    const previewCopy = node.el?.querySelector('[data-textinput-preview-copy]');
    const cfgRaw = NodeStore.ensure(node.id, 'TextInput').config || {};
    const cfg = normalizeTextInputConfig(cfgRaw);
    const state = getTextInputState(node);

    if (state) {
      state.textarea = textarea || null;
      state.previewLabel = previewLabel || null;
      state.previewBody = previewBody || null;
      state.previewCopy = previewCopy || null;
    }

    if (previewCopy && !previewCopy._textInputBound) {
      previewCopy.addEventListener('click', async (e) => {
        e.preventDefault();
        const currentState = getTextInputState(node);
        const copyText = currentState?.previewCopyValue ?? currentState?.previewText ?? '';
        if (!copyText) {
          setBadge('Preview empty', false);
          return;
        }
        try {
          await navigator.clipboard.writeText(copyText);
          setBadge('Preview copied');
        } catch (err) {
          setBadge(`Copy failed: ${err?.message || err}`, false);
        }
      });
      previewCopy._textInputBound = true;
    }

    if (textarea) {
      textarea.placeholder = cfg.placeholder || 'Type a message…';
      if (!node._textInputReady) {
        let suppressInput = false;
        const getLatestConfig = () => {
          const latest = NodeStore.ensure(node.id, 'TextInput').config || {};
          return normalizeTextInputConfig(latest);
        };
        const applyDraft = () => {
          if (suppressInput) return;
          NodeStore.update(node.id, { type: 'TextInput', text: textarea.value });
          refreshTextInputDraft(node);
        };
        state.setValue = (value, { silent = true } = {}) => {
          suppressInput = true;
          textarea.value = value ?? '';
          suppressInput = false;
          if (!silent) {
            NodeStore.update(node.id, { type: 'TextInput', text: textarea.value });
          }
          refreshTextInputDraft(node);
        };
        const send = () => {
          const currentCfg = getLatestConfig();
          const current = textarea.value || '';
          const trimmed = current.trim();
          const result = composeTextInputPayload(node, currentCfg, state, {
            text: trimmed,
            origin: 'manual'
          });
          if (!result) {
            setTextInputPreview(node, '', { status: 'draft', origin: 'manual' });
            return;
          }
          Router.sendFrom(node.id, 'text', result.payload);
          suppressInput = true;
          textarea.value = '';
          suppressInput = false;
          NodeStore.update(node.id, {
            type: 'TextInput',
            text: '',
            lastSent: trimmed,
            lastPreview: result.preview,
            lastPreviewCopy: result.copyValue,
            lastPreviewOrigin: 'manual'
          });
          setTextInputPreview(node, result.preview, { status: 'emitted', origin: 'manual', copyValue: result.copyValue });
          state.previewText = result.preview;
        };
        sendBtn?.addEventListener('click', send);
        textarea.addEventListener('keydown', (e) => {
          if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
            e.preventDefault();
            send();
          }
        });
        textarea.addEventListener('input', applyDraft);
        node._textInputReady = true;
      } else if (!state.setValue) {
        state.setValue = (value, { silent = true } = {}) => {
          textarea.value = value ?? '';
          if (!silent) {
            NodeStore.update(node.id, { type: 'TextInput', text: textarea.value });
          }
          refreshTextInputDraft(node);
        };
      }
      if (typeof cfg.text === 'string') {
        state?.setValue?.(cfg.text, { silent: true });
      }
    }

    if (cfg.lastPreview) {
      setTextInputPreview(node, cfg.lastPreview, {
        status: 'emitted',
        origin: cfg.lastPreviewOrigin || 'manual',
        copyValue: cfg.lastPreviewCopy !== undefined ? cfg.lastPreviewCopy : cfg.lastPreview
      });
    } else {
      refreshTextInputDraft(node);
    }
  }

  function updateTextDisplayContent(nodeId, text) {
    const node = WS.nodes.get(nodeId);
    const normalized = text != null ? String(text) : '';
    NodeStore.update(nodeId, { type: 'TextDisplay', text: normalized });
    const contentEl = node?.el?.querySelector('[data-textdisplay-content]');
    if (contentEl) {
      contentEl.textContent = normalized;
      contentEl.dataset.empty = normalized ? 'false' : 'true';
    }
  }

  function initTextDisplayNode(node) {
    const contentEl = node.el?.querySelector('[data-textdisplay-content]');
    const copyBtn = node.el?.querySelector('[data-textdisplay-copy]');
    const cfg = NodeStore.ensure(node.id, 'TextDisplay').config || {};
    if (contentEl) {
      contentEl.textContent = cfg.text || '';
      contentEl.dataset.empty = cfg.text ? 'false' : 'true';
    }
    if (copyBtn && !copyBtn._textDisplayBound) {
      copyBtn.addEventListener('click', async () => {
        try {
          const current = NodeStore.ensure(node.id, 'TextDisplay').config?.text || '';
          if (!current) {
            setBadge('Nothing to copy', false);
            return;
          }
          const hasClipboard = typeof navigator !== 'undefined' && navigator?.clipboard?.writeText;
          if (hasClipboard) {
            await navigator.clipboard.writeText(current);
            setBadge('Text copied');
          } else if (typeof document !== 'undefined' && document?.body) {
            const textarea = document.createElement('textarea');
            textarea.value = current;
            textarea.style.position = 'fixed';
            textarea.style.opacity = '0';
            document.body.appendChild(textarea);
            textarea.focus();
            textarea.select();
            document.execCommand('copy');
            document.body.removeChild(textarea);
            setBadge('Text copied');
          } else {
            setBadge('Copy not supported here', false);
          }
        } catch (err) {
          setBadge(`Copy failed: ${err?.message || err}`, false);
        }
      });
      copyBtn._textDisplayBound = true;
    }
  }

  function handleTextDisplayInput(nodeId, payload) {
    const text = extractPayloadText(payload);
    updateTextDisplayContent(nodeId, text);
  }

  function extractSystemTextFromNode(node) {
    if (!node) return '';
    const type = node.type;
    if (type === 'TextInput') {
      const cfg = NodeStore.ensure(node.id, 'TextInput').config || {};
      const last = typeof cfg.lastSent === 'string' ? cfg.lastSent : '';
      if (last && last.trim()) return last;
      return '';
    }
    if (type === 'Template') {
      const cfg = NodeStore.ensure(node.id, 'Template').config || {};
      return renderTemplateString(cfg.template, cfg.variables) || '';
    }
    if (type === 'TextDisplay') {
      const cfg = NodeStore.ensure(node.id, 'TextDisplay').config || {};
      return typeof cfg.text === 'string' ? cfg.text : '';
    }
    if (type === 'NknDM') {
      const cfg = NodeStore.ensure(node.id, 'NknDM').config || {};
      if (typeof cfg.lastPayload === 'string') return cfg.lastPayload;
      if (cfg.peer?.address) return cfg.peer.address;
      return '';
    }
    if (type === 'MCP') {
      const cfg = NodeStore.ensure(node.id, 'MCP').config || {};
      if (typeof cfg.lastSystem === 'string' && cfg.lastSystem.trim()) return cfg.lastSystem;
      if (typeof cfg.lastContext === 'string' && cfg.lastContext.trim()) return cfg.lastContext;
      return typeof cfg.lastPrompt === 'string' ? cfg.lastPrompt : '';
    }
    const cfg = NodeStore.ensure(node.id, type).config || {};
    if (typeof cfg.text === 'string') return cfg.text;
    return '';
  }

  function pullLlmSystemInput(nodeId) {
    const llmNode = WS.nodes.get(nodeId);
    const current = NodeStore.ensure(nodeId, 'LLM').config || {};
    if (!llmNode) return current;
    const wires = WS.wires.filter((w) => w.to.node === nodeId && w.to.port === 'system');
    if (!wires.length) return current;
    let chosen = '';
    for (const wire of wires) {
      const sourceNode = WS.nodes.get(wire.from.node);
      const candidate = extractSystemTextFromNode(sourceNode);
      if (!chosen) chosen = candidate || '';
      if (candidate && candidate.trim()) {
        chosen = candidate;
        break;
      }
    }
    const text = typeof chosen === 'string' ? chosen : String(chosen || '');
    const trimmed = text.trim();
    if (!trimmed) return current;
    if (trimmed === current.system && current.useSystem) return current;
    const patch = { type: 'LLM', system: trimmed, useSystem: true };
    return NodeStore.update(nodeId, patch);
  }

  function extractImagePayloadFromNode(node) {
    if (!node) return [];
    const cfg = NodeStore.ensure(node.id, node.type).config || {};
    if (node.type === 'ImageInput') {
      const src = cfg.b64 || cfg.image || '';
      return src ? [src] : [];
    }
    if (node.type === 'MediaStream') {
      const last = cfg.lastFrame || cfg.lastImage || '';
      return last ? [last] : [];
    }
    if (cfg && typeof cfg === 'object') {
      if (cfg.image) return [cfg.image];
      if (cfg.images) return Array.isArray(cfg.images) ? cfg.images : [cfg.images];
      if (cfg.b64) return [cfg.b64];
    }
    return [];
  }

  function pullLlmImageInput(nodeId) {
    const llmNode = WS.nodes.get(nodeId);
    if (!llmNode) return [];
    const wires = WS.wires.filter((w) => w.to.node === nodeId && w.to.port === 'image');
    if (!wires.length) return [];
    for (const wire of wires) {
      const sourceNode = WS.nodes.get(wire.from.node);
      const images = extractImagePayloadFromNode(sourceNode);
      if (Array.isArray(images) && images.length) return images;
    }
    return [];
  }

  function initNknDmNode(node) {
    const copyBtn = node.el?.querySelector('[data-nkndm-copy]');
    const approveBtn = node.el?.querySelector('[data-nkndm-approve]');
    const revokeBtn = node.el?.querySelector('[data-nkndm-revoke]');
    const autoBtn = node.el?.querySelector('[data-nkndm-autochunk]');

    const syncAutoChunkUi = () => {
      const cfg = NodeStore.ensure(node.id, 'NknDM').config || {};
      const on = !!cfg.autoChunk;
      if (autoBtn) {
        autoBtn.textContent = on ? 'Auto chunk: on' : 'Auto chunk: off';
        autoBtn.classList.toggle('active', on);
        autoBtn.setAttribute('aria-pressed', on ? 'true' : 'false');
      }
      const note = node.el?.querySelector('[data-nkndm-chunk-note]');
      if (note) note.textContent = on ? 'Chunk Size (auto)' : 'Chunk Size';
    };

    if (copyBtn && !copyBtn._nkndmCopyBound) {
      copyBtn.addEventListener('click', async () => {
        const localEl = node.el?.querySelector('[data-nkndm-local]');
        const text = localEl?.textContent?.trim();
        if (!text) {
          setBadge('Nothing to copy', false);
          return;
        }
        try {
          await navigator.clipboard.writeText(text);
          setBadge('Local NKN address copied');
        } catch (err) {
          setBadge('Clipboard unavailable', false);
        }
      });
      copyBtn._nkndmCopyBound = true;
    }
    if (approveBtn && !approveBtn._nkndmApproveBound) {
      approveBtn.addEventListener('click', () => {
        NknDM.approvePeer(node.id);
      });
      approveBtn._nkndmApproveBound = true;
    }
    if (revokeBtn && !revokeBtn._nkndmRevokeBound) {
      revokeBtn.addEventListener('click', () => {
        NknDM.revokePeer(node.id);
      });
      revokeBtn._nkndmRevokeBound = true;
    }
    if (autoBtn && !autoBtn._nkndmAutoBound) {
      autoBtn.addEventListener('click', () => {
        const cfg = NodeStore.ensure(node.id, 'NknDM').config || {};
        const next = !cfg.autoChunk;
        NodeStore.update(node.id, { type: 'NknDM', autoChunk: next });
        syncAutoChunkUi();
        setBadge(next ? 'Auto chunk enabled' : 'Auto chunk disabled');
      });
      autoBtn._nkndmAutoBound = true;
    }
    syncAutoChunkUi();
    NknDM.init(node.id);
  }

  function initNoClipNode(node) {
    if (!node || node.type !== 'NoClipBridge') return;
    NoClip?.attachNodeElement?.(node.id, node.el);
    const identityEl = node.el?.querySelector('[data-noclip-self]');
    const roomEl = node.el?.querySelector('[data-noclip-room]');
    const updateIdentity = () => {
      if (!identityEl) return;
      try {
        const identity = NoClipBridgeSync?.getHydraIdentity?.() || {};
        const text = identity.addr || (identity.pub ? `hydra.${identity.pub}` : '—');
        identityEl.textContent = text;
      } catch (_) {
        identityEl.textContent = '—';
      }
    };
    const updateRoomDisplay = () => {
      if (!roomEl) return;
      try {
        const record = NodeStore.ensure(node.id, 'NoClipBridge');
        const raw = record?.config?.room ?? '';
        const resolved = NoClip?.resolveRoomName?.(raw) || raw || '—';
        roomEl.textContent = resolved;
      } catch (_) {
        roomEl.textContent = '—';
      }
    };
    updateIdentity();
    updateRoomDisplay();
    NoClip?.refreshPeerDropdown?.(node.id, { silent: true });

    NoClip?.refresh?.(node.id);
    if (!node._noclipInit) {
      NoClip?.init?.(node.id);
      node._noclipInit = true;
    }

    const syncListEl = node.el?.querySelector('[data-noclip-sync-list]');
    const renderPendingRequests = () => {
      updateIdentity();
      updateRoomDisplay();
      if (!syncListEl) return;
      const requests = NoClipBridgeSync?.listPendingRequests?.() || [];
      syncListEl.innerHTML = '';
      if (!requests.length) {
        const empty = document.createElement('div');
        empty.dataset.empty = 'true';
        empty.textContent = 'None pending';
        syncListEl.appendChild(empty);
        return;
      }
      const formatDelta = (ts) => {
        if (!Number.isFinite(ts)) return '';
        const diff = Math.max(0, Date.now() - ts);
        const sec = Math.floor(diff / 1000);
        if (sec < 60) return `${sec}s ago`;
        const min = Math.floor(sec / 60);
        if (min < 60) return `${min}m ago`;
        const hr = Math.floor(min / 60);
        if (hr < 24) return `${hr}h ago`;
        const day = Math.floor(hr / 24);
        return `${day}d ago`;
      };
      requests.forEach((request) => {
        const item = document.createElement('div');
        item.className = 'noclip-sync-request';
        item.style.marginBottom = '8px';
        const title = document.createElement('div');
        title.innerHTML = `<strong>${request.objectLabel || request.objectId || 'Smart Object'}</strong> • ${request.noclipAddr || request.key}`;
        const meta = document.createElement('div');
        meta.style.fontSize = '11px';
        meta.style.color = 'var(--muted)';
        const when = formatDelta(request.receivedAt);
        const position = request.position && Number.isFinite(request.position.lat) && Number.isFinite(request.position.lon)
          ? `${request.position.lat.toFixed(4)}, ${request.position.lon.toFixed(4)}`
          : '';
        const bits = [];
        if (position) bits.push(position);
        if (when) bits.push(when);
        meta.textContent = bits.join(' • ');

        const actions = document.createElement('div');
        actions.style.display = 'flex';
        actions.style.gap = '6px';
        actions.style.marginTop = '4px';

        const approveBtn = document.createElement('button');
        approveBtn.type = 'button';
        approveBtn.textContent = 'Approve';
        approveBtn.className = 'btn btn-xs';

        const rejectBtn = document.createElement('button');
        rejectBtn.type = 'button';
        rejectBtn.textContent = 'Reject';
        rejectBtn.className = 'btn btn-xs btn-secondary';

        const setBusy = (busy) => {
          approveBtn.disabled = busy;
          rejectBtn.disabled = busy;
          approveBtn.dataset.busy = busy ? 'true' : 'false';
          rejectBtn.dataset.busy = busy ? 'true' : 'false';
        };

        approveBtn.addEventListener('click', async () => {
          if (!request.key) return;
          setBusy(true);
          try {
            await NoClipBridgeSync?.approveSyncRequest?.(request.key);
            setBadge('Sync approved');
          } catch (err) {
            console.error('[Graph][NoClip] approve failed', err);
            setBadge(`Approve failed: ${err?.message || err}`, false);
          } finally {
            setBusy(false);
            renderPendingRequests();
          }
        });

        rejectBtn.addEventListener('click', async () => {
          if (!request.key) return;
          setBusy(true);
          try {
            await NoClipBridgeSync?.rejectSyncRequest?.(request.key, 'Rejected via graph');
            setBadge('Sync rejected');
          } catch (err) {
            console.error('[Graph][NoClip] reject failed', err);
            setBadge(`Reject failed: ${err?.message || err}`, false);
          } finally {
            setBusy(false);
            renderPendingRequests();
          }
        });

        actions.appendChild(approveBtn);
        actions.appendChild(rejectBtn);

        item.appendChild(title);
        item.appendChild(meta);
        item.appendChild(actions);
        syncListEl.appendChild(item);
      });
    };

    if (node._noclipSyncUnsub) {
      try { node._noclipSyncUnsub(); } catch (_) { /* ignore */ }
      node._noclipSyncUnsub = null;
    }
    if (NoClipBridgeSync?.subscribe) {
      node._noclipSyncUnsub = NoClipBridgeSync.subscribe(() => {
        renderPendingRequests();
      });
    }
    renderPendingRequests();
    NoClip?.refreshSessionStatus?.(node.id);

    // Wire up peer dropdown
    const selectEl = node.el?.querySelector('[data-noclip-peer-select]');
    const syncBtn = node.el?.querySelector('[data-noclip-peer-sync]');
    const updateSyncButtonState = () => {
      if (!syncBtn) return;
      const record = NodeStore.ensure(node.id, 'NoClipBridge');
      const current = (selectEl?.value || record?.config?.targetPub || '').trim();
      syncBtn.disabled = !current;
    };
    if (selectEl && !selectEl._noclipBound) {
      selectEl.addEventListener('change', (e) => {
        const selectedPub = (e.target.value || '').trim();
        NoClip?.setTargetPeer?.(node.id, selectedPub);
        if (selectedPub) {
          NoClip?.refresh?.(node.id);
          NoClip?.logToNode?.(node.id, `✓ Selected peer: noclip.${selectedPub.slice(0, 8)}...`, 'success');
        } else {
          NoClip?.logToNode?.(node.id, '⟲ Cleared target peer selection', 'info');
        }
        updateSyncButtonState();
      });
      selectEl._noclipBound = true;
    }

    // Wire up refresh button
    const refreshBtn = node.el?.querySelector('[data-noclip-peer-refresh]');
    if (refreshBtn && !refreshBtn._noclipBound) {
      refreshBtn.addEventListener('click', () => {
        NoClip?.refreshPeerDropdown?.(node.id);
        updateSyncButtonState();
      });
      refreshBtn._noclipBound = true;
    }

    if (syncBtn && !syncBtn._noclipBound) {
      syncBtn.addEventListener('click', async () => {
        const record = NodeStore.ensure(node.id, 'NoClipBridge');
        const selectedPub = (selectEl?.value || record?.config?.targetPub || '').trim();
        if (!selectedPub) {
          setBadge?.('Select a NoClip peer first', false);
          updateSyncButtonState();
          return;
        }
        syncBtn.disabled = true;
        syncBtn.dataset.busy = 'true';
        try {
          await NoClip?.requestSync?.(node.id, selectedPub);
        } catch (err) {
          console.error('[Graph][NoClip] sync request failed', err);
          setBadge?.(`Sync request failed: ${err?.message || err}`, false);
        } finally {
          delete syncBtn.dataset.busy;
          updateSyncButtonState();
        }
      });
      syncBtn._noclipBound = true;
    }

    // Initial peer list population
    if (NoClip?.refreshPeerDropdown) {
      setTimeout(() => NoClip.refreshPeerDropdown(node.id, { silent: true }), 500);
    }
    updateSyncButtonState();
  }

  function openSettings(nodeId) {
    const node = WS.nodes.get(nodeId);
    if (!node) return;
    if (typeof settingsModelSubscription === 'function') {
      try { settingsModelSubscription(); } catch (err) { /* ignore */ }
      settingsModelSubscription = null;
    }
    const rec = NodeStore.ensure(nodeId, node.type);
    const cfg = rec.config || {};
    const modal = qs('#settingsModal');
    const fields = qs('#settingsFields');
    const help = qs('#settingsHelp');
    const form = qs('#settingsForm');
    const isLlm = node.type === 'LLM';
    const isWasmType = WASM_TYPES.has(node.type);
    const wasmMode = isWasmNode(node.type, cfg);
    let memoryManagerSetup = null;
    fields.innerHTML = '';
    const schema = GraphTypes[node.type].schema || [];

    if (isLlm) {
      memoryManagerSetup = () => {
        if (fields.querySelector('.llm-memory-section')) return;

        const memoryLabel = document.createElement('label');
        memoryLabel.textContent = 'Chat history';
        fields.appendChild(memoryLabel);

        const memorySection = document.createElement('div');
        memorySection.className = 'llm-memory-section llm-memory-hidden';

        const memoryDetails = document.createElement('details');
        memoryDetails.className = 'llm-memory-details';
        const memorySummary = document.createElement('summary');
        memorySummary.className = 'llm-memory-summary';
        memorySummary.textContent = 'Chat History (0)';
        memoryDetails.appendChild(memorySummary);

        const memoryNote = document.createElement('div');
        memoryNote.className = 'llm-memory-note';
        memoryNote.textContent = 'Drag entries to reorder. Edit or remove items to curate what persists between turns.';
        memoryDetails.appendChild(memoryNote);

        const memoryList = document.createElement('ul');
        memoryList.className = 'llm-memory-list';
        memoryDetails.appendChild(memoryList);

        const memoryEmpty = document.createElement('div');
        memoryEmpty.className = 'llm-memory-empty';
        memoryEmpty.textContent = 'No chat history yet.';
        memoryDetails.appendChild(memoryEmpty);

        memorySection.appendChild(memoryDetails);

        const memoryFooter = document.createElement('div');
        memoryFooter.className = 'llm-memory-footer';
        const memoryClearBtn = document.createElement('button');
        memoryClearBtn.type = 'button';
        memoryClearBtn.className = 'ghost danger';
        memoryClearBtn.textContent = 'Clear history';
        memoryFooter.appendChild(memoryClearBtn);
        memorySection.appendChild(memoryFooter);

        fields.appendChild(memorySection);

        const sanitizeEntry = (entry) => {
          const roleRaw = entry && typeof entry.role === 'string' ? entry.role.trim().toLowerCase() : '';
          const role = roleRaw === 'assistant' || roleRaw === 'system' ? roleRaw : 'user';
          const content = entry && entry.content != null ? String(entry.content) : '';
          return { role, content };
        };

        const getMemory = () => {
          const current = NodeStore.ensure(nodeId, 'LLM').config || {};
          const list = Array.isArray(current.memory) ? current.memory : [];
          return list.map(sanitizeEntry);
        };

        const formatRoleLabel = (role) => {
          const normalized = String(role || '').toLowerCase();
          if (normalized === 'system') return 'System';
          if (normalized === 'assistant') return 'Assistant';
          if (normalized === 'user') return 'User';
          if (!normalized) return 'User';
          return normalized.charAt(0).toUpperCase() + normalized.slice(1);
        };

        let editingIndex = null;
        let suppressToggleEvent = false;

        const setDetailsOpen = (value) => {
          if (memoryDetails.open === value) return;
          suppressToggleEvent = true;
          memoryDetails.open = value;
          suppressToggleEvent = false;
        };

        memoryDetails.addEventListener('toggle', () => {
          if (suppressToggleEvent) return;
          if (memoryDetails.open) memoryDetails.dataset.userToggled = 'true';
          else delete memoryDetails.dataset.userToggled;
        });

        const updateSummary = (size) => {
          memorySummary.textContent = `Chat History (${size})`;
        };

        const isMemoryEnabled = () => {
          const hiddenToggle = fields.querySelector('input[name="memoryOn"]');
          if (hiddenToggle) return hiddenToggle.value === 'true';
          const selectToggle = fields.querySelector('select[name="memoryOn"]');
          if (selectToggle) return selectToggle.value === 'true';
          return !!cfg.memoryOn;
        };

        const renderMemoryList = () => {
          const memory = getMemory();
          let focusEditor = null;
          memoryList.innerHTML = '';
          updateSummary(memory.length);
          if (!memory.length) memoryEmpty.classList.remove('hidden');
          else memoryEmpty.classList.add('hidden');

          memory.forEach((entry, index) => {
            const item = document.createElement('li');
            item.className = 'llm-memory-item';
            item.dataset.memoryIdx = String(index);

            if (editingIndex === index) {
              item.classList.add('editing');
              item.draggable = false;

              const editorBody = document.createElement('div');
              editorBody.className = 'llm-memory-editor-body';

              const roleSelect = document.createElement('select');
              roleSelect.className = 'llm-memory-role-select';
              ['system', 'user', 'assistant'].forEach((roleKey) => {
                const opt = document.createElement('option');
                opt.value = roleKey;
                opt.textContent = formatRoleLabel(roleKey);
                if (entry.role === roleKey) opt.selected = true;
                roleSelect.appendChild(opt);
              });
              editorBody.appendChild(roleSelect);

              const textarea = document.createElement('textarea');
              textarea.className = 'llm-memory-editor';
              textarea.value = entry.content || '';
              textarea.rows = Math.min(10, Math.max(3, (textarea.value.match(/\n/g) || []).length + 1));
              editorBody.appendChild(textarea);

              item.appendChild(editorBody);

              const editActions = document.createElement('div');
              editActions.className = 'llm-memory-actions editing';

              const saveBtn = document.createElement('button');
              saveBtn.type = 'button';
              saveBtn.className = 'secondary';
              saveBtn.textContent = 'Save';
              saveBtn.addEventListener('click', () => {
                const next = getMemory();
                if (!next[index]) return;
                next[index] = { role: roleSelect.value, content: textarea.value };
                commitMemory(next, 'Chat entry updated');
              });
              editActions.appendChild(saveBtn);

              const cancelBtn = document.createElement('button');
              cancelBtn.type = 'button';
              cancelBtn.className = 'ghost';
              cancelBtn.textContent = 'Cancel';
              cancelBtn.addEventListener('click', () => {
                editingIndex = null;
                renderMemoryList();
                syncVisibility();
              });
              editActions.appendChild(cancelBtn);

              item.appendChild(editActions);
              memoryList.appendChild(item);

              focusEditor = () => {
                textarea.focus();
                const len = textarea.value.length;
                textarea.setSelectionRange(len, len);
              };
              return;
            }

            item.draggable = true;

            const handle = document.createElement('div');
            handle.className = 'llm-memory-handle';
            handle.textContent = '☰';
            handle.title = 'Drag to reorder';
            item.appendChild(handle);

            const body = document.createElement('div');
            body.className = 'llm-memory-body';
            const meta = document.createElement('div');
            meta.className = 'llm-memory-meta';
            meta.textContent = formatRoleLabel(entry.role);
            body.appendChild(meta);
            const content = document.createElement('div');
            content.className = 'llm-memory-content';
            content.textContent = entry.content || '';
            body.appendChild(content);
            item.appendChild(body);

            const actions = document.createElement('div');
            actions.className = 'llm-memory-actions';

            const editBtn = document.createElement('button');
            editBtn.type = 'button';
            editBtn.className = 'ghost';
            editBtn.title = 'Edit entry';
            editBtn.textContent = '✎';
            editBtn.addEventListener('click', () => {
              editingIndex = index;
              renderMemoryList();
              syncVisibility();
            });
            actions.appendChild(editBtn);

            const deleteBtn = document.createElement('button');
            deleteBtn.type = 'button';
            deleteBtn.className = 'ghost danger';
            deleteBtn.title = 'Remove entry';
            deleteBtn.textContent = '✕';
            deleteBtn.addEventListener('click', () => {
              const next = getMemory();
              if (!next[index]) return;
              next.splice(index, 1);
              if (!next.length) delete memoryDetails.dataset.userToggled;
              commitMemory(next, 'Removed chat entry');
            });
            actions.appendChild(deleteBtn);

            item.appendChild(actions);
            memoryList.appendChild(item);
          });

          const enabled = isMemoryEnabled();
          memoryClearBtn.disabled = !enabled || getMemory().length === 0;

          if (!memoryDetails.dataset.userToggled) {
            if (enabled && getMemory().length) setDetailsOpen(true);
            else setDetailsOpen(false);
          }

          if (focusEditor) requestAnimationFrame(focusEditor);
        };

        let dragIndex = null;

        memoryList.addEventListener('dragstart', (ev) => {
          const item = ev.target.closest('.llm-memory-item');
          if (!item || item.classList.contains('editing')) return;
          dragIndex = Number(item.dataset.memoryIdx);
          item.classList.add('dragging');
          ev.dataTransfer.effectAllowed = 'move';
          ev.dataTransfer.setData('text/plain', String(dragIndex));
        });

        memoryList.addEventListener('dragend', (ev) => {
          const item = ev.target.closest('.llm-memory-item');
          if (item) item.classList.remove('dragging');
          dragIndex = null;
          memoryList.querySelectorAll('.drag-over').forEach((el) => el.classList.remove('drag-over'));
        });

        memoryList.addEventListener('dragover', (ev) => {
          const item = ev.target.closest('.llm-memory-item');
          if (!item || item.classList.contains('editing')) return;
          ev.preventDefault();
          item.classList.add('drag-over');
        });

        memoryList.addEventListener('dragleave', (ev) => {
          const item = ev.target.closest('.llm-memory-item');
          if (!item) return;
          item.classList.remove('drag-over');
        });

        memoryList.addEventListener('drop', (ev) => {
          ev.preventDefault();
          memoryList.querySelectorAll('.drag-over').forEach((el) => el.classList.remove('drag-over'));
          const memory = getMemory();
          if (!memory.length) return;
          const fromIndex = dragIndex != null ? dragIndex : Number(ev.dataTransfer?.getData('text/plain'));
          if (!Number.isFinite(fromIndex)) return;
          const targetItem = ev.target.closest('.llm-memory-item');
          let insertIndex = memory.length;
          if (targetItem && targetItem.dataset.memoryIdx !== undefined) {
            insertIndex = Number(targetItem.dataset.memoryIdx);
            if (!Number.isFinite(insertIndex)) insertIndex = memory.length;
          }
          let finalIndex = insertIndex;
          if (fromIndex < insertIndex) finalIndex = Math.max(0, insertIndex - 1);
          if (finalIndex < 0) finalIndex = 0;
          if (finalIndex > memory.length) finalIndex = memory.length;
          if (finalIndex === fromIndex) {
            dragIndex = null;
            return;
          }
          const [moved] = memory.splice(fromIndex, 1);
          memory.splice(finalIndex, 0, moved);
          commitMemory(memory, 'Reordered chat entry');
          dragIndex = null;
        });

        const syncVisibility = () => {
          const enabled = isMemoryEnabled();
          memorySection.classList.toggle('llm-memory-hidden', !enabled);
          memoryClearBtn.disabled = !enabled || getMemory().length === 0;
          if (!memoryDetails.dataset.userToggled) {
            if (enabled) setDetailsOpen(true);
            else setDetailsOpen(false);
          } else if (!enabled) {
            setDetailsOpen(false);
          }
        };

        const commitMemory = (entries, message) => {
          const sanitized = entries.map(sanitizeEntry);
          NodeStore.update(nodeId, { memory: sanitized });
          Router.sendFrom(nodeId, 'memory', { type: 'updated', size: sanitized.length });
          if (message) setBadge(message);
          editingIndex = null;
          renderMemoryList();
          syncVisibility();
        };

        memoryClearBtn.addEventListener('click', (ev) => {
          ev.preventDefault();
          if (!getMemory().length) return;
          delete memoryDetails.dataset.userToggled;
          commitMemory([], 'Chat history cleared');
        });

        const hiddenToggle = fields.querySelector('input[name="memoryOn"]');
        const selectToggle = fields.querySelector('select[name="memoryOn"]');
        const toggleButton = hiddenToggle?.parentElement?.querySelector('.toggle-boolean');

        if (toggleButton && !toggleButton._llmMemoryToggleBound) {
          toggleButton.addEventListener('click', () => {
            setTimeout(syncVisibility, 0);
          });
          toggleButton._llmMemoryToggleBound = true;
        } else if (selectToggle && !selectToggle._llmMemoryToggleBound) {
          selectToggle.addEventListener('change', syncVisibility);
          selectToggle._llmMemoryToggleBound = true;
        }

        renderMemoryList();
        syncVisibility();
      };
    }
    for (const field of schema) {
      const key = field.key || '';
      if (isWasmType) {
        const hideRemoteField = wasmMode && ['base', 'relay', 'api', 'model', 'endpointMode', 'prevModel'].includes(key);
        const hideWasmField = !wasmMode && key.startsWith('wasm');
        if (hideRemoteField || hideWasmField) continue;
      }
      const label = document.createElement('label');
      label.textContent = field.label;
      if (node.type === 'NknDM' && field.key === 'address') {
        const row = document.createElement('div');
        row.className = 'nkndm-settings-row';
        const input = document.createElement('input');
        input.type = 'text';
        input.name = field.key;
        input.placeholder = field.placeholder || '';
        input.value = (cfg[field.key] !== undefined && cfg[field.key] !== null)
          ? String(cfg[field.key])
          : '';
        row.appendChild(input);

        const sendBtn = document.createElement('button');
        sendBtn.type = 'button';
        sendBtn.className = 'secondary';
        sendBtn.textContent = 'Send';
        sendBtn.dataset.nkndmSettingsSend = 'true';
        row.appendChild(sendBtn);

        const pingBtn = document.createElement('button');
        pingBtn.type = 'button';
        pingBtn.className = 'ghost';
        pingBtn.textContent = 'Ping';
        pingBtn.dataset.nkndmSettingsPing = 'true';
        row.appendChild(pingBtn);

        const spinner = document.createElement('span');
        spinner.className = 'dm-spinner hidden';
        spinner.dataset.nkndmSettingsSpinner = 'true';
        row.appendChild(spinner);

        const status = document.createElement('span');
        status.className = 'nkndm-settings-status';
        status.dataset.nkndmSettingsStatus = 'idle';
        row.appendChild(status);

        const wrap = document.createElement('div');
        wrap.className = 'nkndm-settings-control';
        wrap.appendChild(row);

        const peerInfo = document.createElement('div');
        peerInfo.className = 'nkndm-settings-peer';
        peerInfo.dataset.nkndmSettingsPeer = 'true';
        peerInfo.textContent = '—';
        wrap.appendChild(peerInfo);

        fields.appendChild(label);
        fields.appendChild(wrap);
        continue;
      }
      if (field.type === 'logicRules') {
        const editor = createLogicRulesEditor(node, field, cfg);
        fields.appendChild(label);
        fields.appendChild(editor);
        continue;
      }
      if (field.type === 'fieldMap') {
        const editor = createFieldMapEditor(field, cfg);
        fields.appendChild(label);
        fields.appendChild(editor);
        continue;
      }
      if (field.type === 'filterList') {
        const editor = createFilterListEditor(field, cfg);
        fields.appendChild(label);
        fields.appendChild(editor);
        continue;
      }
      if (field.type === 'select') {
        if (node.type === 'TTS' && field.key === 'wasmVoicePreset') {
          const wrapper = document.createElement('div');
          wrapper.className = 'wasm-voice-wrap';
          const select = document.createElement('select');
          select.name = field.key;
          const addOption = (val, text = val) => {
            const opt = document.createElement('option');
            opt.value = String(val);
            opt.textContent = String(text);
            select.appendChild(opt);
          };
          const applyOptions = () => {
            select.innerHTML = '';
            const options = TTS.getWasmVoiceOptions?.(cfg) || [];
            if (!options.length) addOption('', '— no voices —');
            options.forEach((opt) => {
              addOption(opt.id, opt.label || opt.id);
            });
            const desired = cfg.wasmVoicePreset || options[0]?.id || '';
            select.value = desired;
          };
          applyOptions();
          select.addEventListener('change', () => {
            const options = TTS.getWasmVoiceOptions?.(cfg) || [];
            const chosen = options.find((o) => o.id === select.value);
            const modelInput = fields.querySelector('input[name="wasmPiperModelUrl"]');
            const configInput = fields.querySelector('input[name="wasmPiperConfigUrl"]');
            if (chosen?.modelUrl && modelInput) modelInput.value = chosen.modelUrl;
            if (chosen?.configUrl && configInput) configInput.value = chosen.configUrl;
            cfg.wasmVoicePreset = select.value;
          });

          const customWrap = document.createElement('div');
          customWrap.className = 'wasm-custom-voice';
          customWrap.innerHTML = `
            <div class="muted" style="font-size:11px;">Add custom ONNX voice (stored in this graph)</div>
            <input type="text" data-wasm-custom-name placeholder="Voice label (e.g., My Voice)">
            <input type="url" data-wasm-custom-model placeholder="Model URL (.onnx)">
            <input type="url" data-wasm-custom-config placeholder="Config URL (.onnx.json)">
            <input type="number" data-wasm-custom-size placeholder="Size MB (optional)" min="1" step="1">
            <div class="row" style="gap:8px;flex-wrap:wrap;">
              <button type="button" class="secondary" data-wasm-custom-add>Add custom voice</button>
              <span class="muted" style="font-size:11px;">Custom voices are listed first.</span>
            </div>
          `;
          const addBtn = customWrap.querySelector('[data-wasm-custom-add]');
          addBtn?.addEventListener('click', () => {
            const name = String(customWrap.querySelector('[data-wasm-custom-name]')?.value || '').trim();
            const modelUrl = String(customWrap.querySelector('[data-wasm-custom-model]')?.value || '').trim();
            const configUrl = String(customWrap.querySelector('[data-wasm-custom-config]')?.value || '').trim();
            const sizeMbRaw = Number(customWrap.querySelector('[data-wasm-custom-size]')?.value || '');
            if (!name || !modelUrl || !configUrl) {
              setBadge('Enter name, model URL, and config URL', false);
              return;
            }
            const sizeMB = Number.isFinite(sizeMbRaw) && sizeMbRaw > 0 ? sizeMbRaw : undefined;
            const rec = NodeStore.ensure(node.id, 'TTS');
            const current = Array.isArray(rec.config?.wasmCustomVoices) ? rec.config.wasmCustomVoices.slice() : [];
            const idBase = name.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '') || 'custom_voice';
            let id = idBase;
            let suffix = 1;
            while (current.some((v) => v.id === id)) {
              id = `${idBase}_${suffix++}`;
            }
            current.push({ id, label: name, modelUrl, configUrl, sizeMB });
            const updated = NodeStore.update(node.id, {
              type: 'TTS',
              wasmCustomVoices: current,
              wasmVoicePreset: id,
              wasmPiperModelUrl: modelUrl,
              wasmPiperConfigUrl: configUrl
            });
            cfg = updated || cfg;
            applyOptions();
            select.value = id;
            const modelInput = fields.querySelector('input[name="wasmPiperModelUrl"]');
            const configInput = fields.querySelector('input[name="wasmPiperConfigUrl"]');
            if (modelInput) modelInput.value = modelUrl;
            if (configInput) configInput.value = configUrl;
            setBadge('Custom voice saved');
          });

          wrapper.appendChild(select);
          wrapper.appendChild(customWrap);
          const customButtons = document.createElement('div');
          customButtons.className = 'wasm-custom-buttons model-picker-buttons';
          const renderCustomButtons = () => {
            customButtons.innerHTML = '';
            const options = TTS.getWasmVoiceOptions?.(cfg) || [];
            const customs = options.filter((o) => o.source === 'custom');
            if (!customs.length) return;
            customs.forEach((voice) => {
              const btn = document.createElement('button');
              btn.type = 'button';
              btn.className = 'model-picker-button';
              btn.textContent = voice.label;
              const syncSelected = () => {
                const isActive = select.value === voice.id;
                btn.classList.toggle('selected', isActive);
              };
              syncSelected();
              btn.addEventListener('click', async () => {
                select.value = voice.id;
                syncSelected();
                customButtons.querySelectorAll('button').forEach((b) => {
                  if (b !== btn) b.classList.remove('selected');
                });
                const modelInput = fields.querySelector('input[name="wasmPiperModelUrl"]');
                const configInput = fields.querySelector('input[name="wasmPiperConfigUrl"]');
                if (modelInput) modelInput.value = voice.modelUrl || '';
                if (configInput) configInput.value = voice.configUrl || '';
                const updated = NodeStore.update(node.id, {
                  type: 'TTS',
                  wasmVoicePreset: voice.id,
                  wasmPiperModelUrl: voice.modelUrl,
                  wasmPiperConfigUrl: voice.configUrl
                });
                cfg = updated || cfg;
                btn.dataset.state = 'loading';
                btn.disabled = true;
                btn.textContent = `${voice.label} • pulling…`;
                try {
                  await TTS.listWasmVoices(node.id);
                  btn.dataset.state = 'done';
                  btn.textContent = `${voice.label} ✓`;
                } catch (err) {
                  btn.dataset.state = 'err';
                  btn.textContent = `${voice.label} (retry)`;
                  setBadge(err?.message || 'Failed to load voice', false);
                } finally {
                  btn.disabled = false;
                }
              });
              customButtons.appendChild(btn);
            });
          };
          renderCustomButtons();
          const customLabel = document.createElement('label');
          customLabel.textContent = 'Custom voices';
          customLabel.classList.add('muted');
          fields.appendChild(customLabel);
          fields.appendChild(customButtons);
          fields.appendChild(label);
          fields.appendChild(wrapper);
          convertBooleanSelectsIn(wrapper);
          continue;
        }
        if (node.type === 'TTS' && field.key === 'wasmSpeakerId') {
          const select = document.createElement('select');
          select.name = field.key;
          const currentValue = String(cfg[field.key] ?? field.def ?? '');
          const addOption = (val, text = val) => {
            const opt = document.createElement('option');
            opt.value = String(val);
            opt.textContent = String(text);
            select.appendChild(opt);
          };
          addOption('', '— loading voices —');
          select.value = currentValue || '';
          (async () => {
            try {
              const speakers = await TTS.listWasmVoices?.(node.id);
              select.innerHTML = '';
              if (!speakers || !speakers.length) {
                addOption('', '— no voices —');
                select.value = '';
                return;
              }
              speakers.forEach((sp) => {
                const id = sp?.id ?? sp?.value ?? sp;
                const labelText = sp?.name || sp?.label || `Voice ${id}`;
                addOption(id, labelText);
              });
              const desired = currentValue || String(speakers[0]?.id ?? '');
              select.value = desired;
            } catch (err) {
              select.innerHTML = '';
              if (currentValue) addOption(currentValue, `${currentValue} (saved)`);
              addOption('', '— voices unavailable —');
              select.value = currentValue || '';
            }
          })();
          fields.appendChild(label);
          fields.appendChild(select);
          convertBooleanSelectsIn(select.parentElement || select);
          continue;
        }
        const modelProvider = field.key === 'model' ? MODEL_PROVIDERS[node.type] : null;
        if (modelProvider && typeof modelProvider.ensureModels === 'function') {
          const wrapper = document.createElement('div');
          wrapper.className = 'model-picker';

          const hiddenInput = document.createElement('input');
          hiddenInput.type = 'hidden';
          hiddenInput.name = field.key;
          hiddenInput.value = String(cfg[field.key] ?? field.def ?? '');
          wrapper.appendChild(hiddenInput);

          const buttonRow = document.createElement('div');
          buttonRow.className = 'model-picker-buttons';
          wrapper.appendChild(buttonRow);

          const detailBox = document.createElement('pre');
          detailBox.className = 'model-picker-detail';
          detailBox.textContent = 'Select a model to view details';
          wrapper.appendChild(detailBox);

          let capsRow = null;
          let capsRequestToken = 0;

          const renderCapabilities = (info, state = 'ready') => {
            if (!capsRow) return;
            capsRow.innerHTML = '';
            capsRow.dataset.state = state;

            if (state === 'loading') {
              const chip = document.createElement('span');
              chip.className = 'model-cap-chip pending';
              chip.textContent = 'Loading…';
              capsRow.appendChild(chip);
              return;
            }

            if (state === 'error') {
              const chip = document.createElement('span');
              chip.className = 'model-cap-chip error';
              chip.textContent = 'Unavailable';
              capsRow.appendChild(chip);
              return;
            }

            const caps = Array.isArray(info?.capabilities)
              ? info.capabilities
              : Array.isArray(info)
                ? info
                : [];

            if (!caps.length) {
              const chip = document.createElement('span');
              chip.className = 'model-cap-chip muted';
              chip.textContent = '—';
              capsRow.appendChild(chip);
              return;
            }

            caps.forEach((cap) => {
              const chip = document.createElement('span');
              chip.className = 'model-cap-chip';
              chip.textContent = String(cap);
              capsRow.appendChild(chip);
            });
          };

          if (node.type === 'LLM') {
            capsRow = document.createElement('div');
            capsRow.className = 'model-picker-capabilities';
            renderCapabilities([], 'idle');
            wrapper.appendChild(capsRow);
          }

          fields.appendChild(label);
          fields.appendChild(wrapper);

          const baseInput = fields.querySelector('input[name="base"]');
          const relayInput = fields.querySelector('input[name="relay"]');
          const apiInput = fields.querySelector('input[name="api"]');

          const buildOverrideConfig = ({ mutateRelay = false } = {}) => {
            const override = {};
            const baseVal = baseInput?.value?.trim?.() || '';
            if (baseVal) override.base = baseVal;
            const relayRaw = relayInput?.value?.trim?.() || '';
            if (relayRaw) {
              const parsedRelay = parseNknAddress(relayRaw);
              if (parsedRelay) {
                override.relay = parsedRelay;
                if (mutateRelay && relayInput && relayInput.value !== parsedRelay) relayInput.value = parsedRelay;
              }
            }
            const apiVal = apiInput?.value?.trim?.() || '';
            if (apiVal) override.api = apiVal;
            const modeInput = relayInput?._endpointModeInput || fields.querySelector('input[name="endpointMode"]');
            const modeVal = String(modeInput?.value || '').trim().toLowerCase();
            if (modeVal) override.endpointMode = modeVal;
            return override;
          };

          const applyCapabilities = (caps) => {
            if (node.type !== 'LLM') return;
            const normalized = Array.isArray(caps)
              ? caps.map((c) => String(c).trim()).filter(Boolean)
              : [];
            NodeStore.update(node.id, { type: 'LLM', capabilities: normalized });
            refreshLlmControls(node.id);
          };

          const setSelection = (meta) => {
            if (!meta) return;
            hiddenInput.value = meta.id;
            const pretty = JSON.stringify(meta.raw || {}, null, 2);
            detailBox.textContent = pretty;
            detailBox.dataset.modelId = meta.id;
            wrapper.dataset.selectedModel = meta.id;
            buttonRow.querySelectorAll('button').forEach((btn) => {
              if (btn.dataset.modelId === meta.id) btn.classList.add('selected');
              else btn.classList.remove('selected');
            });

            if (capsRow) {
              const cachedInfo = typeof modelProvider.getModelInfo === 'function'
                ? modelProvider.getModelInfo(node.id, meta.id)
                : null;
              if (cachedInfo && Array.isArray(cachedInfo.capabilities) && cachedInfo.capabilities.length) {
                renderCapabilities(cachedInfo, 'ready');
                applyCapabilities(cachedInfo.capabilities);
              } else {
                renderCapabilities([], 'loading');
              }

              const requestToken = ++capsRequestToken;
              const override = buildOverrideConfig();
              const hasOverride = Object.keys(override).length > 0;
              let fetchOptions = null;
              if (hasOverride) {
                fetchOptions = { override };
                const cfgCurrent = (NodeStore.ensure(node.id, node.type)?.config) || {};
                const differs = ['base', 'relay', 'api', 'endpointMode'].some((key) => {
                  const oVal = String(override[key] || '').trim();
                  const current = key === 'endpointMode'
                    ? String(cfgCurrent.endpointMode || 'auto').trim()
                    : String(cfgCurrent[key] || '').trim();
                  if (!oVal) return false;
                  return oVal !== current;
                });
                if (differs || !cfgCurrent.base) fetchOptions.force = true;
              }
              const promise = typeof modelProvider.fetchModelInfo === 'function'
                ? modelProvider.fetchModelInfo(node.id, meta.id, fetchOptions || {})
                : null;
              if (promise && typeof promise.then === 'function') {
                promise
                  .then((info) => {
                    if (requestToken !== capsRequestToken) return;
                    if (info) renderCapabilities(info, 'ready');
                    else if (!cachedInfo) renderCapabilities([], 'idle');
                    if (info) applyCapabilities(info.capabilities);
                  })
                  .catch(() => {
                    if (requestToken !== capsRequestToken) return;
                    if (!cachedInfo) renderCapabilities([], 'error');
                  });
              } else if (!cachedInfo) {
                renderCapabilities([], 'idle');
              }
            }
            applyCapabilities(meta.capabilities);
          };

          const showStatus = (text) => {
            buttonRow.innerHTML = '';
            const status = document.createElement('div');
            status.className = 'model-picker-status';
            status.textContent = text;
            buttonRow.appendChild(status);
          };

          const renderButtons = () => {
            const models = modelProvider.listModels?.(node.id) || [];
            if (!models.length) {
              showStatus('No models found');
              return;
            }
            buttonRow.innerHTML = '';
            models.forEach((meta) => {
              const btn = document.createElement('button');
              btn.type = 'button';
              btn.dataset.modelId = meta.id;
              btn.className = 'model-picker-button';
              btn.textContent = meta.label;
              btn.addEventListener('click', () => setSelection(meta));
              buttonRow.appendChild(btn);
            });
            const currentId = hiddenInput.value && modelProvider.getModelInfo?.(node.id, hiddenInput.value)
              ? hiddenInput.value
              : models[0].id;
            const selectedMeta = modelProvider.getModelInfo?.(node.id, currentId) || models[0];
            setSelection(selectedMeta);
          };

          const cachedModels = modelProvider.listModels?.(node.id) || [];
          if (cachedModels.length) renderButtons();
          else showStatus('No models found');

          const handleFetchError = (err) => {
            const available = modelProvider.listModels?.(node.id) || [];
            if (available.length) return;
            showStatus(`Failed to load models: ${err?.message || err}`);
          };

          const fetchControls = document.createElement('div');
          fetchControls.className = 'model-fetch-row';
          const fetchButton = document.createElement('button');
          fetchButton.type = 'button';
          fetchButton.className = 'ghost model-fetch-button';
          fetchButton.textContent = 'Fetch models';
          fetchControls.appendChild(fetchButton);
          wrapper.appendChild(fetchControls);

          const triggerFetch = async ({ force = true } = {}) => {
            const override = buildOverrideConfig({ mutateRelay: true });
            fetchButton.disabled = true;
            try {
              showStatus('Fetching models…');
              await modelProvider.ensureModels(node.id, { force, override });
              renderButtons();
            } catch (err) {
              handleFetchError(err);
            } finally {
              fetchButton.disabled = false;
            }
          };

          fetchButton.addEventListener('click', () => triggerFetch({ force: true }));

          if (!cachedModels.length) {
            triggerFetch({ force: true }).catch(() => {});
          }

          if (node.type === 'LLM') {
            const pullWrap = document.createElement('div');
            pullWrap.className = 'model-pull llm';
            const pullInput = document.createElement('input');
            pullInput.type = 'text';
            pullInput.placeholder = 'Model to pull (e.g. llama3.2)';
            pullInput.className = 'model-pull-input';
            const pullButton = document.createElement('button');
            pullButton.type = 'button';
            pullButton.className = 'model-pull-button';
            pullButton.textContent = 'Pull model';
            pullButton.disabled = true;
            const pullLog = document.createElement('pre');
            pullLog.className = 'model-pull-log';
            pullLog.textContent = '';
            pullWrap.appendChild(pullInput);
            pullWrap.appendChild(pullButton);
            pullWrap.appendChild(pullLog);
            wrapper.appendChild(pullWrap);

            pullInput.addEventListener('input', () => {
              pullButton.disabled = !pullInput.value.trim();
            });

            pullButton.addEventListener('click', async () => {
              if (!pullInput.value.trim()) return;
              if (typeof modelProvider.pullModel !== 'function') {
                pullLog.textContent = 'Pull unsupported for this provider\n';
                return;
              }
              const override = buildOverrideConfig({ mutateRelay: true });
              pullButton.disabled = true;
              pullInput.disabled = true;
              pullLog.textContent = '';
              try {
                await modelProvider.pullModel?.(node.id, {
                  model: pullInput.value.trim(),
                  override,
                  onEvent: (evt) => {
                    if (!evt || typeof evt !== 'object') return;
                    const status = evt.status || evt.message || '';
                    const total = Number(evt.total || 0);
                    const completed = Number(evt.completed || 0);
                    let line = status || JSON.stringify(evt);
                    if (total > 0 && completed >= 0) {
                      const pct = Math.min(100, Math.round((completed / total) * 100));
                      if (Number.isFinite(pct)) line = `${line} (${pct}%)`;
                    }
                    pullLog.textContent += `${line}\n`;
                    pullLog.scrollTop = pullLog.scrollHeight;
                  }
                });
                pullLog.textContent += 'Pull complete\n';
                await triggerFetch({ force: true });
              } catch (err) {
                const msg = err?.message || err || 'Pull failed';
                pullLog.textContent += `Error: ${msg}\n`;
              } finally {
                pullInput.disabled = false;
                pullButton.disabled = !pullInput.value.trim();
              }
            });
          } else if (node.type === 'TTS') {
            const pullToggle = document.createElement('button');
            pullToggle.type = 'button';
            pullToggle.className = 'model-pull-toggle';
            pullToggle.textContent = 'Pull voice model';
            pullToggle.setAttribute('aria-expanded', 'false');
            const pullForm = document.createElement('div');
            pullForm.className = 'model-pull-form hidden';
            const jsonInput = document.createElement('input');
            jsonInput.type = 'url';
            jsonInput.placeholder = 'Voice manifest (.onnx.json) URL';
            jsonInput.className = 'model-pull-input';
            const modelInput = document.createElement('input');
            modelInput.type = 'url';
            modelInput.placeholder = 'Voice model (.onnx) URL';
            modelInput.className = 'model-pull-input';
            const nameInput = document.createElement('input');
            nameInput.type = 'text';
            nameInput.placeholder = 'Optional voice name';
            nameInput.className = 'model-pull-input';
            const pullAction = document.createElement('button');
            pullAction.type = 'button';
            pullAction.className = 'model-pull-button';
            pullAction.textContent = 'Download voice';
            pullAction.disabled = true;
            const pullLog = document.createElement('pre');
            pullLog.className = 'model-pull-log';
            pullLog.textContent = '';

            pullForm.appendChild(jsonInput);
            pullForm.appendChild(modelInput);
            pullForm.appendChild(nameInput);
            pullForm.appendChild(pullAction);
            pullForm.appendChild(pullLog);

            wrapper.appendChild(pullToggle);
            wrapper.appendChild(pullForm);

            const updateToggleState = () => {
              const active = !pullForm.classList.contains('hidden');
              pullToggle.setAttribute('aria-expanded', active ? 'true' : 'false');
            };

            pullToggle.addEventListener('click', () => {
              pullForm.classList.toggle('hidden');
              updateToggleState();
            });

            const updateActionState = () => {
              pullAction.disabled = !(jsonInput.value.trim() && modelInput.value.trim());
            };

            jsonInput.addEventListener('input', updateActionState);
            modelInput.addEventListener('input', updateActionState);

            pullAction.addEventListener('click', async () => {
              if (!jsonInput.value.trim() || !modelInput.value.trim()) return;
              if (typeof modelProvider.pullModel !== 'function') {
                pullLog.textContent = 'Pull unsupported for this provider\n';
                return;
              }
              const override = buildOverrideConfig({ mutateRelay: true });
              pullAction.disabled = true;
              jsonInput.disabled = true;
              modelInput.disabled = true;
              nameInput.disabled = true;
              pullLog.textContent = '';
              try {
                await modelProvider.pullModel?.(node.id, {
                  onnxJsonUrl: jsonInput.value.trim(),
                  onnxModelUrl: modelInput.value.trim(),
                  name: nameInput.value.trim(),
                  override,
                  onEvent: (evt) => {
                    if (!evt || typeof evt !== 'object') return;
                    const status = evt.status || evt.message || '';
                    const total = Number(evt.total || 0);
                    const completed = Number(evt.completed || 0);
                    let line = status || JSON.stringify(evt);
                    if (total > 0 && completed >= 0) {
                      const pct = Math.min(100, Math.round((completed / total) * 100));
                      if (Number.isFinite(pct)) line = `${line} (${pct}%)`;
                    }
                    pullLog.textContent += `${line}\n`;
                    pullLog.scrollTop = pullLog.scrollHeight;
                  }
                });
                pullLog.textContent += 'Pull complete\n';
                await triggerFetch({ force: true });
              } catch (err) {
                const msg = err?.message || err || 'Pull failed';
                pullLog.textContent += `Error: ${msg}\n`;
              } finally {
                jsonInput.disabled = false;
                modelInput.disabled = false;
                nameInput.disabled = false;
                updateActionState();
              }
            });

            updateToggleState();
          }

          if (typeof modelProvider.subscribeModels === 'function') {
            settingsModelSubscription = modelProvider.subscribeModels(node.id, () => {
              const models = modelProvider.listModels?.(node.id) || [];
              if (models.length) renderButtons();
              else showStatus('No models found');
            });
          }

          if (cachedModels.length) {
            renderButtons();
          } else {
            triggerFetch({ force: true }).catch(handleFetchError);
          }
          let pendingRelayFetch = null;
          let lastRelayKey = '';

          const runRelayFetch = ({ force = false } = {}) => {
            const override = buildOverrideConfig({ mutateRelay: true });
            const baseVal = override.base || '';
            const relayVal = override.relay || '';
            const apiVal = override.api || '';
            const modeInput = relayInput?._endpointModeInput || fields.querySelector('input[name="endpointMode"]');
            const modeVal = String((override.endpointMode ?? modeInput?.value ?? 'auto')).toLowerCase();
            if (modeVal === 'local') return;
            if (!baseVal) return;
            const hasRelay = !!relayVal;
            if (modeVal === 'remote' && !hasRelay) return;
            if (!hasRelay) return;
            const key = `${baseVal}|${relayVal}|${apiVal}|${modeVal}`;
            if (!force && key === lastRelayKey) return;
            lastRelayKey = key;
            const existing = modelProvider.listModels?.(node.id) || [];
            if (!existing.length) showStatus('Fetching models…');
            triggerFetch({ force: true }).catch(handleFetchError);
          };

          const scheduleRelayFetch = () => {
            if (pendingRelayFetch) clearTimeout(pendingRelayFetch);
            pendingRelayFetch = setTimeout(() => runRelayFetch({ force: true }), 400);
          };

          if (relayInput) {
            relayInput.addEventListener('input', scheduleRelayFetch);
            relayInput.addEventListener('blur', () => runRelayFetch({ force: true }));
          }
          if (baseInput) baseInput.addEventListener('change', () => runRelayFetch({ force: true }));
          if (apiInput) apiInput.addEventListener('change', () => runRelayFetch({ force: true }));

          if (relayInput && relayInput.value && !cachedModels.length) runRelayFetch({ force: true });

          continue;
        }
        const select = document.createElement('select');
        select.name = field.key;
        const currentValue = String((cfg[field.key] ?? field.def ?? ''));
        const addOption = (val, text = val) => {
          const opt = document.createElement('option');
          opt.value = String(val);
          opt.textContent = String(text);
          select.appendChild(opt);
        };
        if ((field.key === 'model' || (node.type === 'ASR' && field.key === 'prevModel')) &&
          (node.type === 'LLM' || node.type === 'TTS' || node.type === 'ASR')) {
          addOption('', '— loading models… —');
          select.value = '';
          (async () => {
            try {
              const base = cfg.base || '';
              const api = cfg.api || '';
              const relay = cfg.relay || '';
              const viaNkn = !!relay;
              const list = node.type === 'LLM'
                ? await discoverLLMModels(base, api, viaNkn, relay)
                : node.type === 'TTS'
                  ? await discoverTTSModels(base, api, viaNkn, relay)
                  : await discoverASRModels(base, api, viaNkn, relay);
              select.innerHTML = '';
              if (!list.length) addOption('', '— no models found —');
              for (const name of list) addOption(name);
              if (currentValue && !list.includes(currentValue)) addOption(currentValue, `${currentValue} (saved)`);
              select.value = currentValue || (list[0] ?? '');
            } catch (err) {
              select.innerHTML = '';
              if (currentValue) addOption(currentValue, `${currentValue} (saved)`);
              addOption('', '— fetch failed —');
              select.value = currentValue || '';
            }
          })();
        } else {
          for (const opt of field.options || []) addOption(opt);
          select.value = currentValue;
        }
        fields.appendChild(label);
        fields.appendChild(select);
        convertBooleanSelectsIn(select.parentElement || select);
      } else {
        const input = field.type === 'textarea' ? document.createElement('textarea') : document.createElement('input');
        if (field.type !== 'textarea') input.type = field.type || 'text';
        if (field.placeholder) input.placeholder = field.placeholder;
        input.name = field.key;
        input.value = (cfg[field.key] !== undefined && cfg[field.key] !== null) ? String(cfg[field.key]) : String(field.def ?? '');
        if (field.step) input.step = field.step;
        if (field.min !== undefined) input.min = field.min;
        if (field.max !== undefined) input.max = field.max;
        let control = input;
        if (field.key === 'relay') {
          const wrap = document.createElement('div');
          wrap.className = 'input-with-btn relay-input-wrap';
          wrap.appendChild(input);

          const scanBtn = document.createElement('button');
          scanBtn.type = 'button';
          scanBtn.className = 'ghost';
          scanBtn.textContent = 'Scan QR';
          scanBtn.addEventListener('click', (e) => {
            e.preventDefault();
            openQrScanner(input);
          });
          wrap.appendChild(scanBtn);

          const modeBtn = document.createElement('button');
          modeBtn.type = 'button';
          modeBtn.className = 'ghost endpoint-mode-btn';
          wrap.appendChild(modeBtn);

          const modeHidden = document.createElement('input');
          modeHidden.type = 'hidden';
          modeHidden.name = 'endpointMode';
          const MODE_ORDER = ['auto', 'remote', 'local'];
          const MODE_LABEL = { auto: 'Auto', remote: 'Remote', local: 'Local' };
          let modeValue = String(cfg.endpointMode || 'auto').toLowerCase();
          if (!MODE_ORDER.includes(modeValue)) modeValue = 'auto';
          modeHidden.value = modeValue;

          const applyModeUI = () => {
            const labelText = MODE_LABEL[modeValue] || 'Auto';
            modeBtn.textContent = `Endpoint: ${labelText}`;
            const isLocal = modeValue === 'local';
            input.disabled = isLocal;
            input.classList.toggle('relay-disabled', isLocal);
          };

          modeBtn.addEventListener('click', (e) => {
            e.preventDefault();
            const idx = MODE_ORDER.indexOf(modeValue);
            modeValue = MODE_ORDER[(idx + 1) % MODE_ORDER.length];
            modeHidden.value = modeValue;
            applyModeUI();
          });

          applyModeUI();

          input._endpointModeInput = modeHidden;
          input._endpointModeButton = modeBtn;

          fields.appendChild(label);
          fields.appendChild(wrap);
          fields.appendChild(modeHidden);
          continue;
        }
        fields.appendChild(label);
        fields.appendChild(control);
      }
    }
    if (node.type === 'MediaStream') {
      const mediaSection = document.createElement('div');
      mediaSection.className = 'media-settings';

      const heading = document.createElement('div');
      heading.className = 'muted';
      heading.textContent = 'Media Targets';
      mediaSection.appendChild(heading);

      const addRow = document.createElement('div');
      addRow.className = 'media-settings-add';

      const addInput = document.createElement('input');
      addInput.type = 'text';
      addInput.placeholder = 'hydra.peer';
      addInput.autocomplete = 'off';
      addInput.spellcheck = false;
      addInput.className = 'media-settings-input';
      addRow.appendChild(addInput);

      const addBtn = document.createElement('button');
      addBtn.type = 'button';
      addBtn.className = 'secondary';
      addBtn.textContent = 'Add';
      addRow.appendChild(addBtn);

      const scanBtn = document.createElement('button');
      scanBtn.type = 'button';
      scanBtn.className = 'ghost';
      scanBtn.textContent = 'Scan QR';
      addRow.appendChild(scanBtn);

      mediaSection.appendChild(addRow);

      const list = document.createElement('div');
      list.className = 'media-settings-list';
      mediaSection.appendChild(list);

      fields.appendChild(mediaSection);

      const handleAdd = (value) => {
        const text = String(value || '').trim();
        if (!text) {
          setBadge('Enter a NKN address', false);
          return;
        }
        const { added, address } = Media.addTarget(node.id, text, { auto: true, handshake: true });
        if (!address) {
          setBadge('Invalid NKN address', false);
          return;
        }
        if (added) setBadge(`Added media peer ${Media.formatAddress(address)}`);
        else setBadge('Target already added');
        addInput.value = '';
        renderTargets();
        refreshNodeTransport(node.id);
      };

      addBtn.addEventListener('click', (ev) => {
        ev.preventDefault();
        handleAdd(addInput.value);
      });

      addInput.addEventListener('keydown', (ev) => {
        if (ev.key === 'Enter') {
          ev.preventDefault();
          handleAdd(addInput.value);
        }
      });

      scanBtn.addEventListener('click', (ev) => {
        ev.preventDefault();
        openQrScanner(addInput, (txt) => {
          if (!txt) return;
          handleAdd(txt);
        });
      });

      const renderTargets = () => {
        const targets = Media.getTargets(node.id);
        const activeSet = new Set(Media.getActiveTargets(node.id));
        const metaMap = Media.getPeerMeta(node.id);
        list.innerHTML = '';
        if (!targets.length) {
          const empty = document.createElement('div');
          empty.className = 'media-settings-empty';
          empty.textContent = 'No NKN targets configured.';
          list.appendChild(empty);
          return;
        }
        targets.forEach((addr) => {
          const row = document.createElement('div');
          row.className = 'media-settings-item';
          row.dataset.address = addr;

          const info = document.createElement('div');
          info.className = 'media-settings-item-info';

          const labelEl = document.createElement('div');
          labelEl.className = 'media-settings-item-address';
          labelEl.textContent = Media.formatAddress(addr);
          labelEl.title = addr;
          info.appendChild(labelEl);

          const meta = metaMap[addr] || {};
          const handshake = meta.handshake || {};
          const status = handshake.status || 'idle';
          const direction = handshake.direction || 'idle';
          let statusText = 'Idle';
          if (status === 'accepted') {
            statusText = meta.viewing ? 'Streaming (viewing)' : 'Connected';
          } else if (status === 'pending') {
            statusText = direction === 'incoming' ? 'Awaiting your response' : 'Awaiting peer';
          } else if (status === 'declined') {
            statusText = 'Declined';
          } else if (meta.remoteActive) {
            statusText = 'Remote ready';
          }
          if (meta.latency != null && status === 'accepted') {
            statusText += ` • ${Math.round(meta.latency)} ms`;
          }
          const statusEl = document.createElement('div');
          statusEl.className = `media-settings-status status-${status}`;
          statusEl.textContent = statusText;
          info.appendChild(statusEl);

          if (activeSet.has(addr)) {
            const activeBadge = document.createElement('span');
            activeBadge.className = 'media-settings-active';
            activeBadge.textContent = 'Active';
            info.appendChild(activeBadge);
          }

          row.appendChild(info);

          const actions = document.createElement('div');
          actions.className = 'media-settings-item-actions';

          const handshakeBtn = document.createElement('button');
          handshakeBtn.type = 'button';
          handshakeBtn.className = 'ghost';
          handshakeBtn.textContent = status === 'accepted' ? 'Sync' : 'Request';
          handshakeBtn.disabled = status === 'pending' && direction === 'incoming';
          handshakeBtn.title = 'Send handshake to this peer';
          handshakeBtn.addEventListener('click', () => {
            Media.requestHandshake(node.id, addr, { wantActive: activeSet.has(addr) });
          });
          actions.appendChild(handshakeBtn);

          const pingBtn = document.createElement('button');
          pingBtn.type = 'button';
          pingBtn.className = 'ghost';
          pingBtn.textContent = 'Ping';
          pingBtn.disabled = status !== 'accepted';
          pingBtn.title = 'Send heartbeat ping';
          pingBtn.addEventListener('click', async () => {
            await Media.sendPing(node.id, addr);
          });
          actions.appendChild(pingBtn);

          const acceptBtn = document.createElement('button');
          acceptBtn.type = 'button';
          acceptBtn.className = 'ghost';
          acceptBtn.textContent = '✔';
          acceptBtn.title = 'Accept incoming offer';
          acceptBtn.disabled = !(status === 'pending' && direction === 'incoming');
          acceptBtn.addEventListener('click', async () => {
            await Media.acceptHandshake(node.id, addr);
            renderTargets();
            refreshNodeTransport(node.id);
          });
          actions.appendChild(acceptBtn);

          const declineBtn = document.createElement('button');
          declineBtn.type = 'button';
          declineBtn.className = 'ghost';
          declineBtn.textContent = '✖';
          declineBtn.title = 'Decline incoming offer';
          declineBtn.disabled = !(status === 'pending' && direction === 'incoming');
          declineBtn.addEventListener('click', async () => {
            await Media.declineHandshake(node.id, addr);
            renderTargets();
            refreshNodeTransport(node.id);
          });
          actions.appendChild(declineBtn);

          const activeBtn = document.createElement('button');
          activeBtn.type = 'button';
          activeBtn.className = activeSet.has(addr) ? 'secondary' : 'ghost';
          activeBtn.textContent = activeSet.has(addr) ? 'Deactivate' : 'Activate';
          activeBtn.title = 'Toggle streaming to this peer';
          activeBtn.disabled = status !== 'accepted';
          activeBtn.addEventListener('click', () => {
            Media.setTargetActive(node.id, addr, !activeSet.has(addr));
            renderTargets();
            refreshNodeTransport(node.id);
          });
          actions.appendChild(activeBtn);

          const removeBtn = document.createElement('button');
          removeBtn.type = 'button';
          removeBtn.className = 'ghost';
          removeBtn.textContent = '🗑';
          removeBtn.title = 'Remove target';
          removeBtn.addEventListener('click', async () => {
            await Media.removeTarget(node.id, addr);
            renderTargets();
            refreshNodeTransport(node.id);
          });
          actions.appendChild(removeBtn);

          row.appendChild(actions);
          list.appendChild(row);
        });
      };

      const unsubscribeMedia = Media.subscribe(node.id, () => {
        renderTargets();
        refreshNodeTransport(node.id);
      });

      const previousSubscription = settingsModelSubscription;
      settingsModelSubscription = () => {
        if (typeof unsubscribeMedia === 'function') {
          try { unsubscribeMedia(); } catch (err) { /* ignore */ }
        }
        if (typeof previousSubscription === 'function') {
          try { previousSubscription(); } catch (err) { /* ignore */ }
        }
      };

      renderTargets();
    }

    if (node.type === 'Meshtastic') {
      const meshLabel = document.createElement('label');
      meshLabel.textContent = 'Peers';
      fields.appendChild(meshLabel);
      Meshtastic.renderSettings?.(node.id, fields);
    }
    if (node.type === 'WebSerial') {
      WebSerial.renderSettings?.(node.id, fields);
    }

    convertBooleanSelectsIn(fields);
    if (typeof memoryManagerSetup === 'function') memoryManagerSetup();
    if (!fields._boolToggleObserver) {
      const observer = new MutationObserver((mutations) => {
        for (const m of mutations) {
          m.addedNodes?.forEach((node) => {
            if (!(node instanceof HTMLElement)) return;
            convertBooleanSelectsIn(node);
          });
        }
      });
      observer.observe(fields, { childList: true, subtree: true });
      fields._boolToggleObserver = observer;
    }
    form.dataset.nodeId = nodeId;
    if (node.type === 'NoClipBridge') {
      const resolvedRoom = NoClip?.resolveRoomName?.(cfg.room) || 'auto';
      help.innerHTML = `${GraphTypes[node.type].title} • ${nodeId}<br><span class="muted">Auto room resolves to <code>${resolvedRoom}</code>. Set an override to join a different discovery room.</span>`;
    } else {
      help.textContent = `${GraphTypes[node.type].title} • ${nodeId}`;
    }
    if (node.type === 'MCP') {
      const testRow = document.createElement('div');
      testRow.className = 'row';
      testRow.style.marginTop = '8px';
      const statusBtn = document.createElement('button');
      statusBtn.className = 'ghost';
      statusBtn.type = 'button';
      statusBtn.textContent = 'Refresh status';
      statusBtn.addEventListener('click', (e) => {
        e.preventDefault();
        const res = MCP.refresh(node.id, { quiet: false });
        if (res && typeof res.catch === 'function') res.catch(() => { });
      });
      testRow.appendChild(statusBtn);
      const queryBtn = document.createElement('button');
      queryBtn.className = 'ghost';
      queryBtn.type = 'button';
      queryBtn.textContent = 'Emit test query';
      queryBtn.addEventListener('click', (e) => {
        e.preventDefault();
        const payload = { text: 'Diagnostics', system: 'Provide a connectivity status summary.' };
        const res = MCP.onQuery(node.id, payload);
        if (res && typeof res.catch === 'function') res.catch(() => { });
      });
      testRow.appendChild(queryBtn);
      fields.appendChild(testRow);
    }
    if (node.type === 'NknDM') {
      const sendBtn = fields.querySelector('[data-nkndm-settings-send]');
      const pingBtn = fields.querySelector('[data-nkndm-settings-ping]');
      if (sendBtn && !sendBtn._nkndmBound) {
        sendBtn.addEventListener('click', (e) => {
          e.preventDefault();
          const input = fields.querySelector('input[name="address"]');
          const spinner = fields.querySelector('[data-nkndm-settings-spinner]');
          const statusEl = fields.querySelector('[data-nkndm-settings-status]');
          const raw = input?.value?.trim?.() || '';
          const normalized = raw ? (raw.toLowerCase().startsWith('graph.') ? raw.replace(/^graph\./i, 'graph.') : `graph.${raw.replace(/^graph\./i, '')}`) : '';
          if (input && normalized !== raw) {
            input.value = normalized;
            input.dispatchEvent(new Event('input', { bubbles: true }));
          }
          NodeStore.update(nodeId, { type: 'NknDM', address: normalized });
          if (spinner) spinner.classList.remove('hidden');
          if (statusEl) {
            statusEl.textContent = '…';
            statusEl.classList.add('pending');
            statusEl.classList.remove('ok', 'err');
          }
          NknDM.refresh(nodeId);
          NknDM.sendHandshake(nodeId, () => {
            if (spinner) spinner.classList.add('hidden');
            NknDM.refresh(nodeId);
          });
        });
        sendBtn._nkndmBound = true;
      }
      if (pingBtn && !pingBtn._nkndmBound) {
        pingBtn.addEventListener('click', (e) => {
          e.preventDefault();
          NknDM.sendProbe(nodeId);
        });
        pingBtn._nkndmBound = true;
      }
      NknDM.refresh(nodeId);
    }
    modal.classList.remove('hidden');
    modal.setAttribute('aria-hidden', 'false');
  }

  function closeSettings() {
    const modal = qs('#settingsModal');
    const form = qs('#settingsForm');
    delete form.dataset.nodeId;
    modal.classList.add('hidden');
    modal.setAttribute('aria-hidden', 'true');
    if (typeof settingsModelSubscription === 'function') {
      try { settingsModelSubscription(); } catch (err) { /* ignore */ }
      settingsModelSubscription = null;
    }
    closeQrScanner();
  }

  function runNodeConfigSideEffects(node, cfg, { quiet = false } = {}) {
    if (!node) return;
    const nodeId = node.id;
    const nodeType = node.type;
    const config = cfg && typeof cfg === 'object' ? cfg : {};
    const typeInfo = GraphTypes[nodeType];
    const wasmMode = isWasmNode(nodeType, config);
    if (typeInfo?.supportsNkn) {
      if (wasmMode) {
        clearNodeRelayState(nodeId);
        if (typeInfo.relayKey === 'relay') updateTransportButton();
        refreshNodeTransport(nodeId);
      } else {
        const relayValue = getRelayValueFromConfig(config, typeInfo);
        if (relayValue) ensurePendingRelayState(nodeId, DEFAULT_PENDING_MESSAGE);
        else clearNodeRelayState(nodeId);
        if (typeInfo.relayKey === 'relay') updateTransportButton();
        refreshNodeTransport(nodeId);
      }
    }
    if (nodeType === 'ASR') {
      ASR.refreshConfig?.(nodeId, { wasm: wasmMode });
    }
    if (nodeType === 'TTS') {
      TTS.refreshUI(nodeId);
      TTS.refreshConfig?.(nodeId, { wasm: wasmMode });
    }
    if (nodeType === 'TextInput') {
      initTextInputNode(node);
    }
    if (nodeType === 'Template') {
      const leftSide = node.el?.querySelector('.side.left');
      setupTemplateNode(node, leftSide);
      pullTemplateInputs(nodeId);
    }
    if (nodeType === 'Meshtastic') {
      refreshMeshtasticPorts(nodeId);
      Meshtastic.refresh?.(nodeId);
    }
    if (nodeType === 'LogicGate') {
      const leftSide = node.el?.querySelector('.side.left');
      setupLogicGateNode(node, leftSide);
    }
    if (nodeType === 'NknDM') {
      initNknDmNode(node);
    }
    if (nodeType === 'NoClipBridge') {
      initNoClipNode(node);
    }
    if (nodeType === 'MCP') {
      const res = MCP.refresh(nodeId, { quiet: true });
      if (res && typeof res.catch === 'function') res.catch(() => { });
    }
    if (nodeType === 'MediaStream') {
      Media.refresh(nodeId);
    }
    if (nodeType === 'WebScraper') {
      WebScraper.refresh?.(nodeId);
    }
    if (nodeType === 'LLM') {
      refreshLlmControls(nodeId);
    }
    if (nodeType === 'Payments') {
      Payments.refresh?.(nodeId);
    }
    if (nodeType === 'FaceLandmarks') {
      Promise.resolve(Vision?.Face?.refresh?.(nodeId, config)).catch(() => {});
    }
    if (nodeType === 'PoseLandmarks') {
      Promise.resolve(Vision?.Pose?.refresh?.(nodeId, config)).catch(() => {});
    }
    scheduleModelPrefetch(nodeId, nodeType, quiet ? 0 : 150);
  }

  function applyNodeConfigSnapshot(nodeId, nodeType, config, { quiet = false } = {}) {
    if (!nodeId || !nodeType) return;
    const normalized = config && typeof config === 'object' ? deepClone(config) : {};
    NodeStore.saveCfg(nodeId, nodeType, normalized);
    const node = WS.nodes.get(nodeId);
    if (node) {
      runNodeConfigSideEffects(node, normalized, { quiet: true });
    }
    if (!quiet) setBadge('Settings applied');
  }

  function bindLogControls() {
    const toggleBtn = qs('#logToggleBtn');
    const logWrap = qs('#logdata');
    const undoBtn = qs('#undoBtn');
    const redoBtn = qs('#redoBtn');
    if (!logWrap || !toggleBtn) {
      History.setUpdate(() => {});
      return;
    }
const showLogsIcon = '<img src="img/chevron-up.svg" alt="" class="icon inverted">';
const hideLogsIcon = '<img src="img/chevron-down.svg" alt="" class="icon inverted">';
    const applyCollapsed = (collapsed, { persist = true } = {}) => {
      if (collapsed) {
        logWrap.classList.add('collapsed');
        toggleBtn.innerHTML = showLogsIcon;
        toggleBtn.setAttribute('aria-pressed', 'false');
      } else {
        logWrap.classList.remove('collapsed');
        toggleBtn.innerHTML = hideLogsIcon;
        toggleBtn.setAttribute('aria-pressed', 'true');
      }
      if (persist) {
        try { LS.set(LOG_COLLAPSED_KEY, collapsed); } catch (_) { /* ignore */ }
      }
    };

    const storedCollapsed = LS.get(LOG_COLLAPSED_KEY, false);
    applyCollapsed(Boolean(storedCollapsed), { persist: false });

    toggleBtn.addEventListener('click', (e) => {
      e.preventDefault();
      const next = !logWrap.classList.contains('collapsed');
      applyCollapsed(next);
    });

    if (undoBtn) {
      undoBtn.addEventListener('click', (e) => {
        e.preventDefault();
        History.undo();
      });
    }

    if (redoBtn) {
      redoBtn.addEventListener('click', (e) => {
        e.preventDefault();
        History.redo();
      });
    }

    History.setUpdate(({ canUndo, canRedo }) => {
      if (undoBtn) undoBtn.disabled = !canUndo;
      if (redoBtn) redoBtn.disabled = !canRedo;
    });
  }

  function bindModal() {
    qs('#closeSettings')?.addEventListener('click', closeSettings);
    qs('#cancelSettings')?.addEventListener('click', closeSettings);
    qs('#closeBackdrop')?.addEventListener('click', closeSettings);
    qs('#saveSettings')?.addEventListener('click', (e) => {
      e.preventDefault();
      const form = qs('#settingsForm');
      const nodeId = form.dataset.nodeId;
      if (!nodeId) return;
      const node = WS.nodes.get(nodeId);
      if (!node) return;
      const fd = new FormData(form);
      const patch = {};
      for (const [k, v] of fd.entries()) {
        const schema = (GraphTypes[node.type].schema || []).find((s) => s.key === k);
        if (!schema) {
          patch[k] = String(v);
          continue;
        }
        if (schema.type === 'logicRules') {
          let parsed = [];
          try {
            parsed = JSON.parse(v);
          } catch (_) {
            parsed = [];
          }
          patch[k] = sanitizeLogicRules(parsed);
          continue;
        }
        if (schema.type === 'fieldMap') {
          let parsed = [];
          try {
            const raw = JSON.parse(v);
            if (Array.isArray(raw)) parsed = raw;
          } catch (_) {
            parsed = [];
          }
          patch[k] = parsed.map((entry) => {
            const normalized = entry && typeof entry === 'object' ? entry : {};
            return {
              key: typeof normalized.key === 'string' ? normalized.key : '',
              mode: typeof normalized.mode === 'string' ? normalized.mode : 'literal',
              value: normalized.value !== undefined && normalized.value !== null ? String(normalized.value) : ''
            };
          });
          continue;
        }
        if (schema.type === 'filterList') {
          let parsed = [];
          try {
            const raw = JSON.parse(v);
            if (Array.isArray(raw)) parsed = raw;
          } catch (_) {
            parsed = [];
          }
          const normalized = [];
          const seen = new Set();
          parsed.forEach((value) => {
            let text = String(value ?? '');
            try {
              text = text.normalize('NFKC');
            } catch (err) {
              // ignore
            }
            const trimmed = text.trim();
            if (!trimmed || seen.has(trimmed)) return;
            seen.add(trimmed);
            normalized.push(trimmed);
          });
          patch[k] = normalized;
          continue;
        }
        if (schema.type === 'number' || schema.type === 'range') {
          const num = Number(String(v).trim());
          patch[k] = Number.isFinite(num) ? num : undefined;
        } else if (schema.type === 'select') {
          if (v === 'true' || v === 'false') patch[k] = v === 'true';
          else if (/^\d+$/.test(String(v))) patch[k] = Number(v);
          else patch[k] = String(v);
        } else {
          patch[k] = String(v);
        }
      }
      if (patch.model && MODEL_PROVIDERS[node.type]?.getModelInfo) {
        const info = MODEL_PROVIDERS[node.type].getModelInfo(nodeId, patch.model);
        if (info && node.type === 'LLM') {
          patch.capabilities = info.capabilities || [];
        }
      }
      patch.type = node.type;
      const prevConfigObj = NodeStore.load(nodeId);
      const beforeCfg = prevConfigObj?.config ? deepClone(prevConfigObj.config) : {};
      const updatedCfgRaw = NodeStore.update(nodeId, patch);
      const updatedCfg = deepClone(updatedCfgRaw);
      runNodeConfigSideEffects(node, updatedCfg, { quiet: false });
      if (!History.isSilent()) {
        try {
          const beforeJson = JSON.stringify(beforeCfg ?? {});
          const afterJson = JSON.stringify(updatedCfg ?? {});
          if (beforeJson !== afterJson) {
            History.push({
              type: 'node.config',
              nodeId,
              nodeType: node.type,
              before: beforeCfg,
              after: updatedCfg
            });
          }
        } catch (_) {
          History.push({
            type: 'node.config',
            nodeId,
            nodeType: node.type,
            before: beforeCfg,
            after: updatedCfg
          });
        }
      }
      setBadge('Settings saved');
      closeSettings();
    });
  }

  function addNode(type, x = 70, y = 70, opts = {}) {
    const id = opts.id || uid();
    const node = {
      id,
      type,
      x,
      y,
      sizeLocked: Boolean(opts.sizeLocked)
    };
    if (Number.isFinite(opts.width)) node.w = opts.width;
    if (Number.isFinite(opts.height)) node.h = opts.height;
    NodeStore.ensure(id, type);
    if (opts.config && typeof opts.config === 'object') {
      const cloned = deepClone(opts.config);
      NodeStore.saveObj(id, { id, type, config: cloned });
    }
    node.el = makeNodeEl(node);
    WS.canvas.appendChild(node.el);
    WS.nodes.set(id, node);
    scheduleModelPrefetch(id, type, 300);
    if (type === 'TTS') requestAnimationFrame(() => TTS.refreshUI(id));
    if (type === 'TextInput') requestAnimationFrame(() => initTextInputNode(node));
    if (type === 'TextDisplay') requestAnimationFrame(() => initTextDisplayNode(node));
    if (type === 'Template') requestAnimationFrame(() => {
      const leftSide = node.el?.querySelector('.side.left');
      setupTemplateNode(node, leftSide);
      pullTemplateInputs(id);
    });
    if (type === 'Meshtastic') requestAnimationFrame(() => initMeshtasticNode(node));
    if (type === 'LogicGate') requestAnimationFrame(() => {
      const leftSide = node.el?.querySelector('.side.left');
      setupLogicGateNode(node, leftSide);
    });
    if (type === 'ImageInput') requestAnimationFrame(() => initImageInputNode(node));
    if (type === 'NknDM') requestAnimationFrame(() => initNknDmNode(node));
    if (type === 'FaceLandmarks') requestAnimationFrame(() => initFaceLandmarksNode(node));
    if (type === 'MCP') requestAnimationFrame(() => MCP.init(id));
    if (type === 'MediaStream') {
      requestAnimationFrame(() => Media.init(id));
      try {
        node._mediaSubscription = Media.subscribe(id, () => {
          refreshNodeTransport(id);
        });
      } catch (err) {
        // ignore subscription errors
      }
    }
    if (type === 'Orientation') requestAnimationFrame(() => Orientation.init(id));
    if (type === 'Location') requestAnimationFrame(() => Location.init(id));
    if (type === 'FileTransfer') requestAnimationFrame(() => FileTransfer.init(id));
    if (type === 'Payments') requestAnimationFrame(() => initPaymentsNode(node));
    if (type === 'WebScraper') requestAnimationFrame(() => WebScraper.init(id));
    if (type === 'FaceLandmarks') requestAnimationFrame(() => Vision?.Face?.init?.(id));
    if (type === 'PoseLandmarks') requestAnimationFrame(() => Vision?.Pose?.init?.(id));
    if (opts.select) setSelectedNode(id, { focus: true });
    if (!History.isSilent()) {
      const snapshot = historySnapshotNode(id);
      if (snapshot) History.push({ type: 'node.add', node: snapshot });
    }
    saveGraph();
    requestRedraw();
    return node;
  }

  function removeNode(nodeId) {
    const node = WS.nodes.get(nodeId);
    if (!node) return;
    const nodeSnapshot = History.isSilent() ? null : historySnapshotNode(nodeId);
    const connectedWires = WS.wires.filter((w) => w.from.node === nodeId || w.to.node === nodeId);
    const wireSnapshots = (!History.isSilent() && connectedWires.length)
      ? connectedWires.map((w) => cloneWireForHistory(w)).filter(Boolean)
      : [];
    deselectNode(nodeId);
    if (node._ro) {
      try {
        node._ro.disconnect();
      } catch (err) {
        // ignore
      }
      delete node._ro;
    }
    if (node._mo) {
      try {
        node._mo.disconnect();
      } catch (err) {
        // ignore
      }
      delete node._mo;
    }
    if (MODEL_PROVIDERS[node.type]?.dispose) {
      try { MODEL_PROVIDERS[node.type].dispose(nodeId); } catch (err) { /* ignore */ }
    }
    if (typeof node._mediaSubscription === 'function') {
      try { node._mediaSubscription(); } catch (err) { /* ignore */ }
      delete node._mediaSubscription;
    }
    if (node.type === 'NknDM') {
      NknDM.dispose(nodeId);
    }
    if (node.type === 'NoClipBridge') {
      if (typeof node._noclipSyncUnsub === 'function') {
        try { node._noclipSyncUnsub(); } catch (err) { /* ignore */ }
        delete node._noclipSyncUnsub;
      }
      NoClip?.dispose?.(nodeId);
      NoClip?.attachNodeElement?.(nodeId, null);
    }
    if (node.type === 'MCP') {
      MCP.dispose(nodeId);
    }
    if (node.type === 'MediaStream') {
      Media.dispose(nodeId);
    }
    if (node.type === 'FileTransfer') {
      FileTransfer.dispose(nodeId);
    }
    if (node.type === 'Payments') {
      Payments.dispose?.(nodeId);
    }
    if (node.type === 'Orientation') {
      Orientation.dispose(nodeId);
    }
    if (node.type === 'Location') {
      Location.dispose(nodeId);
    }
    if (node.type === 'Meshtastic') {
      Meshtastic.dispose?.(nodeId);
    }
    if (node.type === 'WebSerial') {
      WebSerial.dispose?.(nodeId);
    }
    if (node.type === 'WebScraper') {
      WebScraper.dispose?.(nodeId);
    }
    if (node.type === 'FaceLandmarks') {
      FaceViewer?.dispose?.(nodeId);
      Vision?.Face?.dispose?.(nodeId);
    }
    if (node.type === 'PoseLandmarks') {
      Vision?.Pose?.dispose?.(nodeId);
    }

    connectedWires.forEach((w) => detachWire(w));
    node.el.remove();
    WS.nodes.delete(nodeId);
    NodeStore.erase(nodeId);
    syncRouterFromWS();
    saveGraph();
    if (!History.isSilent() && nodeSnapshot) {
      History.push({ type: 'node.remove', node: nodeSnapshot, wires: wireSnapshots });
    }
    requestRedraw();
    if (ASR.ownerId === nodeId) ASR.stop();
  }

  function bindToolbar() {
    const menuToggle = qs('#nodeMenuToggle');
    const menuList = qs('#nodeMenuList');

    const nodeButtons = [
      { type: 'ASR', label: 'ASR Node', x: 90, y: 120 },
      { type: 'LLM', label: 'LLM Node', x: 360, y: 160 },
      { type: 'TTS', label: 'TTS Node', x: 650, y: 200 },
      { type: 'TextInput', label: 'Text Input', x: 420, y: 120 },
      { type: 'TextDisplay', label: 'Text Display', x: 540, y: 180 },
      { type: 'Template', label: 'Template', x: 540, y: 220 },
      { type: 'LogicGate', label: 'Logic Gate', x: 520, y: 260 },
      { type: 'ImageInput', label: 'Image Input', x: 600, y: 220 },
      { type: 'NknDM', label: 'NKN DM', x: 720, y: 140 },
      { type: 'NoClipBridge', label: 'NoClip Bridge', x: 760, y: 140 },
      { type: 'FileTransfer', label: 'File Transfer', x: 780, y: 200 },
      { type: 'Payments', label: 'Payments', x: 820, y: 260 },
      { type: 'MCP', label: 'MCP Server', x: 260, y: 220 },
      { type: 'MediaStream', label: 'Media Stream', x: 320, y: 260 },
      { type: 'WebScraper', label: 'Web Scraper', x: 380, y: 220 },
      { type: 'FaceLandmarks', label: 'Face Viewer', x: 360, y: 300 },
      { type: 'PoseLandmarks', label: 'Pose Landmarks', x: 360, y: 360 },
      { type: 'Meshtastic', label: 'Meshtastic', x: 260, y: 120 },
      { type: 'WebSerial', label: 'Web Serial', x: 320, y: 120 },
      { type: 'Orientation', label: 'Orientation', x: 220, y: 260 },
      { type: 'Location', label: 'Location', x: 200, y: 320 }
    ];

    if (menuList) {
      menuList.innerHTML = '';

      const createPreview = (label) => {
        const el = document.createElement('div');
        el.className = 'node-drag-preview';
        el.textContent = label;
        el.style.position = 'fixed';
        el.style.pointerEvents = 'none';
        el.style.zIndex = '999';
        el.style.padding = '8px 12px';
        el.style.borderRadius = '8px';
        el.style.background = 'rgba(20, 24, 30, 0.92)';
        el.style.color = '#fff';
        el.style.fontSize = '0.85rem';
        el.style.boxShadow = '0 10px 24px rgba(0, 0, 0, 0.35)';
        el.style.transform = 'translate(-50%, -50%)';
        el.style.userSelect = 'none';
        return el;
      };

      const spawnNodeAtPoint = (type, point) => {
        const pt = { x: Math.round(point.x), y: Math.round(point.y) };
        const node = addNode(type, pt.x, pt.y, { select: true });
        if (node?.el) {
          positionNodeCentered(node, pt);
        }
        return node;
      };

      const attachInteractions = (btn, def) => {
        const DRAG_THRESHOLD = 6;
        let pointerState = null;
        let suppressClick = false;

        const cleanupPreview = () => {
          if (pointerState?.preview) {
            try {
              pointerState.preview.remove();
            } catch (err) {
              // ignore removal issues
            }
          }
          pointerState = null;
        };

        const start = (ev) => {
          if (ev.button !== undefined && ev.button !== 0) return;
          ev.preventDefault();
          ev.stopPropagation();
          suppressClick = false;
          pointerState = {
            pointerId: ev.pointerId,
            type: def.type,
            label: def.label,
            startX: ev.clientX,
            startY: ev.clientY,
            dragging: false,
            preview: null
          };
          btn.setPointerCapture?.(ev.pointerId);
        };

        const move = (ev) => {
          if (!pointerState || ev.pointerId !== pointerState.pointerId) return;
          const dx = ev.clientX - pointerState.startX;
          const dy = ev.clientY - pointerState.startY;
          if (!pointerState.dragging) {
            if (Math.hypot(dx, dy) < DRAG_THRESHOLD) return;
            pointerState.dragging = true;
            pointerState.preview = createPreview(def.label);
            document.body.appendChild(pointerState.preview);
          }
          if (pointerState.preview) {
            pointerState.preview.style.left = `${ev.clientX}px`;
            pointerState.preview.style.top = `${ev.clientY}px`;
          }
        };

        const finish = (ev, cancelled = false) => {
          if (!pointerState || ev.pointerId !== pointerState.pointerId) return;
          ev.preventDefault();
          btn.releasePointerCapture?.(ev.pointerId);
          const wasDragging = pointerState.dragging;
          const dropClientX = ev.clientX;
          const dropClientY = ev.clientY;
          cleanupPreview();
          suppressClick = true;
          if (cancelled) return;
          if (wasDragging) {
            const rootRect = WS.root?.getBoundingClientRect?.();
            if (
              rootRect &&
              (dropClientX < rootRect.left ||
                dropClientX > rootRect.right ||
                dropClientY < rootRect.top ||
                dropClientY > rootRect.bottom)
            ) {
              menuList.classList.add('hidden');
              return;
            }
            const pt = clientToWorkspace(dropClientX, dropClientY);
            spawnNodeAtPoint(def.type, pt);
          } else {
            const center = currentViewCenter();
            spawnNodeAtPoint(def.type, center);
          }
          menuList.classList.add('hidden');
        };

        btn.addEventListener('pointerdown', start, { passive: false });
        btn.addEventListener('pointermove', move, { passive: false });
        btn.addEventListener('pointerup', (ev) => finish(ev, false), { passive: false });
        btn.addEventListener('pointercancel', (ev) => finish(ev, true), { passive: false });

        btn.addEventListener('keydown', (ev) => {
          if (ev.key !== 'Enter' && ev.key !== ' ') return;
          ev.preventDefault();
          menuList.classList.add('hidden');
          const center = currentViewCenter();
          spawnNodeAtPoint(def.type, center);
        });

        btn.addEventListener('click', (ev) => {
          if (suppressClick) {
            suppressClick = false;
            return;
          }
          ev.preventDefault();
          menuList.classList.add('hidden');
          const center = currentViewCenter();
          spawnNodeAtPoint(def.type, center);
        });
      };

      nodeButtons.forEach((btnDef) => {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'ghost node-menu-entry';
        btn.textContent = btnDef.label;
        attachInteractions(btn, btnDef);
        menuList.appendChild(btn);
      });
    }

    if (menuToggle && menuList) {
      menuToggle.addEventListener('click', (e) => {
        e.stopPropagation();
        menuList.classList.toggle('hidden');
      });

      document.addEventListener('click', (e) => {
        if (!menuList.classList.contains('hidden')) {
          const target = e.target;
          if (!menuList.contains(target) && target !== menuToggle) {
            menuList.classList.add('hidden');
          }
        }
      });
    }

  }

  function cancelLinking() {
    if (WS.portSel?.el) WS.portSel.el.classList.remove('sel');
    WS.portSel = null;
    clearTempLink();
    setBadge('Linking cancelled');
  }

  function bindWorkspaceCancels() {
    window.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && (WS.portSel || WS.drag)) cancelLinking();
    });
    WS.el.addEventListener('pointerdown', (e) => {
      if (e.target === WS.el && (WS.portSel || WS.drag)) cancelLinking();
    });
    WS.svg.addEventListener('pointerdown', (e) => {
      if (e.target === WS.svg && (WS.portSel || WS.drag)) cancelLinking();
    });
  }

  Meshtastic?.setRefreshPortsHandler?.((nodeId) => refreshMeshtasticPorts(nodeId));

  function bindNodeSelectionControls() {
    document.addEventListener('pointermove', updatePointerLocation, { passive: true });
    document.addEventListener('pointerdown', updatePointerLocation, { passive: true });

    window.addEventListener('keydown', (e) => {
      if (e.defaultPrevented) return;
      const key = typeof e.key === 'string' ? e.key.toLowerCase() : '';
      const modifier = e.metaKey || e.ctrlKey;
      if (!modifier) return;
      if (key === 'Escape') return;
      if (isEditableTarget(e.target)) return;

      if (key === 'z') {
        e.preventDefault();
        if (e.shiftKey) History.redo();
        else History.undo();
        return;
      }

      if (key === 'y') {
        e.preventDefault();
        History.redo();
        return;
      }

      if (key === 's') {
        e.preventDefault();
        if (e.shiftKey) saveWorkspaceAsInteractive();
        else {
          const flowSave = getFlowSaveHandler();
          if (flowSave) flowSave();
          else downloadWorkspaceFile();
        }
        return;
      }

      if (key === 'a') {
        e.preventDefault();
        selectAllNodes();
        return;
      }

      if (key === 'i') {
        e.preventDefault();
        openGraphImportDialog();
        return;
      }

      if (key === 'd') {
        if (!WS.selectedNodeId) return;
        e.preventDefault();
        duplicateNode(WS.selectedNodeId, currentViewCenter(), { select: true, badge: true });
        return;
      }

      if (key === 'c') {
        if (!WS.selectedNodeId) return;
        e.preventDefault();
        const snapshot = snapshotNode(WS.selectedNodeId);
        if (!snapshot) return;
        WS.clipboard = snapshot;
        setBadge('Node copied');
        return;
      }

      if (key === 'v') {
        if (!WS.clipboard) return;
        e.preventDefault();
        const dropPoint = workspacePointFromPointer();
        const node = duplicateNodeFromSnapshot(WS.clipboard, dropPoint, { select: true });
        if (node) setBadge('Node pasted');
      }
    });
  }

  function clamp(v, min, max) {
    return Math.max(min, Math.min(max, v));
  }

  function zoomAt(clientX, clientY, dz) {
    const rect = WS.root?.getBoundingClientRect?.() || document.body.getBoundingClientRect();
    const mx = clientX - rect.left;
    const my = clientY - rect.top;
    const s0 = WS.view.scale;
    const s1 = clamp(s0 * Math.exp(dz), 0.4, 3.0);
    if (s1 === s0) return;
    const wx = (mx - WS.view.x) / s0;
    const wy = (my - WS.view.y) / s0;
    WS.view.x = mx - wx * s1;
    WS.view.y = my - wy * s1;
    WS.view.scale = s1;
    applyViewTransform();
  }

  function _hasScrollableY(el) {
    const cs = getComputedStyle(el);
    return (cs.overflowY === 'auto' || cs.overflowY === 'scroll') && el.scrollHeight > el.clientHeight;
  }

  function _hasScrollableX(el) {
    const cs = getComputedStyle(el);
    return (cs.overflowX === 'auto' || cs.overflowX === 'scroll') && el.scrollWidth > el.clientWidth;
  }

  function _nodeCanConsumeWheel(target, dx, dy, rootStop) {
    let cur = target instanceof Element ? target : null;
    while (cur && cur !== rootStop) {
      const isNode = cur.classList?.contains?.('node');
      if (_hasScrollableY(cur)) {
        const top = cur.scrollTop;
        const max = cur.scrollHeight - cur.clientHeight;
        if ((dy < 0 && top > 0) || (dy > 0 && top < max - 1)) return true;
      }
      if (_hasScrollableX(cur)) {
        const left = cur.scrollLeft;
        const maxX = cur.scrollWidth - cur.clientWidth;
        if ((dx < 0 && left > 0) || (dx > 0 && left < maxX - 1)) return true;
      }
      if (isNode) {
        cur = cur.parentElement;
        break;
      }
      cur = cur.parentElement;
    }
    return false;
  }

  function bindViewportControls() {
    let pan = null;
    const touches = new Map();
    let pinch = null;

    const updateTouch = (id, x, y) => {
      if (touches.has(id)) touches.set(id, { x, y });
    };

    const removeTouch = (id) => {
      touches.delete(id);
      if (pinch && !pinch.ids.every((pid) => touches.has(pid))) pinch = null;
    };

    const maybeStartPinch = () => {
      if (pinch || touches.size < 2 || WS.drag) return;
      const entries = Array.from(touches.entries()).slice(0, 2);
      if (entries.length < 2) return;
      const [[idA, a], [idB, b]] = entries;
      const dist = Math.max(10, Math.hypot(a.x - b.x, a.y - b.y));
      pinch = { ids: [idA, idB], startDist: dist, startScale: WS.view.scale };
      pan = null;
    };

    const onPanDown = (e) => {
      const hitNode = e.target.closest('.node');
      const hitPort = e.target.closest('.wp-port');
      const hitResize = e.target.closest('[data-resize]');
      const hitWire = e.target.closest('path[data-id]');
      const nodeId = hitNode?.dataset?.id;
      if (nodeId) setSelectedNode(nodeId);
      const isBackground = !hitNode && !hitPort && !hitResize && !hitWire && (
        e.target === WS.root ||
        e.target === WS.el ||
        e.target === WS.svg ||
        e.target.closest('#workspace') ||
        e.target.closest('#linksSvg')
      );
      if (isBackground) clearSelectedNode();
      if (e.pointerType === 'touch') {
        touches.set(e.pointerId, { x: e.clientX, y: e.clientY });
        maybeStartPinch();
        if (pinch) {
          e.preventDefault();
          return;
        }
        if (!isBackground) return;
        e.preventDefault();
      }
      if (!isBackground || pinch) return;
      pan = { id: e.pointerId, sx: e.clientX, sy: e.clientY, ox: WS.view.x, oy: WS.view.y, pointerType: e.pointerType };
      if (e.pointerType !== 'touch') WS.root?.setPointerCapture?.(e.pointerId);
      if (e.pointerType === 'mouse') document.body.style.cursor = 'grabbing';
    };

    const onPanMove = (e) => {
      if (touches.has(e.pointerId)) {
        updateTouch(e.pointerId, e.clientX, e.clientY);
        if (!pinch) maybeStartPinch();
      }
      if (pinch) {
        const pts = pinch.ids.map((id) => touches.get(id)).filter(Boolean);
        if (pts.length < 2) {
          pinch = null;
          return;
        }
        const dist = Math.max(10, Math.hypot(pts[0].x - pts[1].x, pts[0].y - pts[1].y));
        const target = clamp(pinch.startScale * (dist / pinch.startDist), 0.4, 3.0);
        const current = WS.view.scale;
        if (target > 0 && Math.abs(target - current) > 1e-3) {
          const dz = Math.log(target / current);
          zoomAt((pts[0].x + pts[1].x) / 2, (pts[0].y + pts[1].y) / 2, dz);
        }
        return;
      }
      if (!pan || pan.id !== e.pointerId) return;
      const dx = e.clientX - pan.sx;
      const dy = e.clientY - pan.sy;
      WS.view.x = pan.ox + dx;
      WS.view.y = pan.oy + dy;
      applyViewTransform();
      saveGraph();
    };

    const onPanUp = (e) => {
      if (pan && pan.id === e.pointerId) {
        pan = null;
        if (e.pointerType === 'mouse') document.body.style.cursor = '';
      }
      removeTouch(e.pointerId);
    };

    const onPanCancel = (e) => {
      if (pan && pan.id === e.pointerId) {
        pan = null;
        if (e.pointerType === 'mouse') document.body.style.cursor = '';
      }
      removeTouch(e.pointerId);
    };

    const onWheel = (e) => {
      const dx = e.deltaX || 0;
      const dy = e.deltaY || 0;
      const canScroll = _nodeCanConsumeWheel(e.target, dx, dy, WS.root);
      if (canScroll) return;
      e.preventDefault();
      const intensity = e.deltaMode === 1 ? 0.05 : 0.0015;
      const dz = -dy * intensity;
      const beforeScale = WS.view.scale;
      zoomAt(e.clientX, e.clientY, dz);
      if (Math.abs(WS.view.scale - beforeScale) > 1e-4) saveGraph();
    };

    const rootEl = WS.root || WS.el;
    rootEl.addEventListener('pointerdown', onPanDown, { passive: false });
    rootEl.addEventListener('pointermove', onPanMove, { passive: false });
    rootEl.addEventListener('pointerup', onPanUp, { passive: false });
    rootEl.addEventListener('pointercancel', onPanCancel, { passive: false });
    rootEl.addEventListener('pointerleave', onPanCancel, { passive: false });
    rootEl.addEventListener('wheel', onWheel, { passive: false });
  }

  function init() {
    WS.el = qs('#workspace');
    WS.svg = qs('#linksSvg');
    WS.root = WS.el?.parentElement || document.body;
    WS.canvas = qs('#wsCanvas');
    if (!WS.canvas) {
      WS.canvas = document.createElement('div');
      WS.canvas.id = 'wsCanvas';
      WS.el.appendChild(WS.canvas);
    }
    WS.root.style.position = 'relative';
    if (WS.svg) {
      WS.svg.style.position = 'absolute';
      WS.svg.style.left = '0';
      WS.svg.style.top = '0';
      WS.svg.style.width = '100%';
      WS.svg.style.height = '100%';
    }
    ensureSvgLayer();
    bindToolbar();
    bindModal();
    bindLogControls();
    History.load();
    bindWorkspaceCancels();
    bindNodeSelectionControls();
    bindViewportControls();
    applyViewTransform();
    loadGraph();
    window.addEventListener('resize', () => requestRedraw());
    setTimeout(() => requestRedraw(), 50);
  }

  return {
    init,
    addNode,
    save: saveGraph,
    load: loadGraph,
    exportWorkspace: exportWorkspaceSnapshot,
    importWorkspace: importWorkspaceSnapshot,
    getNode: (id) => WS.nodes.get(id),
    refreshTransportButtons: refreshAllNodeTransport,
    setRelayState: setNodeRelayState,
    getRelayState: getNodeRelayState,
    undo: () => History.undo(),
    redo: () => History.redo(),
    canUndo: () => History.canUndo(),
    canRedo: () => History.canRedo(),
    download: () => downloadWorkspaceFile(),
    openImportDialog: () => openGraphImportDialog(),
    saveAs: () => saveWorkspaceAsInteractive(),
    setFlowSaveHandler,
    refreshMeshtasticPorts,
    openDocs
  };
}

export { createGraph };
