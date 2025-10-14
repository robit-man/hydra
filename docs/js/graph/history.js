import { cloneWireForHistory, sanitizeHistoryNode, deepClone } from './utils.js';

const HISTORY_KEY = 'graph.history.v1';
const HISTORY_MAX = 50;

function createHistory({ LS }) {
  let entries = [];
  let index = -1;
  let silent = 0;
  let updateUi = null;
  let undoHandler = null;
  let redoHandler = null;

  const notify = () => {
    updateUi?.({ canUndo: index >= 0, canRedo: index + 1 < entries.length });
  };

  const persist = () => {
    try {
      LS.set(HISTORY_KEY, { entries, index });
    } catch (_) {
      /* ignore storage failures */
    }
  };

  const sanitizeEntry = (raw) => {
    if (!raw || typeof raw !== 'object') return null;
    const type = String(raw.type || '');
    if (!type) return null;
    if (type === 'node.add' || type === 'node.remove') {
      const node = sanitizeHistoryNode(raw.node);
      if (!node) return null;
      const wires = Array.isArray(raw.wires)
        ? raw.wires.map((w) => cloneWireForHistory(w)).filter(Boolean)
        : [];
      return { type, node, wires };
    }
    if (type === 'wire.add' || type === 'wire.remove') {
      const wire = cloneWireForHistory(raw.wire);
      if (!wire) return null;
      return { type, wire };
    }
    if (type === 'node.config') {
      const nodeId = String(raw.nodeId || (raw.node && raw.node.id) || '');
      const nodeType = String(raw.nodeType || (raw.node && raw.node.type) || '');
      if (!nodeId || !nodeType) return null;
      const before = raw.before && typeof raw.before === 'object' ? deepClone(raw.before) : {};
      const after = raw.after && typeof raw.after === 'object' ? deepClone(raw.after) : {};
      return { type, nodeId, nodeType, before, after };
    }
    return null;
  };

  const load = () => {
    const stored = LS.get(HISTORY_KEY, null);
    if (!stored || typeof stored !== 'object') {
      entries = [];
      index = -1;
      notify();
      return;
    }
    const rawEntries = Array.isArray(stored.entries) ? stored.entries : [];
    entries = rawEntries
      .map((entry) => sanitizeEntry(entry))
      .filter(Boolean)
      .slice(-HISTORY_MAX);
    if (typeof stored.index === 'number') {
      index = Math.min(Math.max(Math.floor(stored.index), -1), entries.length - 1);
    } else {
      index = entries.length - 1;
    }
    if (entries.length && index < 0) index = entries.length - 1;
    persist();
    notify();
  };

  const push = (entry) => {
    if (!entry || typeof entry !== 'object') return;
    if (silent) return;
    const sanitized = sanitizeEntry(entry);
    if (!sanitized) return;
    if (index + 1 < entries.length) {
      entries = entries.slice(0, index + 1);
    }
    entries.push(sanitized);
    if (entries.length > HISTORY_MAX) {
      const excess = entries.length - HISTORY_MAX;
      entries.splice(0, excess);
      index = Math.max(index - excess, -1);
    }
    index = entries.length - 1;
    persist();
    notify();
  };

  const runSilent = (fn) => {
    silent += 1;
    try {
      return fn();
    } finally {
      silent -= 1;
    }
  };

  const undo = () => {
    if (index < 0) return false;
    const entry = entries[index];
    runSilent(() => undoHandler?.(entry));
    index -= 1;
    persist();
    notify();
    return true;
  };

  const redo = () => {
    if (index + 1 >= entries.length) return false;
    const entry = entries[index + 1];
    runSilent(() => redoHandler?.(entry));
    index += 1;
    persist();
    notify();
    return true;
  };

  const clear = () => {
    entries = [];
    index = -1;
    persist();
    notify();
  };

  const setUpdate = (fn) => {
    updateUi = typeof fn === 'function' ? fn : null;
    notify();
  };

  const setHandlers = ({ onUndo, onRedo } = {}) => {
    undoHandler = typeof onUndo === 'function' ? onUndo : null;
    redoHandler = typeof onRedo === 'function' ? onRedo : null;
  };

  const silence = (fn) => runSilent(fn);
  const isSilent = () => silent > 0;
  const canUndo = () => index >= 0;
  const canRedo = () => index + 1 < entries.length;
  const emit = () => notify();

  return {
    load,
    push,
    undo,
    redo,
    clear,
    runSilent,
    setUpdate,
    setHandlers,
    silence,
    isSilent,
    canUndo,
    canRedo,
    emit
  };
}

export { createHistory };
