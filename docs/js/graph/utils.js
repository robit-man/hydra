import { convertBooleanSelect } from '../utils.js';

const deepClone = (value) => {
  if (value == null) return value;
  try {
    return JSON.parse(JSON.stringify(value));
  } catch (_) {
    if (Array.isArray(value)) return value.map((entry) => deepClone(entry));
    if (typeof value === 'object') return { ...value };
    return value;
  }
};

const isEditableTarget = (target) => {
  if (!(target instanceof Element)) return false;
  const tag = target.tagName;
  if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return true;
  if (target.isContentEditable) return true;
  return Boolean(target.closest('[contenteditable="true"]'));
};

const cloneWireForHistory = (wireLike) => {
  if (!wireLike) return null;
  const from = wireLike.from || {};
  const to = wireLike.to || {};
  const fromNode = String(from.node || '');
  const toNode = String(to.node || '');
  const fromPort = String(from.port || '');
  const toPort = String(to.port || '');
  if (!fromNode || !toNode || !fromPort || !toPort) return null;
  return {
    from: { node: fromNode, port: fromPort },
    to: { node: toNode, port: toPort }
  };
};

const sanitizeHistoryNode = (data) => {
  if (!data || typeof data !== 'object') return null;
  const id = String(data.id || data.sourceId || '');
  const type = String(data.type || '');
  if (!id || !type) return null;
  return {
    id,
    type,
    x: Number.isFinite(data.x) ? data.x : 0,
    y: Number.isFinite(data.y) ? data.y : 0,
    w: Number.isFinite(data.w) ? data.w : 0,
    h: Number.isFinite(data.h) ? data.h : 0,
    sizeLocked: Boolean(data.sizeLocked),
    config: data.config && typeof data.config === 'object' ? deepClone(data.config) : {}
  };
};

const convertBooleanSelectsIn = (container) => {
  if (!container) return;
  const selects = container instanceof HTMLSelectElement
    ? [container]
    : Array.from(container.querySelectorAll('select'));
  selects.forEach((sel) => convertBooleanSelect(sel));
};

export {
  deepClone,
  isEditableTarget,
  cloneWireForHistory,
  sanitizeHistoryNode,
  convertBooleanSelectsIn
};
