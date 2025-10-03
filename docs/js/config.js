import { LS } from './utils.js';

function generateGraphId() {
  try {
    if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
      return crypto.randomUUID();
    }
  } catch (err) {
    // ignore and fall back
  }
  const rand = Math.random().toString(36).slice(2, 10);
  const ts = Date.now().toString(36);
  return `graph-${ts}-${rand}`;
}

const CFG = LS.get('graph.cfg', {
  transport: 'nkn',
  wires: []
});

if (!CFG.graphId) {
  CFG.graphId = generateGraphId();
  try {
    LS.set('graph.cfg', CFG);
  } catch (err) {
    // ignore store failures
  }
}

if (CFG.transport !== 'nkn') {
  CFG.transport = 'nkn';
  try {
    LS.set('graph.cfg', CFG);
  } catch (err) {
    // ignore store failures
  }
}

function saveCFG() {
  if (!CFG.graphId) CFG.graphId = generateGraphId();
  LS.set('graph.cfg', CFG);
}

export { CFG, saveCFG };
