import { LS } from './utils.js';

const CFG_STORAGE_KEY = 'graph.cfg';
const CFG_PERSIST_ALLOWLIST = new Set([
  'graphId',
  'transport',
  'wires',
  'routerTargetNknAddress',
  'routerAutoResolve',
  'routerAutoResolveIntervalMs',
  'routerLastResolveStatus',
  'routerLastResolveError',
  'routerLastResolvedAt',
  'featureFlags'
]);
const CFG_PERSIST_DROP_KEYS = new Set([
  'routerResolvedEndpoints',
  'routerLastResolveResult'
]);
const CFG_SENSITIVE_KEY_EXACT = new Set([
  'seed',
  'seed_hex',
  'password',
  'passphrase',
  'api_key',
  'apikey',
  'token',
  'access_token',
  'refresh_token',
  'secret',
  'private_key',
  'authorization',
  'bearer'
]);
const CFG_SENSITIVE_KEY_REGEX = /(seed_hex|(?:^|_)seed(?:$|_)|password|passphrase|api_?key|token|secret|private_?key|authorization|bearer)/;
const CFG_SENSITIVE_EXCEPTIONS = new Set([
  'routertargetnknaddress'
]);

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

function cloneCfgValue(value) {
  if (Array.isArray(value)) return value.map((entry) => cloneCfgValue(entry));
  if (value && typeof value === 'object') {
    const out = {};
    for (const [key, entry] of Object.entries(value)) {
      out[key] = cloneCfgValue(entry);
    }
    return out;
  }
  return value;
}

function normalizeCfgKey(key) {
  return String(key || '').trim().toLowerCase().replace(/-/g, '_');
}

function isSensitiveCfgKey(key) {
  const normalized = normalizeCfgKey(key);
  if (!normalized) return false;
  if (CFG_SENSITIVE_EXCEPTIONS.has(normalized)) return false;
  if (CFG_SENSITIVE_KEY_EXACT.has(normalized)) return true;
  return CFG_SENSITIVE_KEY_REGEX.test(normalized);
}

function sanitizeCfgForStorage(rawCfg) {
  const source = rawCfg && typeof rawCfg === 'object' ? rawCfg : {};
  const out = {};
  for (const [key, value] of Object.entries(source)) {
    if (!CFG_PERSIST_ALLOWLIST.has(key)) continue;
    if (CFG_PERSIST_DROP_KEYS.has(key)) continue;
    if (isSensitiveCfgKey(key)) continue;
    out[key] = cloneCfgValue(value);
  }
  return out;
}

const FEATURE_FLAG_DEFAULTS = Object.freeze({
  routerControlPlaneApi: true,
  resolverAutoApply: true,
  cloudflaredManager: false
});

function sanitizeFeatureFlags(rawFlags) {
  const source = rawFlags && typeof rawFlags === 'object' ? rawFlags : {};
  return {
    routerControlPlaneApi: source.routerControlPlaneApi !== false,
    resolverAutoApply: source.resolverAutoApply !== false,
    cloudflaredManager: source.cloudflaredManager === true
  };
}

const CFG_DEFAULTS = {
  transport: 'nkn',
  wires: [],
  routerTargetNknAddress: '',
  routerAutoResolve: false,
  routerAutoResolveIntervalMs: 60000,
  routerLastResolveStatus: 'idle',
  routerLastResolveError: '',
  routerLastResolvedAt: 0,
  featureFlags: sanitizeFeatureFlags(FEATURE_FLAG_DEFAULTS)
};
const CFG = {
  ...CFG_DEFAULTS,
  ...sanitizeCfgForStorage(LS.get(CFG_STORAGE_KEY, {}))
};

if (!CFG.graphId) {
  CFG.graphId = generateGraphId();
  try {
    LS.set(CFG_STORAGE_KEY, sanitizeCfgForStorage(CFG));
  } catch (err) {
    // ignore store failures
  }
}

if (CFG.transport !== 'nkn') {
  CFG.transport = 'nkn';
  try {
    LS.set(CFG_STORAGE_KEY, sanitizeCfgForStorage(CFG));
  } catch (err) {
    // ignore store failures
  }
}

if (CFG.routerTargetNknAddress === undefined) CFG.routerTargetNknAddress = '';
if (CFG.routerAutoResolve === undefined) CFG.routerAutoResolve = false;
if (!Number.isFinite(Number(CFG.routerAutoResolveIntervalMs))) CFG.routerAutoResolveIntervalMs = 60000;
if (!CFG.routerLastResolveStatus) CFG.routerLastResolveStatus = 'idle';
if (CFG.routerLastResolveError === undefined) CFG.routerLastResolveError = '';
if (!Number.isFinite(Number(CFG.routerLastResolvedAt))) CFG.routerLastResolvedAt = 0;
if (!Array.isArray(CFG.wires)) CFG.wires = [];
CFG.featureFlags = sanitizeFeatureFlags(CFG.featureFlags);
try {
  LS.set(CFG_STORAGE_KEY, sanitizeCfgForStorage(CFG));
} catch (err) {
  // ignore store failures
}

function saveCFG() {
  if (!CFG.graphId) CFG.graphId = generateGraphId();
  CFG.featureFlags = sanitizeFeatureFlags(CFG.featureFlags);
  LS.set(CFG_STORAGE_KEY, sanitizeCfgForStorage(CFG));
}

export { CFG, saveCFG };
