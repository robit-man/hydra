const PERMISSION_DESCRIPTOR = { name: 'local-network' };

const PRIVATE_HOST_SUFFIXES = ['.local', '.lan', '.home', '.internal', '.intranet'];
const LOOPBACK_HOSTS = new Set(['localhost', 'localhost.localdomain', '::1', '0:0:0:0:0:0:0:1']);
const IPV4_PRIVATE = [
  /^127\./,
  /^10\./,
  /^192\.168\./,
  /^169\.254\./,
  /^172\.(1[6-9]|2[0-9]|3[0-1])\./
];

let cachedStatus = null;
let pending = null;
let primed = false;

function supportsLocalNetworkPermission() {
  return typeof navigator !== 'undefined' &&
    navigator.permissions &&
    typeof navigator.permissions.query === 'function';
}

function normalizeHost(host) {
  return String(host || '').trim().replace(/\.$/, '').toLowerCase();
}

function isPrivateHostname(host) {
  const value = normalizeHost(host);
  if (!value) return false;
  if (LOOPBACK_HOSTS.has(value)) return true;
  if (value.startsWith('[') && value.endsWith(']')) {
    return isPrivateHostname(value.slice(1, -1));
  }
  if (value.includes(':')) {
    if (value === '::1' || value === '0:0:0:0:0:0:0:1') return true;
    if (value.startsWith('fe80') || value.startsWith('fd') || value.startsWith('fc')) return true;
    return false;
  }
  for (const re of IPV4_PRIVATE) {
    if (re.test(value)) return true;
  }
  return PRIVATE_HOST_SUFFIXES.some((suffix) => value.endsWith(suffix));
}

function parseUrl(input) {
  if (!input) return null;
  try {
    if (typeof window !== 'undefined' && window.location) {
      return new URL(String(input), window.location.href);
    }
    return new URL(String(input));
  } catch (err) {
    return null;
  }
}

function isLocalNetworkUrl(url) {
  const parsed = parseUrl(url);
  if (!parsed) return false;
  return isPrivateHostname(parsed.hostname || '');
}

async function queryLocalNetworkPermission() {
  if (!supportsLocalNetworkPermission()) {
    cachedStatus = { state: 'unsupported' };
    return cachedStatus;
  }
  try {
    const status = await navigator.permissions.query(PERMISSION_DESCRIPTOR);
    cachedStatus = status;
    return status;
  } catch (err) {
    cachedStatus = { state: 'error', error: err };
    return cachedStatus;
  }
}

async function requestLocalNetworkPermission() {
  if (!supportsLocalNetworkPermission()) {
    cachedStatus = { state: 'unsupported' };
    return cachedStatus;
  }
  if (pending) return pending;
  pending = (async () => {
    if (cachedStatus?.state === 'granted') return cachedStatus;
    if (navigator.permissions && typeof navigator.permissions.request === 'function') {
      try {
        const status = await navigator.permissions.request(PERMISSION_DESCRIPTOR);
        cachedStatus = status;
        return status;
      } catch (err) {
        cachedStatus = { state: 'error', error: err };
        return cachedStatus;
      }
    }
    return queryLocalNetworkPermission();
  })();
  try {
    return await pending;
  } finally {
    pending = null;
  }
}

async function ensureLocalNetworkAccess(options = {}) {
  const { requireGesture = false } = options || {};
  if (cachedStatus?.state === 'granted') return cachedStatus;
  const current = await queryLocalNetworkPermission();
  if (current?.state === 'granted' || current?.state === 'denied') return current;
  if (requireGesture) {
    return requestLocalNetworkPermission();
  }
  return current;
}

async function ensureLocalNetworkAccessForUrl(url, options = {}) {
  if (!isLocalNetworkUrl(url)) {
    return cachedStatus || null;
  }
  return ensureLocalNetworkAccess(options);
}

function primeLocalNetworkRequest(target = document) {
  if (!supportsLocalNetworkPermission()) return;
  if (primed) return;
  primed = true;

  const handler = () => {
    target.removeEventListener('pointerdown', handler);
    target.removeEventListener('keydown', handler);
    requestLocalNetworkPermission().catch(() => {});
  };

  target.addEventListener('pointerdown', handler, { once: true, passive: true });
  target.addEventListener('keydown', handler, { once: true, passive: true });
}

function getCachedLocalNetworkState() {
  return cachedStatus?.state || null;
}

export {
  ensureLocalNetworkAccess,
  ensureLocalNetworkAccessForUrl,
  primeLocalNetworkRequest,
  getCachedLocalNetworkState,
  supportsLocalNetworkPermission,
  isLocalNetworkUrl
};
