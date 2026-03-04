import { createDiscovery as createNatsDiscovery } from './nats.js';
import {
  DEFAULT_INTEROP_CONTRACT,
  INTEROP_ERROR_CODES,
  normalizeInteropEnvelope,
  withInteropContractFields
} from './interopContract.js';

const DEFAULT_SERVERS = ['wss://demo.nats.io:8443'];
const CONNECT_TIMEOUT_MS = 12000;
const MESSAGE_ACK_TIMEOUT_MS = 10000;
const EARTH_RADIUS_M = 6371000;
const MAX_DM_PAYLOAD_CHARS = 42000;
const MESSAGE_ID_TTL_MS = 2 * 60 * 1000;
const CHUNK_ASSEMBLY_TTL_MS = 90 * 1000;
const MAX_MESSAGE_CACHE = 1024;
const MAX_CHUNK_ASSEMBLIES = 64;
const EVENT_BY_TYPE = Object.freeze({
  'interop-contract-get': 'interop.contract.get',
  'hybrid-bridge-state': 'interop.bridge.state',
  'hybrid-bridge-handshake': 'interop.bridge.handshake',
  'hybrid-bridge-handshake-ack': 'interop.bridge.handshake_ack',
  'hybrid-bridge-ack': 'interop.bridge.ack',
  'hybrid-bridge-update': 'interop.asset.pose',
  'hybrid-bridge-resource': 'interop.asset.resource',
  'hybrid-bridge-command': 'interop.asset.command',
  'hybrid-bridge-geometry': 'interop.asset.geometry',
  'smart-object-audio-output': 'interop.asset.media',
  'smart-object-audio': 'interop.asset.media',
  'hybrid-chat': 'interop.chat.message',
  'hybrid-friend-request': 'interop.social.friend_request',
  'hybrid-friend-response': 'interop.social.friend_response',
  'market-service-catalog': 'market.service.catalog',
  'market-service-status': 'market.service.status',
  'market-quote-request': 'market.quote.request',
  'market-quote-result': 'market.quote.result',
  'market-credit-balance': 'market.credit.balance',
  'market-access-ticket-issue': 'market.access.ticket.issue',
  'market-access-ticket-verify': 'market.access.ticket.verify',
  'market-usage-record': 'market.usage.record'
});
const TYPE_BY_EVENT = Object.freeze({
  'interop.contract.get': 'interop-contract-get',
  'interop.bridge.state': 'hybrid-bridge-state',
  'interop.bridge.handshake': 'hybrid-bridge-handshake',
  'interop.bridge.handshake_ack': 'hybrid-bridge-handshake-ack',
  'interop.bridge.ack': 'hybrid-bridge-ack',
  'interop.asset.pose': 'hybrid-bridge-update',
  'interop.asset.resource': 'hybrid-bridge-resource',
  'interop.asset.command': 'hybrid-bridge-command',
  'interop.asset.geometry': 'hybrid-bridge-geometry',
  'interop.asset.media': 'smart-object-audio',
  'interop.chat.message': 'hybrid-chat',
  'interop.social.friend_request': 'hybrid-friend-request',
  'interop.social.friend_response': 'hybrid-friend-response',
  'market.service.catalog': 'market-service-catalog',
  'market.service.status': 'market-service-status',
  'market.quote.request': 'market-quote-request',
  'market.quote.result': 'market-quote-result',
  'market.credit.balance': 'market-credit-balance',
  'market.access.ticket.issue': 'market-access-ticket-issue',
  'market.access.ticket.verify': 'market-access-ticket-verify',
  'market.usage.record': 'market-usage-record'
});
const GEOMETRY_TYPE_HINTS = new Set(['pointcloud', 'geometry', 'mesh', 'glb', 'gltf', 'model']);
const MEDIA_TYPE_HINTS = new Set(['audio', 'video', 'image', 'media']);
const MUTATION_FAMILIES = new Set(['pose', 'geometry', 'media', 'resource']);

const toRadians = (deg) => (deg * Math.PI) / 180;

const haversineDistance = (aLat, aLon, bLat, bLon) => {
  if (![aLat, aLon, bLat, bLon].every(Number.isFinite)) return Number.POSITIVE_INFINITY;
  const dLat = toRadians(bLat - aLat);
  const dLon = toRadians(bLon - aLon);
  const lat1 = toRadians(aLat);
  const lat2 = toRadians(bLat);
  const sinLat = Math.sin(dLat / 2);
  const sinLon = Math.sin(dLon / 2);
  const aa = sinLat * sinLat + sinLon * sinLon * Math.cos(lat1) * Math.cos(lat2);
  const c = 2 * Math.atan2(Math.sqrt(aa), Math.sqrt(1 - aa));
  return EARTH_RADIUS_M * c;
};

const normalizeGeo = (geo) => {
  if (!geo || typeof geo !== 'object') return null;
  const lat = Number(geo.lat);
  const lon = Number(geo.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  const normalized = { lat, lon };
  if (Number.isFinite(geo.radius)) normalized.radius = Number(geo.radius);
  if (typeof geo.gh === 'string') normalized.gh = geo.gh;
  if (Number.isFinite(geo.prec)) normalized.prec = Number(geo.prec);
  if (Number.isFinite(geo.eye)) normalized.eye = Number(geo.eye);
  if (Number.isFinite(geo.ground)) normalized.ground = Number(geo.ground);
  if (Number.isFinite(geo.ts)) normalized.ts = Number(geo.ts);
  return normalized;
};

const ensureArray = (value) => (Array.isArray(value) ? value : []);
const textOrEmpty = (value) => (typeof value === 'string' ? value : '');

const stableJson = (value) => {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch (_) {
    try {
      return String(value);
    } catch (err) {
      return '';
    }
  }
};

const checksumHex = (value) => {
  const text = stableJson(value);
  let hash = 2166136261;
  for (let i = 0; i < text.length; i++) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(16).padStart(8, '0');
};

const chunkText = (value, maxChars = MAX_DM_PAYLOAD_CHARS) => {
  const text = textOrEmpty(value);
  if (!text) return [];
  const size = Math.max(1024, Number(maxChars) || MAX_DM_PAYLOAD_CHARS);
  if (text.length <= size) return [text];
  const chunks = [];
  for (let i = 0; i < text.length; i += size) {
    chunks.push(text.slice(i, i + size));
  }
  return chunks;
};

const normalizeMessageType = (rawType, rawEvent) => {
  const type = String(rawType || '').trim();
  if (type) return type;
  const event = String(rawEvent || '').trim();
  return TYPE_BY_EVENT[event] || '';
};

const normalizeMessageEvent = (rawEvent, rawType) => {
  const event = String(rawEvent || '').trim();
  if (event) return event;
  const type = String(rawType || '').trim();
  return EVENT_BY_TYPE[type] || '';
};

const inferResourceFamily = (resource = {}, fallback = {}) => {
  const typeHint = String(resource.type || fallback.type || '').trim().toLowerCase();
  if (GEOMETRY_TYPE_HINTS.has(typeHint)) return 'geometry';
  if (MEDIA_TYPE_HINTS.has(typeHint)) return 'media';
  if (typeHint.includes('geometry') || typeHint.includes('pointcloud') || typeHint.includes('glb') || typeHint.includes('gltf')) {
    return 'geometry';
  }
  if (typeHint.includes('audio') || typeHint.includes('video') || typeHint.includes('media')) {
    return 'media';
  }
  const eventHint = String(resource.event || fallback.event || '').trim().toLowerCase();
  if (eventHint.includes('asset.geometry')) return 'geometry';
  if (eventHint.includes('asset.media')) return 'media';
  if (resource.pointcloud || fallback.pointcloud || resource.geometry || fallback.geometry || resource.glb || fallback.glb || resource.gltf || fallback.gltf) {
    return 'geometry';
  }
  if ((resource.data && Array.isArray(resource.data.vertices)) || (fallback.data && Array.isArray(fallback.data.vertices))) {
    return 'geometry';
  }
  if ((resource.chunk != null && resource.total != null) || (fallback.chunk != null && fallback.total != null)) {
    const maybeContentType = String(resource.contentType || resource.content_type || fallback.contentType || fallback.content_type || '').toLowerCase();
    if (maybeContentType.includes('model') || maybeContentType.includes('pointcloud') || maybeContentType.includes('json')) {
      return 'geometry';
    }
  }
  if (resource.audioPacket || fallback.audioPacket || resource.videoPacket || fallback.videoPacket) {
    return 'media';
  }
  return 'resource';
};

function createNoClipBridge({ NodeStore, Router, Net, CFG, setBadge, log }) {
  const NODE_STATE = new Map();
  const TARGET_INDEX = new Map();
  const NOCLIP_PEERS = new Map(); // Track discovered NoClip peers
  let syncAdapter = null;
  let discovery = null;
  let discoveryInit = null;
  let overrideRoom = null;
  let sharedPeerDiscovery = null;
  let sharedPeerUnsub = null;
  let sharedDmRegistered = false;
  let latestRouterCatalog = null;
  let latestCatalogChecksum = '';

  const nowMs = () => Date.now();

  const newMessageId = (prefix = 'hb') =>
    `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;

  const normalizeTransportLabel = (value) => {
    const key = String(value || '').trim().toLowerCase();
    if (key === 'cloudflared' || key === 'cf') return 'cloudflare';
    if (key === 'localhost' || key === 'lan') return 'local';
    if (key === 'cloudflare' || key === 'upnp' || key === 'nats' || key === 'nkn' || key === 'local') return key;
    return '';
  };

  const endpointFromValue = (value) => {
    if (!value) return '';
    if (typeof value === 'string') return value.trim();
    if (typeof value !== 'object') return '';
    return String(
      value.endpoint ||
      value.base_url ||
      value.baseUrl ||
      value.http_endpoint ||
      value.httpEndpoint ||
      value.nkn_address ||
      value.address ||
      ''
    ).trim();
  };

  const candidateTimestampMs = (value, fallback = 0) => {
    if (!value || typeof value !== 'object') return Number(fallback || 0);
    const direct = Number(
      value.last_verified_ms ||
      value.lastVerifiedMs ||
      value.updated_at_ms ||
      value.updatedAtMs ||
      value.last_seen_ms ||
      value.lastSeenMs ||
      0
    );
    if (Number.isFinite(direct) && direct > 0) return direct;
    const iso = String(value.last_verified || value.lastVerified || value.updated_at || value.updatedAt || '').trim();
    if (iso) {
      const parsed = Date.parse(iso);
      if (Number.isFinite(parsed) && parsed > 0) return parsed;
    }
    return Number(fallback || 0);
  };

  const normalizePeerCandidates = (rawValue, fallbackMs = 0) => {
    const src = rawValue && typeof rawValue === 'object' ? rawValue : {};
    const out = {};
    const assign = (key, value) => {
      const transport = normalizeTransportLabel(key || value?.transport || value?.selected_transport || value?.selectedTransport);
      if (!transport) return;
      const endpoint = endpointFromValue(value);
      if (!endpoint) return;
      out[transport] = {
        endpoint,
        lastVerifiedMs: candidateTimestampMs(value, fallbackMs)
      };
    };
    Object.entries(src).forEach(([key, value]) => assign(key, value));
    return out;
  };

  const selectedTransportForPacket = (state, packet = {}) => {
    const fromPacket = normalizeTransportLabel(packet.selected_transport || packet.selectedTransport || packet.transport || packet.mode);
    if (fromPacket) return fromPacket;
    const fromCfg = normalizeTransportLabel(state?.cfg?.selectedTransport || state?.cfg?.transport || state?.cfg?.mode);
    if (fromCfg) return fromCfg;
    const fromGlobal = normalizeTransportLabel(CFG?.transport);
    if (fromGlobal) return fromGlobal;
    return 'nkn';
  };

  const toNonNegativeInt = (value, fallback = 0) => {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return Math.max(0, Math.floor(n));
  };

  const pruneMessageCache = (state) => {
    if (!state?.receivedMessageIds) return;
    const now = nowMs();
    for (const [key, expiresAt] of state.receivedMessageIds.entries()) {
      if (!key || expiresAt <= now) state.receivedMessageIds.delete(key);
    }
    while (state.receivedMessageIds.size > MAX_MESSAGE_CACHE) {
      const oldest = state.receivedMessageIds.keys().next();
      if (oldest.done) break;
      state.receivedMessageIds.delete(oldest.value);
    }
  };

  const rememberInboundMessage = (state, messageId) => {
    const key = String(messageId || '').trim();
    if (!state?.receivedMessageIds || !key) return true;
    pruneMessageCache(state);
    const now = nowMs();
    const current = Number(state.receivedMessageIds.get(key) || 0);
    if (current > now) return false;
    state.receivedMessageIds.set(key, now + MESSAGE_ID_TTL_MS);
    return true;
  };

  const pruneChunkAssemblies = (state) => {
    if (!state?.chunkAssemblies) return;
    const now = nowMs();
    for (const [key, assembly] of state.chunkAssemblies.entries()) {
      if (!key || !assembly || Number(assembly.expiresAt || 0) <= now) {
        state.chunkAssemblies.delete(key);
      }
    }
    while (state.chunkAssemblies.size > MAX_CHUNK_ASSEMBLIES) {
      const oldest = state.chunkAssemblies.keys().next();
      if (oldest.done) break;
      state.chunkAssemblies.delete(oldest.value);
    }
  };

  const sanitizePubKey = (value) => {
    if (!value) return '';
    const text = String(value).trim().toLowerCase();
    const match = text.match(/([0-9a-f]{64})$/);
    return match ? match[1] : '';
  };

  const sanitizeAddr = (value) => {
    if (!value) return '';
    const text = String(value).trim();
    if (!text) return '';
    if (/^[a-z0-9_-]+\.[0-9a-f]{64}$/i.test(text)) return text;
    // Use noclip. prefix for NoClip peers (changed from web.)
    if (/^[0-9a-f]{64}$/i.test(text)) return `noclip.${text.toLowerCase()}`;
    return '';
  };

  const normalizeCatalogSummary = (value = {}) => {
    const src = value && typeof value === 'object' ? value : {};
    return {
      serviceCount: toNonNegativeInt(src.serviceCount ?? src.service_count, 0),
      publishedCount: toNonNegativeInt(src.publishedCount ?? src.published_count, 0),
      healthyCount: toNonNegativeInt(src.healthyCount ?? src.healthy_count, 0),
      catalogReady: src.catalogReady === true || src.catalog_ready === true
    };
  };

  const normalizeCatalogProvider = (value = {}) => {
    const src = value && typeof value === 'object' ? value : {};
    const addressesRaw = Array.isArray(src.router_nkn_addresses)
      ? src.router_nkn_addresses
      : (Array.isArray(src.routerNknAddresses) ? src.routerNknAddresses : []);
    const routerNknAddresses = addressesRaw
      .map((entry) => String(entry || '').trim())
      .filter(Boolean)
      .slice(0, 16);
    const routerNkn = String(src.router_nkn || src.routerNkn || '').trim() || routerNknAddresses[0] || '';
    return {
      providerId: String(src.provider_id || src.providerId || '').trim(),
      providerLabel: String(src.provider_label || src.providerLabel || '').trim(),
      providerNetwork: String(src.provider_network || src.providerNetwork || '').trim().toLowerCase() || 'hydra',
      providerContact: String(src.provider_contact || src.providerContact || '').trim(),
      providerKeyFingerprint: String(
        src.provider_key_fingerprint ||
        src.providerKeyFingerprint ||
        ''
      ).trim().toLowerCase(),
      routerNkn,
      routerNknAddresses
    };
  };

  const normalizeCatalogService = (value = {}) => {
    const src = value && typeof value === 'object' ? value : {};
    const serviceId = String(src.service_id || src.serviceId || src.service || '').trim();
    if (!serviceId) return null;
    const pricing = src.pricing && typeof src.pricing === 'object' ? src.pricing : {};
    const endpointCandidates = src.endpoint_candidates && typeof src.endpoint_candidates === 'object'
      ? src.endpoint_candidates
      : (src.endpointCandidates && typeof src.endpointCandidates === 'object' ? src.endpointCandidates : {});
    const readEndpoint = (entry) => {
      if (!entry) return '';
      if (typeof entry === 'string') return entry.trim();
      if (typeof entry === 'object') {
        return String(
          entry.base_url ||
          entry.baseUrl ||
          entry.http_endpoint ||
          entry.httpEndpoint ||
          entry.nkn_address ||
          entry.address ||
          ''
        ).trim();
      }
      return '';
    };
    return {
      serviceId,
      status: String(src.status || '').trim(),
      healthy: src.healthy === true,
      enabled: src.enabled !== false,
      visibility: String(src.visibility || '').trim().toLowerCase() || 'public',
      selectedTransport: normalizeTransportLabel(src.selected_transport || src.selectedTransport || src.transport),
      selectedEndpoint: String(src.selected_endpoint || src.selectedEndpoint || src.base_url || src.http_endpoint || '').trim(),
      endpointCandidates: {
        cloudflare: readEndpoint(endpointCandidates.cloudflare),
        nkn: readEndpoint(endpointCandidates.nkn),
        local: readEndpoint(endpointCandidates.local),
        upnp: readEndpoint(endpointCandidates.upnp),
        nats: readEndpoint(endpointCandidates.nats)
      },
      pricing: {
        currency: String(pricing.currency || '').trim().toUpperCase(),
        unit: String(pricing.unit || '').trim().toLowerCase(),
        basePrice: Number.isFinite(Number(pricing.base_price))
          ? Math.max(0, Number(pricing.base_price))
          : (Number.isFinite(Number(pricing.basePrice)) ? Math.max(0, Number(pricing.basePrice)) : 0)
      }
    };
  };

  const normalizeMarketplaceCatalog = (value) => {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
    const src = value;
    const servicesRaw = Array.isArray(src.services) ? src.services : [];
    const services = servicesRaw
      .map((entry) => normalizeCatalogService(entry))
      .filter(Boolean)
      .slice(0, 128);
    return {
      generatedAtMs: toNonNegativeInt(src.generated_at_ms ?? src.generatedAtMs, nowMs()),
      provider: normalizeCatalogProvider(src.provider || {}),
      summary: normalizeCatalogSummary(src.summary || {}),
      services
    };
  };

  const normalizeMarketStatusEntry = (value = {}) => {
    const src = value && typeof value === 'object' ? value : {};
    const serviceId = String(src.service_id || src.serviceId || src.service || '').trim();
    if (!serviceId) return null;
    const status = String(src.status || '').trim().toLowerCase();
    return {
      serviceId,
      status: status || 'unknown',
      healthy: src.healthy === true || status === 'online',
      visibility: String(src.visibility || '').trim().toLowerCase() || 'public',
      selectedTransport: normalizeTransportLabel(src.selected_transport || src.selectedTransport || src.transport),
      selectedEndpoint: String(src.selected_endpoint || src.selectedEndpoint || src.endpoint || '').trim(),
      staleRejected: src.stale_rejected === true || src.staleRejected === true,
      ts: toNonNegativeInt(src.ts || src.timestamp || nowMs(), nowMs())
    };
  };

  const mergeMarketServiceStatus = (existing = {}, updates = []) => {
    const base = existing && typeof existing === 'object' ? { ...existing } : {};
    ensureArray(updates).forEach((entry) => {
      const normalized = normalizeMarketStatusEntry(entry);
      if (!normalized?.serviceId) return;
      base[normalized.serviceId] = normalized;
    });
    return base;
  };

  if (!latestRouterCatalog && CFG?.routerMarketplaceCatalog && typeof CFG.routerMarketplaceCatalog === 'object') {
    const seeded = normalizeMarketplaceCatalog(CFG.routerMarketplaceCatalog);
    if (seeded) {
      latestRouterCatalog = seeded;
      latestCatalogChecksum = checksumHex(seeded);
    }
  }

  const getCatalogProviderMeta = (catalog = latestRouterCatalog) => {
    const normalized = normalizeMarketplaceCatalog(catalog);
    if (!normalized) return {};
    const provider = normalized.provider || {};
    const summary = normalized.summary || {};
    return {
      providerId: provider.providerId || '',
      providerLabel: provider.providerLabel || '',
      providerNetwork: provider.providerNetwork || 'hydra',
      providerFingerprint: provider.providerKeyFingerprint || '',
      routerNkn: provider.routerNkn || '',
      routerNknAddresses: Array.isArray(provider.routerNknAddresses) ? provider.routerNknAddresses.slice(0, 8) : [],
      marketplaceCatalogSummary: {
        serviceCount: toNonNegativeInt(summary.serviceCount, 0),
        publishedCount: toNonNegativeInt(summary.publishedCount, 0),
        healthyCount: toNonNegativeInt(summary.healthyCount, 0),
        generatedAtMs: toNonNegativeInt(normalized.generatedAtMs, nowMs())
      }
    };
  };

  const buildDiscoveryMeta = (extra = {}) => {
    const providerMeta = getCatalogProviderMeta();
    const base = {
      ids: ['hydra', 'graph', 'bridge', 'marketplace'],
      kind: 'hydra',
      network: 'hydra',
      graphId: CFG?.graphId || ''
    };
    return {
      ...base,
      ...(providerMeta.providerId ? { providerId: providerMeta.providerId } : {}),
      ...(providerMeta.providerLabel ? { providerLabel: providerMeta.providerLabel } : {}),
      ...(providerMeta.providerNetwork ? { providerNetwork: providerMeta.providerNetwork } : {}),
      ...(providerMeta.providerFingerprint ? { providerFingerprint: providerMeta.providerFingerprint } : {}),
      ...(providerMeta.routerNkn ? { routerNkn: providerMeta.routerNkn } : {}),
      ...(providerMeta.routerNknAddresses?.length ? { routerNknAddresses: providerMeta.routerNknAddresses } : {}),
      ...(providerMeta.marketplaceCatalogSummary ? { marketplaceCatalogSummary: providerMeta.marketplaceCatalogSummary } : {}),
      ...extra
    };
  };

  const emitMarketUiEvent = (eventName, detail = {}) => {
    if (typeof window === 'undefined' || typeof window.dispatchEvent !== 'function') return;
    try {
      window.dispatchEvent(new CustomEvent(eventName, {
        detail: {
          ...(detail && typeof detail === 'object' ? detail : {}),
          ts: Number(detail?.ts || nowMs()) || nowMs()
        }
      }));
    } catch (_) {
      // ignore DOM dispatch failures
    }
  };

  const formatUsdMicros = (rawMicros) => {
    const numeric = Number(rawMicros);
    if (!Number.isFinite(numeric) || numeric < 0) return '';
    return (numeric / 1_000_000).toFixed(4).replace(/\.?0+$/, '');
  };

  const creditPreviewText = (balance = {}) => {
    const available = String(balance.available ?? balance.balance ?? '').trim();
    if (available) return `Credits: ${available} USDC`;
    const availableMicros = String(balance.available_micros ?? balance.availableMicros ?? '').trim();
    if (availableMicros) {
      const usd = formatUsdMicros(availableMicros);
      if (usd) return `Credits: ${usd} USDC`;
    }
    return 'Credits: --';
  };

  const quotePreviewText = (quote = {}) => {
    const amount = String(quote.estimatedCharge || quote.amount || '').trim();
    if (amount) return `Estimate: ${amount} USDC`;
    const amountMicros = String(
      quote.estimatedChargeMicros ||
      quote.estimated_charge_micros ||
      quote.amountMicros ||
      quote.amount_micros ||
      ''
    ).trim();
    if (amountMicros) {
      const usd = formatUsdMicros(amountMicros);
      if (usd) return `Estimate: ${usd} USDC`;
    }
    return 'Estimate: --';
  };

  const sanitizeRoomName = (value) => {
    const raw = String(value || 'default').trim().toLowerCase();
    const cleaned = raw.replace(/[^a-z0-9_.-]+/g, '_');
    return cleaned || 'default';
  };

  const deriveAutoRoom = () => 'nexus';

  const resolveRoomName = (value) => {
    const raw = (value || '').trim();
    if (!raw) return deriveAutoRoom();
    const lowered = raw.toLowerCase();
    if (lowered === 'auto' || lowered === 'default' || lowered === 'hybrid-bridge') {
      return deriveAutoRoom();
    }
    const sanitized = sanitizeRoomName(raw);
    return sanitized || deriveAutoRoom();
  };

  const consumePeerParam = () => {
    try {
      const url = new URL(window.location.href);
      const peerParam = url.searchParams.get('peer');

      if (!peerParam) return null;

      // Format: <prefix>.<hex64> or just <hex64>
      const parts = peerParam.split('.');
      let hex = '';

      if (parts.length === 2) {
        hex = parts[1];
      } else if (parts.length === 1) {
        hex = parts[0];
      } else {
        return null;
      }

      // Validate and sanitize
      const sanitized = sanitizePubKey(hex);
      if (!sanitized) return null;

      // Remove from URL
      url.searchParams.delete('peer');
      const newUrl = url.pathname + (url.searchParams.toString() ? `?${url.searchParams.toString()}` : '') + url.hash;

      try {
        window.history.replaceState({}, document.title, newUrl);
      } catch (err) {
        // ignore history failures
      }

      return sanitized;

    } catch (err) {
      return null;
    }
  };

  const normalizeIncomingMessage = (rawMsg = {}) => {
    if (!rawMsg || typeof rawMsg !== 'object') {
      return {
        ok: false,
        type: '',
        event: '',
        message: {},
        errorCode: INTEROP_ERROR_CODES.INVALID_ENVELOPE,
        error: 'payload must be an object',
        contractStatus: null
      };
    }
    const parsed = normalizeInteropEnvelope(rawMsg, {
      expectedContract: DEFAULT_INTEROP_CONTRACT,
      requireTypeOrEvent: true
    });
    const base = parsed.envelope && typeof parsed.envelope === 'object'
      ? parsed.envelope
      : rawMsg;
    const payload = base.payload && typeof base.payload === 'object' ? base.payload : null;
    const message = payload ? { ...payload, ...base } : { ...base };
    const type = normalizeMessageType(message.type || rawMsg.type, message.event || rawMsg.event);
    const event = normalizeMessageEvent(message.event || rawMsg.event, type);
    if (type && !message.type) message.type = type;
    if (event && !message.event) message.event = event;
    if (!message.messageId && message.message_id) message.messageId = String(message.message_id);
    if (!message.assetId && message.asset_id) message.assetId = String(message.asset_id);
    if (!message.contentType && message.content_type) message.contentType = String(message.content_type);
    if (!message.selected_transport && message.selectedTransport) {
      message.selected_transport = message.selectedTransport;
    }
    return {
      ok: !!parsed.ok,
      type,
      event,
      message,
      errorCode: parsed.errorCode || '',
      error: parsed.error || '',
      contractStatus: parsed.contractStatus || null
    };
  };

  const registerInboundChunk = (state, fromPub, message = {}) => {
    if (!state?.chunkAssemblies) return { complete: false, payload: null };
    const total = Math.max(1, Number(message.total || 1) || 1);
    if (total <= 1) return { complete: true, payload: textOrEmpty(message.chunk_payload || message.chunkPayload || message.data_chunk || message.chunkData || '') };
    const chunkNumberRaw = Number(message.chunk_number || message.chunkNumber || 0);
    const chunkRaw = Number(message.chunk || message.chunk_index || 0);
    const idx = Number.isFinite(chunkNumberRaw) && chunkNumberRaw > 0
      ? Math.max(0, chunkNumberRaw - 1)
      : Math.max(0, chunkRaw);
    const chunkTextValue = textOrEmpty(
      message.chunk_payload ||
      message.chunkPayload ||
      message.data_chunk ||
      message.chunkData ||
      message.b64_chunk ||
      ''
    );
    if (!chunkTextValue) return { complete: false, payload: null };

    const assetId = String(message.assetId || message.asset_id || 'asset').trim() || 'asset';
    const transferId = String(message.transferId || message.transfer_id || '').trim();
    const key = `${fromPub}:${assetId}:${transferId || message.checksum || total}`;
    pruneChunkAssemblies(state);

    let assembly = state.chunkAssemblies.get(key);
    if (!assembly || Number(assembly.total || 0) !== total) {
      assembly = {
        key,
        assetId,
        total,
        chunks: new Array(total),
        received: new Set(),
        createdAt: nowMs(),
        expiresAt: nowMs() + CHUNK_ASSEMBLY_TTL_MS,
        contentType: String(message.contentType || message.content_type || 'application/json').trim(),
        checksum: String(message.checksum || '').trim(),
        transferId
      };
      state.chunkAssemblies.set(key, assembly);
    }

    if (idx >= total) return { complete: false, payload: null };
    if (!assembly.received.has(idx)) {
      assembly.chunks[idx] = chunkTextValue;
      assembly.received.add(idx);
    }
    assembly.expiresAt = nowMs() + CHUNK_ASSEMBLY_TTL_MS;

    if (assembly.received.size < total) {
      return { complete: false, payload: null };
    }

    const merged = assembly.chunks.join('');
    state.chunkAssemblies.delete(key);
    return { complete: true, payload: merged, meta: assembly };
  };

  const formatLastSeen = (ts) => {
    if (!Number.isFinite(ts)) return '';
    const ms = ts > 1e12 ? ts : ts * 1000;
    const delta = Math.max(0, Date.now() - ms);
    const sec = Math.floor(delta / 1000);
    if (sec < 60) return `${sec}s ago`;
    const min = Math.floor(sec / 60);
    if (min < 60) return `${min}m ago`;
    const hr = Math.floor(min / 60);
    if (hr < 24) return `${hr}h ago`;
    const day = Math.floor(hr / 24);
    return `${day}d ago`;
  };

  let dropdownRefreshTimer = null;
  const schedulePeerDropdownRefresh = () => {
    if (dropdownRefreshTimer) return;
    dropdownRefreshTimer = setTimeout(() => {
      dropdownRefreshTimer = null;
      NODE_STATE.forEach((_, nodeId) => refreshPeerDropdown(nodeId, { silent: true }));
    }, 200);
  };

  const updateNoclipBadge = () => {
    try {
      const badge = document.querySelector('#noclipPeerBadge');
      if (!badge) return;

      const count = NOCLIP_PEERS.size;
      if (count > 0) {
        badge.textContent = String(count);
        badge.classList.remove('hidden');
      } else {
        badge.classList.add('hidden');
      }
    } catch (err) {
      // Silently fail if badge element doesn't exist
    }
    schedulePeerDropdownRefresh();
  };

  const syncPeersFromShared = (list = []) => {
    const staleKeys = new Set(NOCLIP_PEERS.keys());
    const snapshot = Array.isArray(list) ? list : [];
    snapshot.forEach((peer) => {
      const pub = sanitizePubKey(peer?.nknPub || peer?.pub || peer?.addr || '');
      if (!pub) return;
      const peerLast = Number(peer?.last || nowMs()) || nowMs();
      const selectedTransport = normalizeTransportLabel(
        peer?.selectedTransport ||
        peer?.selected_transport ||
        peer?.transport ||
        peer?.meta?.selectedTransport ||
        peer?.meta?.selected_transport
      );
      const endpointCandidates = normalizePeerCandidates(
        peer?.endpointCandidates ||
        peer?.endpoint_candidates ||
        peer?.candidates ||
        peer?.meta?.endpointCandidates ||
        peer?.meta?.endpoint_candidates ||
        {},
        peerLast > 1e12 ? peerLast : (peerLast * 1000)
      );
      const discoverySource = String(
        peer?.discoverySource ||
        peer?.discovery_source ||
        peer?.meta?.discoverySource ||
        peer?.meta?.discovery_source ||
        'peerDiscovery.shared'
      ).trim();
      staleKeys.delete(pub);
      NOCLIP_PEERS.set(pub, {
        pub,
        addr: peer.addr || `noclip.${pub}`,
        meta: {
          ...(peer.meta || {}),
          selectedTransport: selectedTransport || '',
          endpointCandidates,
          discoverySource
        },
        selectedTransport: selectedTransport || '',
        endpointCandidates,
        discoverySource,
        last: peerLast
      });
      const watchers = TARGET_INDEX.get(pub);
      if (watchers && watchers.size) {
        const updates = {
          addr: peer.addr || '',
          meta: {
            ...(peer.meta || {}),
            selectedTransport: selectedTransport || '',
            endpointCandidates,
            discoverySource
          },
          state: {
            selectedTransport: selectedTransport || '',
            endpointCandidates,
            discoverySource
          },
          lastTs: peerLast
        };
        watchers.forEach((nodeId) => {
          const st = ensureNodeState(nodeId);
          if (!st) return;
          upsertRemotePeer(st, pub, updates);
          Router.sendFrom(nodeId, 'peers', {
            nodeId,
            peers: peersForOutput(st)
          });
        });
      }
    });
    staleKeys.forEach((key) => {
      NOCLIP_PEERS.delete(key);
      const watchers = TARGET_INDEX.get(key);
      if (watchers && watchers.size) {
        watchers.forEach((nodeId) => {
          const st = ensureNodeState(nodeId);
          if (!st) return;
          if (st.remotePeers?.has?.(key)) st.remotePeers.delete(key);
          Router.sendFrom(nodeId, 'peers', {
            nodeId,
            peers: peersForOutput(st)
          });
        });
      }
    });
    updateNoclipBadge();
  };

  const bindPeerDiscovery = (api) => {
    if (sharedPeerDiscovery === api) return;
    if (sharedPeerUnsub) {
      try { sharedPeerUnsub(); } catch (_) { /* ignore */ }
      sharedPeerUnsub = null;
    }
    sharedPeerDiscovery = api || null;
    if (!sharedPeerDiscovery) return;
    if (!sharedDmRegistered && typeof sharedPeerDiscovery.registerDmHandler === 'function') {
      sharedPeerDiscovery.registerDmHandler((evt) => handleDiscoveryDm(evt));
      sharedDmRegistered = true;
    }
    if (typeof sharedPeerDiscovery.getNoclipPeers === 'function') {
      try {
        syncPeersFromShared(sharedPeerDiscovery.getNoclipPeers());
      } catch (err) {
        console.warn('[NoClipBridge] getNoclipPeers failed:', err);
      }
    }
    if (typeof sharedPeerDiscovery.subscribeNoclipPeers === 'function') {
      sharedPeerUnsub = sharedPeerDiscovery.subscribeNoclipPeers((peers) => {
        syncPeersFromShared(peers || []);
      });
    }
  };

  const ensureNodeState = (nodeId) => {
    const key = String(nodeId || '').trim();
    if (!key) return null;
    if (!NODE_STATE.has(key)) {
      const record = NodeStore.ensure(key, 'NoClipBridge') || { config: {} };
      const cfg = record.config || {};
      NODE_STATE.set(key, {
        nodeId: key,
        graphId: CFG?.graphId || '',
        cfg,
        resolvedRoom: resolveRoomName(cfg.room),
        targetPub: sanitizePubKey(cfg.targetPub),
        targetAddr: sanitizeAddr(cfg.targetAddr) || null,
        remotePeers: new Map(),
        sessions: new Map(),
        sessionIndex: new Map(),
        sessionId: '',
        lastHandshakeAt: 0,
        lastState: null,
        badgeLimiter: 0,
        pendingAcks: new Map(),
        receivedMessageIds: new Map(),
        chunkAssemblies: new Map(),
        rootEl: null
      });
      registerTarget(key, sanitizePubKey(cfg.targetPub));
    }
    return NODE_STATE.get(key);
  };

  const unregisterTarget = (nodeId, pub) => {
    if (!pub) return;
    const set = TARGET_INDEX.get(pub);
    if (!set) return;
    set.delete(nodeId);
    if (!set.size) TARGET_INDEX.delete(pub);
  };

  const registerTarget = (nodeId, pub) => {
    const normalized = sanitizePubKey(pub);
    // Remove previous associations first
    for (const [key, set] of TARGET_INDEX.entries()) {
      if (set.has(nodeId)) {
        set.delete(nodeId);
        if (!set.size) TARGET_INDEX.delete(key);
      }
    }
    if (!normalized) return;
    if (!TARGET_INDEX.has(normalized)) TARGET_INDEX.set(normalized, new Set());
    TARGET_INDEX.get(normalized).add(nodeId);
  };

  const upsertRemotePeer = (state, pub, updates = {}) => {
    if (!state) return null;
    const normalized = sanitizePubKey(pub);
    if (!normalized) return null;
    let entry = state.remotePeers.get(normalized);
    if (!entry) {
      entry = {
        pub: normalized,
        addr: '',
        meta: {},
        geo: null,
        pose: null,
        state: null,
        lastTs: 0
      };
    }
    if (updates.addr) {
      const addr = sanitizeAddr(updates.addr);
      if (addr) entry.addr = addr;
    }
    if (updates.meta) {
      entry.meta = { ...(entry.meta || {}), ...(updates.meta || {}) };
    }
    if (updates.geo) {
      const geo = normalizeGeo(updates.geo);
      if (geo) entry.geo = geo;
    }
    if (updates.pose) {
      entry.pose = updates.pose;
    }
    if (updates.state) {
      entry.state = { ...(entry.state || {}), ...(updates.state || {}) };
    }
    if (updates.lastTs) {
      entry.lastTs = Number(updates.lastTs) || entry.lastTs;
    }
    state.remotePeers.set(normalized, entry);
    return entry;
  };

  const peersForOutput = (state) => {
    if (!state) return [];
    return Array.from(state.remotePeers.values()).map((entry) => ({
      nknPub: entry.pub,
      addr: entry.addr || `noclip.${entry.pub}`,
      selectedTransport: String(
        entry?.state?.selectedTransport ||
        entry?.state?.selected_transport ||
        entry?.meta?.selectedTransport ||
        entry?.meta?.selected_transport ||
        ''
      ).trim().toLowerCase(),
      endpointCandidates: entry?.state?.endpointCandidates || entry?.state?.endpoint_candidates || entry?.meta?.endpointCandidates || {},
      discoverySource: String(entry?.state?.discoverySource || entry?.state?.discovery_source || entry?.meta?.discoverySource || '').trim(),
      meta: entry.meta || {},
      geo: entry.geo || null,
      pose: entry.pose || null,
      state: entry.state || null,
      last: entry.lastTs || 0
    }));
  };

  const textId = (value) => {
    if (value === null || value === undefined) return '';
    const text = String(value).trim();
    return text || '';
  };

  const pickFirstId = (...values) => {
    for (const value of values) {
      const text = textId(value);
      if (text) return text;
    }
    return '';
  };

  const normalizeTargetFields = (source = {}) => {
    const src = source && typeof source === 'object' ? source : {};
    const objectUuid = pickFirstId(
      src.objectUuid,
      src.object_uuid,
      src.objectId,
      src.object_id
    );
    return {
      sessionId: pickFirstId(src.sessionId, src.session_id),
      objectUuid,
      objectId: objectUuid,
      overlayId: pickFirstId(src.overlayId, src.overlay_id),
      itemId: pickFirstId(src.itemId, src.item_id),
      layerId: pickFirstId(src.layerId, src.layer_id)
    };
  };

  const mergeTargetFields = (primary = {}, fallback = {}) => {
    const head = normalizeTargetFields(primary);
    const tail = normalizeTargetFields(fallback);
    const objectUuid = head.objectUuid || tail.objectUuid;
    return {
      sessionId: head.sessionId || tail.sessionId,
      objectUuid,
      objectId: objectUuid,
      overlayId: head.overlayId || tail.overlayId,
      itemId: head.itemId || tail.itemId || objectUuid,
      layerId: head.layerId || tail.layerId
    };
  };

  const sessionTargetSnapshot = (session = {}) => {
    const normalized = normalizeTargetFields({
      sessionId: session.sessionId,
      objectUuid: session.objectUuid || session.objectId,
      overlayId: session.overlayId,
      itemId: session.itemId,
      layerId: session.layerId
    });
    if (!normalized.sessionId && !normalized.objectUuid && !normalized.overlayId && !normalized.itemId && !normalized.layerId) {
      return null;
    }
    return normalized;
  };

  const resolveConfiguredTarget = (state) => {
    const cfg = state?.cfg || {};
    const fromCfg = normalizeTargetFields({
      sessionId: cfg.sessionId,
      objectUuid: cfg.objectUuid || cfg.objectId,
      overlayId: cfg.overlayId,
      itemId: cfg.itemId,
      layerId: cfg.layerId
    });
    const fromInterop = normalizeTargetFields(cfg.interopTarget || {});
    const merged = mergeTargetFields(fromCfg, fromInterop);
    if (!merged.sessionId && !merged.objectUuid && !merged.overlayId && !merged.itemId && !merged.layerId) return null;
    return merged;
  };

  const resolveOutboundTargetContext = (state, payload = {}) => {
    const explicit = mergeTargetFields(payload, payload?.target || {});
    const configured = resolveConfiguredTarget(state) || {};
    const merged = mergeTargetFields(explicit, configured);
    const hasTarget = !!(merged.sessionId || merged.objectUuid || merged.overlayId || merged.itemId || merged.layerId);
    return { ...merged, hasTarget };
  };

  const resolveInboundTargetContext = (state, msg = {}, fromPub = '') => {
    const direct = mergeTargetFields(msg, msg.target || {});
    const sessions = state?.sessions instanceof Map ? state.sessions : null;
    let session = null;
    let ambiguous = false;

    if (sessions && sessions.size) {
      if (direct.sessionId && sessions.has(direct.sessionId)) {
        session = sessions.get(direct.sessionId) || null;
      }

      if (!session && direct.objectUuid) {
        const matches = [];
        sessions.forEach((candidate) => {
          const objectId = pickFirstId(candidate?.objectUuid, candidate?.objectId, candidate?.itemId);
          if (objectId && objectId === direct.objectUuid) matches.push(candidate);
        });
        if (matches.length === 1) session = matches[0];
        else if (matches.length > 1) ambiguous = true;
      }

      if (!session && !direct.sessionId && !direct.objectUuid && fromPub) {
        const indexSet = state?.sessionIndex?.get?.(fromPub);
        if (indexSet && indexSet.size === 1) {
          const onlySessionId = Array.from(indexSet.values())[0];
          if (onlySessionId && sessions.has(onlySessionId)) {
            session = sessions.get(onlySessionId) || null;
          }
        } else if (indexSet && indexSet.size > 1) {
          ambiguous = true;
        }
      }
    }

    const mapped = sessionTargetSnapshot(session || {});
    const target = mergeTargetFields(direct, mapped || {});
    const hasTarget = !!(target.sessionId || target.objectUuid || target.overlayId || target.itemId || target.layerId);
    return {
      ...target,
      ambiguous,
      hasTarget,
      session: session ? { ...session } : null
    };
  };

  const applyTargetToPayload = (payload, target) => {
    if (!payload || typeof payload !== 'object') return payload;
    if (!target || !target.hasTarget) return payload;
    const merged = { ...payload };
    if (target.sessionId && !merged.sessionId) merged.sessionId = target.sessionId;
    if (target.objectUuid && !merged.objectUuid && !merged.objectId) {
      merged.objectUuid = target.objectUuid;
      merged.objectId = target.objectUuid;
    }
    if (!merged.target || typeof merged.target !== 'object') merged.target = {};
    if (target.overlayId && !merged.target.overlayId) merged.target.overlayId = target.overlayId;
    if (target.itemId && !merged.target.itemId) merged.target.itemId = target.itemId;
    if (target.layerId && !merged.target.layerId) merged.target.layerId = target.layerId;
    if (target.sessionId && !merged.target.sessionId) merged.target.sessionId = target.sessionId;
    if (target.objectUuid && !merged.target.objectUuid) merged.target.objectUuid = target.objectUuid;
    return merged;
  };

  const selectTargets = (state, filters = {}) => {
    const list = peersForOutput(state);
    if (!list.length) return [];
    const requireLocation = !!filters.requireLocation;
    const geohashPrefix = typeof filters.geohashPrefix === 'string' ? filters.geohashPrefix.trim().toLowerCase() : '';
    const near = filters.near && typeof filters.near === 'object' ? filters.near : null;
    const radius = Number.isFinite(near?.radiusMeters) ? Number(near.radiusMeters) : null;
    const nearLat = Number(near?.lat);
    const nearLon = Number(near?.lon);

    let filtered = list.filter((peer) => {
      const geo = peer.geo;
      if (requireLocation && !geo) return false;
      if (geohashPrefix && (!geo?.gh || !geo.gh.toLowerCase().startsWith(geohashPrefix))) return false;
      if (radius && Number.isFinite(nearLat) && Number.isFinite(nearLon)) {
        if (!geo) return false;
        const distance = haversineDistance(nearLat, nearLon, geo.lat, geo.lon);
        if (!Number.isFinite(distance) || distance > radius) return false;
      }
      if (Array.isArray(filters.tags) && filters.tags.length) {
        const tagSet = new Set(
          (Array.isArray(peer.meta?.tags) ? peer.meta.tags : [])
            .map((tag) => String(tag || '').trim().toLowerCase())
            .filter(Boolean)
        );
        const missing = filters.tags.some((tag) => !tagSet.has(String(tag || '').trim().toLowerCase()));
        if (missing) return false;
      }
      return true;
    });

    if (Number.isFinite(filters.maxPeers) && filters.maxPeers > 0) {
      filtered = filtered.slice(0, Math.max(1, Math.floor(filters.maxPeers)));
    }

    return filtered.map((peer) => peer.nknPub).filter(Boolean);
  };

  const waitForNknClient = (timeoutMs = CONNECT_TIMEOUT_MS) =>
    new Promise((resolve, reject) => {
      const start = nowMs();
      const step = () => {
        if (Net.nkn?.client && Net.nkn.ready) {
          resolve(Net.nkn.client);
          return;
        }
        if (nowMs() - start >= timeoutMs) {
          reject(new Error('NKN unavailable'));
          return;
        }
        setTimeout(step, 250);
      };
      step();
    });

  async function ensureDiscovery() {
    if (sharedPeerDiscovery?.sendDm) {
      const dm = (pub, payload) => sharedPeerDiscovery.sendDm(pub, payload);
      const handshake = async (pub, meta = {}, { wantAck = true } = {}) => {
        const packet = withInteropContractFields({
          type: 'hybrid-bridge-handshake',
          event: 'interop.bridge.handshake',
          pub,
          capabilities: ['graph', 'resources', 'commands', 'audio', 'marketplace'],
          clientType: 'hydra',
          graphId: CFG?.graphId || '',
          meta: buildDiscoveryMeta(meta || {}),
          wantAck: wantAck !== false,
          ts: nowMs()
        }, DEFAULT_INTEROP_CONTRACT);
        return dm(pub, packet);
      };
      return {
        dm,
        handshake
      };
    }
    if (discovery) return discovery;
    if (discoveryInit) return discoveryInit;
    discoveryInit = (async () => {
      try {
        Net.ensureNkn?.();
      } catch (err) {
        log?.(`[noclip] ensureNkn failed: ${err?.message || err}`);
      }
      let client = null;
      try {
        client = await waitForNknClient();
      } catch (_) {
        // best-effort: allow discovery with synthetic id
      }
      const pub = sanitizePubKey(client?.getPublicKey?.());
      const addr = client?.addr || Net.nkn?.addr || '';
      const resolvedRoom = resolveRoomName(overrideRoom ?? NodeStore?.defaultsByType?.NoClipBridge?.room ?? '');
      const discoveryClient = await createNatsDiscovery({
        room: resolvedRoom,
        servers: DEFAULT_SERVERS,
        me: {
          nknPub: pub || `hydra-${Math.random().toString(36).slice(2, 10)}`,
          addr: addr || '',
          meta: buildDiscoveryMeta()
        }
      });
      discoveryClient.on('dm', handleDiscoveryDm);
      discoveryClient.on('peer', (peer) => {
        const normalized = sanitizePubKey(peer?.nknPub);
        if (!normalized) return;
        const peerLast = Number(peer?.last || nowMs()) || nowMs();
        const selectedTransport = normalizeTransportLabel(
          peer?.selectedTransport ||
          peer?.selected_transport ||
          peer?.transport ||
          peer?.meta?.selectedTransport ||
          peer?.meta?.selected_transport
        );
        const endpointCandidates = normalizePeerCandidates(
          peer?.endpointCandidates ||
          peer?.endpoint_candidates ||
          peer?.candidates ||
          peer?.meta?.endpointCandidates ||
          peer?.meta?.endpoint_candidates ||
          {},
          peerLast > 1e12 ? peerLast : (peerLast * 1000)
        );
        const discoverySource = String(
          peer?.discoverySource ||
          peer?.discovery_source ||
          peer?.meta?.discoverySource ||
          peer?.meta?.discovery_source ||
          'nats.peer'
        ).trim();

        // Track NoClip peers (identified by network: 'noclip' in metadata)
        const addr = (peer.addr || peer.nknPub || '').toLowerCase();
        const isNoClip = peer.meta?.network === 'noclip' || addr.startsWith('noclip.');
        if (isNoClip) {
          NOCLIP_PEERS.set(normalized, {
            pub: normalized,
            addr: peer.addr || `noclip.${normalized}`,
            meta: {
              ...(peer.meta || {}),
              selectedTransport: selectedTransport || '',
              endpointCandidates,
              discoverySource
            },
            selectedTransport: selectedTransport || '',
            endpointCandidates,
            discoverySource,
            last: peerLast
          });
          updateNoclipBadge();
        }

        const watchers = TARGET_INDEX.get(normalized);
        if (!watchers || !watchers.size) return;
        const updates = {
          addr: peer.addr || '',
          meta: {
            ...(peer.meta || {}),
            selectedTransport: selectedTransport || '',
            endpointCandidates,
            discoverySource
          },
          state: {
            selectedTransport: selectedTransport || '',
            endpointCandidates,
            discoverySource
          },
          lastTs: peerLast
        };
        for (const nodeId of watchers) {
          const st = ensureNodeState(nodeId);
          if (!st) continue;
          upsertRemotePeer(st, normalized, updates);
          Router.sendFrom(nodeId, 'peers', {
            nodeId,
            peers: peersForOutput(st)
          });
        }
      });
      discovery = discoveryClient;
      return discoveryClient;
    })().catch((err) => {
      discoveryInit = null;
      throw err;
    });
    return discoveryInit;
  }

  function handleDiscoveryDm(evt) {
    if (!evt) return;
    const rawMsg = evt.msg || {};
    const from = sanitizePubKey(
      evt.from ||
      rawMsg.pub ||
      rawMsg.from ||
      rawMsg.source_address ||
      rawMsg.sourceAddress
    );
    if (!from) return;
    const watchers = TARGET_INDEX.get(from);
    if (!watchers || !watchers.size) return;

    const normalized = normalizeIncomingMessage(rawMsg);
    const msg = normalized.message || {};
    const type = normalized.type || normalizeMessageType(msg.type, msg.event);
    const event = normalized.event || normalizeMessageEvent(msg.event, type);

    if (type && !msg.type) msg.type = type;
    if (event && !msg.event) msg.event = event;
    const selectedTransport = selectedTransportForPacket(null, msg);
    msg.transport = normalizeTransportLabel(msg.transport) || selectedTransport;
    msg.selected_transport = normalizeTransportLabel(msg.selected_transport || msg.selectedTransport) || msg.transport;
    if (!msg.selectedTransport) msg.selectedTransport = msg.selected_transport;

    const watcherIds = Array.from(watchers).filter(Boolean);
    if (!watcherIds.length) return;
    const primaryNodeId = watcherIds[0];

    const sendAck = (status = 'ok', detail = 'received', errorCode = '') => {
      const inReplyTo = String(msg.messageId || msg.message_id || '').trim();
      if (!inReplyTo) return;
      const st = ensureNodeState(primaryNodeId);
      if (!st) return;
      sendBridgePayload(primaryNodeId, st, {
        type: 'hybrid-bridge-ack',
        event: 'interop.bridge.ack',
        inReplyTo,
        status,
        detail,
        error_code: status === 'ok' ? '' : String(errorCode || INTEROP_ERROR_CODES.INVALID_ENVELOPE),
        sessionId: msg.sessionId || msg.session_id || '',
        ts: nowMs()
      }, { targets: [from] });
    };

    if (!normalized.ok) {
      const errorCode = String(normalized.errorCode || INTEROP_ERROR_CODES.INVALID_ENVELOPE);
      const detail = String(normalized.error || 'invalid interop payload');
      for (const nodeId of watcherIds) {
        Router.sendFrom(nodeId, 'events', {
          nodeId,
          peer: from,
          type: 'interop.error',
          payload: {
            event: 'interop.error',
            error_code: errorCode,
            error: detail,
            contract_status: normalized.contractStatus || null,
            raw: msg
          }
        });
      }
      sendAck('error', `${errorCode}:${detail}`, errorCode);
      return;
    }

    if (!type && !event) {
      sendAck(
        'error',
        `${INTEROP_ERROR_CODES.MISSING_REQUIRED_FIELDS}:missing type/event`,
        INTEROP_ERROR_CODES.MISSING_REQUIRED_FIELDS
      );
      return;
    }

    const messageId = String(msg.messageId || msg.message_id || '').trim();
    if (messageId) {
      let duplicateCount = 0;
      for (const nodeId of watcherIds) {
        const st = ensureNodeState(nodeId);
        if (!st) continue;
        if (!rememberInboundMessage(st, messageId)) duplicateCount += 1;
      }
      if (duplicateCount === watcherIds.length) {
        sendAck('ok', 'duplicate');
        return;
      }
    }

    if (type === 'hybrid-bridge-geometry' || event === 'interop.asset.geometry') {
      const total = Math.max(1, Number(msg.total || 1) || 1);
      const chunked = total > 1 || !!(
        msg.chunk_payload ||
        msg.chunkPayload ||
        msg.data_chunk ||
        msg.chunkData ||
        msg.b64_chunk
      );
      if (chunked) {
        const assembledByNode = new Map();
        for (const nodeId of watcherIds) {
          const st = ensureNodeState(nodeId);
          if (!st) continue;
          const result = registerInboundChunk(st, from, msg);
          if (result.complete && typeof result.payload === 'string') {
            assembledByNode.set(nodeId, result);
          }
        }
        sendAck('ok', 'chunk-received');
        if (!assembledByNode.size) return;

        for (const nodeId of watcherIds) {
          const result = assembledByNode.get(nodeId);
          if (!result) continue;
          const assembledPayload = {
            ...msg,
            type: 'hybrid-bridge-geometry',
            event: 'interop.asset.geometry',
            chunk: 1,
            total: 1,
            chunk_payload: '',
            payload_data: result.payload,
            payload_encoding: 'utf8',
            contentType: msg.contentType || msg.content_type || result.meta?.contentType || 'application/json',
            checksum: msg.checksum || result.meta?.checksum || checksumHex(result.payload),
            assembled: true
          };
          Router.sendFrom(nodeId, 'events', {
            nodeId,
            peer: from,
            type: 'hybrid-bridge-geometry',
            payload: assembledPayload
          });
        }
        return;
      }
    }

    if (type === 'hybrid-bridge-state') {
      for (const nodeId of watcherIds) {
        const st = ensureNodeState(nodeId);
        if (!st) continue;
        const targetCtx = resolveInboundTargetContext(st, msg, from);
        const statePayload = applyTargetToPayload(msg, targetCtx);
        if (Array.isArray(msg.peers)) {
          msg.peers.forEach((entry) => {
            const peerPub = entry?.nknPub || entry?.pub || entry?.addr;
            if (!peerPub) return;
            upsertRemotePeer(st, peerPub, {
              addr: entry.addr || '',
              meta: entry.meta || {},
              geo: entry.geo,
              pose: entry.pose,
              state: entry.state,
              lastTs: entry.last || msg.ts || nowMs()
            });
          });
          Router.sendFrom(nodeId, 'peers', {
            nodeId,
            peer: from,
            peers: peersForOutput(st)
          });
        }
        Router.sendFrom(nodeId, 'state', {
          nodeId,
          peer: from,
          state: statePayload.state || null,
          pose: statePayload.pose || null,
          selected_transport: statePayload.selected_transport || statePayload.transport || '',
          sessionId: statePayload.sessionId || '',
          objectUuid: statePayload.objectUuid || statePayload.objectId || '',
          target: statePayload.target || null,
          ts: statePayload.ts || nowMs()
        });
        if (statePayload.pose) {
          Router.sendFrom(nodeId, 'pose', {
            nodeId,
            peer: from,
            pose: statePayload.pose,
            selected_transport: statePayload.selected_transport || statePayload.transport || '',
            sessionId: statePayload.sessionId || '',
            objectUuid: statePayload.objectUuid || statePayload.objectId || '',
            target: statePayload.target || null,
            ts: statePayload.ts || nowMs()
          });
          Router.sendFrom(nodeId, 'overlayPose', {
            nodeId,
            peer: from,
            type,
            payload: statePayload,
            target: targetCtx.hasTarget ? {
              sessionId: targetCtx.sessionId || '',
              objectUuid: targetCtx.objectUuid || '',
              overlayId: targetCtx.overlayId || '',
              itemId: targetCtx.itemId || '',
              layerId: targetCtx.layerId || ''
            } : null
          });
        }
      }
      return;
    }

    if (type === 'hybrid-friend-response' || type === 'hybrid-bridge-log') {
      for (const nodeId of watcherIds) {
        Router.sendFrom(nodeId, 'events', {
          nodeId,
          peer: from,
          type,
          payload: msg
        });
      }
      return;
    }

    if (type === 'market-service-catalog' || event === 'market.service.catalog') {
      const catalogRaw = msg.catalog || msg.marketplaceCatalog || msg.marketplace_catalog || msg.payload || msg;
      const catalog = normalizeMarketplaceCatalog(catalogRaw);
      const summary = normalizeCatalogSummary(
        msg.catalog_summary || msg.catalogSummary || msg.summary || catalog?.summary || {}
      );
      const provider = normalizeCatalogProvider(
        msg.provider || catalog?.provider || {}
      );
      const catalogChecksum = String(
        msg.catalogChecksum ||
        msg.catalog_checksum ||
        (catalog ? checksumHex(catalog) : '')
      ).trim();

      for (const nodeId of watcherIds) {
        const st = ensureNodeState(nodeId);
        if (!st) continue;
        const catalogState = catalog
          ? {
              generatedAtMs: catalog.generatedAtMs,
              provider: catalog.provider,
              summary: catalog.summary,
              services: catalog.services
            }
          : null;
        upsertRemotePeer(st, from, {
          meta: {
            providerId: provider.providerId || '',
            providerLabel: provider.providerLabel || '',
            providerNetwork: provider.providerNetwork || '',
            providerFingerprint: provider.providerKeyFingerprint || '',
            routerNkn: provider.routerNkn || '',
            routerNknAddresses: provider.routerNknAddresses || [],
            marketplaceCatalogSummary: summary,
            ...(catalogChecksum ? { catalogChecksum } : {})
          },
          state: {
            marketplaceCatalog: catalogState,
            marketplaceCatalogSummary: summary,
            provider,
            ...(catalogChecksum ? { catalogChecksum } : {})
          },
          lastTs: msg.ts || nowMs()
        });
        Router.sendFrom(nodeId, 'marketCatalog', {
          nodeId,
          peer: from,
          catalog: catalogState,
          summary,
          provider,
          catalogChecksum: catalogChecksum || '',
          ts: msg.ts || nowMs()
        });
        Router.sendFrom(nodeId, 'events', {
          nodeId,
          peer: from,
          type: 'market-service-catalog',
          payload: {
            ...msg,
            catalog: catalogState,
            summary,
            provider,
            catalogChecksum: catalogChecksum || ''
          }
        });
        Router.sendFrom(nodeId, 'peers', {
          nodeId,
          peer: from,
          peers: peersForOutput(st)
        });
      }
      emitMarketUiEvent('hydra-market-catalog', {
        peer: from,
        catalog: catalog
          ? {
              generated_at_ms: catalog.generatedAtMs,
              provider: catalog.provider,
              summary: catalog.summary,
              services: catalog.services
            }
          : null,
        summary,
        provider,
        catalogChecksum: catalogChecksum || '',
        ts: msg.ts || nowMs()
      });
      if (msg.expectAck === true) sendAck('ok', 'catalog-received');
      return;
    }

    if (type === 'market-service-status' || event === 'market.service.status') {
      const summary = normalizeCatalogSummary(
        msg.catalog_summary || msg.catalogSummary || msg.summary || {}
      );
      const provider = normalizeCatalogProvider(msg.provider || {});
      const services = ensureArray(msg.services || msg.statuses || msg.entries)
        .map((entry) => normalizeMarketStatusEntry(entry))
        .filter(Boolean);
      const statusMap = mergeMarketServiceStatus({}, services);
      const catalogChecksum = String(msg.catalogChecksum || msg.catalog_checksum || '').trim();

      for (const nodeId of watcherIds) {
        const st = ensureNodeState(nodeId);
        if (!st) continue;
        const existingPeer = st.remotePeers?.get?.(from) || {};
        const priorStatus = existingPeer?.state?.marketServiceStatus || {};
        upsertRemotePeer(st, from, {
          meta: {
            providerId: provider.providerId || '',
            providerLabel: provider.providerLabel || '',
            providerNetwork: provider.providerNetwork || '',
            providerFingerprint: provider.providerKeyFingerprint || '',
            routerNkn: provider.routerNkn || '',
            routerNknAddresses: provider.routerNknAddresses || [],
            marketplaceCatalogSummary: summary,
            ...(catalogChecksum ? { catalogChecksum } : {})
          },
          state: {
            marketplaceCatalogSummary: summary,
            provider,
            marketServiceStatus: mergeMarketServiceStatus(
              priorStatus,
              services
            ),
            ...(catalogChecksum ? { catalogChecksum } : {})
          },
          lastTs: msg.ts || nowMs()
        });
        Router.sendFrom(nodeId, 'marketStatus', {
          nodeId,
          peer: from,
          summary,
          provider,
          services,
          serviceStatusMap: statusMap,
          catalogChecksum: catalogChecksum || '',
          ts: msg.ts || nowMs()
        });
        Router.sendFrom(nodeId, 'events', {
          nodeId,
          peer: from,
          type: 'market-service-status',
          payload: {
            ...msg,
            summary,
            provider,
            services,
            serviceStatusMap: statusMap,
            catalogChecksum: catalogChecksum || ''
          }
        });
        Router.sendFrom(nodeId, 'peers', {
          nodeId,
          peer: from,
          peers: peersForOutput(st)
        });
      }
      emitMarketUiEvent('hydra-market-status', {
        peer: from,
        summary,
        provider,
        services,
        serviceStatusMap: statusMap,
        catalogChecksum: catalogChecksum || '',
        ts: msg.ts || nowMs()
      });
      if (msg.expectAck === true) sendAck('ok', 'status-received');
      return;
    }

    if (type === 'market-quote-result' || event === 'market.quote.result') {
      const quote = msg.quote && typeof msg.quote === 'object' ? msg.quote : msg;
      const label = quotePreviewText(quote);
      for (const nodeId of watcherIds) {
        Router.sendFrom(nodeId, 'marketQuote', {
          nodeId,
          peer: from,
          quote,
          text: label,
          ts: msg.ts || nowMs()
        });
        Router.sendFrom(nodeId, 'events', {
          nodeId,
          peer: from,
          type: 'market-quote-result',
          payload: {
            ...msg,
            quote
          }
        });
      }
      emitMarketUiEvent('hydra-market-quote-preview', {
        peer: from,
        quote,
        text: label,
        ts: msg.ts || nowMs()
      });
      if (msg.expectAck === true) sendAck('ok', 'quote-received');
      return;
    }

    if (type === 'market-credit-balance' || event === 'market.credit.balance') {
      const balance = msg.balance && typeof msg.balance === 'object' ? msg.balance : msg;
      const text = creditPreviewText(balance);
      for (const nodeId of watcherIds) {
        Router.sendFrom(nodeId, 'marketCredit', {
          nodeId,
          peer: from,
          balance,
          text,
          ts: msg.ts || nowMs()
        });
        Router.sendFrom(nodeId, 'events', {
          nodeId,
          peer: from,
          type: 'market-credit-balance',
          payload: {
            ...msg,
            balance
          }
        });
      }
      emitMarketUiEvent('hydra-market-credit-preview', {
        peer: from,
        balance,
        text,
        ts: msg.ts || nowMs()
      });
      if (msg.expectAck === true) sendAck('ok', 'credit-balance-received');
      return;
    }

    if (type === 'hybrid-bridge-resource' || type === 'hybrid-bridge-command' || type === 'hybrid-bridge-geometry') {
      for (const nodeId of watcherIds) {
        const st = ensureNodeState(nodeId);
        const targetCtx = resolveInboundTargetContext(st, msg, from);
        const payload = applyTargetToPayload(msg, targetCtx);
        Router.sendFrom(nodeId, 'events', {
          nodeId,
          peer: from,
          type,
          payload
        });
        Router.sendFrom(nodeId, 'overlayIngress', {
          nodeId,
          peer: from,
          type,
          payload,
          target: targetCtx.hasTarget ? {
            sessionId: targetCtx.sessionId || '',
            objectUuid: targetCtx.objectUuid || '',
            overlayId: targetCtx.overlayId || '',
            itemId: targetCtx.itemId || '',
            layerId: targetCtx.layerId || '',
            ambiguous: !!targetCtx.ambiguous
          } : null
        });
        if (type === 'hybrid-bridge-geometry') {
          Router.sendFrom(nodeId, 'overlayGeometry', {
            nodeId,
            peer: from,
            type,
            payload,
            target: targetCtx.hasTarget ? {
              sessionId: targetCtx.sessionId || '',
              objectUuid: targetCtx.objectUuid || '',
              overlayId: targetCtx.overlayId || '',
              itemId: targetCtx.itemId || '',
              layerId: targetCtx.layerId || '',
              ambiguous: !!targetCtx.ambiguous
            } : null
          });
        }
      }
      if (msg.expectAck === true) sendAck('ok', 'applied');
      return;
    }

    if (type === 'hybrid-chat') {
      for (const nodeId of watcherIds) {
        Router.sendFrom(nodeId, 'chat', {
          nodeId,
          peer: from,
          message: msg
        });
      }
      return;
    }

    if (type === 'hybrid-bridge-handshake') {
      // Handle handshake request from NoClip peer
      for (const nodeId of watcherIds) {
        const st = ensureNodeState(nodeId);
        if (!st) continue;
        st.lastHandshakeAt = nowMs();

        // Send handshake event to node
        Router.sendFrom(nodeId, 'handshake', {
          nodeId,
          peer: from,
          capabilities: msg.capabilities || [],
          clientType: msg.clientType || 'unknown',
          graphId: msg.graphId || '',
          ts: msg.ts || nowMs()
        });

        // Auto-respond with acknowledgment
        sendBridgePayload(nodeId, st, {
          type: 'hybrid-bridge-handshake-ack',
          capabilities: ['graph', 'resources', 'commands', 'data-export', 'audio'],
          graphId: CFG?.graphId || nodeId,
          nodeId,
          ts: nowMs()
        });
        if (syncAdapter && typeof syncAdapter.updateSessionStatus === 'function') {
          syncAdapter.updateSessionStatus({
            noclipPub: from,
            hydraBridgeNodeId: nodeId,
            status: 'connected',
            lastHandshakeAt: st.lastHandshakeAt
          });
        } else {
          updateSessionsByPub(nodeId, from, { status: 'connected', lastHandshakeAt: st.lastHandshakeAt });
        }
        refreshSessionStatus(nodeId);
      }
      return;
    }

    if (type === 'smart-object-audio' || event === 'interop.asset.media') {
      const isAudioPacket =
        msg.audioPacket ||
        String(msg.kind || msg.op || '').toLowerCase() === 'audio' ||
        String(msg.contentType || msg.content_type || '').toLowerCase().startsWith('audio/');
      if (!isAudioPacket) {
        for (const nodeId of watcherIds) {
          const st = ensureNodeState(nodeId);
          const targetCtx = resolveInboundTargetContext(st, msg, from);
          const payload = applyTargetToPayload(msg, targetCtx);
          Router.sendFrom(nodeId, 'events', {
            nodeId,
            peer: from,
            type: 'hybrid-bridge-media',
            payload
          });
          Router.sendFrom(nodeId, 'overlayMedia', {
            nodeId,
            peer: from,
            type: 'hybrid-bridge-media',
            payload,
            target: targetCtx.hasTarget ? {
              sessionId: targetCtx.sessionId || '',
              objectUuid: targetCtx.objectUuid || '',
              overlayId: targetCtx.overlayId || '',
              itemId: targetCtx.itemId || '',
              layerId: targetCtx.layerId || '',
              ambiguous: !!targetCtx.ambiguous
            } : null
          });
        }
        if (msg.expectAck === true) sendAck('ok', 'media-received');
        return;
      }
      // Handle audio packets from NoClip Smart Objects for ASR
      for (const nodeId of watcherIds) {
        const st = ensureNodeState(nodeId);
        const targetCtx = resolveInboundTargetContext(st, msg, from);
        const payload = applyTargetToPayload(msg, targetCtx);
        Router.sendFrom(nodeId, 'audioInput', {
          nodeId,
          peer: from,
          objectId: payload.objectId || payload.objectUuid || payload.target?.itemId || msg.objectId,
          sessionId: payload.sessionId || '',
          target: payload.target || null,
          audioPacket: payload.audioPacket || payload,
          ts: payload.ts || nowMs()
        });
        Router.sendFrom(nodeId, 'overlayMedia', {
          nodeId,
          peer: from,
          type: 'hybrid-bridge-media',
          payload,
          target: targetCtx.hasTarget ? {
            sessionId: targetCtx.sessionId || '',
            objectUuid: targetCtx.objectUuid || '',
            overlayId: targetCtx.overlayId || '',
            itemId: targetCtx.itemId || '',
            layerId: targetCtx.layerId || '',
            ambiguous: !!targetCtx.ambiguous
          } : null
        });
      }
      if (msg.expectAck === true) sendAck('ok', 'audio-received');
      return;
    }

    if (type === 'hybrid-bridge-ack') {
      const messageId = msg.inReplyTo || msg.messageId;
      const status = (msg.status || 'ok').toLowerCase();
      const detail = msg.detail || msg.error || '';
      for (const nodeId of watcherIds) {
        const st = ensureNodeState(nodeId);
        if (!st) continue;
        resolvePendingAck(st, messageId, { status, detail });
      }
      return;
    }

    // Forward unknown events without dropping them so migration traffic stays observable.
    for (const nodeId of watcherIds) {
      Router.sendFrom(nodeId, 'events', {
        nodeId,
        peer: from,
        type: type || event || 'interop.unknown',
        payload: msg
      });
    }
  }

  const normalizeBoolean = (value, def = true) => {
    if (typeof value === 'boolean') return value;
    if (value == null) return def;
    const text = String(value).trim().toLowerCase();
    if (!text) return def;
    if (['true', '1', 'yes', 'on'].includes(text)) return true;
    if (['false', '0', 'no', 'off'].includes(text)) return false;
    return def;
  };

  const maybeBadge = (state, message, success = true) => {
    if (!setBadge || !message) return;
    const now = nowMs();
    if (now - (state.badgeLimiter || 0) < 1500) return;
    state.badgeLimiter = now;
    setBadge(message, success);
  };

  const ensureHandshake = async (nodeId, state) => {
    if (!state || !state.targetPub) return;
    try {
      await ensureDiscovery();
      const provider = getCatalogProviderMeta();
      const handshakeMeta = buildDiscoveryMeta({
        nodeId,
        catalogChecksum: latestCatalogChecksum || '',
      });
      const packet = {
        type: 'hybrid-bridge-handshake',
        event: 'interop.bridge.handshake',
        capabilities: ['graph', 'resources', 'commands', 'audio', 'marketplace'],
        clientType: 'hydra',
        graphId: CFG?.graphId || '',
        nodeId,
        provider,
        catalog_summary: provider.marketplaceCatalogSummary || {},
        catalogChecksum: latestCatalogChecksum || '',
        meta: handshakeMeta,
        ts: nowMs()
      };
      const ok = await sendBridgePayload(nodeId, state, packet, { targets: [state.targetPub] });
      if (ok) {
        state.lastHandshakeAt = nowMs();
      }
    } catch (err) {
      log?.(`[noclip] ensureHandshake failed: ${err?.message || err}`);
    }
  };

  const normalizePose = (payload) => {
    if (!payload || typeof payload !== 'object') return null;
    const out = { };
    if (Array.isArray(payload.position) && payload.position.length >= 3) {
      out.position = payload.position.slice(0, 3).map((v) => Number.isFinite(v) ? Number(v) : 0);
    }
    if (Array.isArray(payload.rotation) && payload.rotation.length) {
      out.rotation = payload.rotation.slice(0, 4).map((v) => Number.isFinite(v) ? Number(v) : 0);
    } else if (Array.isArray(payload.quaternion) && payload.quaternion.length) {
      out.rotation = payload.quaternion.slice(0, 4).map((v) => Number.isFinite(v) ? Number(v) : 0);
    } else if (typeof payload.yaw === 'number' || typeof payload.pitch === 'number' || typeof payload.roll === 'number') {
      out.euler = {
        yaw: Number(payload.yaw) || 0,
        pitch: Number(payload.pitch) || 0,
        roll: Number(payload.roll) || 0
      };
    }
    if (payload.scale != null) {
      if (Array.isArray(payload.scale)) {
        out.scale = payload.scale.slice(0, 3).map((v) => Number.isFinite(v) ? Number(v) : 1);
      } else if (Number.isFinite(payload.scale)) {
        out.scale = [Number(payload.scale), Number(payload.scale), Number(payload.scale)];
      }
    }
    if (payload.meta && typeof payload.meta === 'object') {
      out.meta = payload.meta;
    }
    if (!out.position && !out.rotation && !out.euler && !out.scale && !out.meta) {
      return null;
    }
    return out;
  };

  const resolvePacketFamily = (packetType, packetEvent, packet = {}) => {
    const type = String(packetType || '').trim().toLowerCase();
    const event = String(packetEvent || '').trim().toLowerCase();
    if (type === 'hybrid-bridge-update' || event === 'interop.asset.pose') return 'pose';
    if (type === 'hybrid-bridge-geometry' || event === 'interop.asset.geometry') return 'geometry';
    if (type === 'hybrid-bridge-command' || event === 'interop.asset.command') return 'command';
    if (
      type === 'smart-object-audio'
      || type === 'smart-object-audio-output'
      || event === 'interop.asset.media'
      || packet?.audioPacket
      || packet?.videoPacket
    ) return 'media';
    if (type === 'hybrid-bridge-resource' || event === 'interop.asset.resource') {
      return inferResourceFamily(
        packet?.resource && typeof packet.resource === 'object' ? packet.resource : {},
        packet
      );
    }
    return inferResourceFamily(
      packet?.resource && typeof packet.resource === 'object' ? packet.resource : {},
      packet
    );
  };

  const normalizePacketMetering = (packet = {}) => {
    const src = packet?.metering && typeof packet.metering === 'object' ? packet.metering : {};
    const readInt = (...values) => {
      for (const value of values) {
        const n = Number(value);
        if (Number.isFinite(n) && n >= 0) return Math.round(n);
      }
      return 0;
    };
    const readFloat = (...values) => {
      for (const value of values) {
        const n = Number(value);
        if (Number.isFinite(n) && n > 0) return n;
      }
      return 1;
    };
    return {
      quote_id: String(src.quote_id || src.quoteId || packet.quote_id || packet.quoteId || '').trim(),
      charge_id: String(src.charge_id || src.chargeId || packet.charge_id || packet.chargeId || '').trim(),
      reservation_id: String(
        src.reservation_id
        || src.reservationId
        || src.credit_reservation_id
        || packet.reservation_id
        || packet.reservationId
        || packet.credit_reservation_id
        || ''
      ).trim(),
      requested_units: readFloat(src.requested_units, src.requestedUnits, packet.requested_units, packet.requestedUnits, 1),
      price_per_unit_micros: readInt(src.price_per_unit_micros, src.pricePerUnitMicros, packet.price_per_unit_micros, packet.pricePerUnitMicros),
      max_charge_micros: readInt(src.max_charge_micros, src.maxChargeMicros, packet.max_charge_micros, packet.maxChargeMicros),
      settled_micros: readInt(src.settled_micros, src.settledMicros, packet.settled_micros, packet.settledMicros),
      correlation_id: String(src.correlation_id || src.correlationId || packet.correlation_id || packet.correlationId || '').trim(),
      usage_label: String(src.usage_label || src.usageLabel || packet.usage_label || packet.usageLabel || '').trim().toLowerCase(),
      transport_tag: String(src.transport_tag || src.transportTag || packet.transport_tag || packet.transportTag || '').trim().toLowerCase()
    };
  };

  const applyPacketMarketplaceAuth = (packet = {}) => {
    const auth = packet?.auth && typeof packet.auth === 'object' ? { ...packet.auth } : {};
    const accessTicket = String(
      packet.access_ticket
      || packet.accessTicket
      || packet.ticket
      || auth.ticket
      || auth.access_ticket
      || ''
    ).trim();
    if (accessTicket) {
      packet.access_ticket = accessTicket;
      packet.accessTicket = accessTicket;
      packet.ticket = accessTicket;
      auth.ticket = accessTicket;
      auth.access_ticket = accessTicket;
    }
    const metering = normalizePacketMetering(packet);
    packet.metering = {
      ...(packet.metering && typeof packet.metering === 'object' ? packet.metering : {}),
      ...metering
    };
    if (metering.quote_id) packet.quote_id = metering.quote_id;
    if (metering.charge_id) packet.charge_id = metering.charge_id;
    if (metering.reservation_id) packet.reservation_id = metering.reservation_id;
    if (metering.correlation_id) packet.correlation_id = metering.correlation_id;
    if (metering.transport_tag) packet.transport_tag = metering.transport_tag;
    if (metering.usage_label) packet.usage_label = metering.usage_label;
    if (!auth.scope && packet.scope) auth.scope = String(packet.scope);
    if (!auth.audience && packet.audience) auth.audience = String(packet.audience);
    if (Object.keys(auth).length) {
      packet.auth = auth;
    }
    return { accessTicket, metering };
  };

  const evaluateOutboundMutationPreflight = (packet = {}, packetType = '', packetEvent = '') => {
    const family = resolvePacketFamily(packetType, packetEvent, packet);
    const isMutation = MUTATION_FAMILIES.has(family);
    const requireAuth = isMutation && packet.requireAuth !== false && packet.authOptional !== true;
    if (isMutation && packet.requireAuth === undefined && packet.authOptional !== true) {
      packet.requireAuth = true;
    }
    const { accessTicket, metering } = applyPacketMarketplaceAuth(packet);
    const billable = packet.billable === true
      || packet.requireMarketplaceAuth === true
      || packet.requireBillingPreflight === true
      || Number(metering.price_per_unit_micros || 0) > 0
      || Number(metering.max_charge_micros || 0) > 0
      || Number(metering.settled_micros || 0) > 0
      || !!String(metering.quote_id || '').trim()
      || !!String(metering.charge_id || '').trim()
      || !!String(metering.reservation_id || '').trim();
    const requireBilling = isMutation && packet.requireBillingPreflight !== false;
    if (requireAuth && !accessTicket) {
      return {
        ok: false,
        error_code: 'ticket_missing',
        error: 'overlay mutation requires access_ticket',
        status: 403,
        family,
        billable,
        metering
      };
    }
    if (requireBilling && billable) {
      if (!String(metering.quote_id || '').trim()) {
        return {
          ok: false,
          error_code: 'quote_missing',
          error: 'billable mutation requires quote_id',
          status: 402,
          family,
          billable,
          metering
        };
      }
      const reservationMicros = Number(metering.settled_micros || 0) > 0
        ? Number(metering.settled_micros || 0)
        : Number(metering.max_charge_micros || 0);
      const hasReservation = !!String(metering.charge_id || metering.reservation_id || '').trim() || reservationMicros > 0;
      if (!hasReservation) {
        return {
          ok: false,
          error_code: 'credit_reservation_missing',
          error: 'billable mutation requires charge/reservation',
          status: 402,
          family,
          billable,
          metering
        };
      }
      const requestedUnits = Math.max(1, Number(metering.requested_units || 1) || 1);
      const estimatedMicros = Math.max(0, Math.round(requestedUnits * Math.max(0, Number(metering.price_per_unit_micros || 0) || 0)));
      if (reservationMicros > 0 && estimatedMicros > 0 && estimatedMicros > reservationMicros) {
        return {
          ok: false,
          error_code: 'insufficient_credit_reservation',
          error: `estimated charge ${estimatedMicros} exceeds reservation ${reservationMicros}`,
          status: 402,
          family,
          billable,
          metering
        };
      }
    }
    return {
      ok: true,
      error_code: '',
      error: '',
      status: 200,
      family,
      billable,
      metering
    };
  };

  const injectAuthAndMeteringHints = (packet = {}, source = {}) => {
    const src = source && typeof source === 'object' ? source : {};
    const authSrc = src.auth && typeof src.auth === 'object' ? src.auth : {};
    const authOut = packet.auth && typeof packet.auth === 'object' ? { ...packet.auth } : {};
    const accessTicket = String(
      src.access_ticket
      || src.accessTicket
      || src.ticket
      || authSrc.ticket
      || authSrc.access_ticket
      || ''
    ).trim();
    if (accessTicket) {
      packet.access_ticket = accessTicket;
      packet.accessTicket = accessTicket;
      packet.ticket = accessTicket;
      authOut.ticket = accessTicket;
      authOut.access_ticket = accessTicket;
    }
    if (src.scope || authSrc.scope) authOut.scope = String(src.scope || authSrc.scope || '').trim();
    if (src.audience || authSrc.audience) authOut.audience = String(src.audience || authSrc.audience || '').trim();
    if (src.nonce || authSrc.nonce) authOut.nonce = String(src.nonce || authSrc.nonce || '').trim();
    if (Object.keys(authOut).length) packet.auth = authOut;

    const meteringSource = src.metering && typeof src.metering === 'object' ? src.metering : {};
    packet.quote_id = String(
      src.quote_id
      || src.quoteId
      || meteringSource.quote_id
      || meteringSource.quoteId
      || packet.quote_id
      || ''
    ).trim();
    packet.charge_id = String(
      src.charge_id
      || src.chargeId
      || meteringSource.charge_id
      || meteringSource.chargeId
      || packet.charge_id
      || ''
    ).trim();
    packet.reservation_id = String(
      src.reservation_id
      || src.reservationId
      || src.credit_reservation_id
      || src.creditReservationId
      || meteringSource.reservation_id
      || meteringSource.reservationId
      || meteringSource.credit_reservation_id
      || packet.reservation_id
      || ''
    ).trim();
    packet.correlation_id = String(
      src.correlation_id
      || src.correlationId
      || meteringSource.correlation_id
      || meteringSource.correlationId
      || packet.correlation_id
      || ''
    ).trim();
    packet.usage_label = String(
      src.usage_label
      || src.usageLabel
      || meteringSource.usage_label
      || meteringSource.usageLabel
      || packet.usage_label
      || ''
    ).trim().toLowerCase();
    packet.transport_tag = String(
      src.transport_tag
      || src.transportTag
      || meteringSource.transport_tag
      || meteringSource.transportTag
      || packet.transport_tag
      || ''
    ).trim().toLowerCase();
    const readNum = (value, fallback = 0) => {
      const n = Number(value);
      if (!Number.isFinite(n)) return Number(fallback) || 0;
      return Math.max(0, Math.round(n));
    };
    packet.price_per_unit_micros = readNum(
      src.price_per_unit_micros ?? src.pricePerUnitMicros ?? meteringSource.price_per_unit_micros ?? meteringSource.pricePerUnitMicros ?? packet.price_per_unit_micros,
      packet.price_per_unit_micros || 0
    );
    packet.max_charge_micros = readNum(
      src.max_charge_micros ?? src.maxChargeMicros ?? meteringSource.max_charge_micros ?? meteringSource.maxChargeMicros ?? packet.max_charge_micros,
      packet.max_charge_micros || 0
    );
    packet.settled_micros = readNum(
      src.settled_micros ?? src.settledMicros ?? meteringSource.settled_micros ?? meteringSource.settledMicros ?? packet.settled_micros,
      packet.settled_micros || 0
    );
    const requestedUnits = Number(
      src.requested_units
      ?? src.requestedUnits
      ?? meteringSource.requested_units
      ?? meteringSource.requestedUnits
      ?? packet.requested_units
      ?? 1
    );
    packet.requested_units = Number.isFinite(requestedUnits) && requestedUnits > 0 ? requestedUnits : 1;
    if (src.requireAuth !== undefined) packet.requireAuth = src.requireAuth;
    if (src.authOptional !== undefined) packet.authOptional = src.authOptional;
    if (src.requireBillingPreflight !== undefined) packet.requireBillingPreflight = src.requireBillingPreflight;
    if (src.requireMarketplaceAuth !== undefined) packet.requireMarketplaceAuth = src.requireMarketplaceAuth;
    if (src.billable !== undefined) packet.billable = src.billable;
    return packet;
  };

  async function sendBridgePayload(nodeId, state, payload, { targets } = {}) {
    const targetList = ensureArray(targets)
      .map((value) => {
        if (!value) return '';
        if (typeof value === 'string') {
          const text = value.trim();
          const stripped = text.replace(/^graph\./i, '');
          return sanitizePubKey(stripped);
        }
        const candidate = value?.pub || value?.nknPub || value?.addr || '';
        const stripped = String(candidate).replace(/^graph\./i, '');
        return sanitizePubKey(stripped);
      })
      .filter(Boolean);
    if (state?.targetPub && !targetList.length) targetList.push(state.targetPub);
    if (!targetList.length) {
      maybeBadge(state || {}, 'No eligible peers selected', false);
      return false;
    }
    let disco;
    try {
      disco = await ensureDiscovery();
    } catch (err) {
      log?.(`[noclip] discovery init failed: ${err?.message || err}`);
      maybeBadge(state, 'Discovery unavailable', false);
      return false;
    }
    if (!disco) return false;
    let success = false;
    for (const targetPub of targetList) {
      const sourceAddress = String(Net?.nkn?.addr || state?.cfg?.address || state?.cfg?.targetAddr || '').trim();
      const targetAddress = `noclip.${targetPub}`;
      const packet = payload && typeof payload === 'object'
        ? { ...payload }
        : { payload };
      const packetType = normalizeMessageType(packet.type, packet.event);
      const packetEvent = normalizeMessageEvent(packet.event, packetType);
      if (packetType && !packet.type) packet.type = packetType;
      if (packetEvent && !packet.event) packet.event = packetEvent;
      const enforcement = evaluateOutboundMutationPreflight(packet, packetType, packetEvent);
      if (!enforcement.ok) {
        const code = String(enforcement.error_code || INTEROP_ERROR_CODES.INVALID_ENVELOPE);
        const detail = String(enforcement.error || 'bridge preflight denied');
        maybeBadge(state, `Bridge preflight denied (${code})`, false);
        if (nodeId) {
          logToNode(nodeId, `⚠️ ${detail} (${code})`, 'error');
        }
        continue;
      }
      packet.enforcement = {
        ok: true,
        family: enforcement.family || '',
        billable: !!enforcement.billable,
        metering: enforcement.metering && typeof enforcement.metering === 'object' ? { ...enforcement.metering } : {}
      };
      if (!packet.messageId) packet.messageId = newMessageId('hb');
      if (!packet.ts) packet.ts = nowMs();
      Object.assign(packet, withInteropContractFields(packet, DEFAULT_INTEROP_CONTRACT));
      if (!packet.source_network) packet.source_network = 'hydra';
      if (!packet.target_network) packet.target_network = 'noclip';
      if (!packet.source_address && sourceAddress) packet.source_address = sourceAddress;
      if (!packet.target_address) packet.target_address = targetAddress;
      const selectedTransport = selectedTransportForPacket(state, packet);
      if (!packet.transport) packet.transport = selectedTransport;
      if (!packet.selected_transport) packet.selected_transport = selectedTransport;
      if (!packet.selectedTransport) packet.selectedTransport = packet.selected_transport;
      if (!packet.contentType && packet.content_type) packet.contentType = packet.content_type;
      if (!packet.content_type && packet.contentType) packet.content_type = packet.contentType;
      if (!packet.assetId && packet.asset_id) packet.assetId = packet.asset_id;
      if (!packet.asset_id && packet.assetId) packet.asset_id = packet.assetId;
      if (packet.assetId || packet.asset_id) {
        const stableAssetId = String(packet.assetId || packet.asset_id || '').trim();
        packet.assetId = stableAssetId;
        packet.asset_id = stableAssetId;
      }
      if (packet.chunk != null && packet.total != null) {
        const chunk = Number(packet.chunk);
        const total = Number(packet.total);
        if (Number.isFinite(chunk)) packet.chunk = chunk;
        if (Number.isFinite(total)) packet.total = total;
      }
      if (!packet.checksum) {
        const checkSource =
          packet.chunk_payload ||
          packet.chunkPayload ||
          packet.data_chunk ||
          packet.payload_data ||
          packet.audioPacket ||
          packet.pose ||
          packet.resource ||
          packet.command ||
          packet.payload;
        packet.checksum = checksumHex(checkSource);
      }
      try {
        await disco.dm(targetPub, { ...packet, target: targetPub });
        success = true;
      } catch (err) {
        log?.(`[noclip] send failed to ${targetPub}: ${err?.message || err}`);
      }
    }
    if (!success) maybeBadge(state, 'Bridge send failed', false);
    return success;
  }

  const requestSync = async (nodeId, targetPub, options = {}) => {
    const state = nodeId ? ensureNodeState(nodeId) : null;
    const badgeState = state || {};
    const silent = normalizeBoolean(options?.silent ?? options?.suppressBadge, false);
    const record = nodeId ? NodeStore.ensure(nodeId, 'NoClipBridge') : null;
    const config = record?.config || {};
    const normalized = sanitizePubKey(targetPub || state?.targetPub || config.targetPub || '');
    if (!normalized) {
      if (!silent) maybeBadge(badgeState, 'Select a NoClip peer first', false);
      throw new Error('NoClip peer not selected');
    }

    if (nodeId && state && state.targetPub !== normalized) {
      setTargetPeer(nodeId, normalized);
    }

    const resolvedRoom = resolveRoomName(config.room || state?.room || '');
    const hydraAddr = Net?.nkn?.addr || Net?.nkn?.client?.addr || '';
    const hydraPub = sanitizePubKey(Net?.nkn?.client?.getPublicKey?.() || hydraAddr);
    const selectedTransport = selectedTransportForPacket(state, options || {});
    const payload = {
      type: 'noclip-bridge-sync-request',
      event: 'interop.bridge.sync_request',
      messageId: newMessageId('sync'),
      hydraAddr: hydraAddr || (hydraPub ? `hydra.${hydraPub}` : ''),
      hydraPub,
      hydraGraphId: CFG?.graphId || '',
      bridgeNodeId: nodeId,
      discoveryRoom: resolvedRoom,
      source_network: 'hydra',
      target_network: 'noclip',
      selected_transport: selectedTransport,
      transport: selectedTransport,
      timestamp: Date.now()
    };
    Object.assign(payload, withInteropContractFields(payload, DEFAULT_INTEROP_CONTRACT));
    if (options.objectId) payload.objectId = options.objectId;
    if (options.objectLabel) payload.objectLabel = options.objectLabel;
    if (options.objectConfig && typeof options.objectConfig === 'object') {
      payload.objectConfig = { ...options.objectConfig };
    }

    try {
      if (sharedPeerDiscovery?.sendDm) {
        await sharedPeerDiscovery.sendDm(normalized, payload);
      } else {
        const disco = await ensureDiscovery();
        if (!disco?.dm) throw new Error('Discovery channel unavailable');
        await disco.dm(normalized, payload);
      }
      if (nodeId) {
        logToNode(nodeId, `→ Sync request sent to noclip.${normalized.slice(0, 8)}…`, 'info');
      }
      if (!silent) {
        maybeBadge(badgeState, `Sync request sent to noclip.${normalized.slice(0, 8)}…`);
      }
      if (!nodeId && !silent) {
        setBadge?.(`Sync request sent to noclip.${normalized.slice(0, 8)}…`);
      }
    } catch (err) {
      if (nodeId) {
        logToNode(nodeId, `✗ Sync request failed: ${err?.message || err}`, 'error');
      }
      if (!silent) {
        maybeBadge(badgeState, `Sync request failed: ${err?.message || err}`, false);
      }
      if (!nodeId && !silent) {
        setBadge?.(`Sync request failed: ${err?.message || err}`, false);
      }
      throw err;
    }
  };

  const setTargetPeer = (nodeId, pub) => {
    const st = ensureNodeState(nodeId);
    if (!st) return;
    const normalized = sanitizePubKey(pub);
    if (st.targetPub && st.targetPub !== normalized) {
      unregisterTarget(nodeId, st.targetPub);
    }
    if (normalized) registerTarget(nodeId, normalized);
    st.targetPub = normalized || '';
    st.targetAddr = normalized ? `noclip.${normalized}` : '';

    const record = NodeStore.ensure(nodeId, 'NoClipBridge');
    const nextCfg = {
      ...(record?.config || {}),
      targetPub: st.targetPub,
      targetAddr: st.targetAddr
    };
    NodeStore.saveCfg(nodeId, 'NoClipBridge', nextCfg);
    if (record) record.config = nextCfg;
    st.cfg = nextCfg;
    refreshPeerDropdown(nodeId, { silent: true });
  };

  const createMessageId = () => `msg-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

  const registerPendingAck = (state, messageId, info) => {
    if (!state || !messageId) return;
    if (!state.pendingAcks) state.pendingAcks = new Map();
    const attempt = Math.max(1, Number(info?.attempt || 1) || 1);
    const maxAttempts = Math.max(attempt, Number(info?.maxAttempts || 1) || 1);
    const timer = setTimeout(() => {
      state.pendingAcks.delete(messageId);
      const canRetry = typeof info?.onTimeout === 'function' && attempt < maxAttempts;
      if (canRetry) {
        const nextAttempt = attempt + 1;
        Promise.resolve()
          .then(() => info.onTimeout(nextAttempt))
          .then((ok) => {
            if (!ok) {
              if (info?.nodeId) {
                logToNode(info.nodeId, `⚠️ Ack retry failed for ${info.type || 'message'} (${messageId.slice(0, 8)})`, 'warn');
              }
              return;
            }
            registerPendingAck(state, messageId, {
              ...info,
              attempt: nextAttempt,
              maxAttempts
            });
            if (info?.nodeId) {
              logToNode(info.nodeId, `↺ Retrying ${info.type || 'message'} ack (${nextAttempt}/${maxAttempts})`, 'warn');
            }
          })
          .catch((err) => {
            if (info?.nodeId) {
              logToNode(info.nodeId, `⚠️ Ack retry error for ${info.type || 'message'}: ${err?.message || err}`, 'error');
            }
          });
        return;
      }
      if (info?.nodeId) {
        logToNode(info.nodeId, `⚠️ Awaiting ack for ${info.type || 'message'} (${messageId.slice(0, 8)})`, 'warn');
      }
    }, MESSAGE_ACK_TIMEOUT_MS);
    state.pendingAcks.set(messageId, { ...info, timer });
  };

  const resolvePendingAck = (state, messageId, ack = {}) => {
    if (!state?.pendingAcks || !state.pendingAcks.size || !messageId) return;
    const entry = state.pendingAcks.get(messageId);
    if (!entry) return;
    clearTimeout(entry.timer);
    state.pendingAcks.delete(messageId);
    const status = ack.status || 'ok';
    const detail = ack.detail || ack.error || '';
    const errorCode = String(ack.error_code || ack.errorCode || '').trim();
    const statusCode = Number(ack.status_code || ack.statusCode || 0) || 0;
    if (entry.nodeId) {
      const prefix = status === 'ok' ? '✅' : '⚠️';
      const logType = status === 'ok' ? 'success' : 'error';
      const label = entry.type || 'message';
      const codeSuffix = errorCode ? ` • ${errorCode}` : '';
      const httpSuffix = statusCode > 0 ? ` • ${statusCode}` : '';
      const logMessage = `${prefix} ${label} ${status}${httpSuffix}${codeSuffix}${detail ? ` • ${detail}` : ''}`;
      logToNode(entry.nodeId, logMessage, logType);
    }
  };

  const sendTypedBridgeMessage = async (
    nodeId,
    state,
    message,
    { expectAck = true, targets = undefined, maxAckRetries = 0 } = {}
  ) => {
    if (!state) return { ok: false, messageId: null };
    const messageId = message.messageId || createMessageId();
    const packet = {
      ...message,
      nodeId,
      messageId,
      ts: nowMs()
    };
    const ok = await sendBridgePayload(nodeId, state, packet, { targets });
    if (ok && expectAck && messageId) {
      const resendPayload = { ...packet };
      registerPendingAck(state, messageId, {
        nodeId,
        type: packet.type,
        sessionId: packet.sessionId,
        objectUuid: packet.objectUuid,
        attempt: 1,
        maxAttempts: Math.max(1, Number(maxAckRetries || 0) + 1),
        onTimeout: async () => sendBridgePayload(nodeId, state, resendPayload, { targets })
      });
    }
    return { ok, messageId };
  };

  /**
   * Log a message to the NoClipBridge node's UI log
   */
  function logToNode(nodeId, message, type = 'info') {
    const node = NodeStore?.get?.(nodeId);
    if (!node || !node.el) return;

    const logEl = node.el.querySelector('[data-noclip-log]');
    if (!logEl) return;

    const entry = document.createElement('div');
    entry.className = `log-entry log-${type}`;

    const timestamp = new Date().toLocaleTimeString();
    entry.innerHTML = `<span style="color:#666;">[${timestamp}]</span> ${message}`;

    logEl.appendChild(entry);

    // Auto-scroll to bottom
    logEl.scrollTop = logEl.scrollHeight;

    // Limit to 50 entries
    const entries = logEl.querySelectorAll('.log-entry');
    if (entries.length > 50) {
      entries[0].remove();
    }
  }

  /**
   * Get list of discovered NoClip peers
   */
  function getDiscoveredNoClipPeers() {
    return Array.from(NOCLIP_PEERS.values());
  }

  function listSessions(nodeId) {
    const st = NODE_STATE.get(nodeId);
    if (!st?.sessions?.size) return [];
    return Array.from(st.sessions.values()).map((session) => ({ ...session }));
  }

  /**
   * Refresh the peer dropdown for a node
   */
  function refreshPeerDropdown(nodeId, { silent = false } = {}) {
    const state = ensureNodeState(nodeId);
    const rootEl = state?.rootEl;
    if (!rootEl) return;

    const selectEl = rootEl.querySelector('[data-noclip-peer-select]');
    if (!selectEl) return;

    const currentValue = (selectEl.value || state?.targetPub || state?.cfg?.targetPub || '').trim();
    const peers = getDiscoveredNoClipPeers().sort((a, b) => (b.last || 0) - (a.last || 0));

    // Clear and rebuild options
    selectEl.innerHTML = '<option value="">-- Select NoClip Peer --</option>';

    peers.forEach(peer => {
      const rawPub = peer?.nknPub || peer?.pub || peer?.addr || '';
      const cleanPub = sanitizePubKey(rawPub);
      if (!cleanPub) return;
      const option = document.createElement('option');
      option.value = cleanPub;
      const alias = peer.meta?.username || peer.meta?.name || peer.meta?.alias || `NoClip ${cleanPub.slice(0, 6)}…`;
      const shortPub = `${cleanPub.slice(0, 6)}…${cleanPub.slice(-4)}`;
      const lastSeen = formatLastSeen(peer.last);
      const selectedTransport = normalizeTransportLabel(
        peer?.selectedTransport ||
        peer?.selected_transport ||
        peer?.meta?.selectedTransport ||
        peer?.meta?.selected_transport
      );
      const parts = [alias, shortPub];
      if (selectedTransport) parts.push(selectedTransport);
      if (lastSeen) parts.push(lastSeen);
      if (peer.geo && Number.isFinite(peer.geo.lat) && Number.isFinite(peer.geo.lon)) {
        parts.push(`${Number(peer.geo.lat).toFixed(2)}, ${Number(peer.geo.lon).toFixed(2)}`);
      }
      option.textContent = parts.join(' • ');
      option.title = `noclip.${cleanPub}`;
      selectEl.appendChild(option);
    });

    // Restore selection if it still exists
    if (currentValue && Array.from(selectEl.options).some(opt => opt.value === currentValue)) {
      selectEl.value = currentValue;
    } else if (!selectEl.value && state?.targetPub) {
      selectEl.value = state.targetPub;
    }

    if (!silent) logToNode(nodeId, `Found ${peers.length} NoClip peer(s)`, 'info');
    refreshSessionStatus(nodeId);
  }

  const refreshSessionStatus = (nodeId) => {
    const state = NODE_STATE.get(nodeId);
    const rootEl = state?.rootEl;
    if (!rootEl) return;
    const statusEl = rootEl.querySelector('[data-noclip-session-status]');
    const listEl = rootEl.querySelector('[data-noclip-session-list]');
    if (!statusEl) return;
    const st = NODE_STATE.get(nodeId);
    if (!st || !st.sessions || !st.sessions.size) {
      statusEl.textContent = 'No active sessions';
      statusEl.style.color = 'var(--muted)';
      if (listEl) {
        listEl.innerHTML = '<div data-empty>Sessions will appear here after approval.</div>';
      }
      return;
    }
    const sessions = Array.from(st.sessions.values());
    const listFragments = document.createDocumentFragment();
    const summaries = sessions.map((session) => {
      const label = session.objectLabel || session.objectUuid || session.sessionId;
      const status = (session.status || 'pending').replace(/[_-]/g, ' ');
      const coords = session.position && Number.isFinite(session.position.lat) && Number.isFinite(session.position.lon)
        ? ` @ ${session.position.lat.toFixed(4)}, ${session.position.lon.toFixed(4)}`
        : '';
      return `${label}: ${status}${coords}`;
    });
    statusEl.textContent = summaries.join(' • ');
    statusEl.style.color = 'var(--accent)';
    if (listEl) {
      listEl.innerHTML = '';
      const formatDelta = (ts) => {
        if (!Number.isFinite(ts)) return '';
        const diff = Math.max(0, nowMs() - ts);
        const sec = Math.floor(diff / 1000);
        if (sec < 60) return `${sec}s ago`;
        const min = Math.floor(sec / 60);
        if (min < 60) return `${min}m ago`;
        const hr = Math.floor(min / 60);
        if (hr < 24) return `${hr}h ago`;
        const day = Math.floor(hr / 24);
        return `${day}d ago`;
      };
      sessions.forEach((session) => {
        const row = document.createElement('div');
        row.className = 'noclip-session-row';
        row.style.marginBottom = '6px';
        const status = (session.status || 'pending').replace(/[_-]/g, ' ');
        const label = session.objectLabel || session.objectUuid || session.sessionId;
        const geoParts = [];
        if (session.position && Number.isFinite(session.position.lat) && Number.isFinite(session.position.lon)) {
          geoParts.push(`${session.position.lat.toFixed(4)}, ${session.position.lon.toFixed(4)}`);
        } else if (session.geo && Number.isFinite(session.geo.lat) && Number.isFinite(session.geo.lon)) {
          geoParts.push(`${session.geo.lat.toFixed(4)}, ${session.geo.lon.toFixed(4)}`);
        }
        if (Number.isFinite(session.position?.alt)) geoParts.push(`${Number(session.position.alt).toFixed(1)}m`);
        const geoText = geoParts.length ? ` • ${geoParts.join(' • ')}` : '';
        const bridge = session.hydraBridgeNodeId ? ` • bridge ${session.hydraBridgeNodeId}` : '';
        const last = formatDelta(session.updatedAt);
        row.innerHTML = `<strong>${label}</strong> • ${status}${geoText}${bridge}${last ? ` • ${last}` : ''}`;
        listFragments.appendChild(row);
      });
      listEl.appendChild(listFragments);
    }
  };

  const storeSessionForNode = (nodeId, session) => {
    if (!session) return null;
    const st = ensureNodeState(nodeId);
    if (!st) return null;
    if (!st.sessions) st.sessions = new Map();
    if (!st.sessionIndex) st.sessionIndex = new Map();
    const normalizedPub = sanitizePubKey(session.noclipPub || session.noclipAddr || '');
    const sessionId = String(session.sessionId || `${normalizedPub || 'session'}-${nowMs().toString(36)}`);
    const record = {
      ...st.sessions.get(sessionId),
      ...session,
      sessionId,
      noclipPub: normalizedPub,
      objectUuid: pickFirstId(session.objectUuid, session.objectId, session.itemId),
      overlayId: pickFirstId(session.overlayId, session.overlay_id),
      itemId: pickFirstId(session.itemId, session.item_id, session.objectUuid, session.objectId),
      layerId: pickFirstId(session.layerId, session.layer_id),
      updatedAt: nowMs()
    };
    if (session.position && typeof session.position === 'object') {
      record.position = { ...session.position };
    }
    if (session.geo && typeof session.geo === 'object') {
      record.geo = { ...session.geo };
    }
    st.sessions.set(sessionId, record);
    if (normalizedPub) {
      if (!st.sessionIndex.has(normalizedPub)) st.sessionIndex.set(normalizedPub, new Set());
      st.sessionIndex.get(normalizedPub).add(sessionId);
    }
    refreshSessionStatus(nodeId);
    return record;
  };

  const updateSessionsByPub = (nodeId, pub, updates = {}) => {
    const st = NODE_STATE.get(nodeId);
    if (!st?.sessions?.size) return [];
    const normalized = sanitizePubKey(pub);
    const list = [];
    for (const [sessionId, existing] of st.sessions.entries()) {
      if (normalized && existing.noclipPub !== normalized) continue;
      const merged = {
        ...existing,
        ...updates,
        updatedAt: nowMs()
      };
      if (updates.position && typeof updates.position === 'object') {
        merged.position = { ...(existing.position || {}), ...updates.position };
      }
      if (updates.geo && typeof updates.geo === 'object') {
        merged.geo = { ...(existing.geo || {}), ...updates.geo };
      }
      st.sessions.set(sessionId, merged);
      list.push(merged);
    }
    if (list.length) refreshSessionStatus(nodeId);
    return list;
  };

  const handleSessionUpdate = (session) => {
    if (!session) return;
    const nodeId = session.hydraBridgeNodeId || session.bridgeNodeId || session.nodeId;
    if (!nodeId) return;
    const stored = storeSessionForNode(nodeId, session);
    if (!stored) return;
    const cfgPatch = {};
    if (stored.sessionId) cfgPatch.sessionId = stored.sessionId;
    if (stored.objectUuid) cfgPatch.objectUuid = stored.objectUuid;
    if (stored.overlayId) cfgPatch.overlayId = stored.overlayId;
    if (stored.itemId) cfgPatch.itemId = stored.itemId;
    if (stored.layerId) cfgPatch.layerId = stored.layerId;
    if (!Object.keys(cfgPatch).length) return;
    cfgPatch.interopTarget = {
      sessionId: cfgPatch.sessionId || '',
      objectUuid: cfgPatch.objectUuid || '',
      overlayId: cfgPatch.overlayId || '',
      itemId: cfgPatch.itemId || '',
      layerId: cfgPatch.layerId || ''
    };
    try {
      const currentCfg = NodeStore.ensure(nodeId, 'NoClipBridge')?.config || {};
      const changed = Object.entries(cfgPatch).some(([key, value]) => {
        if (key === 'interopTarget') {
          const curr = currentCfg.interopTarget || {};
          return (
            String(curr.sessionId || '') !== String(value.sessionId || '') ||
            String(curr.objectUuid || '') !== String(value.objectUuid || '') ||
            String(curr.overlayId || '') !== String(value.overlayId || '') ||
            String(curr.itemId || '') !== String(value.itemId || '') ||
            String(curr.layerId || '') !== String(value.layerId || '')
          );
        }
        return String(currentCfg[key] || '') !== String(value || '');
      });
      if (changed) {
        NodeStore.update(nodeId, { type: 'NoClipBridge', ...cfgPatch });
      }
    } catch (err) {
      log?.(`[noclip-bridge] Failed to persist session target for ${nodeId}: ${err?.message || err}`);
    }
  };

  const refreshDiscoveryMetadata = async () => {
    if (!discovery) return;
    try {
      if (discovery.me && typeof discovery.me === 'object') {
        discovery.me.meta = buildDiscoveryMeta(discovery.me.meta || {});
      }
      if (typeof discovery.presence === 'function') {
        await discovery.presence(discovery.me?.meta || buildDiscoveryMeta());
      }
    } catch (err) {
      log?.(`[noclip] discovery presence update failed: ${err?.message || err}`);
    }
  };

  const publishMarketplaceCatalog = async (options = {}) => {
    const opts = options && typeof options === 'object' ? options : {};
    const targetPubFilter = sanitizePubKey(opts.targetPub || opts.target || '');
    const includeStatus = opts.includeStatus !== false;
    const force = opts.force === true;
    const catalog = normalizeMarketplaceCatalog(latestRouterCatalog);
    if (!catalog) {
      return { ok: false, reason: 'no_catalog', attempted: 0, sent: 0 };
    }
    const checksum = String(opts.catalogChecksum || latestCatalogChecksum || checksumHex(catalog)).trim();
    const ts = nowMs();
    const catalogPayloadBase = {
      type: 'market-service-catalog',
      event: 'market.service.catalog',
      provider: catalog.provider,
      summary: catalog.summary,
      catalog,
      catalogChecksum: checksum,
      generated_at_ms: catalog.generatedAtMs,
      ts
    };
    const statusEntries = ensureArray(catalog.services).map((entry) => ({
      service_id: entry.serviceId,
      status: entry.status,
      healthy: entry.healthy === true,
      visibility: entry.visibility || 'public',
      selected_transport: entry.selectedTransport || '',
      selected_endpoint: entry.selectedEndpoint || '',
      stale_rejected: entry.staleRejected === true,
      ts
    }));
    const statusPayloadBase = {
      type: 'market-service-status',
      event: 'market.service.status',
      provider: catalog.provider,
      summary: catalog.summary,
      services: statusEntries,
      catalogChecksum: checksum,
      ts
    };

    let attempted = 0;
    let sent = 0;
    const nodeEntries = Array.from(NODE_STATE.entries());
    for (const [nodeId, st] of nodeEntries) {
      if (!st?.targetPub) continue;
      if (targetPubFilter && st.targetPub !== targetPubFilter) continue;
      attempted += 1;
      const selectedTransport = selectedTransportForPacket(st, {});
      const catalogPayload = {
        ...catalogPayloadBase,
        selected_transport: selectedTransport,
        transport: selectedTransport
      };
      const catalogOk = await sendBridgePayload(nodeId, st, catalogPayload, {
        targets: [st.targetPub]
      });
      if (!catalogOk) continue;
      sent += 1;
      if (includeStatus) {
        const statusPayload = {
          ...statusPayloadBase,
          selected_transport: selectedTransport,
          transport: selectedTransport
        };
        await sendBridgePayload(nodeId, st, statusPayload, {
          targets: [st.targetPub]
        });
      }
    }

    if (attempted === 0 || sent > 0 || force) {
      await refreshDiscoveryMetadata();
    }

    if (sent > 0 && opts.silent !== true) {
      setBadge?.(`Marketplace catalog published to ${sent}/${attempted} peer${attempted === 1 ? '' : 's'}`);
    } else if (attempted > 0 && sent === 0 && opts.silent !== true) {
      setBadge?.('Marketplace catalog publish failed', false);
    }

    return { ok: sent > 0, attempted, sent, checksum };
  };

  const onRouterCatalog = (catalogValue, options = {}) => {
    const opts = options && typeof options === 'object' ? options : {};
    const normalized = normalizeMarketplaceCatalog(catalogValue);
    if (!normalized) {
      return { ok: false, reason: 'invalid_catalog' };
    }
    const incomingTs = toNonNegativeInt(
      normalized.generatedAtMs || catalogValue?.generated_at_ms || catalogValue?.generatedAtMs,
      0
    );
    const currentTs = toNonNegativeInt(latestRouterCatalog?.generatedAtMs, 0);
    if (!opts.force && incomingTs > 0 && currentTs > 0 && incomingTs < currentTs) {
      return { ok: false, reason: 'stale_catalog', generatedAtMs: incomingTs, currentGeneratedAtMs: currentTs };
    }
    const checksum = checksumHex(normalized);
    const changed = checksum !== latestCatalogChecksum;
    if (!changed && !opts.force) {
      return { ok: true, changed: false, checksum };
    }
    latestRouterCatalog = normalized;
    latestCatalogChecksum = checksum;
    emitMarketUiEvent('hydra-market-catalog', {
      peer: '',
      catalog: {
        generated_at_ms: normalized.generatedAtMs,
        provider: normalized.provider,
        summary: normalized.summary,
        services: normalized.services
      },
      summary: normalized.summary,
      provider: normalized.provider,
      catalogChecksum: checksum,
      ts: incomingTs || normalized.generatedAtMs || nowMs()
    });
    emitMarketUiEvent('hydra-market-status', {
      peer: '',
      summary: normalized.summary,
      provider: normalized.provider,
      services: normalized.services.map((entry) => ({
        service_id: entry.serviceId,
        status: entry.status,
        healthy: entry.healthy === true,
        visibility: entry.visibility || 'public',
        selected_transport: entry.selectedTransport || ''
      })),
      catalogChecksum: checksum,
      ts: incomingTs || normalized.generatedAtMs || nowMs()
    });
    if (CFG && typeof CFG === 'object') {
      CFG.routerMarketplaceCatalog = catalogValue;
      if (incomingTs > 0) CFG.routerLastCatalogAt = incomingTs;
    }
    if (opts.broadcast !== false) {
      void publishMarketplaceCatalog({
        catalogChecksum: checksum,
        includeStatus: opts.includeStatus !== false,
        silent: opts.silent === true,
        force: opts.force === true,
        targetPub: opts.targetPub || ''
      });
    } else {
      void refreshDiscoveryMetadata();
    }
    return {
      ok: true,
      changed: true,
      checksum,
      generatedAtMs: incomingTs || normalized.generatedAtMs || nowMs(),
      serviceCount: ensureArray(normalized.services).length
    };
  };

  return {
    init(nodeId) {
      const st = ensureNodeState(nodeId);
      if (!st) return;
      if (normalizeBoolean(st.cfg?.autoConnect, true)) {
        ensureHandshake(nodeId, st);
      }

      // Log init
      logToNode(nodeId, 'NoClipBridge initialized', 'info');
      refreshSessionStatus(nodeId);
    },

    refresh(nodeId) {
      const st = ensureNodeState(nodeId);
      if (!st) return;
      const record = NodeStore.ensure(nodeId, 'NoClipBridge');
      const cfg = record?.config || {};
      st.cfg = cfg;
      st.graphId = CFG?.graphId || st.graphId;
      const rawRoom = (cfg.room ?? '').trim();
      const loweredRoom = rawRoom.toLowerCase();
      let explicitRoom = null;
      if (rawRoom && loweredRoom !== 'auto' && loweredRoom !== 'default' && loweredRoom !== 'hybrid-bridge') {
        explicitRoom = sanitizeRoomName(rawRoom);
      } else {
        if (rawRoom && loweredRoom === 'hybrid-bridge') {
          const updated = NodeStore.update(nodeId, { type: 'NoClipBridge', room: 'auto' });
          if (updated && typeof updated === 'object') cfg.room = updated.room;
        }
      }
      overrideRoom = explicitRoom;
      const resolvedRoom = resolveRoomName(explicitRoom || cfg.room || '');
      st.resolvedRoom = resolvedRoom;
      const rootEl = st.rootEl;
      if (rootEl) {
        const roomEl = rootEl.querySelector('[data-noclip-room]');
        if (roomEl) roomEl.textContent = resolvedRoom;
      }

      // Check for ?peer= URL parameter and use it if no targetPub configured
      let targetPub = sanitizePubKey(cfg.targetPub);
      if (!targetPub) {
        const urlPeer = consumePeerParam();
        if (urlPeer) {
          targetPub = urlPeer;
          // Update config with URL peer
          if (record && record.config) {
            record.config.targetPub = targetPub;
          }
          log?.(`[noclipBridge] Using peer from URL: ${targetPub.slice(0, 8)}...`);
        }
      }

      st.targetPub = targetPub;
      st.targetAddr = sanitizeAddr(cfg.targetAddr) || null;
      st.remotePeers.clear();
      registerTarget(nodeId, st.targetPub);
      if (normalizeBoolean(cfg.autoConnect, true) && st.targetPub) {
        ensureHandshake(nodeId, st);
      }
      schedulePeerDropdownRefresh();
      refreshSessionStatus(nodeId);
    },

    async onPose(nodeId, payload) {
      const st = ensureNodeState(nodeId);
      if (!st) return;
      const pose = normalizePose(payload);
      if (!pose) return;
      const targetCtx = resolveOutboundTargetContext(st, payload || {});
      const selectedTransport = selectedTransportForPacket(st, payload || {});
      const packet = {
        type: 'hybrid-bridge-update',
        event: 'interop.asset.pose',
        nodeId,
        pose,
        assetId: `pose-${nodeId}`,
        contentType: 'application/json',
        checksum: checksumHex(pose),
        selected_transport: selectedTransport,
        transport: selectedTransport,
        ts: nowMs()
      };
      injectAuthAndMeteringHints(packet, payload || {});
      if (targetCtx.hasTarget) {
        packet.sessionId = targetCtx.sessionId || '';
        packet.objectUuid = targetCtx.objectUuid || targetCtx.itemId || '';
        packet.objectId = packet.objectUuid;
        packet.target = {
          overlayId: targetCtx.overlayId || '',
          itemId: targetCtx.itemId || packet.objectUuid || '',
          layerId: targetCtx.layerId || '',
          sessionId: targetCtx.sessionId || '',
          objectUuid: packet.objectUuid || ''
        };
      }
      const ok = await sendBridgePayload(nodeId, st, packet);
      if (ok) maybeBadge(st, 'Sent pose update');
    },

    async onResource(nodeId, payload) {
      const st = ensureNodeState(nodeId);
      if (!st || !payload) return;
      let request = payload;
      if (typeof payload === 'string') request = { resource: { type: 'text', value: payload } };
      if (Array.isArray(payload)) request = { resource: { items: payload } };
      if (!request || typeof request !== 'object') return;

      let resource = request.resource;
      if (!resource || typeof resource !== 'object') {
        if (request && typeof request === 'object') {
          resource = { ...request };
        } else {
          resource = { type: 'object', value: request };
        }
      }

      if (!resource.id) {
        resource.id = `res-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      }

      if (!resource.label) {
        const graphLabel = st.graphId || CFG?.graphId || '';
        resource.label = graphLabel ? `${graphLabel}` : `node:${nodeId}`;
      }

      const filters = request.filters || resource.filters || null;
      const targetSeeds = ensureArray(request.targets || resource.targets);
      let targets = targetSeeds
        .map((target) => sanitizePubKey(typeof target === 'string' ? target : target?.pub || target?.nknPub))
        .filter(Boolean);
      if (!targets.length && filters) {
        targets = selectTargets(st, filters);
      }

      const selectedTransport = selectedTransportForPacket(st, request || {});
      const targetCtx = resolveOutboundTargetContext(st, request || {});
      const assetId = String(
        request.assetId ||
        request.asset_id ||
        resource.assetId ||
        resource.asset_id ||
        resource.id ||
        `asset-${Date.now().toString(36)}`
      ).trim();
      const family = inferResourceFamily(resource, request);
      const outboundType = family === 'geometry'
        ? 'hybrid-bridge-geometry'
        : (family === 'media' ? 'smart-object-audio-output' : 'hybrid-bridge-resource');
      const contentType = String(
        request.contentType ||
        request.content_type ||
        resource.contentType ||
        resource.content_type ||
        (family === 'media' ? (resource.mime || 'application/octet-stream') : 'application/json')
      ).trim() || 'application/json';
      const resourcePayload = {
        ...resource,
        assetId,
        asset_id: assetId,
        contentType,
        content_type: contentType,
        selected_transport: selectedTransport,
        transport: selectedTransport
      };
      if (targetCtx.hasTarget) {
        resourcePayload.sessionId = targetCtx.sessionId || '';
        resourcePayload.objectUuid = targetCtx.objectUuid || targetCtx.itemId || '';
        resourcePayload.objectId = resourcePayload.objectUuid;
        resourcePayload.target = {
          overlayId: targetCtx.overlayId || '',
          itemId: targetCtx.itemId || resourcePayload.objectUuid || '',
          layerId: targetCtx.layerId || '',
          sessionId: targetCtx.sessionId || '',
          objectUuid: resourcePayload.objectUuid || ''
        };
      }

      const serializedResource = stableJson(resourcePayload);
      const checksum = checksumHex(serializedResource);
      const shouldChunk = family === 'geometry' && serializedResource.length > MAX_DM_PAYLOAD_CHARS;

      if (shouldChunk) {
        const transferId = newMessageId('asset');
        const chunks = chunkText(serializedResource, MAX_DM_PAYLOAD_CHARS);
        let sent = 0;
        for (let i = 0; i < chunks.length; i++) {
          const chunkPacket = {
            type: 'hybrid-bridge-geometry',
            event: 'interop.asset.geometry',
            nodeId,
            assetId,
            asset_id: assetId,
            transferId,
            transfer_id: transferId,
            chunk: i,
            chunk_number: i + 1,
            total: chunks.length,
            chunk_payload: chunks[i],
            payload_encoding: 'utf8-json',
            contentType,
            content_type: contentType,
            checksum,
            selected_transport: selectedTransport,
            transport: selectedTransport,
            expectAck: true
          };
          injectAuthAndMeteringHints(chunkPacket, request || {});
          const result = await sendTypedBridgeMessage(nodeId, st, chunkPacket, {
            expectAck: true,
            targets,
            maxAckRetries: 2
          });
          if (result.ok) sent += 1;
        }
        const ok = sent === chunks.length;
        Router.sendFrom(nodeId, 'events', {
          nodeId,
          peer: targets,
          type: 'resource-dispatched',
          payload: {
            resource: resourcePayload,
            filters,
            success: ok,
            targets,
            chunked: true,
            chunks: chunks.length,
            assetId,
            checksum,
            contentType,
            selected_transport: selectedTransport
          }
        });
        if (ok) maybeBadge(st, `Geometry sent (${chunks.length} chunks)`);
        else maybeBadge(st, 'Geometry chunk send failed', false);
        logToNode(nodeId, `📤 Geometry out: ${assetId} • ${chunks.length} chunks`, ok ? 'success' : 'error');
        return;
      }

      const packet = {
        type: outboundType,
        event: normalizeMessageEvent('', outboundType),
        messageId: String(
          request.messageId ||
          request.message_id ||
          resourcePayload.messageId ||
          resourcePayload.message_id ||
          ''
        ).trim() || undefined,
        nodeId,
        issuer: {
          graphId: st.graphId || CFG?.graphId || '',
          nodeId,
          address: st.cfg?.address || st.cfg?.targetAddr || ''
        },
        resource: resourcePayload,
        kind: resourcePayload.kind || family,
        data: resourcePayload.data || null,
        chunk: resourcePayload.chunk ?? request.chunk,
        chunk_number: resourcePayload.chunk_number ?? resourcePayload.chunkNumber ?? request.chunk_number ?? request.chunkNumber,
        total: resourcePayload.total ?? request.total,
        chunk_payload: resourcePayload.chunk_payload || resourcePayload.chunkPayload || request.chunk_payload || request.chunkPayload || '',
        payload_encoding: resourcePayload.payload_encoding || request.payload_encoding || '',
        assetId,
        asset_id: assetId,
        contentType,
        content_type: contentType,
        checksum,
        selected_transport: selectedTransport,
        transport: selectedTransport,
        filters: filters || null,
        ts: nowMs()
      };
      injectAuthAndMeteringHints(packet, request || {});
      if (targetCtx.hasTarget) {
        packet.sessionId = packet.sessionId || targetCtx.sessionId || '';
        packet.objectUuid = packet.objectUuid || targetCtx.objectUuid || targetCtx.itemId || '';
        packet.objectId = packet.objectUuid || packet.objectId || '';
        packet.target = {
          ...(packet.target && typeof packet.target === 'object' ? packet.target : {}),
          overlayId: targetCtx.overlayId || '',
          itemId: targetCtx.itemId || packet.objectUuid || '',
          layerId: targetCtx.layerId || '',
          sessionId: targetCtx.sessionId || '',
          objectUuid: packet.objectUuid || ''
        };
      }

      const ok = outboundType === 'hybrid-bridge-geometry'
        ? (await sendTypedBridgeMessage(nodeId, st, packet, { expectAck: true, targets, maxAckRetries: 1 })).ok
        : await sendBridgePayload(nodeId, st, packet, { targets });
      Router.sendFrom(nodeId, 'events', {
        nodeId,
        peer: targets,
        type: 'resource-dispatched',
        payload: {
          resource: resourcePayload,
          filters,
          success: ok,
          targets,
          assetId,
          checksum,
          contentType,
          selected_transport: selectedTransport
        }
      });
      if (ok) maybeBadge(st, `Resource sent to ${targets?.length || 1} peer${targets?.length === 1 ? '' : 's'}`);

      // Log resource data
      const resType = resourcePayload.type || 'unknown';
      const resLabel = resourcePayload.label || assetId || 'unlabeled';
      logToNode(nodeId, `📤 Resource out: ${resType} "${resLabel}" → ${targets?.length || 0} peer(s)`, 'success');
    },

    async onCommand(nodeId, payload) {
      const st = ensureNodeState(nodeId);
      if (!st) return;
      if (!payload || typeof payload !== 'object') return;
      const targetCtx = resolveOutboundTargetContext(st, payload || {});
      const selectedTransport = selectedTransportForPacket(st, payload || {});
      const contentType = String(payload.contentType || payload.content_type || 'application/json').trim() || 'application/json';
      const packet = {
        type: 'hybrid-bridge-command',
        event: 'interop.asset.command',
        nodeId,
        command: payload,
        contentType,
        content_type: contentType,
        checksum: checksumHex(payload),
        selected_transport: selectedTransport,
        transport: selectedTransport,
        ts: nowMs()
      };
      injectAuthAndMeteringHints(packet, payload || {});
      if (targetCtx.hasTarget) {
        packet.sessionId = targetCtx.sessionId || '';
        packet.objectUuid = targetCtx.objectUuid || targetCtx.itemId || '';
        packet.objectId = packet.objectUuid;
        packet.target = {
          overlayId: targetCtx.overlayId || '',
          itemId: targetCtx.itemId || packet.objectUuid || '',
          layerId: targetCtx.layerId || '',
          sessionId: targetCtx.sessionId || '',
          objectUuid: packet.objectUuid || ''
        };
      }
      await sendTypedBridgeMessage(nodeId, st, packet, {
        expectAck: payload.expectAck !== false,
        targets: payload.targets,
        maxAckRetries: 1
      });
    },

    async onAudioOutput(nodeId, payload) {
      // Handle audio packets from TTS node to send to NoClip Smart Objects
      const st = ensureNodeState(nodeId);
      if (!st || !payload) return;
      const targetCtx = resolveOutboundTargetContext(st, payload || {});
      const selectedTransport = selectedTransportForPacket(st, payload || {});
      const contentType = String(payload.mime || payload.contentType || payload.content_type || 'audio/pcm').trim() || 'audio/pcm';
      const assetId = String(payload.assetId || payload.asset_id || `audio-${Date.now().toString(36)}`).trim();

      const packet = {
        type: 'smart-object-audio-output',
        event: 'interop.asset.media',
        nodeId,
        audioPacket: payload,
        kind: 'audio',
        assetId,
        asset_id: assetId,
        contentType,
        content_type: contentType,
        checksum: checksumHex(payload.data || payload.b64 || payload),
        selected_transport: selectedTransport,
        transport: selectedTransport,
        ts: nowMs()
      };
      injectAuthAndMeteringHints(packet, payload || {});
      if (targetCtx.hasTarget) {
        packet.sessionId = targetCtx.sessionId || '';
        packet.objectUuid = targetCtx.objectUuid || targetCtx.itemId || '';
        packet.objectId = packet.objectUuid;
        packet.target = {
          overlayId: targetCtx.overlayId || '',
          itemId: targetCtx.itemId || packet.objectUuid || '',
          layerId: targetCtx.layerId || '',
          sessionId: targetCtx.sessionId || '',
          objectUuid: packet.objectUuid || ''
        };
      }

      await sendTypedBridgeMessage(nodeId, st, packet, {
        expectAck: payload.expectAck !== false,
        targets: payload.targets,
        maxAckRetries: 1
      });

      // Log audio data
      const dataSize = payload.data?.length || 0;
      logToNode(nodeId, `📤 Audio out: ${dataSize} samples @ ${payload.sampleRate || 'unknown'}Hz`, 'success');
    },

    async onAudioInput(nodeId, payload) {
      // This is called when audio comes from NoClip - route it to connected nodes
      // The actual routing happens in handleDiscoveryDm via 'smart-object-audio' type
      // This handler could be used for outgoing audio requests if needed
      const st = ensureNodeState(nodeId);
      if (!st || !payload) return;

      // For now, just log reception - actual routing happens in handleDiscoveryDm
      log?.(`[noclip-bridge] Received audio input for node ${nodeId}`);
    },

    requestFriend(nodeId, note) {
      const st = ensureNodeState(nodeId);
      if (!st) return;
      const selectedTransport = selectedTransportForPacket(st, {});
      const packet = {
        type: 'hybrid-friend-request',
        event: 'interop.social.friend_request',
        nodeId,
        note: typeof note === 'string' ? note.slice(0, 240) : '',
        selected_transport: selectedTransport,
        transport: selectedTransport,
        ts: nowMs()
      };
      sendBridgePayload(nodeId, st, packet);
    },

    dispose(nodeId) {
      const key = String(nodeId || '').trim();
      if (!key) return;
      const st = NODE_STATE.get(key);
      if (st) st.rootEl = null;
      if (st?.targetPub) unregisterTarget(key, st.targetPub);
      st?.sessions?.clear?.();
      st?.sessionIndex?.clear?.();
      if (st?.pendingAcks) {
        st.pendingAcks.forEach((entry) => clearTimeout(entry.timer));
        st.pendingAcks.clear();
      }
      st?.receivedMessageIds?.clear?.();
      st?.chunkAssemblies?.clear?.();
      NODE_STATE.delete(key);
    },

    registerSyncAdapter(adapter) {
      syncAdapter = adapter;
    },

    onSessionUpdate(session) {
      handleSessionUpdate(session);
    },

    async sendSmartObjectState(nodeId, data = {}) {
      const st = ensureNodeState(nodeId);
      if (!st) return { ok: false, messageId: null };
      const targetCtx = resolveOutboundTargetContext(st, data || {});
      const selectedTransport = selectedTransportForPacket(st, data || {});
      const packet = {
        type: 'smart-object-state',
        event: 'interop.bridge.state',
        sessionId: data.sessionId || data.session?.sessionId || targetCtx.sessionId || '',
        objectUuid: data.objectUuid || data.objectId || targetCtx.objectUuid || targetCtx.itemId || '',
        objectId: data.objectUuid || data.objectId || targetCtx.objectUuid || targetCtx.itemId || '',
        position: data.position || null,
        state: data.state || null,
        capabilities: data.capabilities || null,
        metadata: data.metadata || null,
        contentType: 'application/json',
        content_type: 'application/json',
        checksum: checksumHex(data.state || data.position || data),
        selected_transport: selectedTransport,
        transport: selectedTransport
      };
      injectAuthAndMeteringHints(packet, data || {});
      if (targetCtx.hasTarget) {
        packet.target = {
          overlayId: targetCtx.overlayId || '',
          itemId: targetCtx.itemId || packet.objectUuid || '',
          layerId: targetCtx.layerId || '',
          sessionId: packet.sessionId || '',
          objectUuid: packet.objectUuid || ''
        };
      }
      return sendTypedBridgeMessage(nodeId, st, packet, {
        expectAck: data.expectAck !== false,
        targets: data.targets,
        maxAckRetries: 1
      });
    },

    async sendDecisionResult(nodeId, data = {}) {
      const st = ensureNodeState(nodeId);
      if (!st) return { ok: false, messageId: null };
      const targetCtx = resolveOutboundTargetContext(st, data || {});
      const selectedTransport = selectedTransportForPacket(st, data || {});
      const packet = {
        type: 'decision-result',
        event: 'interop.asset.command',
        sessionId: data.sessionId || data.session?.sessionId || targetCtx.sessionId || '',
        objectUuid: data.objectUuid || data.objectId || targetCtx.objectUuid || targetCtx.itemId || '',
        objectId: data.objectUuid || data.objectId || targetCtx.objectUuid || targetCtx.itemId || '',
        result: data.result || null,
        decision: data.decision || null,
        summary: data.summary || null,
        metadata: data.metadata || null,
        contentType: 'application/json',
        content_type: 'application/json',
        checksum: checksumHex(data.result || data.decision || data.summary || data),
        selected_transport: selectedTransport,
        transport: selectedTransport
      };
      injectAuthAndMeteringHints(packet, data || {});
      if (targetCtx.hasTarget) {
        packet.target = {
          overlayId: targetCtx.overlayId || '',
          itemId: targetCtx.itemId || packet.objectUuid || '',
          layerId: targetCtx.layerId || '',
          sessionId: packet.sessionId || '',
          objectUuid: packet.objectUuid || ''
        };
      }
      return sendTypedBridgeMessage(nodeId, st, packet, {
        expectAck: data.expectAck !== false,
        targets: data.targets,
        maxAckRetries: 1
      });
    },

    async sendGraphQuery(nodeId, data = {}) {
      const st = ensureNodeState(nodeId);
      if (!st) return { ok: false, messageId: null };
      const targetCtx = resolveOutboundTargetContext(st, data || {});
      const selectedTransport = selectedTransportForPacket(st, data || {});
      const packet = {
        type: 'graph-query',
        event: 'interop.asset.command',
        sessionId: data.sessionId || targetCtx.sessionId || '',
        objectUuid: data.objectUuid || targetCtx.objectUuid || targetCtx.itemId || '',
        queryId: data.queryId || data.query?.id || null,
        query: data.query || null,
        variables: data.variables || null,
        metadata: data.metadata || null,
        contentType: 'application/json',
        content_type: 'application/json',
        checksum: checksumHex(data.query || data.variables || data),
        selected_transport: selectedTransport,
        transport: selectedTransport
      };
      packet.objectId = packet.objectUuid || '';
      if (targetCtx.hasTarget) {
        packet.target = {
          overlayId: targetCtx.overlayId || '',
          itemId: targetCtx.itemId || packet.objectUuid || '',
          layerId: targetCtx.layerId || '',
          sessionId: packet.sessionId || '',
          objectUuid: packet.objectUuid || ''
        };
      }
      return sendTypedBridgeMessage(nodeId, st, packet, {
        expectAck: data.expectAck !== false,
        targets: data.targets,
        maxAckRetries: 1
      });
    },

    async sendGraphResponse(nodeId, data = {}) {
      const st = ensureNodeState(nodeId);
      if (!st) return { ok: false, messageId: null };
      const targetCtx = resolveOutboundTargetContext(st, data || {});
      const selectedTransport = selectedTransportForPacket(st, data || {});
      const packet = {
        type: 'graph-response',
        event: 'interop.asset.resource',
        sessionId: data.sessionId || targetCtx.sessionId || '',
        objectUuid: data.objectUuid || targetCtx.objectUuid || targetCtx.itemId || '',
        queryId: data.queryId || '',
        result: data.result || null,
        errors: data.errors || null,
        metadata: data.metadata || null,
        contentType: 'application/json',
        content_type: 'application/json',
        checksum: checksumHex(data.result || data.errors || data),
        selected_transport: selectedTransport,
        transport: selectedTransport
      };
      packet.objectId = packet.objectUuid || '';
      if (targetCtx.hasTarget) {
        packet.target = {
          overlayId: targetCtx.overlayId || '',
          itemId: targetCtx.itemId || packet.objectUuid || '',
          layerId: targetCtx.layerId || '',
          sessionId: packet.sessionId || '',
          objectUuid: packet.objectUuid || ''
        };
      }
      return sendTypedBridgeMessage(nodeId, st, packet, {
        expectAck: data.expectAck === true,
        targets: data.targets,
        maxAckRetries: 1
      });
    },

    // UI helper methods
    attachNodeElement(nodeId, el) {
      const state = ensureNodeState(nodeId);
      if (!state) return;
      state.rootEl = el || null;
    },
    attachPeerDiscovery(api) {
      bindPeerDiscovery(api);
    },
    logToNode,
    refreshPeerDropdown,
    requestSync,
    setTargetPeer,
    getDiscoveredNoClipPeers,
    listSessions,
    onRouterCatalog,
    publishMarketplaceCatalog,
    resolveRoomName
  };
}

export { createNoClipBridge };
