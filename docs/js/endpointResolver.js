const TRANSPORT_PREFERENCE = Object.freeze(['cloudflare', 'upnp', 'nats', 'nkn', 'local']);
const MEDIA_TRANSPORT_PREFERENCE = Object.freeze(['cloudflare', 'upnp', 'nats', 'nkn', 'local']);

const SERVICE_ALIAS_TO_CANONICAL = Object.freeze({
  whisper_asr: 'whisper_asr',
  asr: 'whisper_asr',
  whisper: 'whisper_asr',
  piper_tts: 'piper_tts',
  tts: 'piper_tts',
  piper: 'piper_tts',
  ollama_farm: 'ollama_farm',
  ollama: 'ollama_farm',
  llm: 'ollama_farm',
  mcp_server: 'mcp_server',
  mcp: 'mcp_server',
  context: 'mcp_server',
  web_scrape: 'web_scrape',
  browser: 'web_scrape',
  chrome: 'web_scrape',
  scrape: 'web_scrape',
  depth_any: 'depth_any',
  depth: 'depth_any',
  pointcloud: 'depth_any',
  camera_router: 'camera_router',
  camera: 'camera_router',
  video: 'camera_router',
  audio_router: 'audio_router',
  audio: 'audio_router',
  media_audio: 'audio_router'
});

const CANONICAL_TO_ALIASES = Object.freeze({
  whisper_asr: ['whisper_asr', 'asr', 'whisper'],
  piper_tts: ['piper_tts', 'tts', 'piper'],
  ollama_farm: ['ollama_farm', 'ollama', 'llm'],
  mcp_server: ['mcp_server', 'mcp', 'context'],
  web_scrape: ['web_scrape', 'browser', 'chrome', 'scrape'],
  depth_any: ['depth_any', 'depth', 'pointcloud'],
  camera_router: ['camera_router', 'camera', 'video'],
  audio_router: ['audio_router', 'audio', 'media_audio']
});

function asObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
}

function asString(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function stripTrailingSlash(value) {
  const text = asString(value);
  if (!text) return '';
  if (/^nkn:\/\//i.test(text)) return text;
  return text.replace(/\/+$/, '');
}

function firstString(...values) {
  for (const value of values) {
    const text = asString(value);
    if (text) return text;
  }
  return '';
}

function normalizeServiceName(service) {
  const key = asString(service).toLowerCase();
  if (!key) return '';
  return SERVICE_ALIAS_TO_CANONICAL[key] || key;
}

function normalizeMode(mode, allowNknMode = false) {
  const key = asString(mode).toLowerCase();
  if (key === 'local' || key === 'auto' || key === 'remote') return key;
  if (allowNknMode && key === 'nkn') return 'nkn';
  return 'auto';
}

function normalizeEndpointSource(source, fallback = 'router') {
  const key = asString(source).toLowerCase();
  if (key === 'manual') return 'manual';
  if (key === 'router') return 'router';
  return fallback;
}

function normalizeTransport(transport) {
  const key = asString(transport).toLowerCase();
  if (!key) return '';
  if (key === 'cloudflare' || key === 'cloudflared' || key === 'cf') return 'cloudflare';
  if (key === 'upnp') return 'upnp';
  if (key === 'nats') return 'nats';
  if (key === 'nkn') return 'nkn';
  if (key === 'local' || key === 'localhost' || key === 'lan') return 'local';
  return key;
}

function normalizeTransportList(value, fallback = []) {
  const source = Array.isArray(value)
    ? value
    : (value == null ? [] : [value]);
  const out = [];
  const seen = new Set();
  for (const entry of source) {
    const normalized = normalizeTransport(entry);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(normalized);
  }
  if (out.length) return out;
  if (!Array.isArray(fallback) || !fallback.length) return [];
  const fallbackOut = [];
  const fallbackSeen = new Set();
  for (const entry of fallback) {
    const normalized = normalizeTransport(entry);
    if (!normalized || fallbackSeen.has(normalized)) continue;
    fallbackSeen.add(normalized);
    fallbackOut.push(normalized);
  }
  return fallbackOut;
}

function looksLikeNknAddress(value) {
  const text = asString(value);
  if (!text) return false;
  if (/^nkn:\/\//i.test(text)) return true;
  if (text.includes('://')) return false;
  if (text.includes('/') || text.includes('?') || text.includes('#')) return false;
  return /^[a-zA-Z0-9._-]{8,256}$/.test(text);
}

function extractEndpoint(value) {
  if (!value) return '';
  if (typeof value === 'string') return asString(value);
  const obj = asObject(value);
  return firstString(
    obj.baseUrl,
    obj.base_url,
    obj.httpEndpoint,
    obj.http_endpoint,
    obj.endpoint,
    obj.url,
    obj.origin,
    obj.public_base_url,
    obj.lan_base_url,
    obj.bind_base_url,
    obj.tunnel_url,
    obj.stale_tunnel_url,
    obj.nkn_address,
    obj.address,
    obj.target_address
  );
}

function extractEndpointAny(...values) {
  for (const value of values) {
    const endpoint = extractEndpoint(value);
    if (endpoint) return endpoint;
  }
  return '';
}

function normalizeResolvedEntry(service, rawEntry) {
  const raw = typeof rawEntry === 'string' ? { base_url: rawEntry } : asObject(rawEntry);
  const fallback = asObject(raw.fallback);
  const fallbackCloudflare = asObject(fallback.cloudflare);
  const fallbackUpnp = asObject(fallback.upnp);
  const fallbackNats = asObject(fallback.nats);
  const fallbackNkn = asObject(fallback.nkn);
  const fallbackLocal = asObject(fallback.local);
  const candidatesRaw = asObject(raw.candidates);

  const selectedTransport = normalizeTransport(
    raw.selectedTransport ||
    raw.selected_transport ||
    raw.transport ||
    fallback.selected_transport
  );

  const baseUrl = stripTrailingSlash(extractEndpointAny(raw.baseUrl, raw.base_url, raw.httpEndpoint, raw.http_endpoint, raw));
  const httpEndpoint = stripTrailingSlash(extractEndpointAny(raw.httpEndpoint, raw.http_endpoint, baseUrl));
  const wsEndpoint = asString(raw.wsEndpoint || raw.ws_endpoint);

  const candidates = {
    cloudflare: stripTrailingSlash(extractEndpointAny(
      candidatesRaw.cloudflare,
      raw.cloudflare,
      raw.tunnel_url,
      raw.stale_tunnel_url,
      fallbackCloudflare,
      fallbackCloudflare.base_url,
      fallbackCloudflare.public_base_url
    )),
    upnp: stripTrailingSlash(extractEndpointAny(
      candidatesRaw.upnp,
      raw.upnp,
      fallbackUpnp,
      fallbackUpnp.base_url,
      fallbackUpnp.public_base_url
    )),
    nats: stripTrailingSlash(extractEndpointAny(
      candidatesRaw.nats,
      raw.nats,
      fallbackNats,
      fallbackNats.base_url,
      fallbackNats.public_base_url
    )),
    nkn: extractEndpointAny(
      candidatesRaw.nkn,
      raw.nkn,
      raw.nkn_address,
      fallbackNkn,
      fallbackNkn.nkn_address,
      fallbackNkn.address,
      fallbackNkn.target_address,
      fallbackNkn.base_url,
      fallbackNkn.public_base_url
    ),
    local: stripTrailingSlash(extractEndpointAny(
      candidatesRaw.local,
      raw.local,
      raw.local_fallback,
      fallbackLocal,
      fallbackLocal.base_url,
      fallbackLocal.lan_base_url,
      fallbackLocal.bind_base_url
    ))
  };

  if (selectedTransport && !candidates[selectedTransport]) {
    const selectedCandidate = extractEndpointAny(baseUrl, httpEndpoint);
    if (selectedCandidate) {
      candidates[selectedTransport] = selectedCandidate;
    }
  }

  return {
    service,
    transport: selectedTransport || normalizeTransport(raw.transport),
    selectedTransport: selectedTransport || normalizeTransport(raw.transport),
    selectionReason: asString(raw.selectionReason || raw.selection_reason),
    baseUrl,
    httpEndpoint,
    wsEndpoint,
    remoteRoutable: !!raw.remoteRoutable || !!raw.remote_routable,
    loopbackOnly: !!raw.loopbackOnly || !!raw.loopback_only,
    isPublic: !!raw.isPublic || !!raw.is_public,
    candidates,
    raw
  };
}

function mergeEntries(existing, incoming) {
  if (!existing) return incoming;
  if (!incoming) return existing;
  return {
    ...existing,
    ...incoming,
    service: existing.service || incoming.service,
    transport: incoming.transport || existing.transport,
    selectedTransport: incoming.selectedTransport || existing.selectedTransport,
    selectionReason: incoming.selectionReason || existing.selectionReason,
    baseUrl: incoming.baseUrl || existing.baseUrl,
    httpEndpoint: incoming.httpEndpoint || existing.httpEndpoint,
    wsEndpoint: incoming.wsEndpoint || existing.wsEndpoint,
    remoteRoutable: incoming.remoteRoutable || existing.remoteRoutable,
    loopbackOnly: incoming.loopbackOnly || existing.loopbackOnly,
    isPublic: incoming.isPublic || existing.isPublic,
    candidates: {
      cloudflare: incoming.candidates?.cloudflare || existing.candidates?.cloudflare || '',
      upnp: incoming.candidates?.upnp || existing.candidates?.upnp || '',
      nats: incoming.candidates?.nats || existing.candidates?.nats || '',
      nkn: incoming.candidates?.nkn || existing.candidates?.nkn || '',
      local: incoming.candidates?.local || existing.candidates?.local || ''
    },
    raw: incoming.raw || existing.raw || {}
  };
}

function isLikelyResolvedMap(value) {
  const obj = asObject(value);
  const entries = Object.entries(obj);
  if (!entries.length) return false;
  let score = 0;
  for (const [, raw] of entries.slice(0, 6)) {
    if (typeof raw === 'string') {
      score += 1;
      continue;
    }
    const item = asObject(raw);
    if (
      item.base_url || item.baseUrl ||
      item.http_endpoint || item.httpEndpoint ||
      item.transport || item.selected_transport || item.selectedTransport ||
      item.local || item.cloudflare || item.fallback || item.candidates
    ) {
      score += 2;
    } else if (Object.keys(item).length) {
      score += 1;
    }
  }
  return score > 0;
}

function extractResolvedMap(payload) {
  const source = asObject(payload);
  const snapshot = asObject(source.snapshot);
  const reply = asObject(source.reply);
  const replySnapshot = asObject(reply.snapshot);
  const rawReply = asObject(source.rawReply);
  const rawReplySnapshot = asObject(rawReply.snapshot);

  const candidates = [
    source.resolved,
    snapshot.resolved,
    reply.resolved,
    replySnapshot.resolved,
    rawReply.resolved,
    rawReplySnapshot.resolved,
    source
  ];

  for (const candidate of candidates) {
    if (isLikelyResolvedMap(candidate)) return asObject(candidate);
  }
  return {};
}

function normalizeResolvedMap(payload) {
  const rawMap = extractResolvedMap(payload);
  const out = {};

  for (const [serviceName, rawEntry] of Object.entries(rawMap)) {
    const canonical = normalizeServiceName(serviceName);
    if (!canonical) continue;
    const normalized = normalizeResolvedEntry(canonical, rawEntry);
    out[canonical] = mergeEntries(out[canonical], normalized);
  }

  return out;
}

function getResolvedServiceEntry(payload, service) {
  const normalized = normalizeResolvedMap(payload);
  const canonical = normalizeServiceName(service);
  if (!canonical) return null;
  if (normalized[canonical]) return normalized[canonical];

  const aliases = CANONICAL_TO_ALIASES[canonical] || [canonical];
  for (const alias of aliases) {
    if (normalized[alias]) return normalized[alias];
  }
  return null;
}

function isLoopbackHost(hostname) {
  const host = asString(hostname).toLowerCase();
  if (!host) return false;
  if (host === 'localhost' || host === '::1' || host === '[::1]' || host === '0.0.0.0') return true;
  if (host.startsWith('127.')) return true;
  return false;
}

function isLoopbackEndpoint(endpoint) {
  const text = asString(endpoint);
  if (!text) return false;
  if (looksLikeNknAddress(text)) return false;

  let parsed = null;
  try {
    parsed = new URL(text.includes('://') ? text : `https://${text}`);
  } catch (err) {
    return false;
  }
  return isLoopbackHost(parsed.hostname || '');
}

function resolveServiceEndpoint({
  cfg,
  service,
  routerResolvedEndpoints,
  routerTargetNknAddress = '',
  defaultBase = '',
  allowNknMode = false,
  transportPreference = TRANSPORT_PREFERENCE,
  excludedTransports = []
} = {}) {
  const effective = Object.assign({}, cfg || {});
  const baseManual = stripTrailingSlash(firstString(effective.base, defaultBase));
  const api = asString(effective.api);
  const mode = normalizeMode(effective.endpointMode, allowNknMode);
  const requestedSource = normalizeEndpointSource(effective.endpointSource, 'router');
  const rawRelay = asString(effective.relay);
  const hasRelay = !!rawRelay;
  const relayOnly = mode === 'remote' || mode === 'nkn';
  const viaNkn = hasRelay && (mode === 'auto' || mode === 'remote' || mode === 'nkn');
  const relay = viaNkn ? rawRelay : '';

  const canonicalService = normalizeServiceName(effective.service) || normalizeServiceName(service);
  const entry = getResolvedServiceEntry(routerResolvedEndpoints, canonicalService);
  const remoteRouter = !!asString(routerTargetNknAddress);
  const canUseRouter = requestedSource === 'router';
  const preference = normalizeTransportList(transportPreference, TRANSPORT_PREFERENCE);
  const excluded = new Set(normalizeTransportList(excludedTransports, []));

  let endpointSource = 'manual';
  let selectedTransport = viaNkn ? 'nkn' : 'manual';
  let selectedEndpoint = baseManual;
  let reason = canUseRouter ? 'manual_config' : 'manual_source_selected';
  const rejectedCandidates = [];
  const consideredCandidates = [];

  if (canUseRouter && entry && mode !== 'local') {
    for (const transport of preference) {
      const candidate = asString(entry.candidates?.[transport]);
      if (!candidate) continue;

      consideredCandidates.push({ transport, endpoint: candidate });

      if (excluded.has(transport)) {
        rejectedCandidates.push({ transport, endpoint: candidate, reason: 'transport_excluded' });
        continue;
      }

      if (transport === 'nkn' && !hasRelay) {
        rejectedCandidates.push({ transport, endpoint: candidate, reason: 'nkn_requires_relay' });
        continue;
      }
      if (remoteRouter && isLoopbackEndpoint(candidate)) {
        rejectedCandidates.push({ transport, endpoint: candidate, reason: 'loopback_rejected_remote_router' });
        continue;
      }

      selectedTransport = transport;
      if (transport === 'nkn') {
        selectedEndpoint = stripTrailingSlash(firstString(
          baseManual,
          entry.baseUrl,
          entry.httpEndpoint,
          entry.candidates?.local,
          candidate
        ));
      } else {
        selectedEndpoint = stripTrailingSlash(candidate);
      }
      endpointSource = 'router';
      reason = `router_${transport}_preferred`;
      break;
    }

    if (endpointSource !== 'router') {
      const fallbackEndpoint = stripTrailingSlash(firstString(entry.baseUrl, entry.httpEndpoint));
      const fallbackTransport = normalizeTransport(entry.selectedTransport || entry.transport);
      if (fallbackTransport && excluded.has(fallbackTransport)) {
        rejectedCandidates.push({
          transport: fallbackTransport,
          endpoint: fallbackEndpoint,
          reason: 'fallback_transport_excluded'
        });
        reason = 'router_fallback_excluded';
      } else if (fallbackEndpoint && !(remoteRouter && isLoopbackEndpoint(fallbackEndpoint))) {
        selectedEndpoint = fallbackEndpoint;
        selectedTransport = fallbackTransport || 'router';
        endpointSource = 'router';
        reason = 'router_fallback_base_url';
      } else {
        reason = rejectedCandidates.length ? 'router_candidates_rejected' : 'router_candidates_missing';
      }
    }
  } else if (canUseRouter && mode !== 'local') {
    reason = entry ? reason : 'router_entry_missing';
  }

  if (!selectedEndpoint) {
    selectedEndpoint = baseManual;
    endpointSource = 'manual';
    selectedTransport = 'manual';
    if (!reason) reason = 'manual_fallback_empty_selection';
  }

  const diagnostics = {
    service: canonicalService,
    mode,
    requestedSource,
    remoteRouter,
    routerTargetNknAddress: asString(routerTargetNknAddress),
    manualBase: baseManual,
    chosenTransport: selectedTransport,
    chosenEndpoint: selectedEndpoint,
    source: endpointSource,
    reason,
    transportPreference: preference.slice(),
    excludedTransports: Array.from(excluded),
    consideredCandidates,
    rejectedCandidates
  };

  return {
    service: canonicalService,
    base: selectedEndpoint,
    api,
    relay,
    rawRelay,
    viaNkn,
    mode,
    relayOnly,
    endpointSource,
    requestedEndpointSource: requestedSource,
    selectedTransport,
    resolvedAt: Date.now(),
    resolveDiagnostics: diagnostics
  };
}

function resolveMediaServiceEndpoint({
  cfg,
  service,
  routerResolvedEndpoints,
  routerTargetNknAddress = '',
  defaultBase = '',
  transportPreference = MEDIA_TRANSPORT_PREFERENCE,
  excludedTransports = []
} = {}) {
  return resolveServiceEndpoint({
    cfg,
    service,
    routerResolvedEndpoints,
    routerTargetNknAddress,
    defaultBase,
    allowNknMode: true,
    transportPreference,
    excludedTransports
  });
}

export {
  TRANSPORT_PREFERENCE,
  MEDIA_TRANSPORT_PREFERENCE,
  normalizeResolvedMap,
  getResolvedServiceEntry,
  resolveServiceEndpoint,
  resolveMediaServiceEndpoint,
  normalizeMode,
  isLoopbackEndpoint
};
