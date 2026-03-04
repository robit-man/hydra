import {
  DEFAULT_INTEROP_CONTRACT,
  normalizeInteropContract,
  isInteropContractCompatible
} from './interopContract.js';

function normalizeRouterAddress(raw) {
  const text = String(raw || '').trim();
  if (!text) return '';
  if (text.toLowerCase().startsWith('nkn://')) return text.slice(6).trim();
  return text;
}

const AUTO_MIN_INTERVAL_MS = 10 * 1000;
const AUTO_MAX_INTERVAL_MS = 10 * 60 * 1000;
const AUTO_BACKOFF_CAP = 4;
const AUTO_JITTER_RATIO = 0.2;

function isObject(value) {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function clampAutoIntervalMs(input) {
  const raw = Number(input || 0);
  if (!Number.isFinite(raw)) return 60000;
  return Math.max(AUTO_MIN_INTERVAL_MS, Math.min(AUTO_MAX_INTERVAL_MS, Math.round(raw)));
}

function validateRouterAddress(raw) {
  const target = normalizeRouterAddress(raw);
  if (!target) return { ok: false, target: '', error: 'Missing router target NKN address' };
  if (target.length < 8 || target.length > 256) {
    return { ok: false, target, error: 'Router address length is out of range' };
  }
  if (/\s/.test(target)) {
    return { ok: false, target, error: 'Router address must not contain spaces' };
  }
  if (/[/?#]/.test(target)) {
    return { ok: false, target, error: 'Router address must not include URL path or query fragments' };
  }
  if (!/^[a-zA-Z0-9._-]+$/.test(target)) {
    return { ok: false, target, error: 'Router address contains unsupported characters' };
  }
  return { ok: true, target, error: '' };
}

function toEndpointString(value) {
  if (typeof value === 'string') return value.trim();
  return '';
}

function firstEndpoint(...values) {
  for (const value of values) {
    if (!value) continue;
    if (typeof value === 'string') {
      const text = value.trim();
      if (text) return text;
      continue;
    }
    if (!isObject(value)) continue;
    const fromObj = toEndpointString(value.base_url || value.endpoint || value.url || value.http_endpoint);
    if (fromObj) return fromObj;
  }
  return '';
}

function extractInteropContract(reply) {
  const fallback = normalizeInteropContract(DEFAULT_INTEROP_CONTRACT);
  if (!isObject(reply)) return fallback;
  const candidates = [
    reply.interop_contract,
    reply.snapshot?.interop_contract,
    reply.reply?.interop_contract,
    reply.rawReply?.interop_contract
  ];
  for (const candidate of candidates) {
    if (!isObject(candidate)) continue;
    const normalized = normalizeInteropContract(candidate, fallback);
    if (normalized.name || normalized.version) return normalized;
  }
  return normalizeInteropContract({
    name: reply.interop_contract_name || reply.snapshot?.interop_contract_name || fallback.name,
    version: String(
      reply.interop_contract_version ||
      reply.snapshot?.interop_contract_version ||
      reply.reply?.interop_contract_version ||
      reply.rawReply?.interop_contract_version ||
      ''
    ).trim(),
    compat_min_version:
      reply.interop_contract_compat_min_version ||
      reply.snapshot?.interop_contract_compat_min_version ||
      reply.reply?.interop_contract_compat_min_version ||
      reply.rawReply?.interop_contract_compat_min_version ||
      fallback.compatMinVersion,
    namespace:
      reply.interop_contract_namespace ||
      reply.snapshot?.interop_contract_namespace ||
      reply.reply?.interop_contract_namespace ||
      reply.rawReply?.interop_contract_namespace ||
      fallback.namespace
  }, fallback);
}

function interopContractError(contractStatus) {
  const incoming = contractStatus?.incoming || {};
  const expected = contractStatus?.expected || {};
  return [
    'Interop contract mismatch',
    `expected ${expected.name || '?'}@${expected.version || '?'}`,
    `compat>=${expected.compatMinVersion || '?'}`,
    `got ${incoming.name || '?'}@${incoming.version || '?'}`,
    `compat>=${incoming.compatMinVersion || '?'}`
  ].join(' | ');
}

function extractInteropContractStatus(reply) {
  const incoming = extractInteropContract(reply);
  return isInteropContractCompatible(incoming, DEFAULT_INTEROP_CONTRACT);
}

function extractInteropContractVersion(reply) {
  return String(
    reply.interop_contract_version ||
    reply.snapshot?.interop_contract_version ||
    reply.reply?.interop_contract_version ||
    reply.rawReply?.interop_contract_version ||
    ''
  ).trim() || String(extractInteropContract(reply).version || '');
}

function normalizeResolvedEndpoints(reply) {
  const source = isObject(reply)
    ? (isObject(reply.resolved) ? reply.resolved : (isObject(reply.snapshot) ? reply.snapshot.resolved : null))
    : null;
  if (!isObject(source)) return {};
  const contractVersion = extractInteropContractVersion(reply);

  const normalized = {};
  for (const [service, entryRaw] of Object.entries(source)) {
    if (!isObject(entryRaw)) continue;
    const entry = entryRaw;
    const selectedTransport = String(entry.selected_transport || entry.transport || '').trim();
    const baseUrl = firstEndpoint(entry.base_url, entry.http_endpoint);
    const httpEndpoint = firstEndpoint(entry.http_endpoint, baseUrl);
    const wsEndpoint = toEndpointString(entry.ws_endpoint || '');
    const entryContractVersion = String(
      entry.interop_contract_version ||
      entry.interopContractVersion ||
      contractVersion ||
      ''
    ).trim();
    normalized[service] = {
      service,
      transport: selectedTransport,
      selectedTransport,
      selectionReason: String(entry.selection_reason || '').trim(),
      interopContractVersion: entryContractVersion,
      baseUrl,
      httpEndpoint,
      wsEndpoint,
      remoteRoutable: !!entry.remote_routable,
      loopbackOnly: !!entry.loopback_only,
      isPublic: !!entry.is_public,
      candidates: {
        cloudflare: firstEndpoint(entry.cloudflare, entry.tunnel_url),
        upnp: firstEndpoint(entry.upnp),
        nats: firstEndpoint(entry.nats),
        nkn: firstEndpoint(entry.nkn),
        local: firstEndpoint(entry.local)
      },
      raw: entry
    };
  }
  return normalized;
}

function normalizeCatalog(reply) {
  const source = isObject(reply) ? reply : {};
  const rawCatalogCandidates = [
    source.catalog,
    source.snapshot?.catalog,
    source.reply?.catalog,
    source.rawReply?.catalog
  ];
  let rawCatalog = null;
  for (const candidate of rawCatalogCandidates) {
    if (!isObject(candidate)) continue;
    if (Array.isArray(candidate.services)) {
      rawCatalog = candidate;
      break;
    }
  }
  if (!isObject(rawCatalog)) {
    return {
      generatedAtMs: 0,
      provider: {},
      summary: {},
      services: {},
      raw: null
    };
  }
  const services = {};
  const rawServices = Array.isArray(rawCatalog.services) ? rawCatalog.services : [];
  for (const entryRaw of rawServices) {
    if (!isObject(entryRaw)) continue;
    const service = String(
      entryRaw.service_id ||
      entryRaw.service ||
      entryRaw.watchdog_service ||
      ''
    ).trim();
    if (!service) continue;
    const selectedTransport = String(
      entryRaw.selected_transport ||
      entryRaw.selectedTransport ||
      entryRaw.transport ||
      ''
    ).trim().toLowerCase();
    const candidatesRaw = isObject(entryRaw.endpoint_candidates)
      ? entryRaw.endpoint_candidates
      : (isObject(entryRaw.candidates) ? entryRaw.candidates : {});
    const candidates = {
      cloudflare: firstEndpoint(candidatesRaw.cloudflare),
      upnp: firstEndpoint(candidatesRaw.upnp),
      nats: firstEndpoint(candidatesRaw.nats),
      nkn: firstEndpoint(candidatesRaw.nkn),
      local: firstEndpoint(candidatesRaw.local)
    };
    services[service] = {
      service,
      status: String(entryRaw.status || '').trim(),
      healthy: !!entryRaw.healthy,
      enabled: entryRaw.enabled !== false,
      visibility: String(entryRaw.visibility || '').trim().toLowerCase() || 'public',
      selectedTransport,
      selectedEndpoint: firstEndpoint(
        entryRaw.selected_endpoint,
        entryRaw.base_url,
        entryRaw.http_endpoint
      ),
      staleRejected: !!entryRaw.stale_rejected,
      staleReason: String(entryRaw.stale_reason || '').trim(),
      staleTunnelUrl: firstEndpoint(entryRaw.stale_tunnel_url),
      tunnelError: String(entryRaw.tunnel_error || '').trim(),
      pricing: isObject(entryRaw.pricing) ? { ...entryRaw.pricing } : {},
      candidateReachability: isObject(entryRaw.candidate_reachability) ? { ...entryRaw.candidate_reachability } : {},
      candidates,
      raw: entryRaw
    };
  }
  const generatedAtMs = Number(rawCatalog.generated_at_ms || rawCatalog.generatedAtMs || 0);
  return {
    generatedAtMs: Number.isFinite(generatedAtMs) ? generatedAtMs : 0,
    provider: isObject(rawCatalog.provider) ? { ...rawCatalog.provider } : {},
    summary: isObject(rawCatalog.summary) ? { ...rawCatalog.summary } : {},
    services,
    raw: rawCatalog
  };
}

function createRouterDiscovery({ Net, CFG, saveCFG, setBadge, log, onResolved }) {
  const state = {
    inFlight: null,
    timer: null,
    listeners: new Set(),
    status: String(CFG.routerLastResolveStatus || 'idle'),
    resolveSeq: 0,
    appliedSeq: 0,
    latestResolvedAt: Number(CFG.routerLastResolvedAt || 0),
    autoFailures: 0
  };

  const emit = (extra = {}) => {
    const payload = {
      status: state.status,
      target: String(CFG.routerTargetNknAddress || ''),
      lastResolvedAt: Number(CFG.routerLastResolvedAt || 0),
      lastError: String(CFG.routerLastResolveError || ''),
      lastInteropContractVersion: String(CFG.routerLastInteropContractVersion || ''),
      ...extra
    };
    state.listeners.forEach((fn) => {
      try { fn(payload); } catch (_) { /* ignore */ }
    });
  };

  const setStatus = (status, extra = {}) => {
    state.status = String(status || 'idle');
    CFG.routerLastResolveStatus = state.status;
    if (extra.error !== undefined) CFG.routerLastResolveError = String(extra.error || '');
    if (extra.resolvedAt !== undefined) CFG.routerLastResolvedAt = Number(extra.resolvedAt || 0);
    if (extra.interopContractVersion !== undefined) {
      CFG.routerLastInteropContractVersion = String(extra.interopContractVersion || '');
    }
    saveCFG();
    emit(extra);
  };

  const setTarget = (raw) => {
    const target = normalizeRouterAddress(raw);
    CFG.routerTargetNknAddress = target;
    saveCFG();
    emit();
    return target;
  };

  const nextAutoDelayMs = () => {
    const base = clampAutoIntervalMs(CFG.routerAutoResolveIntervalMs || 60000);
    const exp = Math.min(AUTO_BACKOFF_CAP, Math.max(0, state.autoFailures | 0));
    const backedOff = Math.min(AUTO_MAX_INTERVAL_MS, base * (2 ** exp));
    const jitterSpan = Math.max(250, Math.round(backedOff * AUTO_JITTER_RATIO));
    const jitter = Math.round((Math.random() * (jitterSpan * 2)) - jitterSpan);
    return Math.max(AUTO_MIN_INTERVAL_MS, Math.min(AUTO_MAX_INTERVAL_MS, backedOff + jitter));
  };

  const resolveNow = async (targetOverride = '', options = {}) => {
    if (state.inFlight) return state.inFlight;
    const timeoutMs = Number(options.timeoutMs || 20000);
    const validated = validateRouterAddress(targetOverride || CFG.routerTargetNknAddress || '');
    const target = validated.target;
    if (!validated.ok) {
      setStatus('error', { error: validated.error });
      setBadge(`Router resolve failed: ${validated.error}`, false);
      throw new Error(validated.error);
    }

    const seq = ++state.resolveSeq;
    setStatus('resolving', { error: '' });
    setBadge(`Resolving router ${target}...`);

    state.inFlight = (async () => {
      try {
        const reply = await Net.nknResolveTunnels(target, timeoutMs);
        const ts = Number(reply?.timestamp_ms || Date.now());
        const normalizedCatalog = normalizeCatalog(reply);
        const catalogTs = Number(normalizedCatalog.generatedAtMs || 0);
        const staleBySeq = seq < state.appliedSeq;
        const staleByTs = ts > 0 && state.latestResolvedAt > 0 && ts < state.latestResolvedAt;
        const staleByCatalogTs = catalogTs > 0 && state.latestResolvedAt > 0 && catalogTs < state.latestResolvedAt;
        if (staleBySeq || staleByTs || staleByCatalogTs) {
          log?.(`[router.resolve] stale response discarded target=${target} seq=${seq} ts=${ts} catalogTs=${catalogTs}`);
          emit({ stale: true, reply, target });
          return { stale: true, reply };
        }

        state.appliedSeq = seq;
        state.latestResolvedAt = Math.max(ts || 0, catalogTs || 0);
        state.autoFailures = 0;
        const interopContract = extractInteropContract(reply);
        const interopContractStatus = extractInteropContractStatus(reply);
        if (!interopContractStatus.ok) {
          const mismatch = interopContractError(interopContractStatus);
          const coded = `UNSUPPORTED_CONTRACT_VERSION:${mismatch}`;
          setStatus('error', { error: coded, interopContractVersion: String(interopContract.version || '') });
          setBadge(`Router resolve failed: ${coded}`, false);
          throw new Error(coded);
        }
        const normalizedResolved = normalizeResolvedEndpoints(reply);
        for (const [service, catalogEntry] of Object.entries(normalizedCatalog.services || {})) {
          if (normalizedResolved[service]) continue;
          normalizedResolved[service] = {
            service,
            transport: String(catalogEntry.selectedTransport || ''),
            selectedTransport: String(catalogEntry.selectedTransport || ''),
            selectionReason: String(catalogEntry.staleReason || ''),
            interopContractVersion: String(interopContract.version || ''),
            baseUrl: firstEndpoint(catalogEntry.selectedEndpoint, catalogEntry.candidates?.local),
            httpEndpoint: firstEndpoint(catalogEntry.selectedEndpoint, catalogEntry.candidates?.local),
            wsEndpoint: '',
            remoteRoutable: false,
            loopbackOnly: false,
            isPublic: false,
            candidates: {
              cloudflare: firstEndpoint(catalogEntry.candidates?.cloudflare),
              upnp: firstEndpoint(catalogEntry.candidates?.upnp),
              nats: firstEndpoint(catalogEntry.candidates?.nats),
              nkn: firstEndpoint(catalogEntry.candidates?.nkn),
              local: firstEndpoint(catalogEntry.candidates?.local)
            },
            raw: isObject(catalogEntry.raw) ? catalogEntry.raw : {}
          };
        }
        const resolvedPayload = {
          target,
          requestId: String(reply?.request_id || ''),
          sourceAddress: String(reply?.source_address || ''),
          timestampMs: Math.max(ts || 0, catalogTs || 0),
          interopContract,
          interopContractVersion: String(interopContract.version || ''),
          interopContractCompatMinVersion: String(interopContract.compatMinVersion || ''),
          interopContractNamespace: String(interopContract.namespace || ''),
          interopContractOk: true,
          resolved: normalizedResolved,
          catalog: normalizedCatalog,
          rawReply: reply
        };
        CFG.routerLastResolveResult = reply;
        CFG.routerLastResolvedAt = Math.max(ts || 0, catalogTs || 0);
        CFG.routerLastResolveError = '';
        CFG.routerLastResolveStatus = 'ok';
        CFG.routerLastInteropContractVersion = String(interopContract.version || '');
        CFG.routerLastCatalog = normalizedCatalog.raw;
        saveCFG();
        if (typeof onResolved === 'function') {
          try { onResolved(resolvedPayload); } catch (_) { /* ignore */ }
        }
        log?.(`[router.resolve] target=${target} services=${Object.keys(normalizedResolved).length}`);
        setBadge('Router resolve complete');
        setStatus('ok', {
          resolvedAt: Math.max(ts || 0, catalogTs || 0),
          interopContractVersion: String(interopContract.version || ''),
          interopContractOk: true,
          reply,
          resolved: normalizedResolved,
          catalog: normalizedCatalog,
          target
        });
        return resolvedPayload;
      } catch (err) {
        const msg = err?.message || String(err || 'resolve failed');
        state.autoFailures = Math.min(32, (state.autoFailures | 0) + 1);
        CFG.routerLastResolveError = msg;
        CFG.routerLastResolveStatus = 'error';
        saveCFG();
        setBadge(`Router resolve failed: ${msg}`, false);
        setStatus('error', { error: msg });
        throw err;
      } finally {
        state.inFlight = null;
      }
    })();

    return state.inFlight;
  };

  const stopAuto = () => {
    if (state.timer) {
      clearTimeout(state.timer);
      state.timer = null;
    }
  };

  const scheduleAuto = () => {
    stopAuto();
    if (!CFG.routerAutoResolve) return;
    const interval = nextAutoDelayMs();
    state.timer = setTimeout(async () => {
      try {
        if (CFG.routerTargetNknAddress) await resolveNow('', { timeoutMs: 25000 });
      } catch (_) {
        // keep scheduler alive despite failures
      } finally {
        scheduleAuto();
      }
    }, interval);
  };

  const startAuto = () => {
    scheduleAuto();
  };

  const subscribe = (listener) => {
    if (typeof listener !== 'function') return () => {};
    state.listeners.add(listener);
    emit();
    return () => state.listeners.delete(listener);
  };

  return {
    normalizeRouterAddress,
    validateRouterAddress,
    normalizeResolvedEndpoints,
    setTarget,
    resolveNow,
    startAuto,
    stopAuto,
    subscribe,
    getStatus: () => state.status
  };
}

export {
  createRouterDiscovery,
  normalizeRouterAddress,
  validateRouterAddress,
  normalizeResolvedEndpoints,
  normalizeCatalog
};
