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

function normalizeResolvedEndpoints(reply) {
  const source = isObject(reply)
    ? (isObject(reply.resolved) ? reply.resolved : (isObject(reply.snapshot) ? reply.snapshot.resolved : null))
    : null;
  if (!isObject(source)) return {};

  const normalized = {};
  for (const [service, entryRaw] of Object.entries(source)) {
    if (!isObject(entryRaw)) continue;
    const entry = entryRaw;
    const selectedTransport = String(entry.selected_transport || entry.transport || '').trim();
    const baseUrl = firstEndpoint(entry.base_url, entry.http_endpoint);
    const httpEndpoint = firstEndpoint(entry.http_endpoint, baseUrl);
    const wsEndpoint = toEndpointString(entry.ws_endpoint || '');
    normalized[service] = {
      service,
      transport: selectedTransport,
      selectedTransport,
      selectionReason: String(entry.selection_reason || '').trim(),
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
        const staleBySeq = seq < state.appliedSeq;
        const staleByTs = ts > 0 && state.latestResolvedAt > 0 && ts < state.latestResolvedAt;
        if (staleBySeq || staleByTs) {
          log?.(`[router.resolve] stale response discarded target=${target} seq=${seq} ts=${ts}`);
          emit({ stale: true, reply, target });
          return { stale: true, reply };
        }

        state.appliedSeq = seq;
        state.latestResolvedAt = ts;
        state.autoFailures = 0;
        const normalizedResolved = normalizeResolvedEndpoints(reply);
        const resolvedPayload = {
          target,
          requestId: String(reply?.request_id || ''),
          sourceAddress: String(reply?.source_address || ''),
          timestampMs: ts,
          resolved: normalizedResolved,
          rawReply: reply
        };
        CFG.routerLastResolveResult = reply;
        CFG.routerLastResolvedAt = ts;
        CFG.routerLastResolveError = '';
        CFG.routerLastResolveStatus = 'ok';
        saveCFG();
        if (typeof onResolved === 'function') {
          try { onResolved(resolvedPayload); } catch (_) { /* ignore */ }
        }
        log?.(`[router.resolve] target=${target} services=${Object.keys(normalizedResolved).length}`);
        setBadge('Router resolve complete');
        setStatus('ok', { resolvedAt: ts, reply, resolved: normalizedResolved, target });
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
  normalizeResolvedEndpoints
};
