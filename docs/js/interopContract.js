const DEFAULT_INTEROP_CONTRACT = Object.freeze({
  name: 'hydra_noclip_interop',
  version: '1.0.0',
  compatMinVersion: '1.0.0',
  namespace: 'hydra.noclip.marketplace.v1',
  schema: 'hydra_noclip_marketplace_contract_v1'
});

const INTEROP_ERROR_CODES = Object.freeze({
  INVALID_ENVELOPE: 'INVALID_ENVELOPE',
  MISSING_REQUIRED_FIELDS: 'MISSING_REQUIRED_FIELDS',
  UNSUPPORTED_CONTRACT_VERSION: 'UNSUPPORTED_CONTRACT_VERSION'
});

function asString(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function parseSemver(value) {
  const text = asString(value);
  if (!text) return null;
  const match = text.match(/^v?(?<major>\d+)(?:\.(?<minor>\d+))?(?:\.(?<patch>\d+))?/i);
  if (!match || !match.groups) return null;
  const major = Number(match.groups.major || 0);
  const minor = Number(match.groups.minor || 0);
  const patch = Number(match.groups.patch || 0);
  if (!Number.isFinite(major) || !Number.isFinite(minor) || !Number.isFinite(patch)) return null;
  return [Math.max(0, major | 0), Math.max(0, minor | 0), Math.max(0, patch | 0)];
}

function compareSemver(left, right) {
  const a = parseSemver(left);
  const b = parseSemver(right);
  if (!a && !b) return 0;
  if (!a) return -1;
  if (!b) return 1;
  for (let i = 0; i < 3; i += 1) {
    if (a[i] > b[i]) return 1;
    if (a[i] < b[i]) return -1;
  }
  return 0;
}

function normalizeInteropContract(candidate, fallback = DEFAULT_INTEROP_CONTRACT) {
  const source = candidate && typeof candidate === 'object' ? candidate : {};
  const base = fallback && typeof fallback === 'object' ? fallback : DEFAULT_INTEROP_CONTRACT;
  const version = asString(
    source.version ||
    source.interop_contract_version ||
    source.interopContractVersion ||
    base.version
  );
  const compatMinVersion = asString(
    source.compatMinVersion ||
    source.compat_min_version ||
    source.compatVersionMin ||
    source.compat_version_min ||
    base.compatMinVersion ||
    version
  ) || version;
  return {
    name: asString(source.name || base.name),
    version,
    compatMinVersion,
    namespace: asString(source.namespace || source.contract_namespace || base.namespace),
    schema: asString(source.schema || source.schema_id || base.schema)
  };
}

function isInteropContractCompatible(incoming, expected = DEFAULT_INTEROP_CONTRACT) {
  const normalizedExpected = normalizeInteropContract(expected, DEFAULT_INTEROP_CONTRACT);
  const normalizedIncoming = normalizeInteropContract(incoming, normalizedExpected);
  const nameOk = !!normalizedIncoming.name && normalizedIncoming.name === normalizedExpected.name;
  const versionOk = compareSemver(normalizedIncoming.version, normalizedExpected.compatMinVersion) >= 0;
  const compatOk = compareSemver(normalizedExpected.version, normalizedIncoming.compatMinVersion) >= 0;
  return {
    ok: !!(nameOk && versionOk && compatOk),
    nameOk,
    versionOk,
    compatOk,
    expected: normalizedExpected,
    incoming: normalizedIncoming
  };
}

function extractInteropContractFromEnvelope(envelope, expected = DEFAULT_INTEROP_CONTRACT) {
  const source = envelope && typeof envelope === 'object' ? envelope : {};
  return normalizeInteropContract(
    source.interop_contract || {
      name: source.interop_contract_name,
      version: source.interop_contract_version,
      compat_min_version: source.interop_contract_compat_min_version,
      namespace: source.interop_contract_namespace,
      schema: source.interop_contract_schema
    },
    expected
  );
}

function normalizeInteropEnvelope(envelope, options = {}) {
  const source = envelope && typeof envelope === 'object' && !Array.isArray(envelope)
    ? envelope
    : null;
  if (!source) {
    return {
      ok: false,
      errorCode: INTEROP_ERROR_CODES.INVALID_ENVELOPE,
      error: 'envelope must be an object',
      envelope: null,
      contractStatus: isInteropContractCompatible({}, options.expectedContract || DEFAULT_INTEROP_CONTRACT)
    };
  }

  const expectedContract = normalizeInteropContract(
    options.expectedContract || DEFAULT_INTEROP_CONTRACT,
    DEFAULT_INTEROP_CONTRACT
  );
  const contract = extractInteropContractFromEnvelope(source, expectedContract);
  const contractStatus = isInteropContractCompatible(contract, expectedContract);
  if (!contractStatus.ok) {
    return {
      ok: false,
      errorCode: INTEROP_ERROR_CODES.UNSUPPORTED_CONTRACT_VERSION,
      error: 'interop contract mismatch',
      envelope: withInteropContractFields(source, contract),
      contractStatus
    };
  }

  const merged = withInteropContractFields(source, contractStatus.incoming);
  const requiredFields = Array.isArray(options.requiredFields) ? options.requiredFields : [];
  const missing = requiredFields.filter((field) => !asString(merged[field]));
  if (options.requireTypeOrEvent !== false) {
    const hasType = !!asString(merged.type);
    const hasEvent = !!asString(merged.event);
    if (!hasType && !hasEvent) {
      missing.push('type|event');
    }
  }
  if (missing.length > 0) {
    return {
      ok: false,
      errorCode: INTEROP_ERROR_CODES.MISSING_REQUIRED_FIELDS,
      error: `missing required fields: ${missing.join(', ')}`,
      envelope: merged,
      contractStatus
    };
  }

  return {
    ok: true,
    errorCode: '',
    error: '',
    envelope: merged,
    contractStatus
  };
}

function withInteropContractFields(payload, contract = DEFAULT_INTEROP_CONTRACT) {
  const packet = payload && typeof payload === 'object' ? { ...payload } : {};
  const normalized = normalizeInteropContract(contract, DEFAULT_INTEROP_CONTRACT);
  packet.interop_contract = {
    name: normalized.name,
    version: normalized.version,
    compat_min_version: normalized.compatMinVersion,
    namespace: normalized.namespace,
    schema: normalized.schema
  };
  packet.interop_contract_version = normalized.version;
  packet.interop_contract_compat_min_version = normalized.compatMinVersion;
  packet.interop_contract_namespace = normalized.namespace;
  return packet;
}

export {
  DEFAULT_INTEROP_CONTRACT,
  INTEROP_ERROR_CODES,
  normalizeInteropContract,
  compareSemver,
  isInteropContractCompatible,
  normalizeInteropEnvelope,
  extractInteropContractFromEnvelope,
  withInteropContractFields
};
