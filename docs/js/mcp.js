import { ensureLocalNetworkAccess } from './localNetwork.js';

function createMCP({
  getNode,
  NodeStore,
  Router,
  Net,
  CFG,
  log,
  setBadge,
  setRelayState = () => {}
}) {
  const stateByNode = new Map();

  function ensureState(nodeId) {
    if (!stateByNode.has(nodeId)) {
      stateByNode.set(nodeId, {
        status: 'idle',
        server: null,
        resources: [],
        tools: [],
        busy: false,
        lastError: null,
        lastResponse: null
      });
    }
    return stateByNode.get(nodeId);
  }

  function normalizeBase(cfg) {
    const raw = (cfg.base || '').trim();
    if (!raw) return '';
    return raw.replace(/\/+$/, '');
  }

  function parseListInput(value) {
    if (!value) return [];
    if (Array.isArray(value)) return value.map((v) => String(v).trim()).filter(Boolean);
    return String(value)
      .split(',')
      .map((v) => v.trim())
      .filter(Boolean);
  }

  function boolFromConfig(value, def = false) {
    if (value === undefined || value === null) return def;
    if (typeof value === 'boolean') return value;
    const str = String(value).trim().toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(str)) return true;
    if (['0', 'false', 'no', 'off'].includes(str)) return false;
    return def;
  }

  function updateStatus(nodeId, patch) {
    const state = ensureState(nodeId);
    Object.assign(state, patch || {});
    renderState(nodeId, state);
    try {
      const cfg = NodeStore.ensure(nodeId, 'MCP').config || {};
      const relay = (cfg.relay || '').trim();
      if (!relay) return;
      if (state.status === 'ready') {
        setRelayState(nodeId, { state: 'ok', message: 'Server ready' });
      } else if (state.status === 'error') {
        setRelayState(nodeId, { state: 'err', message: state.lastError || 'Connection error' });
      } else if (state.status === 'loading') {
        setRelayState(nodeId, { state: 'warn', message: 'Connecting…' });
      } else {
        setRelayState(nodeId, { state: 'warn', message: 'Idle' });
      }
    } catch (err) {
      // ignore relay state update issues
    }
  }

  function renderState(nodeId, state) {
    const node = getNode(nodeId);
    if (!node || !node.el) return;
    const statusEl = node.el.querySelector('[data-mcp-status]');
    const serverEl = node.el.querySelector('[data-mcp-server]');
    const resEl = node.el.querySelector('[data-mcp-resources]');
    const ctxEl = node.el.querySelector('[data-mcp-context]');
    const busyClass = state.busy ? 'busy' : '';

    if (statusEl) {
      const label = state.status === 'ready'
        ? 'Connected'
        : state.status === 'error'
          ? 'Error'
          : state.status === 'loading'
            ? 'Connecting…'
            : state.status === 'idle'
              ? 'Idle'
              : state.status || 'Unknown';
      statusEl.textContent = label;
      statusEl.dataset.state = state.status || 'idle';
      statusEl.classList.toggle('busy', !!state.busy);
    }

    if (serverEl) {
      if (state.server) {
        const info = state.server;
        const lines = [];
        if (info.name) lines.push(`${info.name}`);
        if (info.version) lines.push(`Version ${info.version}`);
        if (info.description) lines.push(info.description);
        serverEl.textContent = lines.join('\n');
      } else if (state.lastError) {
        serverEl.textContent = state.lastError;
      } else {
        serverEl.textContent = '(no status)';
      }
    }

    if (resEl) {
      if (state.resources && state.resources.length) {
        resEl.textContent = state.resources
          .map((res) => `${res.name || res.uri} — ${res.description || ''}`.trim())
          .slice(0, 8)
          .join('\n');
      } else {
        resEl.textContent = '(no resources)';
      }
    }

    if (ctxEl) {
      const cfg = NodeStore.ensure(nodeId, 'MCP').config || {};
      const lastCtx = cfg.lastContext || '';
      ctxEl.textContent = lastCtx ? lastCtx : '(no context yet)';
    }

    if (node.el) node.el.dataset.busy = busyClass;
  }

  async function withRequest(nodeId, fn) {
    const state = ensureState(nodeId);
    if (state.busy) {
      throw new Error('MCP node busy');
    }
    updateStatus(nodeId, { busy: true, lastError: null });
    try {
      const result = await fn();
      updateStatus(nodeId, { busy: false });
      return result;
    } catch (err) {
      const message = err && err.message ? err.message : String(err);
      log(`[mcp:${nodeId}] ${message}`);
      updateStatus(nodeId, { busy: false, status: 'error', lastError: message });
      setBadge(`MCP error: ${message}`, false);
      throw err;
    }
  }

  async function fetchStatus(nodeId, { quiet = false } = {}) {
    const cfg = NodeStore.ensure(nodeId, 'MCP').config || {};
    const base = normalizeBase(cfg);
    if (!base) {
      updateStatus(nodeId, { status: 'idle', lastError: 'Configure base URL' });
      if (!quiet) setBadge('Configure MCP base URL', false);
      return null;
    }
    const relay = (cfg.relay || '').trim();
    const viaNkn = !!relay;
    const api = (cfg.api || '').trim();
    const timeout = Number.isFinite(cfg.timeoutMs) ? cfg.timeoutMs : 20000;
    updateStatus(nodeId, { status: 'loading', lastError: null });
    try {
      if (!viaNkn && base) {
        await ensureLocalNetworkAccess({ requireGesture: true });
      }
      const data = await Net.getJSON(base, '/mcp/status', api, viaNkn, relay);
      updateStatus(nodeId, {
        status: 'ready',
        server: data?.server || null,
        resources: Array.isArray(data?.resources) ? data.resources : [],
        tools: Array.isArray(data?.tools) ? data.tools : [],
        lastError: null,
      });
      NodeStore.update(nodeId, {
        type: 'MCP',
        lastStatus: {
          server: data?.server || null,
          protocolVersion: data?.protocolVersion || null,
          ts: data?.ts || Date.now(),
        }
      });
      if (!quiet) setBadge('MCP server ready');
      Router.sendFrom(nodeId, 'status', {
        nodeId,
        status: 'ready',
        server: data?.server || null,
        resources: data?.resources || [],
        tools: data?.tools || [],
        ts: data?.ts || Date.now()
      });
      return data;
    } catch (err) {
      const message = err && err.message ? err.message : String(err);
      updateStatus(nodeId, { status: 'error', lastError: message });
      if (!quiet) setBadge(`MCP status failed: ${message}`, false);
      Router.sendFrom(nodeId, 'status', {
        nodeId,
        status: 'error',
        error: message,
        ts: Date.now()
      });
      throw err;
    }
  }

  function extractQuery(payload) {
    if (payload == null) return { text: '', system: '' };
    if (typeof payload === 'string') return { text: payload, system: '' };
    if (typeof payload === 'number' || typeof payload === 'boolean') return { text: String(payload), system: '' };
    if (typeof payload !== 'object') return { text: String(payload || ''), system: '' };
    const text = payload.text || payload.prompt || payload.query || '';
    const system = payload.system || payload.augmentSystem || '';
    const resourceFilters = payload.resourceFilters || payload.resources || [];
    const toolFilters = payload.toolFilters || payload.tools || [];
    return {
      text: typeof text === 'string' ? text : JSON.stringify(text),
      system: typeof system === 'string' ? system : JSON.stringify(system),
      resourceFilters,
      toolFilters
    };
  }

  function mergeFilters(baseFilters, incoming) {
    const out = new Set();
    parseListInput(baseFilters).forEach((item) => out.add(item));
    parseListInput(incoming).forEach((item) => out.add(item));
    return Array.from(out).filter(Boolean);
  }

  async function runQuery(nodeId, payload) {
    const cfg = NodeStore.ensure(nodeId, 'MCP').config || {};
    const base = normalizeBase(cfg);
    if (!base) {
      setBadge('Configure MCP base URL', false);
      return;
    }
    if (boolFromConfig(cfg.connectOnQuery, true)) {
      const state = ensureState(nodeId);
      if (state.status !== 'ready') {
        try {
          await fetchStatus(nodeId, { quiet: true });
        } catch (err) {
          // keep going; runQuery will report failure if needed
        }
      }
    }

    const { text, system, resourceFilters, toolFilters } = extractQuery(payload);
    const queryText = (text || '').trim();
    if (!queryText) {
      setBadge('MCP query skipped (empty input)', false);
      return;
    }

    const relay = (cfg.relay || '').trim();
    const viaNkn = !!relay;
    const api = (cfg.api || '').trim();
    const timeout = Number.isFinite(cfg.timeoutMs) ? cfg.timeoutMs : 20000;

    if (!viaNkn) {
      await ensureLocalNetworkAccess({ requireGesture: true });
    }

    const requestBody = {
      query: queryText,
      resourceFilters: mergeFilters(cfg.resourceFilters, resourceFilters),
      toolFilters: mergeFilters(cfg.toolFilters, toolFilters),
      augmentSystem: [cfg.augmentSystem || '', system || ''].filter(Boolean).join('\n'),
      client: {
        nodeId,
        graphId: CFG.graphId,
        clientName: cfg.clientName || 'hydra-graph',
        clientVersion: cfg.clientVersion || '0.1.0'
      },
      emitSystem: boolFromConfig(cfg.emitSystem, true),
      emitContext: boolFromConfig(cfg.emitContext, true)
    };

    const resourceCount = Array.isArray(requestBody.resourceFilters) ? requestBody.resourceFilters.length : 0;
    const toolCount = Array.isArray(requestBody.toolFilters) ? requestBody.toolFilters.length : 0;
    log(`[mcp:${nodeId}] query (tags=${resourceCount}, tools=${toolCount})`);

    let result;
    try {
      result = await withRequest(nodeId, async () => {
        return Net.postJSON(base, '/mcp/query', requestBody, api, viaNkn, relay, timeout);
      });
    } catch (err) {
      const message = err && err.message ? err.message : String(err);
      Router.sendFrom(nodeId, 'status', {
        nodeId,
        status: 'error',
        error: message,
        ts: Date.now()
      });
      return;
    }

    NodeStore.update(nodeId, {
      type: 'MCP',
      lastSystem: result?.system || '',
      lastPrompt: result?.prompt || queryText,
      lastContext: result?.context || '',
      lastTs: result?.ts || Date.now()
    });

    updateStatus(nodeId, {
      status: 'ready',
      lastError: null,
      lastResponse: result || null
    });

    if (result?.system) {
      Router.sendFrom(nodeId, 'system', {
        nodeId,
        type: 'text',
        text: result.system,
        server: result.server || null,
        ts: result.ts || Date.now()
      });
    }
    if (result?.prompt) {
      Router.sendFrom(nodeId, 'prompt', {
        nodeId,
        type: 'text',
        text: result.prompt,
        server: result.server || null,
        ts: result.ts || Date.now()
      });
    }
    if (result?.context) {
      Router.sendFrom(nodeId, 'context', {
        nodeId,
        type: 'text',
        text: result.context,
        server: result.server || null,
        ts: result.ts || Date.now()
      });
    }
    if (Array.isArray(result?.resources)) {
      Router.sendFrom(nodeId, 'resources', {
        nodeId,
        type: 'resources',
        resources: result.resources,
        ts: result.ts || Date.now()
      });
    }
    Router.sendFrom(nodeId, 'status', {
      nodeId,
      status: 'ready',
      server: result?.server || null,
      ts: result?.ts || Date.now()
    });
    Router.sendFrom(nodeId, 'raw', {
      nodeId,
      response: result,
      ts: result?.ts || Date.now()
    });

    setBadge('MCP response ready');
  }

  async function runTool(nodeId, payload) {
    const cfg = NodeStore.ensure(nodeId, 'MCP').config || {};
    const base = normalizeBase(cfg);
    if (!base) {
      setBadge('Configure MCP base URL', false);
      return;
    }
    let name = '';
    let args = {};
    if (payload == null) {
      return;
    }
    if (typeof payload === 'string') {
      name = payload.trim();
    } else if (typeof payload === 'object') {
      name = String(payload.name || payload.tool || '').trim();
      if (payload.arguments && typeof payload.arguments === 'object') args = payload.arguments;
      else if (payload.args && typeof payload.args === 'object') args = payload.args;
    }
    if (!name) {
      setBadge('Tool call skipped (no name)', false);
      return;
    }

    const relay = (cfg.relay || '').trim();
    const viaNkn = !!relay;
    const api = (cfg.api || '').trim();
    const timeout = Number.isFinite(cfg.timeoutMs) ? cfg.timeoutMs : 20000;

    log(`[mcp:${nodeId}] tool ${name}`);

    let result;
    try {
      result = await withRequest(nodeId, async () => {
        return Net.postJSON(base, '/mcp/tool', { name, arguments: args }, api, viaNkn, relay, timeout);
      });
    } catch (err) {
      const message = err && err.message ? err.message : String(err);
      Router.sendFrom(nodeId, 'status', {
        nodeId,
        status: 'error',
        error: message,
        ts: Date.now()
      });
      return;
    }

    Router.sendFrom(nodeId, 'raw', {
      nodeId,
      tool: name,
      response: result,
      ts: result?.ts || Date.now()
    });
    if (result?.ok && result?.result?.content) {
      const textParts = [];
      for (const entry of result.result.content) {
        if (entry && typeof entry === 'object' && entry.type === 'text') {
          textParts.push(String(entry.text || ''));
        }
      }
      if (textParts.length) {
        Router.sendFrom(nodeId, 'context', {
          nodeId,
          type: 'text',
          text: textParts.join('\n'),
          tool: name,
          ts: result?.ts || Date.now()
        });
      }
    }
    Router.sendFrom(nodeId, 'status', {
      nodeId,
      status: 'ready',
      tool: name,
      ts: result?.ts || Date.now()
    });
    setBadge(`Tool ${name} complete`);
  }

  async function runResourceRefresh(nodeId, payload) {
    try {
      await fetchStatus(nodeId, { quiet: !!payload?.quiet });
    } catch (err) {
      // already handled in fetchStatus
    }
  }

  function init(nodeId) {
    const cfg = NodeStore.ensure(nodeId, 'MCP').config || {};
    ensureState(nodeId);
    renderState(nodeId, ensureState(nodeId));
    if (boolFromConfig(cfg.autoConnect, true)) {
      fetchStatus(nodeId, { quiet: true }).catch(() => {});
    }
  }

  function dispose(nodeId) {
    stateByNode.delete(nodeId);
  }

  return {
    init,
    dispose,
    refresh: fetchStatus,
    onQuery: runQuery,
    onTool: runTool,
    onRefresh: runResourceRefresh,
    renderState,
  };
}

export { createMCP };
