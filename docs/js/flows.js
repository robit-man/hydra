import { qs, setBadge, LS } from './utils.js';
import { NodeStore } from './nodeStore.js';

function createFlowsLibrary({ Graph, log = () => {} }) {
  const STORAGE_KEY = 'graph.flows';

  const elements = {
    modal: null,
    body: null,
    title: null,
    close: null,
    backdrop: null
  };

  const state = {
    view: 'list',
    selectedFlowId: null,
    editorError: '',
    editorText: '',
    pendingName: ''
  };

  let flows = [];
  const importInput = document.createElement('input');
  importInput.type = 'file';
  importInput.accept = 'application/json';
  importInput.style.display = 'none';
  document.body.appendChild(importInput);

  importInput.addEventListener('change', () => {
    const file = importInput.files && importInput.files[0];
    importInput.value = '';
    if (!file) return;
    file
      .text()
      .then((text) => {
        let parsed;
        try {
          parsed = JSON.parse(text);
        } catch (err) {
          setBadge('Import failed: invalid JSON', false);
          return;
        }
        let snapshot = null;
        let proposedName = file.name.replace(/\.[^.]+$/, '');
        if (parsed && typeof parsed === 'object') {
          if (parsed.data && typeof parsed.data === 'object' && parsed.data.nodes) {
            snapshot = cloneSnapshot(parsed.data);
            proposedName = parsed.name || proposedName || 'Imported Flow';
          } else if (parsed.nodes && Array.isArray(parsed.nodes)) {
            snapshot = cloneSnapshot(parsed);
            if (parsed.name) proposedName = parsed.name;
          }
        }
        if (!snapshot) {
          setBadge('Import failed: unsupported flow format', false);
          return;
        }
        const name = proposedName || `Imported Flow ${new Date().toLocaleString()}`;
        const flow = persistNewFlow(name, snapshot);
        setBadge(`Imported flow “${flow.name}”`);
        renderDetail(flow.id);
      })
      .catch((err) => setBadge(`Import failed: ${err?.message || err}`, false));
  });

  function ensureElements() {
    if (!elements.modal) {
      elements.modal = qs('#flowsModal');
      elements.body = qs('#flowsBody');
      elements.title = qs('[data-flows-title]');
      elements.close = qs('#flowsClose');
      elements.backdrop = qs('#flowsBackdrop');

      elements.close?.addEventListener('click', closeModal);
      elements.backdrop?.addEventListener('click', closeModal);
      document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && elements.modal && !elements.modal.classList.contains('hidden')) {
          closeModal();
        }
      });

      elements.body?.addEventListener('click', onBodyClick);
      elements.body?.addEventListener('submit', (e) => e.preventDefault());
    }
  }

  function loadFlows() {
    const raw = LS.get(STORAGE_KEY, []);
    if (!Array.isArray(raw)) return [];
    return raw
      .map((entry) => normalizeFlow(entry))
      .filter(Boolean)
      .sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
  }

  function normalizeFlow(entry) {
    if (!entry || typeof entry !== 'object') return null;
    const id = typeof entry.id === 'string' && entry.id ? entry.id : generateId();
    const name = typeof entry.name === 'string' && entry.name.trim() ? entry.name.trim() : 'Untitled Flow';
    const createdAt = Number(entry.createdAt) || Date.now();
    const updatedAt = Number(entry.updatedAt) || createdAt;
    const data = entry.data && typeof entry.data === 'object' ? cloneSnapshot(entry.data) : null;
    if (!data) return null;
    return { id, name, createdAt, updatedAt, data };
  }

  function persist() {
    try {
      LS.set(STORAGE_KEY, flows);
    } catch (err) {
      log(`[flows] persist error: ${err?.message || err}`);
    }
  }

  function persistNewFlow(name, snapshot) {
    const now = Date.now();
    const flow = {
      id: generateId(),
      name: name.trim() || `Flow ${new Date(now).toLocaleString()}`,
      createdAt: now,
      updatedAt: now,
      data: cloneSnapshot(snapshot)
    };
    flows.unshift(flow);
    persist();
    return flow;
  }

  function updateFlow(flowId, patch) {
    const idx = flows.findIndex((f) => f.id === flowId);
    if (idx === -1) return null;
    const next = { ...flows[idx], ...patch };
    flows.splice(idx, 1, next);
    persist();
    return next;
  }

  function removeFlow(flowId) {
    const idx = flows.findIndex((f) => f.id === flowId);
    if (idx === -1) return false;
    flows.splice(idx, 1);
    persist();
    return true;
  }

  function openModal() {
    ensureElements();
    flows = loadFlows();
    state.view = 'list';
    state.selectedFlowId = null;
    state.editorError = '';
    state.editorText = '';
    state.pendingName = '';
    renderList();
    if (elements.modal) {
      elements.modal.classList.remove('hidden');
      elements.modal.setAttribute('aria-hidden', 'false');
    }
  }

  function openCreateModal({ presetName = '' } = {}) {
    ensureElements();
    flows = loadFlows();
    state.selectedFlowId = null;
    state.editorError = '';
    state.editorText = '';
    state.pendingName = typeof presetName === 'string' ? presetName : '';
    renderCreate();
    if (elements.modal) {
      elements.modal.classList.remove('hidden');
      elements.modal.setAttribute('aria-hidden', 'false');
    }
  }

  function closeModal() {
    if (elements.modal) {
      elements.modal.classList.add('hidden');
      elements.modal.setAttribute('aria-hidden', 'true');
    }
    state.view = 'list';
    state.selectedFlowId = null;
    state.editorError = '';
    state.editorText = '';
  }

  function onBodyClick(event) {
    const target = event.target instanceof Element ? event.target.closest('[data-action]') : null;
    if (!target) return;
    event.preventDefault();
    const action = target.getAttribute('data-action');

    switch (action) {
      case 'new-flow':
        renderCreate();
        break;
      case 'create-cancel':
        renderList();
        break;
      case 'create-save':
        handleCreateSave();
        break;
      case 'open-flow':
        renderDetail(target.getAttribute('data-flow-id'));
        break;
      case 'detail-back':
        renderList();
        break;
      case 'rename-flow':
        renderRename(state.selectedFlowId);
        break;
      case 'rename-cancel':
        renderDetail(state.selectedFlowId);
        break;
      case 'rename-save':
        handleRenameSave();
        break;
      case 'delete-flow':
        handleDelete();
        break;
      case 'load-flow':
        handleLoad();
        break;
      case 'edit-json':
        renderEditor(state.selectedFlowId);
        break;
      case 'editor-save':
        handleEditorSave();
        break;
      case 'editor-discard':
        renderDetail(state.selectedFlowId);
        break;
      case 'editor-export':
        handleExport();
        break;
      case 'import-flow':
        importInput.click();
        break;
      case 'create-default-flow':
        handleCreateDefault();
        break;
      default:
        break;
    }
  }

  function renderList() {
    state.view = 'list';
    state.selectedFlowId = null;
    state.editorError = '';
    state.editorText = '';
    state.pendingName = '';

    if (elements.title) elements.title.textContent = 'Flows';
    if (!elements.body) return;
    const items = flows
      .map((flow) => {
        const summary = `${flow.data?.nodes?.length || 0} components · updated ${formatTimestamp(flow.updatedAt)}`;
        return `
          <button class="flow-item" data-action="open-flow" data-flow-id="${escapeHtml(flow.id)}">
            <div class="flow-item-title">${escapeHtml(flow.name)}</div>
            <div class="flow-item-meta">${escapeHtml(summary)}</div>
          </button>
        `;
      })
      .join('');

    elements.body.innerHTML = `
      <div class="flow-toolbar" style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:16px;">
        <button data-action="new-flow">Save current graph as flow</button>
        <button class="ghost" data-action="import-flow">Import flow from file</button>
        <button class="ghost" data-action="create-default-flow">Create new flow</button>
      </div>
      <div class="flow-list" style="display:flex;flex-direction:column;gap:8px;">
        ${items || '<div class="muted">(No saved flows yet)</div>'}
      </div>
    `;
  }

  function handleCreateDefault() {
    const snapshot = makeDefaultSnapshot();
    const flow = persistNewFlow(defaultFlowName(), snapshot);
    setBadge(`Created flow “${flow.name}”`);
    renderDetail(flow.id);
  }

  function defaultFlowName() {
    return `New Flow ${new Date().toLocaleString()}`;
  }

  function makeDefaultSnapshot() {
    const asrId = generateNodeId('asr');
    const llmId = generateNodeId('llm');
    const ttsId = generateNodeId('tts');

    const nodes = [
      { id: asrId, type: 'ASR', x: 90, y: 200, w: 0, h: 0 },
      { id: llmId, type: 'LLM', x: 380, y: 180, w: 0, h: 0 },
      { id: ttsId, type: 'TTS', x: 680, y: 200, w: 0, h: 0 }
    ];

    const links = [
      { from: { node: asrId, port: 'final' }, to: { node: llmId, port: 'prompt' } },
      { from: { node: llmId, port: 'final' }, to: { node: ttsId, port: 'text' } }
    ];

    const nodeConfigs = {};
    nodes.forEach((node) => {
      const defaults = NodeStore?.defaultsByType?.[node.type] || {};
      nodeConfigs[node.id] = {
        id: node.id,
        type: node.type,
        config: cloneValue(defaults)
      };
    });

    return { nodes, links, nodeConfigs };
  }

  function renderCreate() {
    state.view = 'create';
    const defaultName = state.pendingName || `Flow ${new Date().toLocaleString()}`;
    if (elements.title) elements.title.textContent = 'Save Flow';
    if (!elements.body) return;
    elements.body.innerHTML = `
      <div class="form-grid" style="display:grid;grid-template-columns:1fr;gap:12px;">
        <label for="flowNameInput">Flow name</label>
        <input id="flowNameInput" type="text" value="${escapeHtml(defaultName)}" autocomplete="off" />
      </div>
      <div class="row" style="margin-top:16px;display:flex;gap:12px;">
        <button data-action="create-save">Save</button>
        <button class="ghost" data-action="create-cancel">Cancel</button>
      </div>
    `;
    const input = qs('#flowNameInput');
    input?.focus();
    input?.select();
  }

  function handleCreateSave() {
    const input = qs('#flowNameInput');
    const name = input && 'value' in input ? String(input.value || '').trim() : '';
    const snapshot = Graph.exportWorkspace();
    const flow = persistNewFlow(name || `Flow ${new Date().toLocaleString()}`, snapshot);
    setBadge(`Saved flow “${flow.name}”`);
    renderDetail(flow.id);
  }

  function renderDetail(flowId) {
    const flow = flows.find((f) => f.id === flowId);
    if (!flow) {
      renderList();
      return;
    }
    state.view = 'detail';
    state.selectedFlowId = flow.id;
    state.editorError = '';
    state.editorText = '';
    if (elements.title) elements.title.textContent = flow.name;
    if (!elements.body) return;

    const components = (flow.data?.nodes || []).map((node, idx) => `${idx + 1}. ${node.type || 'Node'} — ${node.id}`).join('\n');

    elements.body.innerHTML = `
      <div class="flow-detail" style="display:flex;flex-direction:column;gap:16px;">
        <div>
          <div class="flow-name" style="font-weight:600;font-size:1.1rem;">${escapeHtml(flow.name)}</div>
          <div class="muted" style="margin-top:4px;">${escapeHtml(formatDetailMeta(flow))}</div>
        </div>
        <div>
          <div class="muted" style="margin-bottom:6px;">Components</div>
          <pre class="code" style="max-height:220px;overflow:auto;padding:12px;">${escapeHtml(components || '(no components recorded)')}</pre>
        </div>
        <div class="row" style="display:flex;gap:12px;flex-wrap:wrap;">
          <button data-action="load-flow">Load graph into editor</button>
          <button class="secondary" data-action="edit-json">Show JSON in editor</button>
          <button class="ghost" data-action="rename-flow">Rename</button>
        </div>
        <div class="row" style="display:flex;gap:12px;flex-wrap:wrap;">
          <button class="ghost" data-action="detail-back">Back</button>
          <button class="ghost" data-action="delete-flow">Delete</button>
        </div>
      </div>
    `;
  }

  function renderRename(flowId) {
    const flow = flows.find((f) => f.id === flowId);
    if (!flow) {
      renderList();
      return;
    }
    state.view = 'rename';
    state.selectedFlowId = flow.id;
    state.pendingName = flow.name;
    if (elements.title) elements.title.textContent = `Rename ${flow.name}`;
    if (!elements.body) return;
    elements.body.innerHTML = `
      <div class="form-grid" style="display:grid;grid-template-columns:1fr;gap:12px;">
        <label for="flowRenameInput">Flow name</label>
        <input id="flowRenameInput" type="text" value="${escapeHtml(flow.name)}" autocomplete="off" />
      </div>
      <div class="row" style="margin-top:16px;display:flex;gap:12px;">
        <button data-action="rename-save">Save</button>
        <button class="ghost" data-action="rename-cancel">Cancel</button>
      </div>
    `;
    const input = qs('#flowRenameInput');
    input?.focus();
    input?.select();
  }

  function handleRenameSave() {
    const flow = flows.find((f) => f.id === state.selectedFlowId);
    if (!flow) {
      renderList();
      return;
    }
    const input = qs('#flowRenameInput');
    const nextName = input && 'value' in input ? String(input.value || '').trim() : '';
    const name = nextName || flow.name;
    const updated = updateFlow(flow.id, { name, updatedAt: Date.now() });
    if (updated) setBadge(`Renamed flow to “${updated.name}”`);
    renderDetail(flow.id);
  }

  function handleDelete() {
    const flowId = state.selectedFlowId;
    const flow = flows.find((f) => f.id === flowId);
    if (!flow) {
      renderList();
      return;
    }
    const ok = window.confirm(`Delete flow “${flow.name}”?`);
    if (!ok) return;
    removeFlow(flowId);
    setBadge(`Deleted flow “${flow.name}”`);
    renderList();
  }

  function handleLoad() {
    const flow = flows.find((f) => f.id === state.selectedFlowId);
    if (!flow) return;
    const success = Graph.importWorkspace(cloneSnapshot(flow.data), {
      badgeText: `Loaded flow “${flow.name}”`
    });
    if (success) {
      closeModal();
    }
  }

  function renderEditor(flowId) {
    const flow = flows.find((f) => f.id === flowId);
    if (!flow) {
      renderList();
      return;
    }
    state.view = 'editor';
    state.selectedFlowId = flow.id;
    state.editorError = '';
    state.editorText = JSON.stringify(flow.data, null, 2);
    if (elements.title) elements.title.textContent = `Editing ${flow.name}`;
    if (!elements.body) return;
    elements.body.innerHTML = `
      <div style="display:flex;flex-direction:column;gap:12px;">
        ${state.editorError ? `<div class="error" style="color:#c0392b;">${escapeHtml(state.editorError)}</div>` : ''}
        <textarea id="flowEditor" style="min-height:280px;font-family:monospace;font-size:0.9rem;padding:12px;">${escapeHtml(state.editorText)}</textarea>
        <div class="row" style="display:flex;gap:12px;flex-wrap:wrap;">
          <button data-action="editor-save">Save</button>
          <button class="ghost" data-action="editor-discard">Discard</button>
          <button class="secondary" data-action="editor-export">Export</button>
        </div>
      </div>
    `;
    const textarea = qs('#flowEditor');
    textarea?.focus();
  }

  function handleEditorSave() {
    const flow = flows.find((f) => f.id === state.selectedFlowId);
    if (!flow) {
      renderList();
      return;
    }
    const textarea = qs('#flowEditor');
    const text = textarea && 'value' in textarea ? String(textarea.value || '') : '';
    let parsed;
    try {
      parsed = JSON.parse(text);
    } catch (err) {
      state.editorError = `Invalid JSON: ${err?.message || err}`;
      state.editorText = text;
      renderEditor(flow.id);
      return;
    }
    if (!parsed || typeof parsed !== 'object' || !Array.isArray(parsed.nodes)) {
      state.editorError = 'Flow JSON must include a "nodes" array.';
      state.editorText = text;
      renderEditor(flow.id);
      return;
    }
    const updated = updateFlow(flow.id, { data: cloneSnapshot(parsed), updatedAt: Date.now() });
    if (updated) {
      setBadge(`Saved edits to “${updated.name}”`);
      renderDetail(flow.id);
    }
  }

  function handleExport() {
    const flow = flows.find((f) => f.id === state.selectedFlowId);
    if (!flow) return;
    const filename = `${slugify(flow.name || 'flow')}.json`;
    const data = state.view === 'editor' ? (qs('#flowEditor')?.value || JSON.stringify(flow.data, null, 2)) : JSON.stringify(flow.data, null, 2);
    const blob = new Blob([data], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  function formatDetailMeta(flow) {
    const parts = [];
    parts.push(`${flow.data?.nodes?.length || 0} components`);
    if (flow.createdAt) parts.push(`created ${formatTimestamp(flow.createdAt)}`);
    if (flow.updatedAt) parts.push(`updated ${formatTimestamp(flow.updatedAt)}`);
    return parts.join(' · ');
  }

  function formatTimestamp(ts) {
    try {
      return new Date(ts).toLocaleString();
    } catch (err) {
      return String(ts);
    }
  }

  function slugify(text) {
    return String(text || 'flow')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .replace(/-{2,}/g, '-') || 'flow';
  }

  function generateNodeId(type = 'node') {
    const clean = String(type || 'node').toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 5) || 'node';
    try {
      if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
        return `n-${clean}-${crypto.randomUUID().slice(0, 8)}`;
      }
    } catch (err) {
      // ignore
    }
    const rand = Math.random().toString(36).slice(2, 8);
    const time = Date.now().toString(36);
    return `n-${clean}-${time}-${rand}`;
  }

  function cloneValue(value) {
    if (value == null) return value;
    try {
      return JSON.parse(JSON.stringify(value));
    } catch (err) {
      if (typeof value === 'object') return { ...value };
      return value;
    }
  }

  function escapeHtml(str) {
    return String(str || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function generateId() {
    if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
      try {
        return crypto.randomUUID();
      } catch (err) {
        // fall through
      }
    }
    return `flow-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  }

  function cloneSnapshot(snapshot) {
    if (!snapshot || typeof snapshot !== 'object') return { nodes: [], links: [], nodeConfigs: {} };
    try {
      return JSON.parse(JSON.stringify(snapshot));
    } catch (err) {
      const out = { nodes: [], links: [], nodeConfigs: {} };
      out.nodes = Array.isArray(snapshot.nodes) ? snapshot.nodes.slice() : [];
      out.links = Array.isArray(snapshot.links) ? snapshot.links.slice() : [];
      out.nodeConfigs = snapshot.nodeConfigs && typeof snapshot.nodeConfigs === 'object' ? { ...snapshot.nodeConfigs } : {};
      return out;
    }
  }

  const flowsButton = qs('#flowsButton');
  flowsButton?.addEventListener('click', openModal);

  return {
    open: openModal,
    openCreate: openCreateModal,
    list: () => flows.slice(),
    saveSnapshot: (name, snapshot) => {
      if (!snapshot || typeof snapshot !== 'object') return null;
      flows = loadFlows();
      const baseName = typeof name === 'string' && name.trim() ? name.trim() : `Imported Flow ${new Date().toLocaleString()}`;
      return persistNewFlow(baseName, cloneSnapshot(snapshot));
    }
  };
}

export { createFlowsLibrary };
