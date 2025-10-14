function createWorkspaceManager({
  setBadge,
  deepClone,
  exportWorkspaceSnapshot,
  importWorkspaceSnapshot
}) {
  let fileImportInput = null;
  let requestFlowSave = null;

  const slugifyGraphName = (text) =>
    String(text || 'hydra-graph')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .replace(/-{2,}/g, '-') || 'hydra-graph';

  const graphTimestampTag = () => {
    try {
      return new Date().toISOString().replace(/[:.]/g, '-');
    } catch (err) {
      return String(Date.now());
    }
  };

  const normalizeImportedGraph = (parsed) => {
    if (!parsed || typeof parsed !== 'object') return null;
    if (Array.isArray(parsed.nodes)) {
      const name = typeof parsed.name === 'string' ? parsed.name.trim() : '';
      return { snapshot: deepClone(parsed), name };
    }
    if (parsed.data && typeof parsed.data === 'object' && Array.isArray(parsed.data.nodes)) {
      const inner = parsed.data;
      const name = typeof parsed.name === 'string' && parsed.name.trim()
        ? parsed.name.trim()
        : typeof inner.name === 'string'
          ? inner.name.trim()
          : '';
      return { snapshot: deepClone(inner), name };
    }
    return null;
  };

  const buildWorkspacePayload = () => {
    const snapshot = exportWorkspaceSnapshot();
    if (!snapshot || typeof snapshot !== 'object') return null;
    const name = snapshot?.name && typeof snapshot.name === 'string' && snapshot.name.trim()
      ? snapshot.name.trim()
      : 'Hydra Graph';
    const payload = deepClone(snapshot);
    payload.savedAt = new Date().toISOString();
    if (!payload.name) payload.name = name;
    return { payload, name };
  };

  const downloadWorkspaceFile = () => {
    try {
      const bundle = buildWorkspacePayload();
      if (!bundle) {
        setBadge('Nothing to save', false);
        return;
      }
      const { payload, name } = bundle;
      const data = JSON.stringify(payload, null, 2);
      const blob = new Blob([data], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = `${slugifyGraphName(name)}-${graphTimestampTag()}.json`;
      anchor.rel = 'noopener';
      anchor.click();
      URL.revokeObjectURL(url);
      setBadge(`Graph saved${name ? ` as “${name}”` : ''}`);
    } catch (err) {
      setBadge(`Save failed: ${err?.message || err}`, false);
    }
  };

  const saveWorkspaceAsInteractive = () => {
    try {
      const bundle = buildWorkspacePayload();
      if (!bundle) {
        setBadge('Nothing to save', false);
        return;
      }
      const { payload, name } = bundle;
      const data = JSON.stringify(payload, null, 2);
      const suggestedBase = slugifyGraphName(name) || 'hydra-graph';
      const suggestedName = `${suggestedBase}.json`;
      if (typeof window !== 'undefined' && typeof window.showSaveFilePicker === 'function') {
        const opts = {
          suggestedName,
          types: [
            {
              description: 'Hydra Graph JSON',
              accept: { 'application/json': ['.json'] }
            }
          ]
        };
        window.showSaveFilePicker(opts)
          .then((handle) => {
            if (!handle) return null;
            return handle.createWritable()
              .then((writable) => writable.write(data).then(() => writable.close()))
              .then(() => {
                const finalName = handle.name || suggestedName;
                setBadge(`Graph saved as “${finalName}”`);
                return null;
              });
          })
          .catch((err) => {
            if (err && (err.name === 'AbortError' || err.name === 'NotAllowedError')) {
              return;
            }
            setBadge(`Save failed: ${err?.message || err}`, false);
          });
        return;
      }
      downloadWorkspaceFile();
    } catch (err) {
      setBadge(`Save failed: ${err?.message || err}`, false);
    }
  };

  const ensureImportInput = () => {
    if (fileImportInput || typeof document === 'undefined') return fileImportInput;
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = 'application/json,.json';
    input.style.display = 'none';
    input.addEventListener('change', (event) => {
      const target = event.target;
      const files = target?.files ? Array.from(target.files) : [];
      target.value = '';
      if (!files.length) return;
      const file = files[0];
      if (!file) return;
      file
        .text()
        .then((text) => {
          let parsed;
          try {
            parsed = JSON.parse(text);
          } catch (err) {
            setBadge('Graph file is not valid JSON', false);
            return;
          }
          const normalized = normalizeImportedGraph(parsed);
          if (!normalized?.snapshot) {
            setBadge('Selected file is not a compatible graph', false);
            return;
          }
          const badgeText = normalized.name
            ? `Imported “${normalized.name}”`
            : 'Graph imported';
          const ok = importWorkspaceSnapshot(normalized.snapshot, { badgeText });
          if (!ok) setBadge('Graph import failed', false);
        })
        .catch((err) => {
          setBadge(`Unable to read file: ${err?.message || err}`, false);
        });
    });
    document.body.appendChild(input);
    fileImportInput = input;
    return fileImportInput;
  };

  const openGraphImportDialog = () => {
    const input = ensureImportInput();
    if (!input) {
      setBadge('File import unavailable in this environment', false);
      return;
    }
    input.click();
  };

  const setFlowSaveHandler = (fn) => {
    requestFlowSave = typeof fn === 'function' ? fn : null;
  };

  const getFlowSaveHandler = () => requestFlowSave;

  return {
    buildWorkspacePayload,
    downloadWorkspaceFile,
    openGraphImportDialog,
    saveWorkspaceAsInteractive,
    setFlowSaveHandler,
    getFlowSaveHandler
  };
}

export { createWorkspaceManager };
