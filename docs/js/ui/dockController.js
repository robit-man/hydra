function asSide(value) {
  return String(value || '').trim().toLowerCase() === 'right' ? 'right' : 'left';
}

function ensureDockState(cfg) {
  if (!cfg || typeof cfg !== 'object') return { left: { open: false, active: '' }, right: { open: false, active: '' } };
  const current = cfg.uiDockState && typeof cfg.uiDockState === 'object' ? cfg.uiDockState : {};
  const left = current.left && typeof current.left === 'object' ? current.left : {};
  const right = current.right && typeof current.right === 'object' ? current.right : {};
  const normalized = {
    left: {
      open: left.open === true,
      active: String(left.active || '').trim()
    },
    right: {
      open: right.open === true,
      active: String(right.active || '').trim()
    }
  };
  cfg.uiDockState = normalized;
  return normalized;
}

function createDockController({
  side = 'left',
  rootEl = null,
  controlsEl = null,
  panelsEl = null,
  CFG = {},
  saveCFG = () => {},
  onStateChange = null
} = {}) {
  const dockSide = asSide(side);
  const listeners = new Set();
  const buttons = new Map();
  const panels = new Map();
  const root = rootEl || null;
  const controls = controlsEl || root?.querySelector('.hydra-dock-controls') || null;
  const panelRoot = panelsEl || root?.querySelector('.hydra-dock-panels') || null;
  const stateStore = ensureDockState(CFG);
  const state = stateStore[dockSide];
  let mounted = false;

  const emit = () => {
    const payload = { side: dockSide, open: !!state.open, active: String(state.active || '') };
    listeners.forEach((fn) => {
      try {
        fn(payload);
      } catch (_) {
        // ignore listener errors
      }
    });
    if (typeof onStateChange === 'function') {
      try {
        onStateChange(payload);
      } catch (_) {
        // ignore callback errors
      }
    }
  };

  const persist = () => {
    stateStore[dockSide] = {
      open: !!state.open,
      active: String(state.active || '')
    };
    CFG.uiDockState = stateStore;
    saveCFG();
  };

  const render = () => {
    if (root) root.dataset.open = state.open ? 'true' : 'false';
    if (panelRoot) panelRoot.dataset.open = state.open ? 'true' : 'false';
    buttons.forEach((btn, id) => {
      const active = state.open && state.active === id;
      btn.classList.toggle('active', active);
      btn.setAttribute('aria-pressed', active ? 'true' : 'false');
    });
    panels.forEach((panel, id) => {
      const visible = state.open && state.active === id;
      panel.hidden = !visible;
    });
  };

  const close = ({ persistState = true, emitChange = true } = {}) => {
    if (!state.open) return;
    state.open = false;
    render();
    if (persistState) persist();
    if (emitChange) emit();
  };

  const open = (panelId, { persistState = true, emitChange = true } = {}) => {
    const target = String(panelId || '').trim();
    if (!target || !panels.has(target)) return;
    state.active = target;
    state.open = true;
    render();
    if (persistState) persist();
    if (emitChange) emit();
  };

  const toggle = (panelId) => {
    const target = String(panelId || '').trim();
    if (!target || !panels.has(target)) return;
    const isMobileControlsStage = root?.dataset.mobileMode === 'true' && root?.dataset.mobileStage === 'controls';
    if (state.open && state.active === target && !isMobileControlsStage) {
      close();
      return;
    }
    open(target);
  };

  const setButtonTone = (panelId, tone = '') => {
    const target = String(panelId || '').trim();
    const btn = buttons.get(target);
    if (!btn) return;
    const value = String(tone || '').trim().toLowerCase();
    btn.dataset.tone = value || 'idle';
    btn.classList.toggle('tone-ok', value === 'ok');
    btn.classList.toggle('tone-warn', value === 'warn');
    btn.classList.toggle('tone-error', value === 'error');
  };

  const getState = () => ({ side: dockSide, open: !!state.open, active: String(state.active || '') });

  const subscribe = (listener) => {
    if (typeof listener !== 'function') return () => {};
    listeners.add(listener);
    return () => listeners.delete(listener);
  };

  const mount = () => {
    if (mounted) return;
    mounted = true;
    if (controls) {
      controls.querySelectorAll('[data-dock-item]').forEach((btn) => {
        const panelId = String(btn.getAttribute('data-dock-item') || '').trim();
        if (!panelId || buttons.has(panelId)) return;
        buttons.set(panelId, btn);
        btn.addEventListener('click', (event) => {
          event.preventDefault();
          toggle(panelId);
        });
      });
    }
    if (panelRoot) {
      panelRoot.querySelectorAll('[data-dock-panel]').forEach((panel) => {
        const panelId = String(panel.getAttribute('data-dock-panel') || '').trim();
        if (!panelId || panels.has(panelId)) return;
        panels.set(panelId, panel);
      });
      panelRoot.querySelectorAll(`[data-dock-close="${dockSide}"]`).forEach((closeBtn) => {
        closeBtn.addEventListener('click', (event) => {
          event.preventDefault();
          close();
        });
      });
    }
    const activeExists = state.active && panels.has(state.active);
    if (!activeExists) {
      state.active = panels.keys().next().value || '';
    }
    render();
  };

  mount();

  return {
    side: dockSide,
    open,
    close,
    toggle,
    isOpen: () => !!state.open,
    getActive: () => String(state.active || ''),
    getState,
    subscribe,
    setButtonTone,
    render
  };
}

export { createDockController };
