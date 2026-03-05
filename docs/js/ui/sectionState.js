function ensureSectionState(cfg) {
  if (!cfg || typeof cfg !== 'object') return {};
  const source = cfg.uiSectionState && typeof cfg.uiSectionState === 'object' ? cfg.uiSectionState : {};
  cfg.uiSectionState = source;
  return source;
}

function normalizeCollapsedList(value) {
  if (!Array.isArray(value)) return [];
  return value
    .map((entry) => String(entry || '').trim())
    .filter(Boolean)
    .slice(0, 128);
}

function bindSectionState({
  root = null,
  panelKey = '',
  CFG = {},
  saveCFG = () => {}
} = {}) {
  const panelRoot = root || null;
  const key = String(panelKey || '').trim();
  if (!panelRoot || !key) {
    return {
      refresh: () => {},
      dispose: () => {}
    };
  }

  const stateStore = ensureSectionState(CFG);
  const panelState = stateStore[key] && typeof stateStore[key] === 'object' ? stateStore[key] : {};
  stateStore[key] = panelState;
  panelState.collapsed = normalizeCollapsedList(panelState.collapsed);
  const collapsed = new Set(panelState.collapsed);
  const unsubs = [];

  const persist = () => {
    panelState.collapsed = Array.from(collapsed).sort();
    CFG.uiSectionState = stateStore;
    saveCFG();
  };

  const syncSection = (sectionId, sectionEl) => {
    if (!sectionEl) return;
    const isCollapsed = collapsed.has(sectionId);
    sectionEl.classList.toggle('is-collapsed', isCollapsed);
    const content = sectionEl.querySelector('.hydra-section__content');
    if (content) content.hidden = isCollapsed;
    const arrow = sectionEl.querySelector('.hydra-section__arrow');
    if (arrow) arrow.textContent = isCollapsed ? '▸' : '▾';
  };

  const sections = Array.from(panelRoot.querySelectorAll('[data-section-id]'));
  sections.forEach((sectionEl) => {
    const sectionId = String(sectionEl.getAttribute('data-section-id') || '').trim();
    if (!sectionId) return;
    const header = sectionEl.querySelector('.hydra-section__header');
    if (!header) return;
    syncSection(sectionId, sectionEl);
    const onClick = (event) => {
      event.preventDefault();
      if (collapsed.has(sectionId)) collapsed.delete(sectionId);
      else collapsed.add(sectionId);
      syncSection(sectionId, sectionEl);
      persist();
    };
    header.addEventListener('click', onClick);
    unsubs.push(() => header.removeEventListener('click', onClick));
  });

  return {
    refresh: () => {
      sections.forEach((sectionEl) => {
        const sectionId = String(sectionEl.getAttribute('data-section-id') || '').trim();
        if (!sectionId) return;
        syncSection(sectionId, sectionEl);
      });
    },
    dispose: () => {
      unsubs.forEach((fn) => {
        try {
          fn();
        } catch (_) {
          // ignore
        }
      });
      unsubs.length = 0;
    }
  };
}

export { bindSectionState };
