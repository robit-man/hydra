const ASR_DEFAULT_PROMPT =
  "Context: live interactive conversation, not a video or broadcast. " +
  "Transcribe exactly what is spoken. Do not add generic media sign-offs. " +
  "Avoid phrases such as: \"thanks for watching\", \"like and subscribe\", " +
  "\"don't forget to subscribe\", \"link in the description\".";

const SIGNOFF_RE = /\b(thanks(?:,)?\s+for\s+(?:watching|listening)|(?:don['’]t\s+forget\s+to\s+)?(?:like|subscribe)|like\s+and\s+subscribe|link\s+in\s+(?:the\s+)?description)\b/i;

const LS = {
  get(key, fallback) {
    try {
      const raw = localStorage.getItem(key);
      return raw ? JSON.parse(raw) : fallback;
    } catch (err) {
      return fallback;
    }
  },
  set(key, value) {
    localStorage.setItem(key, JSON.stringify(value));
  },
  del(key) {
    localStorage.removeItem(key);
  }
};

const qs = (sel) => document.querySelector(sel);
const qsa = (sel) => Array.from(document.querySelectorAll(sel));

const td = new TextDecoder();

const b64ToBytes = (b64) => {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
};

const TRUE_STRINGS = new Set(['true', '1', 'yes', 'on']);
const FALSE_STRINGS = new Set(['false', '0', 'no', 'off']);

const normalizeBoolChoice = (value) => {
  const text = String(value ?? '').trim().toLowerCase();
  if (TRUE_STRINGS.has(text)) return true;
  if (FALSE_STRINGS.has(text)) return false;
  return null;
};

function toBoolean(value, fallback = false) {
  if (typeof value === 'boolean') return value;
  if (value == null) return fallback;
  if (typeof value === 'number') return value !== 0;
  const normalized = normalizeBoolChoice(value);
  if (normalized !== null) return normalized;
  if (typeof value === 'string') {
    const num = Number(value);
    if (Number.isFinite(num)) return num !== 0;
  }
  return fallback;
}

function convertBooleanSelect(select) {
  if (!select) return null;
  const options = Array.from(select.options || []);
  if (!options.length) return null;
  if (!options.every((opt) => normalizeBoolChoice(opt.value ?? opt.text) !== null)) return null;
  const name = select.name;
  if (!name) return null;

  const wrap = document.createElement('div');
  wrap.className = 'toggle-boolean-wrap';

  const hiddenInput = document.createElement('input');
  hiddenInput.type = 'hidden';
  hiddenInput.name = name;
  wrap.appendChild(hiddenInput);

  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'toggle-boolean';
  wrap.appendChild(btn);

  const apply = (state) => {
    const enabled = !!state;
    const val = enabled ? 'true' : 'false';
    hiddenInput.value = val;
    btn.dataset.state = val;
    btn.textContent = val;
    btn.setAttribute('aria-pressed', val);
  };

  const fallback = normalizeBoolChoice(options[0].value ?? options[0].text);
  const initial = normalizeBoolChoice(select.value ?? options[0].value ?? options[0].text);
  apply(initial === null ? (fallback === null ? true : fallback) : initial);

  btn.addEventListener('click', (ev) => {
    ev.preventDefault();
    apply(hiddenInput.value !== 'true');
  });

  const parent = select.parentElement;
  if (parent) parent.replaceChild(wrap, select);
  return { wrap, button: btn, input: hiddenInput };
}

function convertBooleanSelects(container) {
  if (!container) return;
  const selects = container.querySelectorAll('select');
  selects.forEach((sel) => convertBooleanSelect(sel));
}

function j(x) {
  try {
    return JSON.stringify(x, null, 2);
  } catch (err) {
    return String(x);
  }
}

function log(message) {
  const box = qs('#logBox');
  if (!box) return;
  box.textContent = (box.textContent + '\n' + message).trim().slice(-9000);
  box.scrollTop = box.scrollHeight;
}

function setBadge(message, ok = true) {
  const el = qs('#logBox');
  if (!el) return;
  log(message);
}

export {
  ASR_DEFAULT_PROMPT,
  SIGNOFF_RE,
  LS,
  qs,
  qsa,
  td,
  b64ToBytes,
  convertBooleanSelect,
  convertBooleanSelects,
  toBoolean,
  j,
  log,
  setBadge
};
