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
  j,
  log,
  setBadge
};
