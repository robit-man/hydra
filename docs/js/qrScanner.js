import { qs, setBadge } from './utils.js';

const QRScan = {
  modal: null,
  video: null,
  canvas: null,
  ctx: null,
  stream: null,
  raf: null,
  target: null,
  onResult: null,
  populateTarget: true,
  ready: false
};

let globalResultHandler = null;
const QR_HINT_KEYS = [
  'peer',
  'address',
  'nkn',
  'hydra',
  'noclip',
  'router',
  'router_target',
  'router_nkn',
  'relay',
  'target'
];

function normalizeQrCandidate(value, key = '') {
  const text = String(value || '').trim();
  if (!text) return '';
  const lower = text.toLowerCase();
  if (key === 'hydra') return lower.startsWith('hydra.') ? text : `hydra.${text}`;
  if (key === 'noclip') return lower.startsWith('noclip.') ? text : `noclip.${text}`;
  if (key === 'peer' && !text.includes('.') && /^[a-f0-9]{32,}$/i.test(text)) return `hydra.${text}`;
  return text;
}

function readCandidateFromObject(obj) {
  if (!obj || typeof obj !== 'object') return '';
  for (const key of QR_HINT_KEYS) {
    const candidate = normalizeQrCandidate(obj[key], key);
    if (candidate) return candidate;
  }
  return '';
}

function extractAddressFromQrText(rawText) {
  const text = String(rawText || '').trim();
  if (!text) return '';
  if (/^(hydra|noclip|graph)\./i.test(text)) return text;
  if (/^[a-f0-9]{32,}$/i.test(text)) return `hydra.${text}`;

  try {
    const url = new URL(text);
    for (const key of QR_HINT_KEYS) {
      const candidate = normalizeQrCandidate(url.searchParams.get(key), key);
      if (candidate) return candidate;
    }
  } catch (_) {
    // not a URL
  }

  if (text.startsWith('{') || text.startsWith('[')) {
    try {
      const parsed = JSON.parse(text);
      const candidate = readCandidateFromObject(parsed);
      if (candidate) return candidate;
    } catch (_) {
      // not JSON
    }
  }

  if (text.includes('=')) {
    try {
      const params = new URLSearchParams(text);
      for (const key of QR_HINT_KEYS) {
        const candidate = normalizeQrCandidate(params.get(key), key);
        if (candidate) return candidate;
      }
    } catch (_) {
      // ignore query parse failures
    }
  }

  return '';
}

function registerQrResultHandler(fn) {
  globalResultHandler = typeof fn === 'function' ? fn : null;
}

function setupQrScanner() {
  if (QRScan.ready) return;
  QRScan.modal = qs('#qrModal');
  QRScan.video = qs('#qrVideo');
  QRScan.canvas = qs('#qrCanvas');
  if (QRScan.canvas) QRScan.ctx = QRScan.canvas.getContext('2d');
  qs('#qrClose')?.addEventListener('click', () => closeQrScanner());
  qs('#qrStop')?.addEventListener('click', () => closeQrScanner());
  qs('#qrBackdrop')?.addEventListener('click', () => closeQrScanner());
  window.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && QRScan.modal && !QRScan.modal.classList.contains('hidden')) closeQrScanner();
  });
  QRScan.ready = true;
}

async function openQrScanner(targetInput, onResult, options = {}) {
  setupQrScanner();
  if (!QRScan.modal) return;
  if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
    setBadge('Camera not available', false);
    return;
  }
  if (!window.jsQR) {
    setBadge('QR library missing', false);
    return;
  }
  try {
    closeQrScanner();
  } catch (err) {
    // ignore
  }
  QRScan.target = targetInput || null;
  QRScan.onResult = typeof onResult === 'function' ? onResult : null;
  QRScan.populateTarget = options && options.populateTarget === false ? false : true;
  QRScan.modal.classList.remove('hidden');
  QRScan.modal.setAttribute('aria-hidden', 'false');
  try {
    const stream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: 'environment' } });
    QRScan.stream = stream;
    if (QRScan.video) {
      QRScan.video.srcObject = stream;
      await QRScan.video.play().catch(() => {});
    }
    QRScan.raf = requestAnimationFrame(scanQrFrame);
  } catch (err) {
    closeQrScanner();
    setBadge('Camera access denied', false);
  }
}

function stopQrStream() {
  if (QRScan.raf) cancelAnimationFrame(QRScan.raf);
  QRScan.raf = null;
  if (QRScan.stream) {
    QRScan.stream.getTracks().forEach((track) => {
      try {
        track.stop();
      } catch (err) {
        // ignore
      }
    });
  }
  QRScan.stream = null;
  if (QRScan.video) {
    QRScan.video.pause();
    QRScan.video.srcObject = null;
  }
}

function closeQrScanner() {
  stopQrStream();
  if (QRScan.modal) {
    QRScan.modal.classList.add('hidden');
    QRScan.modal.setAttribute('aria-hidden', 'true');
  }
  QRScan.target = null;
  QRScan.onResult = null;
  QRScan.populateTarget = true;
}

function scanQrFrame() {
  if (!QRScan.video || !QRScan.canvas || !QRScan.ctx) {
    QRScan.raf = requestAnimationFrame(scanQrFrame);
    return;
  }
  if (QRScan.video.readyState < 2) {
    QRScan.raf = requestAnimationFrame(scanQrFrame);
    return;
  }
  const vw = QRScan.video.videoWidth || 0;
  const vh = QRScan.video.videoHeight || 0;
  if (!vw || !vh) {
    QRScan.raf = requestAnimationFrame(scanQrFrame);
    return;
  }
  if (QRScan.canvas.width !== vw) QRScan.canvas.width = vw;
  if (QRScan.canvas.height !== vh) QRScan.canvas.height = vh;
  QRScan.ctx.drawImage(QRScan.video, 0, 0, vw, vh);
  try {
    const image = QRScan.ctx.getImageData(0, 0, vw, vh);
    const code = window.jsQR ? window.jsQR(image.data, vw, vh) : null;
    if (code && code.data) {
      const text = code.data.trim();
      if (text) {
        const parsedText = extractAddressFromQrText(text);
        const resolvedText = parsedText || text;
        if (QRScan.target && QRScan.populateTarget) {
          QRScan.target.value = resolvedText;
          QRScan.target.dispatchEvent(new Event('input', { bubbles: true }));
        }
        if (QRScan.onResult) {
          try {
            QRScan.onResult(resolvedText, {
              rawText: text,
              parsedText
            });
          } catch (err) {
            // ignore consumer errors
          }
        }
        if (globalResultHandler && !QRScan.onResult) {
          try {
            globalResultHandler({
              text: resolvedText,
              rawText: text,
              parsedText,
              target: QRScan.target
            });
          } catch (err) {
            // ignore
          }
        }
        setBadge('QR scanned');
        closeQrScanner();
        return;
      }
    }
  } catch (err) {
    // ignore scan errors
  }
  QRScan.raf = requestAnimationFrame(scanQrFrame);
}

export { setupQrScanner, openQrScanner, closeQrScanner, registerQrResultHandler, extractAddressFromQrText };
