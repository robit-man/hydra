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
  ready: false
};

let globalResultHandler = null;

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

async function openQrScanner(targetInput, onResult) {
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
        if (QRScan.target) {
          QRScan.target.value = text;
          QRScan.target.dispatchEvent(new Event('input', { bubbles: true }));
        }
        if (QRScan.onResult) {
          try {
            QRScan.onResult(text);
          } catch (err) {
            // ignore consumer errors
          }
        }
        if (globalResultHandler && !QRScan.onResult) {
          try {
            globalResultHandler({ text, target: QRScan.target });
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

export { setupQrScanner, openQrScanner, closeQrScanner, registerQrResultHandler };
