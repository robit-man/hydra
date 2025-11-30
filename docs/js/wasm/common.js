import { env as transformersEnv } from '@xenova/transformers';
import * as ort from 'onnxruntime-web';

const DEFAULT_ORT_WASM_PATH =
  (window.__hydraWasm && window.__hydraWasm.ortWasmPath) ||
  'https://cdn.jsdelivr.net/npm/onnxruntime-web@1.23.2/dist/';
const DEFAULT_THREADS = (() => {
  const raw = window.__hydraWasm?.defaultWasmThreads;
  const num = Number(raw);
  if (Number.isFinite(num) && num > 0) return Math.min(8, Math.max(1, Math.round(num)));
  return 1;
})();

const normalizeThreads = (value) => {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) return DEFAULT_THREADS;
  return Math.min(8, Math.max(1, Math.round(num)));
};

class ModelCache {
  constructor({ dbName = 'hydra-wasm-cache', storeName = 'models', expirationMs = 7 * 24 * 60 * 60 * 1000 } = {}) {
    this.dbName = dbName;
    this.storeName = storeName;
    this.dbVersion = 1;
    this.expirationMs = expirationMs;
  }

  async openDB() {
    if (typeof indexedDB === 'undefined') throw new Error('indexedDB unavailable');
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(this.dbName, this.dbVersion);
      request.onerror = () => reject(request.error);
      request.onsuccess = () => resolve(request.result);
      request.onupgradeneeded = (event) => {
        const db = event.target.result;
        if (!db.objectStoreNames.contains(this.storeName)) {
          db.createObjectStore(this.storeName);
        }
      };
    });
  }

  async get(url) {
    try {
      const db = await this.openDB();
      const transaction = db.transaction([this.storeName], 'readonly');
      const store = transaction.objectStore(this.storeName);
      return await new Promise((resolve, reject) => {
        const request = store.get(url);
        request.onerror = () => reject(request.error);
        request.onsuccess = () => {
          const cached = request.result;
          if (cached && Date.now() - cached.timestamp < this.expirationMs) resolve(cached.data);
          else resolve(null);
        };
      });
    } catch (err) {
      console.warn('[wasm] cache get failed', err?.message || err);
      return null;
    }
  }

  async set(url, data) {
    try {
      const db = await this.openDB();
      const transaction = db.transaction([this.storeName], 'readwrite');
      const store = transaction.objectStore(this.storeName);
      await new Promise((resolve, reject) => {
        const request = store.put({ data, timestamp: Date.now() }, url);
        request.onerror = () => reject(request.error);
        request.onsuccess = () => resolve();
      });
    } catch (err) {
      console.warn('[wasm] cache set failed', err?.message || err);
    }
  }
}

const defaultCache = new ModelCache();

async function cachedFetch(url, { cache = defaultCache } = {}) {
  if (!url) throw new Error('Missing URL');
  try {
    const cached = await cache.get(url);
    if (cached) return new Response(cached);
  } catch (err) {
    // ignore cache read errors
  }
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  const data = await res.arrayBuffer();
  try {
    await cache.set(url, data);
  } catch (err) {
    // ignore cache write issues
  }
  return new Response(data, { status: res.status, statusText: res.statusText, headers: res.headers });
}

let ortConfigured = false;
function ensureOrtEnv(options = {}) {
  const threads = normalizeThreads(options.numThreads ?? options.threads ?? DEFAULT_THREADS);
  try {
    transformersEnv.allowLocalModels = false;
    transformersEnv.fetch = cachedFetch;
    if (transformersEnv.backends?.onnx?.wasm) {
      transformersEnv.backends.onnx.wasm.numThreads = threads;
    }
  } catch (err) {
    // ignore
  }
  try {
    if (ort?.env?.wasm) {
      ort.env.wasm.wasmPaths = options.wasmPaths || DEFAULT_ORT_WASM_PATH;
      ort.env.wasm.numThreads = threads;
      ortConfigured = true;
    }
  } catch (err) {
    // ignore
  }
  return { threads, configured: ortConfigured };
}

export { DEFAULT_ORT_WASM_PATH, ModelCache, cachedFetch, defaultCache, ensureOrtEnv, normalizeThreads, ort, transformersEnv as transformers };
