/**
 * Pointcloud node - Depth Anything 3 integration with Three.js 3D Viewer
 * Accepts base64 images, processes them through Depth Anything 3 API, and renders pointclouds
 */

import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { FlyControls } from 'three/addons/controls/FlyControls.js';
import { PointerLockControls } from 'three/addons/controls/PointerLockControls.js';
import { Net } from './net.js';

function createPointcloud({ Router, NodeStore, setBadge, log }) {
  const STATE = new Map();
  const VIEWER_STATE = new Map();
  const MAX_CHUNK_SIZE = 800 * 1024; // 800KB chunks for base64 data

  const CAMERA_MODES = ['orbit', 'fly', 'fps'];
  const CAMERA_MODE_LABELS = {
    orbit: 'Orbit',
    fly: 'Fly',
    fps: 'FPS'
  };

  // ============================================================================
  // UTILITY FUNCTIONS
  // ============================================================================

  function chunkBase64(b64Data, maxSize = MAX_CHUNK_SIZE) {
    if (!b64Data || b64Data.length <= maxSize) {
      return [b64Data];
    }
    const chunks = [];
    for (let i = 0; i < b64Data.length; i += maxSize) {
      chunks.push(b64Data.substring(i, i + maxSize));
    }
    return chunks;
  }

  function reassembleBase64(chunks) {
    if (!Array.isArray(chunks)) return chunks;
    return chunks.join('');
  }

  function ensureState(nodeId) {
    const key = String(nodeId || '').trim();
    if (!key) return null;
    if (!STATE.has(key)) {
      STATE.set(key, {
        processing: false,
        lastJobId: null,
        currentPointcloud: null,
        modelReady: false,
        availableModels: [],
        currentModel: null,
        pollingInterval: null,
        receivedChunks: new Map(),
        floorSelectionActive: false,
        floorPoints: [],
        autoLoadStarted: false,
        modelLoading: false,
        modelLoadPromise: null,
        uploadProgress: ''
      });
    }
    return STATE.get(key);
  }

  function getConfig(nodeId) {
    const rec = NodeStore.ensure(nodeId, 'Pointcloud');
    return rec?.config || {};
  }

  function updateStatus(nodeId, text, tone = 'muted') {
    const el = document.querySelector(`.pointcloud-status[data-pointcloud-status="${nodeId}"]`);
    if (!el) return;
    el.textContent = text;
    el.dataset.state = tone;
    if (tone === 'ok') el.style.color = '#9ef1c2';
    else if (tone === 'pending') el.style.color = '#ffd479';
    else if (tone === 'err') el.style.color = '#ff9b9b';
    else el.style.color = '#ccc';
  }

  function updateUploadProgress(nodeId, text) {
    const el = document.querySelector(`.pointcloud-upload[data-pointcloud-upload="${nodeId}"]`);
    if (el) el.textContent = text || '';
    const st = ensureState(nodeId);
    if (st) st.uploadProgress = text || '';
  }

  function resolveEndpointConfig(nodeId, override = {}) {
    const cfg = Object.assign({}, getConfig(nodeId) || {}, override || {});
    const baseRaw = String(cfg.base || 'http://127.0.0.1:5000').trim();
    const relayRaw = String(cfg.relay || '').trim();
    const api = String(cfg.api || '').trim();
    const modeRaw = String(cfg.endpointMode || 'auto').toLowerCase();
    const viaNkn = !!relayRaw && (modeRaw === 'auto' || modeRaw === 'remote' || modeRaw === 'nkn');
    return {
      base: (baseRaw || 'http://127.0.0.1:5000').replace(/\/+$/, ''),
      api,
      relay: viaNkn ? relayRaw : '',
      viaNkn,
      mode: modeRaw
    };
  }

  const blobToDataUrl = (blob) => new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = (err) => reject(err);
    reader.readAsDataURL(blob);
  });

  async function maybeAutoLoadModel(nodeId, models) {
    const state = ensureState(nodeId);
    if (!state || state.modelReady) return;
    const cfg = getConfig(nodeId);
    if (String(cfg.autoLoadModel ?? 'true') === 'false') return;
    if (state.autoLoadStarted) return;
    state.autoLoadStarted = true;
    const list = Array.isArray(models) ? models : state.availableModels;
    if (!list || !list.length) {
      state.autoLoadStarted = false;
      return;
    }
    const desired = cfg.defaultModel
      || list.find((m) => m.current)?.id
      || list.find((m) => m.downloaded)?.id
      || list[0]?.id;
    if (!desired) {
      state.autoLoadStarted = false;
      return;
    }
    try {
      updateStatus(nodeId, `Loading model ${desired}…`, 'pending');
      const ok = await loadModel(nodeId, desired);
      if (!ok) {
        updateStatus(nodeId, `Model ${desired} failed`, 'err');
      }
    } finally {
      state.autoLoadStarted = false;
    }
  }

  // ============================================================================
  // API FUNCTIONS (existing functionality)
  // ============================================================================

  async function fetchModels(nodeId, opts = {}) {
    const state = ensureState(nodeId);
    if (!state) return;

    const cfg = getConfig(nodeId);
    const baseOverride = opts?.baseOverride || opts?.baseUrl;
    const endpoint = resolveEndpointConfig(nodeId, baseOverride ? { base: baseOverride } : {});
    const baseUrl = endpoint.base;

    try {
      const data = await Net.getJSON(baseUrl, '/api/models/list', endpoint.api, endpoint.viaNkn, endpoint.relay, { service: 'depth_any', useServiceTarget: true });
      state.availableModels = data.models || [];

      const current = state.availableModels.find(m => m.current);
      if (current) {
        state.currentModel = current.id;
        state.modelReady = current.status === 'ready';
        updateStatus(nodeId, `Model: ${current.id} ${state.modelReady ? '✓' : '(not loaded)'}`, state.modelReady ? 'ok' : 'pending');
      } else if (state.availableModels.length) {
        updateStatus(nodeId, `Models found (${state.availableModels.length})`, 'muted');
      } else {
        updateStatus(nodeId, 'No models available', 'err');
      }

      maybeAutoLoadModel(nodeId, state.availableModels).catch(() => {});

      return state.availableModels;
    } catch (err) {
      if (setBadge) setBadge(`Models fetch failed: ${err.message}`, false);
      updateStatus(nodeId, 'Model list failed', 'err');
      return [];
    }
  }

  async function loadModel(nodeId, modelId) {
    const state = ensureState(nodeId);
    if (!state) return false;
    if (!modelId) return false;
    if (state.modelLoading && state.currentModel === modelId && state.modelLoadPromise) {
      return state.modelLoadPromise;
    }

    const endpoint = resolveEndpointConfig(nodeId);
    const baseUrl = endpoint.base;

    const doLoad = async () => {
      state.modelLoading = true;
      state.modelReady = false;
      try {
        updateStatus(nodeId, `Selecting ${modelId}…`, 'pending');
        await Net.postJSON(baseUrl, '/api/models/select', { model_id: modelId }, endpoint.api, endpoint.viaNkn, endpoint.relay, 45000, { service: 'depth_any', useServiceTarget: true });
        state.currentModel = modelId;
        updateStatus(nodeId, `Loading ${modelId}…`, 'pending');
        try {
          await Net.postJSON(baseUrl, '/api/load_model', {}, endpoint.api, endpoint.viaNkn, endpoint.relay, 45000, { service: 'depth_any', useServiceTarget: true });
        } catch (err) {
          const message = err?.message || '';
          if (/already loading/i.test(message)) {
            const ready = await pollModelStatus(nodeId, { endpoint });
            if (ready) {
              state.currentModel = modelId;
              updateStatus(nodeId, `Model ready: ${modelId}`, 'ok');
              return true;
            }
          }
          throw err;
        }

        const ready = await pollModelStatus(nodeId, { endpoint });
        if (!ready) throw new Error('Model did not become ready');
        updateStatus(nodeId, `Model ready: ${modelId}`, 'ok');

        return true;
      } catch (err) {
        if (setBadge) setBadge(`Model load failed: ${err.message}`, false);
        updateStatus(nodeId, `Model failed: ${modelId}`, 'err');
        state.modelReady = false;
        return false;
      } finally {
        state.modelLoading = false;
        state.modelLoadPromise = null;
      }
    };

    state.modelLoadPromise = doLoad();
    return state.modelLoadPromise;
  }

  async function pollModelStatus(nodeId, { once = false, endpoint: endpointOverride = null } = {}) {
    const state = ensureState(nodeId);
    if (!state) return false;

    const endpoint = endpointOverride || resolveEndpointConfig(nodeId);
    const baseUrl = endpoint.base;

    return new Promise((resolve) => {
      const checkStatus = async () => {
        try {
          const data = await Net.getJSON(baseUrl, '/api/model_status', endpoint.api, endpoint.viaNkn, endpoint.relay, { service: 'depth_any', useServiceTarget: true });
          const status = data.status;
          const progress = data.progress || 0;
          if (status === 'ready') {
            state.modelReady = true;
            state.currentModel = data.model || state.currentModel;
            updateStatus(nodeId, `Model ready: ${state.currentModel || 'loaded'}`, 'ok');
            resolve(true);
            return;
          }
          if (status === 'loading') {
            state.modelReady = false;
            updateStatus(nodeId, `Loading model… ${progress}%`, 'pending');
            if (once) return resolve(false);
            return setTimeout(checkStatus, 1000);
          }
          state.modelReady = false;
          updateStatus(nodeId, 'Model not loaded', 'err');
          resolve(false);
        } catch (err) {
          resolve(false);
        }
      };
      checkStatus();
    });
  }

  async function ensureModelReady(nodeId) {
    const state = ensureState(nodeId);
    if (!state) return false;
    const endpoint = resolveEndpointConfig(nodeId);
    const ready = await pollModelStatus(nodeId, { once: true, endpoint });
    if (ready) return true;
    const statusCheck = await pollModelStatus(nodeId, { once: true, endpoint });
    if (statusCheck) return true;
    if (state.currentModel) {
      const loaded = await loadModel(nodeId, state.currentModel);
      if (loaded) return true;
    } else {
      const models = await fetchModels(nodeId);
      const pick = models?.[0]?.id;
      if (pick) {
        const loaded = await loadModel(nodeId, pick);
        if (loaded) return true;
      }
    }
    // If still not ready, try polling for a short period
    const maxAttempts = 20;
    for (let i = 0; i < maxAttempts; i++) {
      const ok = await pollModelStatus(nodeId, { once: true, endpoint });
      if (ok) return true;
      await new Promise((r) => setTimeout(r, 1000));
    }
    return false;
  }

  async function processImage(nodeId, imageData) {
    const state = ensureState(nodeId);
    if (!state) return null;

    const cfg = getConfig(nodeId);
    const endpoint = resolveEndpointConfig(nodeId);
    const baseUrl = endpoint.base;
    const apiKey = cfg.api || endpoint.api || '';
    const useRelay = endpoint.viaNkn && endpoint.relay;

    if (state.processing) {
      if (setBadge) setBadge('Already processing', false);
      return null;
    }

    if (!state.modelReady) {
      const cfg = getConfig(nodeId);
      if (String(cfg.autoLoadModel ?? 'true') !== 'false') {
        if (!state.availableModels.length) await fetchModels(nodeId);
        await maybeAutoLoadModel(nodeId);
        if (!state.modelReady) await ensureModelReady(nodeId);
      }
      if (!state.modelReady) {
        if (setBadge) setBadge('Model not loaded', false);
        updateStatus(nodeId, 'Model not loaded', 'err');
        return null;
      }
    }

    try {
      state.processing = true;
      updateStatus(nodeId, 'Processing image…', 'pending');
      updateUploadProgress(nodeId, '');

      const normalizeImage = async (val, mimeHint = 'image/png') => {
        if (val instanceof Blob) return val;
        if (val instanceof ArrayBuffer) return new Blob([val], { type: 'image/png' });
        if (typeof val === 'string') {
          if (val.startsWith('data:')) {
            return await (await fetch(val)).blob();
          }
          try {
            const byteString = atob(val);
            const bytes = new Uint8Array(byteString.length);
            for (let i = 0; i < byteString.length; i++) bytes[i] = byteString.charCodeAt(i);
            return new Blob([bytes], { type: mimeHint || 'image/png' });
          } catch (_) {
            return null;
          }
        }
        if (val && typeof val === 'object') {
          if (val.dataUrl) return await normalizeImage(val.dataUrl, val.mime || mimeHint);
          if (val.b64 || val.image || val.data) return await normalizeImage(val.b64 || val.image || val.data, val.mime || mimeHint);
          if (val.file instanceof Blob) return val.file;
        }
        return null;
      };

      const mimeGuess = (imageData && typeof imageData === 'object' && imageData.mime) ? imageData.mime : 'image/png';
      const blob = await normalizeImage(imageData, mimeGuess);
      if (!blob) {
        updateStatus(nodeId, 'Invalid image data', 'err');
        state.processing = false;
        return null;
      }

      const filename = imageData?.name || 'image.png';
      const mime = (typeof imageData === 'object' && imageData?.type) ? imageData.type : (mimeGuess || 'image/png');
      const params = {
        resolution: cfg.resolution,
        max_points: cfg.maxPoints,
        align_to_input_ext_scale: cfg.alignToInputScale,
        include_confidence: cfg.includeConfidence,
        apply_confidence_filter: cfg.applyConfidenceFilter,
        process_res_method: cfg.processResMethod || 'upper_bound_resize',
        infer_gs: cfg.inferGs ?? 'false',
        conf_thresh_percentile: cfg.confThreshPercentile ?? 40,
        show_cameras: cfg.showCameras ?? 'true',
        feat_vis_fps: cfg.featVisFps ?? 15
      };

      let result = null;
      if (useRelay) {
        let dataUrl = null;
        if (typeof imageData === 'string') {
          dataUrl = imageData.startsWith('data:') ? imageData : `data:${mime};base64,${imageData}`;
        } else if (imageData && typeof imageData === 'object') {
          if (imageData.dataUrl) dataUrl = imageData.dataUrl;
          else if (imageData.b64 || imageData.image || imageData.data) {
            const b64 = imageData.b64 || imageData.image || imageData.data;
            dataUrl = `data:${imageData.mime || mime};base64,${b64}`;
          }
        }
        if (!dataUrl) {
          try {
            dataUrl = await blobToDataUrl(blob);
          } catch (_) {
            dataUrl = null;
          }
        }
        if (!dataUrl) {
          updateStatus(nodeId, 'Unable to encode image', 'err');
          if (setBadge) setBadge('Unable to encode image for relay', false);
          return null;
        }
        const b64Payload = dataUrl.includes(',') ? dataUrl.split(',')[1] : dataUrl;
        const body = { dataUrl, b64: b64Payload, filename, mime };
        Object.entries(params).forEach(([k, v]) => {
          if (v !== undefined && v !== null && v !== '') body[k] = v;
        });
        // If payload is large, chunk the JSON to stay within DM limits
        const useChunked = b64Payload && b64Payload.length > 60000;
        const progressCb = (seq, total, phase) => {
          const label = phase === 'end' ? 'finishing' : 'uploading';
          updateUploadProgress(nodeId, `Relay ${label} ${seq}/${total}`);
        };
        if (useChunked && Net.postJSONChunked) {
          // Use conservative chunk size to stay under relay DM limits
          result = await Net.postJSONChunked(baseUrl, '/api/process_base64', body, apiKey, true, endpoint.relay, 180000, 16000, progressCb, { service: 'depth_any', useServiceTarget: true });
        } else {
          updateUploadProgress(nodeId, 'Sending request…');
          result = await Net.postJSON(baseUrl, '/api/process_base64', body, apiKey, true, endpoint.relay, 180000, { service: 'depth_any', useServiceTarget: true });
        }
      } else {
        const formData = new FormData();
        formData.append('file', blob, filename);
        Object.entries(params).forEach(([key, val]) => {
          if (val !== undefined && val !== null && val !== '') {
            formData.append(key, val);
          }
        });

        const response = await fetch(`${baseUrl}/api/process`, {
          method: 'POST',
          body: formData,
          headers: apiKey ? { Authorization: apiKey } : undefined
        });

        if (!response.ok) {
          let reason = `HTTP ${response.status}`;
          try {
            const errJson = await response.json();
            reason = errJson?.error || errJson?.message || reason;
          } catch (_) {
            try { reason = await response.text(); } catch (_) { /* ignore */ }
          }
          throw new Error(reason);
        }

        result = await response.json();
      }

      // Basic error/HTML guard
      if (result && typeof result.status === 'number' && result.status >= 400) {
        const bodyText = typeof result.body === 'string' ? result.body.slice(0, 180) : '';
        throw new Error(`Backend error ${result.status}: ${bodyText || result.error || 'unknown'}`);
      }
      if (result && typeof result.body === 'string' && result.body.toLowerCase().includes('<!doctype html')) {
        throw new Error('Backend returned HTML (likely 404) – check endpoint/relay config');
      }

      const directPc = result.pointcloud;
      if (result.status === 'completed' && directPc) {
        state.currentPointcloud = directPc;
        updatePointcloudViewer(nodeId, directPc);
        updateStatus(nodeId, `Pointcloud ready (${state.currentModel || 'model'})`, 'ok');
        updateUploadProgress(nodeId, '');
        return directPc;
      } else if (result.job_id) {
        state.lastJobId = result.job_id;
        const pointcloud = await pollJobStatus(nodeId, result.job_id, endpoint);
        if (pointcloud) {
          updatePointcloudViewer(nodeId, pointcloud);
          updateStatus(nodeId, `Pointcloud ready (${state.currentModel || 'model'})`, 'ok');
          updateUploadProgress(nodeId, '');
        }
        return pointcloud;
      } else {
        throw new Error(`Unexpected response format: ${JSON.stringify(result).slice(0, 120)}`);
      }
    } catch (err) {
      if (setBadge) setBadge(`Processing failed: ${err?.message || err}`, false);
      updateStatus(nodeId, 'Processing failed', 'err');
      updateUploadProgress(nodeId, `Upload failed: ${err?.message || err}`);
      return null;
    } finally {
      state.processing = false;
    }
  }

  async function pollJobStatus(nodeId, jobId, endpointOverride = null) {
    const state = ensureState(nodeId);
    if (!state) return null;

    const endpoint = endpointOverride || resolveEndpointConfig(nodeId);
    const baseUrl = endpoint.base;

    return new Promise((resolve) => {
      const checkJob = async () => {
        try {
          const data = await Net.getJSON(baseUrl, `/api/job/${jobId}`, endpoint.api, endpoint.viaNkn, endpoint.relay, { service: 'depth_any', useServiceTarget: true });

          const pointcloud = data.pointcloud || data.result?.pointcloud;
          if (data.status === 'completed' && pointcloud) {
            state.currentPointcloud = pointcloud;
            if (setBadge) setBadge('Processing complete', true);
            updateStatus(nodeId, `Pointcloud ready (${state.currentModel || 'model'})`, 'ok');
            resolve(pointcloud);
          } else if (data.status === 'processing') {
            if (setBadge) setBadge(`Processing... ${data.progress || 0}%`, true);
            updateStatus(nodeId, `Processing… ${data.progress || 0}%`, 'pending');
            setTimeout(checkJob, 500);
          } else if (data.status === 'failed' || data.status === 'error') {
            if (setBadge) setBadge(`Processing failed: ${data.error || 'Unknown error'}`, false);
            updateStatus(nodeId, 'Processing failed', 'err');
            resolve(null);
          } else {
            setTimeout(checkJob, 500);
          }
        } catch (err) {
          resolve(null);
        }
      };

      checkJob();
    });
  }

  async function exportGLB(nodeId) {
    const state = ensureState(nodeId);
    if (!state || !state.currentPointcloud) return null;

    const endpoint = resolveEndpointConfig(nodeId);
    const baseUrl = endpoint.base;

    try {
      const fullUrl = `${baseUrl}/api/export/glb`;
      const blob = await Net.fetchBlob(fullUrl, endpoint.viaNkn, endpoint.relay, endpoint.api, { service: 'depth_any', useServiceTarget: true });
      const url = URL.createObjectURL(blob);

      const a = document.createElement('a');
      a.href = url;
      a.download = 'pointcloud.glb';
      a.click();
      URL.revokeObjectURL(url);

      return blob;
    } catch (err) {
      if (setBadge) setBadge(`GLB export failed: ${err.message}`, false);
      return null;
    }
  }

  async function alignFloorAuto(nodeId) {
    const state = ensureState(nodeId);
    if (!state || !state.currentPointcloud) return false;

    const endpoint = resolveEndpointConfig(nodeId);
    const baseUrl = endpoint.base;

    try {
      const data = await Net.postJSON(baseUrl, '/api/floor_align', {}, endpoint.api, endpoint.viaNkn, endpoint.relay, 45000, { service: 'depth_any', useServiceTarget: true });
      if (data.vertices) {
        state.currentPointcloud.vertices = data.vertices;
        updatePointcloudViewer(nodeId, state.currentPointcloud);
        if (setBadge) setBadge('Floor aligned', true);
        return true;
      }
      return false;
    } catch (err) {
      if (setBadge) setBadge(`Floor align failed: ${err.message}`, false);
      return false;
    }
  }

  async function alignFloorManual(nodeId, points) {
    const state = ensureState(nodeId);
    if (!state || !state.currentPointcloud) return false;

    const endpoint = resolveEndpointConfig(nodeId);
    const baseUrl = endpoint.base;

    try {
      const data = await Net.postJSON(baseUrl, '/api/floor_align/manual', { points }, endpoint.api, endpoint.viaNkn, endpoint.relay, 45000, { service: 'depth_any', useServiceTarget: true });
      if (data.vertices) {
        state.currentPointcloud.vertices = data.vertices;
        updatePointcloudViewer(nodeId, state.currentPointcloud);
        if (setBadge) setBadge('Floor aligned (manual)', true);
        return true;
      }
      return false;
    } catch (err) {
      if (setBadge) setBadge(`Manual floor align failed: ${err.message}`, false);
      return false;
    }
  }

  function chunkPointcloudData(pointcloud, maxChunkSize = MAX_CHUNK_SIZE) {
    if (!pointcloud) return [];

    const serialized = JSON.stringify(pointcloud);
    if (serialized.length <= maxChunkSize) {
      return [{ chunk: 0, total: 1, data: pointcloud }];
    }

    const verticesPerChunk = Math.floor(maxChunkSize / 100);
    const totalVertices = pointcloud.vertices.length;
    const numChunks = Math.ceil(totalVertices / verticesPerChunk);

    const chunks = [];
    for (let i = 0; i < numChunks; i++) {
      const start = i * verticesPerChunk;
      const end = Math.min(start + verticesPerChunk, totalVertices);

      chunks.push({
        chunk: i,
        total: numChunks,
        data: {
          vertices: pointcloud.vertices.slice(start, end),
          colors: pointcloud.colors ? pointcloud.colors.slice(start, end) : null,
          metadata: i === 0 ? pointcloud.metadata : null
        }
      });
    }

    return chunks;
  }

  function receivePointcloudChunk(nodeId, chunkData) {
    const state = ensureState(nodeId);
    if (!state) return null;

    const { chunk, total, data } = chunkData;

    if (!state.receivedChunks.has('current')) {
      state.receivedChunks.set('current', {
        chunks: new Array(total),
        received: 0
      });
    }

    const assembly = state.receivedChunks.get('current');
    assembly.chunks[chunk] = data;
    assembly.received++;

    if (assembly.received === total) {
      const metadata = assembly.chunks[0].metadata;
      const vertices = [];
      const colors = [];

      for (const chunkData of assembly.chunks) {
        vertices.push(...chunkData.vertices);
        if (chunkData.colors) {
          colors.push(...chunkData.colors);
        }
      }

      const pointcloud = {
        vertices,
        colors: colors.length > 0 ? colors : null,
        metadata
      };

      state.currentPointcloud = pointcloud;
      state.receivedChunks.delete('current');
      updatePointcloudViewer(nodeId, pointcloud);

      return pointcloud;
    }

    return null;
  }

  // ============================================================================
  // THREE.JS VIEWER FUNCTIONS
  // ============================================================================

  function createSectorGrid(distance = 10, center = new THREE.Vector3()) {
    // Match the reference: tiny line bundles with per-vertex alpha
    const gridDistance = distance;
    const lineLength = 0.4;
    const span = gridDistance * 2 + 1;
    const totalVerts = span * span * span * 3 * 2; // 3 axes, 2 verts each
    const positions = new Float32Array(totalVerts * 3);
    const alphas = new Float32Array(totalVerts);
    let p = 0;
    let a = 0;
    const half = lineLength / 2;
    for (let x = -gridDistance; x <= gridDistance; x++) {
      for (let y = -gridDistance; y <= gridDistance; y++) {
        for (let z = -gridDistance; z <= gridDistance; z++) {
          // X axis segment
          positions[p++] = x - half; positions[p++] = y; positions[p++] = z;
          positions[p++] = x + half; positions[p++] = y; positions[p++] = z;
          alphas[a++] = 1; alphas[a++] = 1;

          // Y axis segment
          positions[p++] = x; positions[p++] = y - half; positions[p++] = z;
          positions[p++] = x; positions[p++] = y + half; positions[p++] = z;
          alphas[a++] = 1; alphas[a++] = 1;

          // Z axis segment
          positions[p++] = x; positions[p++] = y; positions[p++] = z - half;
          positions[p++] = x; positions[p++] = y; positions[p++] = z + half;
          alphas[a++] = 1; alphas[a++] = 1;
        }
      }
    }

    const geometry = new THREE.BufferGeometry();
    geometry.setAttribute('position', new THREE.BufferAttribute(positions, 3));
    geometry.setAttribute('alpha', new THREE.BufferAttribute(alphas, 1));
    geometry.computeBoundingSphere();

    const material = new THREE.ShaderMaterial({
      transparent: true,
      depthWrite: false,
      uniforms: {
        uColor: { value: new THREE.Color(0x666666) },
        uOpacity: { value: 0.35 }
      },
      vertexShader: `
        attribute float alpha;
        varying float vAlpha;
        void main() {
          vAlpha = alpha;
          gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
        }
      `,
      fragmentShader: `
        uniform vec3 uColor;
        uniform float uOpacity;
        varying float vAlpha;
        void main() {
          gl_FragColor = vec4(uColor, uOpacity * vAlpha);
        }
      `
    });

    const grid = new THREE.LineSegments(geometry, material);
    grid.position.copy(center);
    return grid;
  }

  function createAxisIndicator(length = 1) {
    const group = new THREE.Group();
    const coneGeom = new THREE.ConeGeometry(0.02, 0.08, 8);
    const makeAxis = (dir, color) => {
      const points = [
        new THREE.Vector3(0, 0, 0),
        new THREE.Vector3().addScaledVector(dir, length)
      ];
      const geom = new THREE.BufferGeometry().setFromPoints(points);
      const mat = new THREE.LineBasicMaterial({ color, linewidth: 3 });
      const line = new THREE.Line(geom, mat);
      const cone = new THREE.Mesh(coneGeom, new THREE.MeshBasicMaterial({ color }));
      cone.position.copy(points[1]);
      cone.lookAt(points[0]);
      group.add(line);
      group.add(cone);
    };
    makeAxis(new THREE.Vector3(1, 0, 0), 0xff0000);
    makeAxis(new THREE.Vector3(0, 1, 0), 0x00ff00);
    makeAxis(new THREE.Vector3(0, 0, 1), 0x0000ff);
    return group;
  }

  function createGroundPlane(size = 400, color = 0x444444) {
    const planeGeom = new THREE.PlaneGeometry(size, size, 1, 1);
    const planeMat = new THREE.MeshBasicMaterial({
      color,
      transparent: true,
      opacity: 0.12,
      side: THREE.DoubleSide
    });
    const plane = new THREE.Mesh(planeGeom, planeMat);
    plane.rotation.x = -Math.PI / 2;
    plane.position.y = -0.01;
    return plane;
  }

  function initViewer(nodeId, container) {
    if (!container) return;

    const cfg = getConfig(nodeId);
    const gridDistance = parseInt(cfg.gridDistance) || 10;

    // Create scene
    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0x000000);
    scene.fog = new THREE.Fog(0x000000, 10, 100);

    // Create camera
    const camera = new THREE.PerspectiveCamera(
      75,
      container.clientWidth / container.clientHeight,
      0.1,
      1000
    );
    camera.position.set(0, 1.6, 6);
    camera.lookAt(new THREE.Vector3(0, 1.6, -1));

    // Create renderer
    const renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setSize(container.clientWidth, container.clientHeight);
    renderer.setPixelRatio(window.devicePixelRatio);
    container.appendChild(renderer.domElement);

    // Add lights
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.6);
    scene.add(ambientLight);

    const directionalLight = new THREE.DirectionalLight(0xffffff, 0.8);
    directionalLight.position.set(10, 10, 10);
    scene.add(directionalLight);

    // Create controls
    const orbitControls = new OrbitControls(camera, renderer.domElement);
    orbitControls.enableDamping = true;
    orbitControls.dampingFactor = 0.05;
    orbitControls.enabled = true;
    orbitControls.target.set(0, 1.6, 0);

    const flyControls = new FlyControls(camera, renderer.domElement);
    flyControls.movementSpeed = 5;
    flyControls.rollSpeed = Math.PI / 8;
    flyControls.dragToLook = true;
    flyControls.enabled = false;

    const pointerLockControls = new PointerLockControls(camera, renderer.domElement);
    scene.add(pointerLockControls.getObject());

    // Create grid
    const grid = createSectorGrid(gridDistance);
    scene.add(grid);

    // Ground plane
    const ground = createGroundPlane(gridDistance * 40);
    scene.add(ground);

    // Store viewer state
    const viewerState = {
      scene,
      camera,
      renderer,
      controls: {
        orbit: orbitControls,
        fly: flyControls,
        fps: pointerLockControls
      },
      currentMode: 'orbit',
      points: null,
      grid,
      ground,
      animationId: null,
      container,
      clock: new THREE.Clock()
    };

    VIEWER_STATE.set(nodeId, viewerState);

    // Start animation loop
    animate(nodeId);

    return viewerState;
  }

  function destroyViewer(nodeId) {
    const viewerState = VIEWER_STATE.get(nodeId);
    if (!viewerState) return;

    if (viewerState.animationId) {
      cancelAnimationFrame(viewerState.animationId);
    }

    if (viewerState.points) {
      viewerState.scene.remove(viewerState.points);
      viewerState.points.geometry.dispose();
      viewerState.points.material.dispose();
    }

    if (viewerState.grid) {
      viewerState.scene.remove(viewerState.grid);
      viewerState.grid.geometry.dispose();
      viewerState.grid.material.dispose();
    }
    if (viewerState.ground) {
      viewerState.scene.remove(viewerState.ground);
      viewerState.ground.geometry.dispose();
      viewerState.ground.material.dispose();
    }

    if (viewerState.renderer) {
      viewerState.renderer.dispose();
      if (viewerState.container && viewerState.renderer.domElement) {
        viewerState.container.removeChild(viewerState.renderer.domElement);
      }
    }

    VIEWER_STATE.delete(nodeId);
  }

  function animate(nodeId) {
    const viewerState = VIEWER_STATE.get(nodeId);
    if (!viewerState) return;

    viewerState.animationId = requestAnimationFrame(() => animate(nodeId));

    const delta = viewerState.clock.getDelta();

    if (viewerState.currentMode === 'orbit') {
      viewerState.controls.orbit.update();
    } else if (viewerState.currentMode === 'fly') {
      viewerState.controls.fly.update(delta);
    }

    viewerState.renderer.render(viewerState.scene, viewerState.camera);
  }

  function updatePointcloudViewer(nodeId, pointcloudData) {
    const viewerState = VIEWER_STATE.get(nodeId);
    if (!viewerState || !pointcloudData) return;

    const cfg = getConfig(nodeId);
    const pointSize = Number(cfg.pointSize) || 0.05;

    // Remove existing points
    if (viewerState.points) {
      viewerState.scene.remove(viewerState.points);
      viewerState.points.geometry.dispose();
      viewerState.points.material.dispose();
    }

    const vertices = pointcloudData.vertices;
    const colors = pointcloudData.colors;

    if (!vertices || vertices.length === 0) return;

    // Create geometry
    const geometry = new THREE.BufferGeometry();

    // Convert vertices array to Float32Array
    const positions = new Float32Array(vertices.flat());
    geometry.setAttribute('position', new THREE.BufferAttribute(positions, 3));

    // Add colors if available
    if (colors && colors.length > 0) {
      const colorArray = new Float32Array(colors.flat().map(c => c / 255));
      geometry.setAttribute('color', new THREE.BufferAttribute(colorArray, 3));
    }

    // Create material
    const material = new THREE.PointsMaterial({
      size: pointSize,
      vertexColors: colors && colors.length > 0,
      sizeAttenuation: true
    });

    // Create points mesh
    viewerState.points = new THREE.Points(geometry, material);
    viewerState.scene.add(viewerState.points);

    // Fit camera to pointcloud
    fitCameraToPoints(viewerState.camera, viewerState.controls.orbit, geometry);
  }

  function fitCameraToPoints(camera, controls, geometry) {
    geometry.computeBoundingSphere();
    const boundingSphere = geometry.boundingSphere;

    if (boundingSphere) {
      const center = boundingSphere.center;
      const radius = boundingSphere.radius;

      const distance = radius / Math.tan((camera.fov / 2) * (Math.PI / 180));
      camera.position.set(
        center.x,
        center.y + distance * 0.5,
        center.z + distance * 1.5
      );
      camera.lookAt(center);

      if (controls) {
        controls.target.copy(center);
        controls.update();
      }
    }
  }

  function setCameraMode(nodeId, mode) {
    const viewerState = VIEWER_STATE.get(nodeId);
    if (!viewerState || !CAMERA_MODES.includes(mode)) return;

    // Disable all controls
    viewerState.controls.orbit.enabled = false;
    viewerState.controls.fly.enabled = false;
    if (viewerState.controls.fps.isLocked) {
      viewerState.controls.fps.unlock();
    }

    // Enable selected control
    if (mode === 'orbit') {
      viewerState.controls.orbit.enabled = true;
    } else if (mode === 'fly') {
      viewerState.controls.fly.enabled = true;
    } else if (mode === 'fps') {
      // FPS mode requires click to lock
      viewerState.renderer.domElement.addEventListener('click', () => {
        viewerState.controls.fps.lock();
      }, { once: true });
    }

    viewerState.currentMode = mode;
    updateCameraModeButton(nodeId, mode);
  }

  function cycleCameraMode(nodeId) {
    const viewerState = VIEWER_STATE.get(nodeId);
    if (!viewerState) return;

    const currentIndex = CAMERA_MODES.indexOf(viewerState.currentMode);
    const nextIndex = (currentIndex + 1) % CAMERA_MODES.length;
    setCameraMode(nodeId, CAMERA_MODES[nextIndex]);
  }

  function resetCamera(nodeId) {
    const viewerState = VIEWER_STATE.get(nodeId);
    if (!viewerState) return;

    viewerState.camera.position.set(0, 5, 10);
    viewerState.camera.lookAt(0, 0, 0);

    if (viewerState.controls.orbit) {
      viewerState.controls.orbit.target.set(0, 0, 0);
      viewerState.controls.orbit.update();
    }
  }

  function updateCameraModeButton(nodeId, mode) {
    const btn = document.querySelector(`[data-node-id="${nodeId}"] .camera-mode-btn`);
    const text = document.querySelector(`[data-node-id="${nodeId}"] .camera-mode-text`);
    if (btn && text) {
      text.textContent = `${CAMERA_MODE_LABELS[mode]} Mode`;
    }
  }

  function toggleFloorSelection(nodeId) {
    const state = ensureState(nodeId);
    if (!state) return;

    state.floorSelectionActive = !state.floorSelectionActive;
    state.floorPoints = [];

    const btn = document.querySelector(`[data-node-id="${nodeId}"] .floor-select-btn`);
    if (btn) {
      btn.classList.toggle('active', state.floorSelectionActive);
    }

    if (state.floorSelectionActive) {
      if (setBadge) setBadge('Click 3 points on floor', true);
    } else {
      if (setBadge) setBadge('Floor selection cancelled', false);
    }
  }

  function onViewerClick(nodeId, event) {
    const state = ensureState(nodeId);
    const viewerState = VIEWER_STATE.get(nodeId);
    if (!state || !viewerState || !state.floorSelectionActive) return;

    const raycaster = new THREE.Raycaster();
    const mouse = new THREE.Vector2();

    const rect = viewerState.renderer.domElement.getBoundingClientRect();
    mouse.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    mouse.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;

    raycaster.setFromCamera(mouse, viewerState.camera);

    if (viewerState.points) {
      const intersects = raycaster.intersectObject(viewerState.points);
      if (intersects.length > 0) {
        const point = intersects[0].point;
        state.floorPoints.push([point.x, point.y, point.z]);

        if (setBadge) setBadge(`Point ${state.floorPoints.length}/3 selected`, true);

        if (state.floorPoints.length === 3) {
          alignFloorManual(nodeId, state.floorPoints);
          state.floorSelectionActive = false;
          state.floorPoints = [];

          const btn = document.querySelector(`[data-node-id="${nodeId}"] .floor-select-btn`);
          if (btn) btn.classList.remove('active');
        }
      }
    }
  }

  function handleResize(nodeId) {
    const viewerState = VIEWER_STATE.get(nodeId);
    if (!viewerState || !viewerState.container) return;

    const width = viewerState.container.clientWidth;
    const height = viewerState.container.clientHeight;

    viewerState.camera.aspect = width / height;
    viewerState.camera.updateProjectionMatrix();
    viewerState.renderer.setSize(width, height);
  }

  // ============================================================================
  // ROUTER MESSAGE HANDLERS
  // ============================================================================

  Router?.on?.('pointcloud.fetchModels', async (msg) => {
    const nodeId = msg?.nodeId;
    if (!nodeId) return;

    const models = await fetchModels(nodeId);
    Router.sendFrom(nodeId, 'models', { models });
  });

  Router?.on?.('pointcloud.loadModel', async (msg) => {
    const nodeId = msg?.nodeId;
    const modelId = msg?.modelId;
    if (!nodeId || !modelId) return;

    const success = await loadModel(nodeId, modelId);
    if (success) {
      Router.sendFrom(nodeId, 'modelLoaded', { modelId });
    }
  });

  Router?.on?.('pointcloud.process', async (msg) => {
    const nodeId = msg?.nodeId;
    const imageData = msg?.image || msg?.b64 || msg?.dataUrl;
    if (!nodeId || !imageData) return;

    const pointcloud = await processImage(nodeId, imageData);
    if (pointcloud) {
      const chunks = chunkPointcloudData(pointcloud);
      for (const chunk of chunks) {
        Router.sendFrom(nodeId, 'pointcloud', chunk);
      }
    }
  });

  Router?.on?.('pointcloud.exportGLB', async (msg) => {
    const nodeId = msg?.nodeId;
    if (!nodeId) return;

    await exportGLB(nodeId);
  });

  Router?.on?.('pointcloud.initViewer', (msg) => {
    const { nodeId, container } = msg;
    if (!nodeId || !container) return;

    initViewer(nodeId, container);
  });

  Router?.on?.('pointcloud.destroyViewer', (msg) => {
    const { nodeId } = msg;
    if (!nodeId) return;

    destroyViewer(nodeId);
  });

  Router?.on?.('pointcloud.cycleCameraMode', (msg) => {
    const { nodeId } = msg;
    if (!nodeId) return;

    cycleCameraMode(nodeId);
  });

  Router?.on?.('pointcloud.resetCamera', (msg) => {
    const { nodeId } = msg;
    if (!nodeId) return;

    resetCamera(nodeId);
  });

  Router?.on?.('pointcloud.toggleFloorSelection', (msg) => {
    const { nodeId } = msg;
    if (!nodeId) return;

    toggleFloorSelection(nodeId);
  });

  Router?.on?.('pointcloud.alignFloorAuto', async (msg) => {
    const { nodeId } = msg;
    if (!nodeId) return;

    await alignFloorAuto(nodeId);
  });

  // ============================================================================
  // INITIALIZATION
  // ============================================================================

  /**
   * Initialize a Pointcloud node - fetch available models and set up state
   * Called automatically when node is created
   */
  async function init(nodeId) {
    const state = ensureState(nodeId);
    if (!state) return;

    updateStatus(nodeId, 'Loading models…', 'pending');
    // Fetch available models from the API
    const models = await fetchModels(nodeId);

    // Output models list to the models port
    if (models && models.length > 0) {
      Router.send({
        type: 'Pointcloud',
        from: nodeId,
        port: 'models',
        data: { models }
      });

      // Also send initial status
      Router.send({
        type: 'Pointcloud',
        from: nodeId,
        port: 'status',
        data: {
          status: 'ready',
          currentModel: state.currentModel,
          modelReady: state.modelReady,
          modelsAvailable: models.length
        }
      });

      // Update UI models list
      updateModelsListUI(nodeId, models);

      if (log) {
        log(`[Pointcloud:${nodeId}] Loaded ${models.length} models. Current: ${state.currentModel || 'none'}`);
      }
    } else {
      updateStatus(nodeId, 'No models available', 'err');
    }

    return state;
  }

  /**
   * Update the models list UI in the node card
   */
  function updateModelsListUI(nodeId, models) {
    const container = document.querySelector(`.pointcloud-models-list[data-node-id="${nodeId}"]`);
    if (!container || !models || models.length === 0) return;

    const cfg = getConfig(nodeId);
    const currentModelId = cfg.defaultModel || models.find(m => m.current)?.id;

    let html = '<div style="font-weight: bold; margin-bottom: 6px; color: #fff;">Available Models:</div>';

    models.forEach(model => {
      const isCurrent = model.id === currentModelId;
      const isDownloaded = model.downloaded;
      const statusIcon = isDownloaded ? '✓' : '○';
      const statusColor = isDownloaded ? '#4CAF50' : '#888';
      const currentBadge = isCurrent ? ' <span style="background: #2196F3; padding: 2px 6px; border-radius: 3px; font-size: 9px;">SELECTED</span>' : '';

      html += `
        <div style="
          padding: 6px 8px;
          margin: 4px 0;
          background: ${isCurrent ? '#2a2a3a' : '#222'};
          border-left: 3px solid ${isCurrent ? '#2196F3' : '#444'};
          border-radius: 3px;
          cursor: pointer;
          transition: background 0.2s;
        "
        onclick="if(Pointcloud?.selectModel) Pointcloud.selectModel('${nodeId}', '${model.id}')"
        onmouseover="this.style.background='#333'"
        onmouseout="this.style.background='${isCurrent ? '#2a2a3a' : '#222'}'">
          <div style="display: flex; align-items: center; gap: 8px;">
            <span style="color: ${statusColor}; font-size: 14px;">${statusIcon}</span>
            <div style="flex: 1;">
              <div style="color: #fff; font-weight: 500;">${model.name}${currentBadge}</div>
              <div style="color: #aaa; font-size: 10px; margin-top: 2px;">${model.description}</div>
              <div style="color: #777; font-size: 9px; margin-top: 2px;">
                Quality: ${model.quality} | Speed: ${model.speed} | Size: ${model.size}
              </div>
            </div>
          </div>
        </div>
      `;
    });

    container.innerHTML = html;
  }

  /**
   * Refresh models list - called when config changes (base URL or relay)
   */
  async function refreshModels(nodeId) {
    return await init(nodeId);
  }

  /**
   * Select and load a model
   */
  async function selectModel(nodeId, modelId) {
    const state = ensureState(nodeId);
    if (!state) return;
    if (state.currentModel === modelId && state.modelReady) return;

    // Update config with selected model
    const rec = NodeStore.ensure(nodeId, 'Pointcloud');
    if (rec?.config) {
      rec.config.defaultModel = modelId;
    }

    // Load the model
    await loadModel(nodeId, modelId);

    // Refresh the UI to show new selection
    updateModelsListUI(nodeId, state.availableModels);

    if (log) {
      log(`[Pointcloud:${nodeId}] Selected model: ${modelId}`);
    }
  }

  // ============================================================================
  // PUBLIC API
  // ============================================================================

  return {
    init,
    refreshModels,
    selectModel,
    updateModelsListUI,
    ensureState,
    fetchModels,
    loadModel,
    processImage,
    exportGLB,
    chunkPointcloudData,
    receivePointcloudChunk,
    alignFloorAuto,
    alignFloorManual,
    initViewer,
    destroyViewer,
    updatePointcloudViewer,
    setCameraMode,
    cycleCameraMode,
    resetCamera,
    toggleFloorSelection,
    onViewerClick,
    handleResize
  };
}

export { createPointcloud };
