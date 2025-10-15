const FACE_DEFAULT_MODEL =
  'https://storage.googleapis.com/mediapipe-models/face_landmarker/face_landmarker/float16/1/face_landmarker.task';
const POSE_DEFAULT_MODEL =
  'https://storage.googleapis.com/mediapipe-models/pose_landmarker/pose_landmarker_lite/float16/1/pose_landmarker_lite.task';
const VISION_WASM_ROOT = 'https://cdn.jsdelivr.net/npm/@mediapipe/tasks-vision@0.10.0/wasm';

function createVision({ getNode, Router, NodeStore, setBadge, log }) {
  const STATE = new Map();
  let visionModulePromise = null;
  let filesetPromise = null;

  const toNumber = (value, fallback = 0) => {
    const num = Number(value);
    return Number.isFinite(num) ? num : fallback;
  };

  const clamp = (value, min, max) => {
    const num = toNumber(value, min);
    return Math.min(Math.max(num, min), max);
  };

  const toBool = (value, def = false) => {
    if (typeof value === 'boolean') return value;
    if (value == null) return def;
    const text = String(value).trim().toLowerCase();
    if (!text) return def;
    if (['true', '1', 'yes', 'on'].includes(text)) return true;
    if (['false', '0', 'no', 'off'].includes(text)) return false;
    return def;
  };

  const nowMs = () => (typeof performance !== 'undefined' && performance.now ? performance.now() : Date.now());

  async function loadVisionModule() {
    if (!visionModulePromise) {
      visionModulePromise = import('https://cdn.jsdelivr.net/npm/@mediapipe/tasks-vision@0.10.0');
    }
    return visionModulePromise;
  }

  async function ensureFileset() {
    if (!filesetPromise) {
      const vision = await loadVisionModule();
      filesetPromise = vision.FilesetResolver.forVisionTasks(VISION_WASM_ROOT);
    }
    return filesetPromise;
  }

  function normalizeFaceConfig(raw = {}) {
    const cfg = typeof raw === 'object' && raw !== null ? raw : {};
    const delegate = String(cfg.delegate || 'GPU').toUpperCase();
    const minDetect = clamp(cfg.minFaceDetectionConfidence, 0, 1) || 0.5;
    const minPresence = clamp(cfg.minFacePresenceConfidence, 0, 1) || 0.5;
    const minTrack = clamp(cfg.minTrackingConfidence, 0, 1) || 0.5;
    return {
      delegate: delegate === 'CPU' ? 'CPU' : 'GPU',
      numFaces: clamp(cfg.numFaces, 1, 4),
      outputBlendshapes: toBool(cfg.outputBlendshapes, true),
      outputWorld: toBool(cfg.outputWorld, false),
      outputHeadPose: toBool(cfg.outputHeadPose, true),
      runningMode: String(cfg.runningMode || 'VIDEO').toUpperCase() === 'IMAGE' ? 'IMAGE' : 'VIDEO',
      modelAssetPath: String(cfg.modelAssetPath || '').trim() || FACE_DEFAULT_MODEL,
      maxFPS: clamp(cfg.maxFPS, 1, 120),
      smoothing: toBool(cfg.smoothing, true),
      minFaceDetectionConfidence: minDetect,
      minFacePresenceConfidence: minPresence,
      minTrackingConfidence: minTrack
    };
  }

  function normalizePoseConfig(raw = {}) {
    const cfg = typeof raw === 'object' && raw !== null ? raw : {};
    const delegate = String(cfg.delegate || 'GPU').toUpperCase();
    return {
      delegate: delegate === 'CPU' ? 'CPU' : 'GPU',
      runningMode: String(cfg.runningMode || 'VIDEO').toUpperCase() === 'IMAGE' ? 'IMAGE' : 'VIDEO',
      modelAssetPath: String(cfg.modelAssetPath || '').trim() || POSE_DEFAULT_MODEL,
      minPoseDetectionConfidence: clamp(cfg.minPoseDetectionConfidence, 0, 1) || 0.5,
      minPoseTrackingConfidence: clamp(cfg.minPoseTrackingConfidence, 0, 1) || 0.5,
      segmentation: toBool(cfg.segmentation, false),
      outputWorld: toBool(cfg.outputWorld, true),
      maxFPS: clamp(cfg.maxFPS, 1, 120),
      smoothing: toBool(cfg.smoothing, true)
    };
  }

  function stateKey(nodeId) {
    return String(nodeId || '').trim();
  }

  function ensureState(nodeId, kind) {
    const key = stateKey(nodeId);
    if (!key) return null;
    if (!STATE.has(key)) {
      STATE.set(key, {
        kind,
        cfg: kind === 'face' ? normalizeFaceConfig() : normalizePoseConfig(),
        instance: null,
        initializing: null,
        lastFrameMs: 0,
        processing: false,
        lastError: '',
        lastBadgeTs: 0,
        cfgApplied: false,
        listeners: new Set()
      });
    }
    const st = STATE.get(key);
    if (st && st.kind !== kind) {
      // Reset if kind changed (should not happen, but stay defensive).
      try {
        st.instance?.close?.();
      } catch (err) {
        // ignore close errors
      }
      STATE.set(key, {
        kind,
        cfg: kind === 'face' ? normalizeFaceConfig() : normalizePoseConfig(),
        instance: null,
        initializing: null,
        lastFrameMs: 0,
        processing: false,
        lastError: '',
        lastBadgeTs: 0,
        cfgApplied: false,
        listeners: new Set()
      });
      return STATE.get(key);
    }
    if (st && !st.listeners) st.listeners = new Set();
    return st;
  }

  function getConfigFromStore(nodeId, type) {
    const rec = NodeStore.ensure(nodeId, type === 'face' ? 'FaceLandmarks' : 'PoseLandmarks');
    const cfg = rec?.config || {};
    return type === 'face' ? normalizeFaceConfig(cfg) : normalizePoseConfig(cfg);
  }

  function reportError(state, message, { once = false } = {}) {
    if (!setBadge || !message) return;
    const trimmed = String(message);
    const now = Date.now();
    if (state.lastError === trimmed && (once || now - state.lastBadgeTs < 2500)) return;
    state.lastError = trimmed;
    state.lastBadgeTs = now;
    setBadge(trimmed, false);
  }

  function clearError(state) {
    state.lastError = '';
  }

  const imageBitmapSupported = typeof createImageBitmap === 'function';

  async function dataUrlToImageData(dataUrl) {
    if (!dataUrl) return null;
    const idx = dataUrl.indexOf(',');
    if (idx >= 0) {
      const header = dataUrl.slice(0, idx);
      const mimeMatch = /data:([^;]+)/i.exec(header);
      const mime = mimeMatch ? mimeMatch[1] : 'image/png';
      const body = dataUrl.slice(idx + 1);
      return base64ToImageData(body, mime);
    }
    try {
      const img = await loadHtmlImage(dataUrl);
      return { kind: 'image', source: img, width: img.naturalWidth || img.width, height: img.naturalHeight || img.height };
    } catch (err) {
      return null;
    }
  }

  async function base64ToImageData(b64, mime) {
    try {
      const byteString = atob(b64);
      const len = byteString.length;
      const bytes = new Uint8Array(len);
      for (let i = 0; i < len; i++) bytes[i] = byteString.charCodeAt(i);
      const blob = new Blob([bytes], { type: mime || 'image/webp' });
      if (imageBitmapSupported) {
        const bitmap = await createImageBitmap(blob);
        return { kind: 'bitmap', source: bitmap, width: bitmap.width, height: bitmap.height, cleanup: () => bitmap.close() };
      }
      const dataUrl = `data:${mime || 'image/webp'};base64,${b64}`;
      const img = await loadHtmlImage(dataUrl);
      return { kind: 'image', source: img, width: img.naturalWidth || img.width, height: img.naturalHeight || img.height };
    } catch (err) {
      return null;
    }
  }

  function loadHtmlImage(src) {
    return new Promise((resolve, reject) => {
      const img = new Image();
      img.onload = () => resolve(img);
      img.onerror = reject;
      img.src = src;
    });
  }

  async function prepareFrame(payload) {
    if (!payload) return null;
    if (payload.video instanceof HTMLVideoElement) {
      const video = payload.video;
      if (video.readyState < HTMLMediaElement.HAVE_CURRENT_DATA) return null;
      const width = video.videoWidth || payload.width || 0;
      const height = video.videoHeight || payload.height || 0;
      if (!width || !height) return null;
      return { kind: 'video', source: video, width, height };
    }
    if (payload.bitmap instanceof ImageBitmap) {
      const bitmap = payload.bitmap;
      return {
        kind: 'bitmap',
        source: bitmap,
        width: bitmap.width || payload.width || 0,
        height: bitmap.height || payload.height || 0,
        cleanup: () => bitmap.close?.()
      };
    }
    if (payload.image instanceof HTMLImageElement) {
      const img = payload.image;
      return {
        kind: 'image',
        source: img,
        width: img.naturalWidth || img.width || payload.width || 0,
        height: img.naturalHeight || img.height || payload.height || 0
      };
    }
    if (typeof payload.dataUrl === 'string') {
      return dataUrlToImageData(payload.dataUrl);
    }
    if (typeof payload.b64 === 'string') {
      return base64ToImageData(payload.b64, payload.mime || payload.contentType || 'image/webp');
    }
    if (payload.canvas instanceof HTMLCanvasElement) {
      const canvas = payload.canvas;
      if (imageBitmapSupported) {
        const bitmap = await createImageBitmap(canvas);
        return { kind: 'bitmap', source: bitmap, width: canvas.width, height: canvas.height, cleanup: () => bitmap.close() };
      }
      return { kind: 'canvas', source: canvas, width: canvas.width, height: canvas.height };
    }
    return null;
  }

  const emitTimestamp = (nodeId, ts) => {
    try {
      Router.sendFrom(nodeId, 'ts', { type: 'timestamp', nodeId, ts });
    } catch (err) {
      // ignore routing errors
    }
  };

  const clampAbs = (value, min, max) => Math.min(Math.max(value, min), max);

  function extractMatrixEntries(matrix) {
    if (!matrix) return null;
    if (Array.isArray(matrix)) return matrix;
    if (matrix.matrix) return extractMatrixEntries(matrix.matrix);
    if (matrix.entries && Array.isArray(matrix.entries)) return Array.from(matrix.entries);
    if (matrix.data && Array.isArray(matrix.data)) return Array.from(matrix.data);
    if (matrix instanceof Float32Array) return Array.from(matrix);
    return null;
  }

  function computeOrientation(entries) {
    if (!Array.isArray(entries) || entries.length < 16) return null;
    const m00 = entries[0];
    const m01 = entries[4];
    const m02 = entries[8];
    const m10 = entries[1];
    const m11 = entries[5];
    const m12 = entries[9];
    const m20 = entries[2];
    const m21 = entries[6];
    const m22 = entries[10];

    let qw, qx, qy, qz;
    const trace = m00 + m11 + m22;
    if (trace > 0) {
      const s = Math.sqrt(trace + 1.0) * 2;
      qw = 0.25 * s;
      qx = (m21 - m12) / s;
      qy = (m02 - m20) / s;
      qz = (m10 - m01) / s;
    } else if (m00 > m11 && m00 > m22) {
      const s = Math.sqrt(1.0 + m00 - m11 - m22) * 2;
      qw = (m21 - m12) / s;
      qx = 0.25 * s;
      qy = (m01 + m10) / s;
      qz = (m02 + m20) / s;
    } else if (m11 > m22) {
      const s = Math.sqrt(1.0 + m11 - m00 - m22) * 2;
      qw = (m02 - m20) / s;
      qx = (m01 + m10) / s;
      qy = 0.25 * s;
      qz = (m12 + m21) / s;
    } else {
      const s = Math.sqrt(1.0 + m22 - m00 - m11) * 2;
      qw = (m10 - m01) / s;
      qx = (m02 + m20) / s;
      qy = (m12 + m21) / s;
      qz = 0.25 * s;
    }

    const norm = Math.hypot(qw, qx, qy, qz) || 1;
    qw /= norm;
    qx /= norm;
    qy /= norm;
    qz /= norm;

    const sinp = clampAbs(2 * (qw * qy - qz * qx), -1, 1);
    const pitch = Math.asin(sinp);
    const roll = Math.atan2(2 * (qw * qx + qy * qz), 1 - 2 * (qx * qx + qy * qy));
    const yaw = Math.atan2(2 * (qw * qz + qx * qy), 1 - 2 * (qy * qy + qz * qz));

    const degrees = {
      pitch: pitch * 180 / Math.PI,
      yaw: yaw * 180 / Math.PI,
      roll: roll * 180 / Math.PI
    };

    const position = {
      x: entries[12] || 0,
      y: entries[13] || 0,
      z: entries[14] || 0
    };

    return {
      quaternion: { w: qw, x: qx, y: qy, z: qz },
      radians: { pitch, yaw, roll },
      degrees,
      position,
      matrix: entries.slice(0, 16)
    };
  }

  function mapLandmarks(list, includeVisibility = false) {
    if (!Array.isArray(list)) return [];
    return list.map((lm) => {
      const out = {
        x: typeof lm.x === 'number' ? lm.x : 0,
        y: typeof lm.y === 'number' ? lm.y : 0
      };
      if (typeof lm.z === 'number') out.z = lm.z;
      if (includeVisibility && typeof lm.visibility === 'number') out.visibility = lm.visibility;
      if (includeVisibility && typeof lm.presence === 'number') out.presence = lm.presence;
      return out;
    });
  }

  function sanitizeBlendshapeCategories(list) {
    if (!Array.isArray(list)) return [];
    return list
      .map((entry) => {
        if (!entry || typeof entry !== 'object') return null;
        return {
          categoryName: typeof entry.categoryName === 'string' ? entry.categoryName : '',
          score: typeof entry.score === 'number' ? entry.score : 0,
          index: typeof entry.index === 'number' ? entry.index : -1,
          displayName: typeof entry.displayName === 'string' ? entry.displayName : ''
        };
      })
      .filter(Boolean);
  }

  function normalizeMatrixEntries(input) {
    if (!input) return null;
    const entries = extractMatrixEntries(input);
    if (!Array.isArray(entries) || entries.length < 16) return null;
    const slice = entries.slice(0, 16);
    return slice instanceof Float32Array ? slice : Float32Array.from(slice);
  }

  function deliverSyntheticFace(nodeId, state) {
    if (!state?.synthetic) return;
    const synthetic = state.synthetic;
    const result = {};
    let hasData = false;

    if (Array.isArray(synthetic.landmarks) && synthetic.landmarks.length) {
      result.faceLandmarks = [
        synthetic.landmarks.map((lm) => {
          const out = { x: lm.x ?? 0, y: lm.y ?? 0 };
          if (lm.z != null) out.z = lm.z;
          if (lm.visibility != null) out.visibility = lm.visibility;
          if (lm.presence != null) out.presence = lm.presence;
          return out;
        })
      ];
      hasData = true;
    }

    if (Array.isArray(synthetic.categories)) {
      result.faceBlendshapes = [
        {
          categories: synthetic.categories.map((cat) => ({
            categoryName: cat.categoryName || '',
            score: typeof cat.score === 'number' ? cat.score : 0,
            index: typeof cat.index === 'number' ? cat.index : -1,
            displayName: cat.displayName || ''
          }))
        }
      ];
      hasData = true;
    }

    const matrices = [];
    if (Array.isArray(synthetic.matrices)) {
      synthetic.matrices.forEach((mat) => {
        if (mat && mat.length >= 16) matrices.push(mat instanceof Float32Array ? mat : Float32Array.from(mat));
      });
    }
    if (synthetic.orientation?.matrix && synthetic.orientation.matrix.length >= 16) {
      const matrix = synthetic.orientation.matrix;
      matrices.push(matrix instanceof Float32Array ? matrix : Float32Array.from(matrix));
    }
    if (matrices.length) {
      result.facialTransformationMatrixes = matrices;
      hasData = true;
    }

    if (!hasData) return;

    const ts = typeof synthetic.ts === 'number' && Number.isFinite(synthetic.ts) ? synthetic.ts : Date.now();
    const frame = {
      width: typeof synthetic.width === 'number' ? synthetic.width : 0,
      height: typeof synthetic.height === 'number' ? synthetic.height : 0,
      synthetic: true,
      ts
    };

    state.lastSyntheticResult = result;
    state.lastSyntheticFrame = frame;
    state.lastViewResult = result;
    state.lastViewFrame = frame;

    notifyFaceViews(nodeId, state, result, frame);
  }

  function handleFaceInputPort(nodeId, port, payload) {
    const state = ensureState(nodeId, 'face');
    if (!state) return;
    if (!state.synthetic) state.synthetic = {};
    const synthetic = state.synthetic;
    let updated = false;

    if (payload && typeof payload === 'object') {
      if (payload.ts != null) {
        const ts = Number(payload.ts);
        if (Number.isFinite(ts)) {
          synthetic.ts = ts;
          updated = true;
        }
      }
      if (payload.width != null) {
        const width = Number(payload.width);
        if (Number.isFinite(width) && width > 0) {
          synthetic.width = width;
          updated = true;
        }
      }
      if (payload.height != null) {
        const height = Number(payload.height);
        if (Number.isFinite(height) && height > 0) {
          synthetic.height = height;
          updated = true;
        }
      }
    }

    switch (port) {
      case 'face': {
        const landmarks = mapLandmarks(payload?.landmarks, false);
        if (landmarks.length) {
          synthetic.landmarks = landmarks;
          updated = true;
        }
        break;
      }
      case 'blendshapes': {
        const categories = sanitizeBlendshapeCategories(payload?.categories);
        if (categories.length || (Array.isArray(payload?.categories) && !categories.length)) {
          synthetic.categories = categories;
          updated = true;
        }
        break;
      }
      case 'world': {
        if (Array.isArray(payload?.matrices)) {
          const matrices = payload.matrices
            .map((mat) => normalizeMatrixEntries(mat?.entries ?? mat))
            .filter(Boolean);
          if (matrices.length) {
            synthetic.matrices = matrices;
            updated = true;
          }
        }
        break;
      }
      case 'orientation': {
        if (payload && typeof payload === 'object') {
          const orientation = {};
          const matrix = normalizeMatrixEntries(payload.matrix ?? payload.entries ?? payload);
          if (matrix) orientation.matrix = matrix;
          if (payload.quaternion && typeof payload.quaternion === 'object') {
            orientation.quaternion = {
              w: typeof payload.quaternion.w === 'number' ? payload.quaternion.w : 0,
              x: typeof payload.quaternion.x === 'number' ? payload.quaternion.x : 0,
              y: typeof payload.quaternion.y === 'number' ? payload.quaternion.y : 0,
              z: typeof payload.quaternion.z === 'number' ? payload.quaternion.z : 0
            };
          }
          if (payload.radians && typeof payload.radians === 'object') {
            orientation.radians = {
              pitch: typeof payload.radians.pitch === 'number' ? payload.radians.pitch : 0,
              yaw: typeof payload.radians.yaw === 'number' ? payload.radians.yaw : 0,
              roll: typeof payload.radians.roll === 'number' ? payload.radians.roll : 0
            };
          }
          if (payload.degrees && typeof payload.degrees === 'object') {
            orientation.degrees = {
              pitch: typeof payload.degrees.pitch === 'number' ? payload.degrees.pitch : 0,
              yaw: typeof payload.degrees.yaw === 'number' ? payload.degrees.yaw : 0,
              roll: typeof payload.degrees.roll === 'number' ? payload.degrees.roll : 0
            };
          }
          if (payload.position && typeof payload.position === 'object') {
            orientation.position = {
              x: typeof payload.position.x === 'number' ? payload.position.x : 0,
              y: typeof payload.position.y === 'number' ? payload.position.y : 0,
              z: typeof payload.position.z === 'number' ? payload.position.z : 0
            };
          }
          if (Object.keys(orientation).length) {
            synthetic.orientation = orientation;
            updated = true;
          }
        }
        break;
      }
      case 'ts': {
        // Already captured above, no additional handling needed.
        updated = true;
        break;
      }
      default:
        return;
    }

    if (updated) deliverSyntheticFace(nodeId, state);
  }

  function emitFace(nodeId, state, result, frame) {
    if (!result || !Array.isArray(result.faceLandmarks)) return;
    const ts = Date.now();
    const width = frame.width || 0;
    const height = frame.height || 0;

    if (result.faceLandmarks.length) {
      const landmarks = mapLandmarks(result.faceLandmarks[0], false);
      try {
        Router.sendFrom(nodeId, 'face', {
          type: 'face',
          nodeId,
          ts,
          width,
          height,
          landmarks
        });
      } catch (err) {
        // ignore routing errors
      }
    }

    if (state.cfg.outputBlendshapes && result.faceBlendshapes?.length) {
      const cats = result.faceBlendshapes[0]?.categories || [];
      const categories = cats.map((c) => ({
        categoryName: c?.categoryName || '',
        score: typeof c?.score === 'number' ? c.score : 0,
        index: typeof c?.index === 'number' ? c.index : -1,
        displayName: c?.displayName || ''
      }));
      try {
        Router.sendFrom(nodeId, 'blendshapes', {
          type: 'blendshapes',
          nodeId,
          ts,
          categories
        });
      } catch (err) {
        // ignore routing errors
      }
    }

    const matrixEntries = extractMatrixEntries(result.facialTransformationMatrixes?.[0]);

    if (state.cfg.outputWorld && Array.isArray(result.facialTransformationMatrixes) && result.facialTransformationMatrixes.length) {
      const matrices = result.facialTransformationMatrixes
        .map((mat) => {
          const entries = extractMatrixEntries(mat);
          if (!entries) return null;
          return { entries: entries.slice(0, 16) };
        })
        .filter(Boolean);
      try {
        Router.sendFrom(nodeId, 'world', {
          type: 'face-world',
          nodeId,
          ts,
          matrices
        });
      } catch (err) {
        // ignore routing errors
      }
    }

    if (state.cfg.outputHeadPose && matrixEntries) {
      const orientation = computeOrientation(matrixEntries);
      if (orientation) {
        try {
          Router.sendFrom(nodeId, 'orientation', {
            type: 'head-pose',
            nodeId,
            ts,
            radians: orientation.radians,
            degrees: orientation.degrees,
            quaternion: orientation.quaternion,
            position: orientation.position,
            matrix: orientation.matrix
          });
        } catch (err) {
          // ignore routing errors
        }
      }
    }

    emitTimestamp(nodeId, ts);
  }

  function notifyFaceViews(nodeId, state, result, frame) {
    if (!state?.listeners || !state.listeners.size) return;
    state.listeners.forEach((listener) => {
      try {
        if (typeof listener === 'function') listener(result, frame, state.cfg);
        else if (listener && typeof listener.update === 'function') listener.update(result, frame, state.cfg);
      } catch (err) {
        // ignore listener errors
      }
    });
  }

  function emitPose(nodeId, state, result, frame) {
    if (!result || !Array.isArray(result.landmarks)) return;
    const ts = Date.now();
    const width = frame.width || 0;
    const height = frame.height || 0;

    if (result.landmarks.length) {
      const landmarks = mapLandmarks(result.landmarks[0], true);
      try {
        Router.sendFrom(nodeId, 'pose', {
          type: 'pose',
          nodeId,
          ts,
          width,
          height,
          landmarks
        });
      } catch (err) {
        // ignore routing errors
      }
    }

    if (state.cfg.outputWorld && Array.isArray(result.worldLandmarks) && result.worldLandmarks.length) {
      const world = mapLandmarks(result.worldLandmarks[0], true);
      try {
        Router.sendFrom(nodeId, 'world', {
          type: 'pose-world',
          nodeId,
          ts,
          landmarks: world
        });
      } catch (err) {
        // ignore routing errors
      }
    }

    if (state.cfg.segmentation && Array.isArray(result.segmentationMasks) && result.segmentationMasks.length) {
      let maskPayload = null;
      const mask = result.segmentationMasks[0];
      try {
        if (mask && typeof mask.clone === 'function') {
          maskPayload = mask.clone();
          mask.close?.();
        } else {
          maskPayload = mask || null;
        }
      } catch (err) {
        maskPayload = mask || null;
      }
      if (maskPayload) {
        try {
          Router.sendFrom(nodeId, 'segmentation', {
            type: 'pose-segmentation',
            nodeId,
            ts,
            mask: maskPayload,
            width,
            height
          });
        } catch (err) {
          // ignore routing errors
        }
      }
    }

    emitTimestamp(nodeId, ts);
  }

  async function rebuildInstance(nodeId, state) {
    if (!state) return null;
    const key = stateKey(nodeId);
    if (!key) return null;
    if (state.initializing) {
      try {
        await state.initializing;
      } catch (err) {
        // swallow prior init errors
      }
    }
    if (state.instance) {
      try {
        state.instance.close?.();
      } catch (err) {
        // ignore close errors
      }
      state.instance = null;
    }
    const cfg = state.cfg;
    const create = async () => {
      const vision = await loadVisionModule();
      const fileset = await ensureFileset();
      if (state.kind === 'face') {
        const options = {
          baseOptions: {
            modelAssetPath: cfg.modelAssetPath,
            delegate: cfg.delegate
          },
          runningMode: cfg.runningMode,
          numFaces: cfg.numFaces,
          outputFaceBlendshapes: cfg.outputBlendshapes,
          outputFacialTransformationMatrixes: cfg.outputWorld || cfg.outputHeadPose,
          minFaceDetectionConfidence: cfg.minFaceDetectionConfidence,
          minFacePresenceConfidence: cfg.minFacePresenceConfidence,
          minTrackingConfidence: cfg.minTrackingConfidence
        };
        try {
          return await vision.FaceLandmarker.createFromOptions(fileset, options);
        } catch (err) {
          if (cfg.delegate === 'GPU') {
            options.baseOptions.delegate = 'CPU';
            try {
              const cpuInstance = await vision.FaceLandmarker.createFromOptions(fileset, options);
              cfg.delegate = 'CPU';
              try {
                NodeStore.update(nodeId, { type: 'FaceLandmarks', delegate: 'CPU' });
              } catch (err2) {
                // ignore store update issues
              }
              reportError(state, 'Face Landmarks falling back to CPU delegate', { once: true });
              return cpuInstance;
            } catch (err2) {
              throw err2;
            }
          }
          throw err;
        }
      } else {
        const options = {
          baseOptions: {
            modelAssetPath: cfg.modelAssetPath,
            delegate: cfg.delegate
          },
          runningMode: cfg.runningMode,
          minPoseDetectionConfidence: cfg.minPoseDetectionConfidence,
          minPoseTrackingConfidence: cfg.minPoseTrackingConfidence,
          outputSegmentationMasks: cfg.segmentation,
          resultSmoothing: cfg.smoothing
        };
        try {
          return await vision.PoseLandmarker.createFromOptions(fileset, options);
        } catch (err) {
          if (cfg.delegate === 'GPU') {
            options.baseOptions.delegate = 'CPU';
            try {
              const cpuInstance = await vision.PoseLandmarker.createFromOptions(fileset, options);
              cfg.delegate = 'CPU';
              try {
                NodeStore.update(nodeId, { type: 'PoseLandmarks', delegate: 'CPU' });
              } catch (err2) {
                // ignore store update errors
              }
              reportError(state, 'Pose Landmarks falling back to CPU delegate', { once: true });
              return cpuInstance;
            } catch (err2) {
              throw err2;
            }
          }
          throw err;
        }
      }
    };

    state.initializing = create()
      .then((instance) => {
        state.instance = instance;
        state.lastFrameMs = 0;
        clearError(state);
        return instance;
      })
      .catch((err) => {
        const message = `[vision] ${state.kind === 'face' ? 'Face' : 'Pose'} init failed: ${err?.message || err}`;
        if (log) {
          try { log(message); } catch (_) { /* ignore */ }
        }
        reportError(state, message);
        throw err;
      })
      .finally(() => {
        state.initializing = null;
      });

    try {
      return await state.initializing;
    } catch (_) {
      return null;
    }
  }

  async function ensureInstance(nodeId, state) {
    if (!state) return null;
    if (state.instance) return state.instance;
    return await rebuildInstance(nodeId, state);
  }

  function computeThrottle(state, portName) {
    const fps = clamp(state?.cfg?.maxFPS, 1, 240);
    const timestamp = nowMs();
    if (portName === 'image') {
      return { allowed: true, timestamp };
    }
    const minInterval = 1000 / fps;
    const last = state?.lastFrameMs || 0;
    return { allowed: !last || timestamp - last >= minInterval, timestamp };
  }

  function processFace(nodeId, port, payload) {
    const state = ensureState(nodeId, 'face');
    if (!state) return;
    if (!state.cfgApplied) {
      state.cfg = getConfigFromStore(nodeId, 'face');
      state.cfgApplied = true;
    }
    const throttle = computeThrottle(state, port);
    if (!throttle.allowed) return;
    if (state.processing) return;
    state.processing = true;
    Promise.resolve()
      .then(async () => {
        state.synthetic = null;
        const frame = await prepareFrame(payload);
        if (!frame) return;
        const timestamp = throttle.timestamp ?? nowMs();
        frame.ts = timestamp;
        state.lastFrameMs = timestamp;
        const instance = await ensureInstance(nodeId, state);
        if (!instance) return;
        let result = null;
        const source = frame.source;
        if (!source) return;
        const useVideoMode = state.cfg.runningMode === 'VIDEO';
        if (useVideoMode && typeof instance.detectForVideo === 'function') {
          result = instance.detectForVideo(source, timestamp);
        } else if (typeof instance.detect === 'function') {
          result = instance.detect(source);
        }
        if (frame.cleanup) {
          try { frame.cleanup(); } catch (_) { /* ignore */ }
        }
        if (result) {
          state.lastViewResult = result;
          state.lastViewFrame = {
            width: frame.width || 0,
            height: frame.height || 0,
            ts: frame.ts || throttle.timestamp || nowMs(),
            synthetic: false
          };
          emitFace(nodeId, state, result, frame);
          notifyFaceViews(nodeId, state, result, frame);
        }
      })
      .catch((err) => {
        const message = `[vision] Face processing failed: ${err?.message || err}`;
        if (log) {
          try { log(message); } catch (_) { /* ignore */ }
        }
        reportError(state, message);
      })
      .finally(() => {
        state.processing = false;
      });
  }

  function processPose(nodeId, port, payload) {
    const state = ensureState(nodeId, 'pose');
    if (!state) return;
    if (!state.cfgApplied) {
      state.cfg = getConfigFromStore(nodeId, 'pose');
      state.cfgApplied = true;
    }
    const throttle = computeThrottle(state, port);
    if (!throttle.allowed) return;
    if (state.processing) return;
    state.processing = true;
    Promise.resolve()
      .then(async () => {
        const frame = await prepareFrame(payload);
        if (!frame) return;
        state.lastFrameMs = throttle.timestamp;
        const instance = await ensureInstance(nodeId, state);
        if (!instance) return;
        const timestamp = nowMs();
        let result = null;
        const source = frame.source;
        if (!source) return;
        const useVideoMode = state.cfg.runningMode === 'VIDEO';
        if (useVideoMode && typeof instance.detectForVideo === 'function') {
          result = instance.detectForVideo(source, timestamp);
        } else if (typeof instance.detect === 'function') {
          result = instance.detect(source);
        }
        if (frame.cleanup) {
          try { frame.cleanup(); } catch (_) { /* ignore */ }
        }
        if (result) {
          emitPose(nodeId, state, result, frame);
        }
      })
      .catch((err) => {
        const message = `[vision] Pose processing failed: ${err?.message || err}`;
        if (log) {
          try { log(message); } catch (_) { /* ignore */ }
        }
        reportError(state, message);
      })
      .finally(() => {
        state.processing = false;
      });
  }

  const Face = {
    init(nodeId) {
      const state = ensureState(nodeId, 'face');
      if (!state) return;
      state.cfg = getConfigFromStore(nodeId, 'face');
      state.cfgApplied = true;
      state.lastFrameMs = 0;
      state.processing = false;
      return rebuildInstance(nodeId, state);
    },
    refresh(nodeId, cfg) {
      const state = ensureState(nodeId, 'face');
      if (!state) return;
      state.cfg = normalizeFaceConfig(cfg || NodeStore.ensure(nodeId, 'FaceLandmarks').config || {});
      state.cfgApplied = true;
      state.lastFrameMs = 0;
      return rebuildInstance(nodeId, state);
    },
    dispose(nodeId) {
      const key = stateKey(nodeId);
      if (!STATE.has(key)) return;
      const state = STATE.get(key);
      if (state?.listeners) {
        state.listeners.forEach((listener) => {
          try {
            if (listener && typeof listener.dispose === 'function') listener.dispose();
          } catch (err) {
            // ignore listener dispose errors
          }
        });
        state.listeners.clear();
      }
      if (state?.instance) {
        try { state.instance.close?.(); } catch (_) { /* ignore */ }
      }
      STATE.delete(key);
    },
    onInput(nodeId, port, payload) {
      if (port === 'media' || port === 'image') {
        processFace(nodeId, port, payload);
        return;
      }
      if (port === 'face' || port === 'blendshapes' || port === 'world' || port === 'orientation' || port === 'ts') {
        handleFaceInputPort(nodeId, port, payload);
      }
    },
    attachView(nodeId, listener) {
      const state = ensureState(nodeId, 'face');
      if (!state || !listener) return () => {};
      if (!state.listeners) state.listeners = new Set();
      state.listeners.add(listener);
      try {
        const lastResult = state.lastViewResult || state.lastSyntheticResult;
        if (lastResult) {
          const lastFrame = state.lastViewFrame || state.lastSyntheticFrame || {
            width: 0,
            height: 0,
            synthetic: true,
            ts: Date.now()
          };
          if (typeof listener === 'function') listener(lastResult, lastFrame, state.cfg);
          else if (listener && typeof listener.update === 'function') listener.update(lastResult, lastFrame, state.cfg);
        }
      } catch (err) {
        // ignore listener priming errors
      }
      return () => {
        const key = stateKey(nodeId);
        const current = STATE.get(key);
        if (!current?.listeners) return;
        current.listeners.delete(listener);
        try {
          if (listener && typeof listener.dispose === 'function') listener.dispose();
        } catch (_) {
          // ignore
        }
      };
    },
    detachView(nodeId, listener) {
      const key = stateKey(nodeId);
      const state = STATE.get(key);
      if (!state?.listeners || !listener) return;
      state.listeners.delete(listener);
    }
  };

  const Pose = {
    init(nodeId) {
      const state = ensureState(nodeId, 'pose');
      if (!state) return;
      state.cfg = getConfigFromStore(nodeId, 'pose');
      state.cfgApplied = true;
      state.lastFrameMs = 0;
      state.processing = false;
      return rebuildInstance(nodeId, state);
    },
    refresh(nodeId, cfg) {
      const state = ensureState(nodeId, 'pose');
      if (!state) return;
      state.cfg = normalizePoseConfig(cfg || NodeStore.ensure(nodeId, 'PoseLandmarks').config || {});
      state.cfgApplied = true;
      state.lastFrameMs = 0;
      return rebuildInstance(nodeId, state);
    },
    dispose(nodeId) {
      const key = stateKey(nodeId);
      if (!STATE.has(key)) return;
      const state = STATE.get(key);
      if (state?.instance) {
        try { state.instance.close?.(); } catch (_) { /* ignore */ }
      }
      STATE.delete(key);
    },
    onInput(nodeId, port, payload) {
      if (port !== 'media' && port !== 'image') return;
      processPose(nodeId, port, payload);
    }
  };

  return { Face, Pose };
}

export { createVision };
