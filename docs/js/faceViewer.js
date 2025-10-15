const BLEND_MAP = {
  browDownLeft: 'browDown_L',
  browDownRight: 'browDown_R',
  browInnerUp: 'browInnerUp',
  browOuterUpLeft: 'browOuterUp_L',
  browOuterUpRight: 'browOuterUp_R',
  cheekPuff: 'cheekPuff',
  cheekSquintLeft: 'cheekSquint_L',
  cheekSquintRight: 'cheekSquint_R',
  eyeBlinkLeft: 'eyeBlink_L',
  eyeBlinkRight: 'eyeBlink_R',
  eyeLookDownLeft: 'eyeLookDown_L',
  eyeLookDownRight: 'eyeLookDown_R',
  eyeLookInLeft: 'eyeLookIn_L',
  eyeLookInRight: 'eyeLookIn_R',
  eyeLookOutLeft: 'eyeLookOut_L',
  eyeLookOutRight: 'eyeLookOut_R',
  eyeLookUpLeft: 'eyeLookUp_L',
  eyeLookUpRight: 'eyeLookUp_R',
  eyeSquintLeft: 'eyeSquint_L',
  eyeSquintRight: 'eyeSquint_R',
  eyeWideLeft: 'eyeWide_L',
  eyeWideRight: 'eyeWide_R',
  jawForward: 'jawForward',
  jawLeft: 'jawLeft',
  jawOpen: 'jawOpen',
  jawRight: 'jawRight',
  mouthClose: 'mouthClose',
  mouthDimpleLeft: 'mouthDimple_L',
  mouthDimpleRight: 'mouthDimple_R',
  mouthFrownLeft: 'mouthFrown_L',
  mouthFrownRight: 'mouthFrown_R',
  mouthFunnel: 'mouthFunnel',
  mouthLeft: 'mouthLeft',
  mouthLowerDownLeft: 'mouthLowerDown_L',
  mouthLowerDownRight: 'mouthLowerDown_R',
  mouthPressLeft: 'mouthPress_L',
  mouthPressRight: 'mouthPress_R',
  mouthPucker: 'mouthPucker',
  mouthRight: 'mouthRight',
  mouthRollLower: 'mouthRollLower',
  mouthRollUpper: 'mouthRollUpper',
  mouthShrugLower: 'mouthShrugLower',
  mouthShrugUpper: 'mouthShrugUpper',
  mouthSmileLeft: 'mouthSmile_L',
  mouthSmileRight: 'mouthSmile_R',
  mouthStretchLeft: 'mouthStretch_L',
  mouthStretchRight: 'mouthStretch_R',
  mouthUpperUpLeft: 'mouthUpperUp_L',
  mouthUpperUpRight: 'mouthUpperUp_R',
  noseSneerLeft: 'noseSneer_L',
  noseSneerRight: 'noseSneer_R'
};

const FACE_MODEL_URL = 'https://threejs.org/examples/models/gltf/facecap.glb';
const BASIS_TRANSCODER_URL = 'https://unpkg.com/three@0.152.2/examples/jsm/libs/basis/';

function createFaceViewer({ Vision }) {
  const viewers = new Map();
  let threePromise = null;

  async function loadThree() {
    if (!threePromise) {
      threePromise = Promise.all([
        import('three'),
        import('three/addons/controls/OrbitControls.js'),
        import('three/addons/loaders/GLTFLoader.js'),
        import('three/addons/loaders/KTX2Loader.js'),
        import('three/addons/libs/meshopt_decoder.module.js')
      ])
        .then(([THREE, controlsMod, gltfMod, ktx2Mod, meshoptMod]) => ({
          THREE,
          OrbitControls: controlsMod.OrbitControls,
          GLTFLoader: gltfMod.GLTFLoader,
          KTX2Loader: ktx2Mod.KTX2Loader,
          MeshoptDecoder: meshoptMod.MeshoptDecoder
        }))
        .catch((err) => {
          threePromise = null;
          throw err;
        });
    }
    return threePromise;
  }

  function findHeadMesh(root) {
    let candidate = null;
    root.traverse((node) => {
      if (node.isMesh && node.morphTargetDictionary && node.morphTargetDictionary['eyeBlink_L'] !== undefined) {
        candidate = node;
      }
    });
    return candidate;
  }

  function resetPoseGroup(viewer) {
    if (!viewer?.poseGroup) return;
    const group = viewer.poseGroup;
    while (group.children.length) {
      group.remove(group.children[0]);
    }
    group.rotation.set(0, 0, 0);
    group.quaternion.set(0, 0, 0, 1);
    viewer.hasOrientation = false;
  }

  function extractMatrixEntries(matrix) {
    if (!matrix) return null;
    if (Array.isArray(matrix)) return matrix;
    if (matrix.matrix) return extractMatrixEntries(matrix.matrix);
    if (matrix.entries && Array.isArray(matrix.entries)) return Array.from(matrix.entries);
    if (matrix.data && Array.isArray(matrix.data)) return Array.from(matrix.data);
    if (matrix instanceof Float32Array) return Array.from(matrix);
    return null;
  }

  function applyBlendshapes(viewer, categories) {
    if (!viewer.head || !viewer.morphMap || !viewer.head.morphTargetInfluences || !Object.keys(viewer.morphMap).length) {
      return false;
    }
    const influences = viewer.head.morphTargetInfluences;
    if (viewer.morphIndices) {
      for (const idx of viewer.morphIndices) influences[idx] = 0;
    }
    categories.forEach((cat) => {
      if (!cat || typeof cat.categoryName !== 'string') return;
      const idx = viewer.morphMap[cat.categoryName];
      if (idx === undefined) return;
      influences[idx] = cat.score || 0;
    });
    return true;
  }

  function updateEyes(viewer, categories) {
    if (!viewer.THREE || !viewer.eyeL || !viewer.eyeR) return;
    let lH = 0;
    let rH = 0;
    let lV = 0;
    let rV = 0;
    for (const entry of categories) {
      const name = entry?.categoryName;
      const score = entry?.score || 0;
      switch (name) {
        case 'eyeLookInLeft':
          lH += score;
          break;
        case 'eyeLookOutLeft':
          lH -= score;
          break;
        case 'eyeLookInRight':
          rH -= score;
          break;
        case 'eyeLookOutRight':
          rH += score;
          break;
        case 'eyeLookUpLeft':
          lV -= score;
          break;
        case 'eyeLookDownLeft':
          lV += score;
          break;
        case 'eyeLookUpRight':
          rV -= score;
          break;
        case 'eyeLookDownRight':
          rV += score;
          break;
        default:
          break;
      }
    }
    const clamp = viewer.THREE.MathUtils.clamp;
    const limit = viewer.eyeLimit;
    if (viewer.eyeL) {
      viewer.eyeL.rotation.z = clamp(lH, -1, 1) * limit;
      viewer.eyeL.rotation.x = clamp(lV, -1, 1) * limit;
    }
    if (viewer.eyeR) {
      viewer.eyeR.rotation.z = clamp(rH, -1, 1) * limit;
      viewer.eyeR.rotation.x = clamp(rV, -1, 1) * limit;
    }
  }

  function updateFromResult(viewer, result) {
    if (!viewer) return;
    viewer.lastResult = result;
    if (!result) return;
    const categories = result.faceBlendshapes?.[0]?.categories || [];
    viewer.latestCategories = categories;

    const matrixEntries = extractMatrixEntries(result.facialTransformationMatrixes?.[0]);
    viewer.latestMatrixEntries = matrixEntries;

    if (viewer.ready) {
      const THREE = viewer.THREE;
      let orientationApplied = false;
     if (matrixEntries && viewer.poseGroup) {
        viewer.tempMatrix = viewer.tempMatrix || new THREE.Matrix4();
        viewer.tempQuaternion = viewer.tempQuaternion || new THREE.Quaternion();
        viewer.tempMatrix.fromArray(matrixEntries);
        viewer.tempQuaternion.setFromRotationMatrix(viewer.tempMatrix);
        if (!viewer.hasOrientation) {
          viewer.poseGroup.quaternion.copy(viewer.tempQuaternion);
          viewer.hasOrientation = true;
        } else {
          viewer.poseGroup.quaternion.slerp(viewer.tempQuaternion, 0.35);
        }
        orientationApplied = true;
      }

      const hasMorphTargets = applyBlendshapes(viewer, categories);

      if (!orientationApplied) {
        viewer.hasOrientation = false;
        const target = viewer.poseGroup || viewer.head;
        if (target) {
          let yaw = 0;
          let pitch = 0;
          let roll = 0;
          for (const entry of categories) {
            const name = entry?.categoryName;
            const score = entry?.score || 0;
            if (!name) continue;
            if (name.startsWith('eyeLook')) continue;
            if (name.includes('Left')) yaw -= score * 0.05;
            if (name.includes('Right')) yaw += score * 0.05;
            if (name.includes('Up')) pitch -= score * 0.05;
            if (name.includes('Down')) pitch += score * 0.05;
            if (name.includes('Smile') || name.includes('Happy')) roll += score * 0.03;
          }
          target.rotation.set(pitch, yaw, roll);
        }
      } else if (!hasMorphTargets && viewer.head) {
        viewer.head.rotation.set(0, 0, 0);
      }

      updateEyes(viewer, categories);
    }
  }

  function markReady(viewer) {
    if (!viewer) return;
    if (viewers.get(viewer.id) !== viewer) return;
    viewer.ready = true;
    viewer.container?.classList.add('face-viewer-ready');
    if (viewer.statusEl) {
      viewer.statusEl.textContent = '';
      viewer.statusEl.style.opacity = '0';
    }
    if (viewer.lastResult) updateFromResult(viewer, viewer.lastResult);
  }

  function createFallbackHead(viewer) {
    const { THREE, poseGroup } = viewer;
    if (!poseGroup) return;
    resetPoseGroup(viewer);
    const container = new THREE.Group();
    const head = new THREE.Mesh(
      new THREE.SphereGeometry(0.7, 32, 32),
      new THREE.MeshStandardMaterial({ color: 0xffffff, roughness: 0.35, metalness: 0.1 })
    );
    const eyeGeom = new THREE.SphereGeometry(0.09, 20, 20);
    const eyeMat = new THREE.MeshPhysicalMaterial({ color: 0xffffff, roughness: 0.1, metalness: 0.0 });
    const eyeL = new THREE.Mesh(eyeGeom, eyeMat);
    const eyeR = new THREE.Mesh(eyeGeom, eyeMat);
    eyeL.position.set(-0.22, 0.12, 0.62);
    eyeR.position.set(0.22, 0.12, 0.62);
    container.add(head, eyeL, eyeR);
    poseGroup.add(container);
    viewer.root = container;
    viewer.head = head;
    viewer.eyeL = eyeL;
    viewer.eyeR = eyeR;
    viewer.morphMap = {};
    viewer.morphIndices = [];
  }

  async function init(nodeId, container, statusEl) {
    if (!container) return;
    dispose(nodeId);

    let deps;
    try {
      deps = await loadThree();
    } catch (err) {
      if (statusEl) statusEl.textContent = 'Three.js unavailable';
      return;
    }

    const THREE = deps.THREE;
    let renderer;
    try {
      renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
    } catch (err) {
      if (statusEl) statusEl.textContent = 'WebGL renderer unavailable';
      return;
    }
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
    renderer.setClearColor(0x0b0d10, 1);
    container.appendChild(renderer.domElement);
    container.classList.remove('face-viewer-ready');

    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0x101316);
    const camera = new THREE.PerspectiveCamera(45, 1, 0.05, 100);
    camera.position.set(0, 0.05, 3.4);

    const controls = new deps.OrbitControls(camera, renderer.domElement);
    controls.target.set(0, 0.03, 0);
    controls.enableDamping = true;
    controls.enablePan = false;
    controls.minDistance = 2.4;
    controls.maxDistance = 4;

    scene.add(new THREE.HemisphereLight(0xffffff, 0x222233, 0.6));
    const dir = new THREE.DirectionalLight(0xffffff, 0.9);
    dir.position.set(0.5, 1, 0.7);
    scene.add(dir);

    const poseGroup = new THREE.Group();
    scene.add(poseGroup);

    const viewer = {
      id: nodeId,
      container,
      statusEl,
      renderer,
      scene,
      camera,
      controls,
      poseGroup,
      THREE,
      ready: false,
      latestCategories: null,
      latestMatrixEntries: null,
      lastResult: null,
      morphMap: {},
      morphIndices: [],
      eyeLimit: THREE.MathUtils.degToRad(30)
    };

    const resize = () => {
      const rect = container.getBoundingClientRect();
      const width = Math.max(10, rect.width || container.clientWidth || 240);
      const height = Math.max(10, rect.height || 180);
      renderer.setSize(width, height, false);
      camera.aspect = width / height;
      camera.updateProjectionMatrix();
    };

    resize();
    const resizeObserver = new ResizeObserver(resize);
    resizeObserver.observe(container);
    viewer.resizeObserver = resizeObserver;

    renderer.setAnimationLoop(() => {
      controls.update();
      renderer.render(scene, camera);
    });

    const gltfLoader = new deps.GLTFLoader();
    const ktx2 = new deps.KTX2Loader().setTranscoderPath(BASIS_TRANSCODER_URL);
    await ktx2.detectSupport(renderer);
    gltfLoader.setKTX2Loader(ktx2);
    gltfLoader.setMeshoptDecoder(deps.MeshoptDecoder);
    gltfLoader.setCrossOrigin?.('anonymous');

    gltfLoader.load(
      FACE_MODEL_URL,
      (gltf) => {
        if (viewers.get(nodeId) !== viewer) return;
        const root = gltf.scene;
        resetPoseGroup(viewer);
        viewer.poseGroup.add(root);
        viewer.root = root;
        viewer.head = findHeadMesh(root) || root;
        viewer.eyeL = root.getObjectByName('eyeLeft') || null;
        viewer.eyeR = root.getObjectByName('eyeRight') || null;
        if (viewer.head) {
          viewer.head.material = new THREE.MeshStandardMaterial({ color: 0xffffff, roughness: 0.35, metalness: 0.0 });
          const dict = viewer.head.morphTargetDictionary || {};
          const morphMap = {};
          const indices = [];
          for (const [blend, morph] of Object.entries(BLEND_MAP)) {
            const idx = dict[morph];
            if (idx !== undefined) {
              morphMap[blend] = idx;
              indices.push(idx);
            }
          }
          viewer.morphMap = morphMap;
          viewer.morphIndices = indices;
        } else {
          viewer.morphMap = {};
          viewer.morphIndices = [];
        }
        markReady(viewer);
      },
      undefined,
      (err) => {
        if (console && console.warn) console.warn('[vision] face viewer GLB load failed', err);
        createFallbackHead(viewer);
        if (statusEl) statusEl.textContent = 'Fallback viewer';
        markReady(viewer);
      }
    );

    if (Vision?.Face?.attachView) {
      const listener = {
        update: (result) => updateFromResult(viewer, result),
        dispose: () => {}
      };
      const detach = Vision.Face.attachView(nodeId, listener);
      viewer.detach = detach;
    }

    viewers.set(nodeId, viewer);
    return viewer;
  }

  function dispose(nodeId) {
    const viewer = viewers.get(nodeId);
    if (!viewer) return;
    try {
      if (typeof viewer.detach === 'function') viewer.detach();
    } catch (err) {
      // ignore detach errors
    }
    try {
      if (viewer.resizeObserver) viewer.resizeObserver.disconnect();
    } catch (err) {
      // ignore
    }
    try {
      viewer.renderer?.setAnimationLoop(null);
    } catch (err) {
      // ignore
    }
    try {
      viewer.controls?.dispose();
    } catch (err) {
      // ignore
    }
    try {
      viewer.renderer?.dispose();
    } catch (err) {
      // ignore
    }
    try {
      resetPoseGroup(viewer);
      if (viewer.scene && viewer.poseGroup) viewer.scene.remove(viewer.poseGroup);
    } catch (err) {
      // ignore
    }
    try {
      if (viewer.renderer?.domElement?.parentElement === viewer.container) {
        viewer.container.removeChild(viewer.renderer.domElement);
      }
    } catch (err) {
      // ignore
    }
    viewer.container?.classList.remove('face-viewer-ready');
    if (viewer.statusEl) {
      viewer.statusEl.textContent = 'Viewer stopped';
      viewer.statusEl.style.opacity = '1';
    }
    viewers.delete(nodeId);
  }

  return { init, dispose };
}

export { createFaceViewer };
