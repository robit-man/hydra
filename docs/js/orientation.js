function createOrientationNode({ getNode, Router, NodeStore, setBadge, log }) {
  const states = new Map();

  function ensureState(id) {
    if (!states.has(id)) {
      states.set(id, {
        handler: null,
        running: false,
        lastData: null,
        queueTimer: null
      });
    }
    return states.get(id);
  }

  function config(id) {
    return NodeStore.ensure(id, 'Orientation').config || {};
  }

  function start(id) {
    const st = ensureState(id);
    if (st.running) return;
    if (typeof window === 'undefined') {
      setBadge('Orientation unavailable', false);
      return;
    }
    const hasPermission = typeof DeviceOrientationEvent !== 'undefined' && typeof DeviceOrientationEvent.requestPermission === 'function';
    const attach = () => {
      const handler = (event) => onOrientation(id, event);
      st.handler = handler;
      window.addEventListener('deviceorientation', handler, { passive: true });
      st.running = true;
      NodeStore.update(id, { type: 'Orientation', running: true });
      updateButton(id);
      setStatus(id, 'Streaming');
    };
    if (hasPermission) {
      DeviceOrientationEvent.requestPermission()
        .then((res) => {
          if (res === 'granted') attach();
          else setBadge('Orientation permission denied', false);
        })
        .catch((err) => {
          setBadge(`Orientation permission error: ${err?.message || err}`, false);
        });
    } else {
      attach();
    }
  }

  function stop(id) {
    const st = ensureState(id);
    if (!st.running) return;
    if (st.handler && typeof window !== 'undefined') {
      window.removeEventListener('deviceorientation', st.handler);
    }
    if (st.queueTimer) {
      clearTimeout(st.queueTimer);
      st.queueTimer = null;
    }
    st.handler = null;
    st.running = false;
    NodeStore.update(id, { type: 'Orientation', running: false });
    updateButton(id);
    setStatus(id, 'Stopped');
  }

  function onOrientation(id, event) {
    const st = ensureState(id);
    if (!st.running) return;
    const cfg = config(id);
    const format = (cfg.format || 'raw').toLowerCase();
    const alpha = typeof event.alpha === 'number' ? event.alpha : null;
    const beta = typeof event.beta === 'number' ? event.beta : null;
    const gamma = typeof event.gamma === 'number' ? event.gamma : null;
    if (alpha === null && beta === null && gamma === null) return;

    const radAlpha = alpha != null ? alpha * (Math.PI / 180) : 0;
    const radBeta = beta != null ? beta * (Math.PI / 180) : 0;
    const radGamma = gamma != null ? gamma * (Math.PI / 180) : 0;

    let payload;
    if (format === 'quaternion') {
      payload = toQuaternion(radAlpha, radBeta, radGamma);
    } else if (format === 'euler') {
      payload = {
        type: 'orientation.euler',
        alpha: radAlpha,
        beta: radBeta,
        gamma: radGamma
      };
    } else {
      payload = {
        type: 'orientation.raw',
        alpha,
        beta,
        gamma
      };
    }

    st.lastData = payload;
    if (st.queueTimer) return;
    st.queueTimer = setTimeout(() => {
      st.queueTimer = null;
      if (!st.lastData) return;
      Router.sendFrom(id, 'orientation', {
        nodeId: id,
        ts: Date.now(),
        format,
        ...st.lastData
      });
      st.lastData = null;
    }, 10);
  }

  function toQuaternion(alpha, beta, gamma) {
    const c1 = Math.cos(alpha / 2);
    const c2 = Math.cos(beta / 2);
    const c3 = Math.cos(gamma / 2);
    const s1 = Math.sin(alpha / 2);
    const s2 = Math.sin(beta / 2);
    const s3 = Math.sin(gamma / 2);

    return {
      type: 'orientation.quaternion',
      w: c1 * c2 * c3 - s1 * s2 * s3,
      x: c1 * s2 * c3 - s1 * c2 * s3,
      y: c1 * s2 * s3 + s1 * c2 * c3,
      z: c1 * c2 * s3 + s1 * s2 * c3
    };
  }

  function updateButton(id) {
    const node = getNode(id);
    if (!node || !node.el) return;
    const btn = node.el.querySelector('.orientationToggle');
    const st = ensureState(id);
    if (btn) btn.textContent = st.running ? '■' : '▶';
  }

  function setStatus(id, text) {
    const node = getNode(id);
    const el = node?.el?.querySelector('[data-orientation-status]');
    if (el) el.textContent = text;
  }

  function init(id) {
    ensureState(id);
    updateButton(id);
    setStatus(id, 'Idle');
  }

  function toggle(id) {
    const st = ensureState(id);
    if (st.running) stop(id);
    else start(id);
  }

  function refresh(id) {
    const st = ensureState(id);
    if (st.running) {
      stop(id);
      start(id);
    }
  }

  function dispose(id) {
    stop(id);
    states.delete(id);
  }

  return {
    init,
    dispose,
    toggle,
    refresh,
    isRunning: (id) => ensureState(id).running
  };
}

export { createOrientationNode };
