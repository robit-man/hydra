const BASE32 = '0123456789bcdefghjkmnpqrstuvwxyz';

function toGeohash(lat, lon, precision) {
  let minLat = -90, maxLat = 90;
  let minLon = -180, maxLon = 180;
  let hash = '';
  let bit = 0;
  let ch = 0;
  let even = true;

  while (hash.length < precision) {
    if (even) {
      const mid = (minLon + maxLon) / 2;
      if (lon >= mid) {
        ch |= 1 << (4 - bit);
        minLon = mid;
      } else {
        maxLon = mid;
      }
    } else {
      const mid = (minLat + maxLat) / 2;
      if (lat >= mid) {
        ch |= 1 << (4 - bit);
        minLat = mid;
      } else {
        maxLat = mid;
      }
    }
    even = !even;
    if (bit < 4) bit++;
    else {
      hash += BASE32[ch];
      bit = 0;
      ch = 0;
    }
  }
  return hash;
}

function roundTo(value, precision) {
  const factor = Math.pow(10, precision);
  return Math.round(value * factor) / factor;
}

function createLocationNode({ getNode, Router, NodeStore, setBadge, log }) {
  const states = new Map();

  function ensureState(id) {
    if (!states.has(id)) {
      states.set(id, {
        watchId: null,
        running: false,
        lastPayload: null,
        queueTimer: null
      });
    }
    return states.get(id);
  }

  function cfg(id) {
    return NodeStore.ensure(id, 'Location').config || {};
  }

  function updateButton(id) {
    const node = getNode(id);
    const btn = node?.el?.querySelector('.locationToggle');
    const st = ensureState(id);
    if (btn) btn.textContent = st.running ? '■' : '▶';
  }

  function setStatus(id, text) {
    const el = getNode(id)?.el?.querySelector('[data-location-status]');
    if (el) el.textContent = text;
  }

  function start(id) {
    const st = ensureState(id);
    if (st.running) return;
    if (!navigator.geolocation) {
      setBadge('Geolocation not supported', false);
      return;
    }
    try {
      st.watchId = navigator.geolocation.watchPosition(
        (pos) => onPosition(id, pos),
        (err) => {
          setBadge(`Geolocation error: ${err.message}`, false);
          stop(id);
        },
        { enableHighAccuracy: true, maximumAge: 500, timeout: 10000 }
      );
      st.running = true;
      NodeStore.update(id, { type: 'Location', running: true });
      updateButton(id);
      setStatus(id, 'Tracking…');
    } catch (err) {
      setBadge(`Geolocation failure: ${err?.message || err}`, false);
    }
  }

  function stop(id) {
    const st = ensureState(id);
    if (!st.running) return;
    if (st.watchId !== null && navigator.geolocation) {
      try { navigator.geolocation.clearWatch(st.watchId); } catch (_) {}
    }
    st.watchId = null;
    st.running = false;
    st.lastPayload = null;
    if (st.queueTimer) {
      clearTimeout(st.queueTimer);
      st.queueTimer = null;
    }
    NodeStore.update(id, { type: 'Location', running: false });
    updateButton(id);
    setStatus(id, 'Stopped');
  }

  function onPosition(id, pos) {
    const st = ensureState(id);
    if (!st.running) return;
    const cfgObj = cfg(id);
    const format = (cfgObj.format || 'raw').toLowerCase();
    const precision = Math.max(1, Math.min(12, Number(cfgObj.precision) || 6));

    const lat = pos.coords.latitude;
    const lon = pos.coords.longitude;
    const ts = pos.timestamp || Date.now();

    let payload;
    if (format === 'geohash') {
      const hash = toGeohash(lat, lon, precision);
      payload = {
        type: 'location.geohash',
        geohash: hash,
        precision,
        ts
      };
    } else {
      payload = {
        type: 'location.latlon',
        lat: roundTo(lat, precision),
        lon: roundTo(lon, precision),
        precision,
        ts
      };
    }

    st.lastPayload = payload;
    if (st.queueTimer) return;
    st.queueTimer = setTimeout(() => {
      st.queueTimer = null;
      if (!st.lastPayload) return;
      Router.sendFrom(id, 'location', st.lastPayload);
      st.lastPayload = null;
    }, 50);
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
    toggle,
    refresh,
    dispose,
    isRunning: (id) => ensureState(id).running
  };
}

export { createLocationNode };
