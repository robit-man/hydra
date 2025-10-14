function createAudioHelpers() {
  const AudioCtx = window.AudioContext || window.webkitAudioContext || null;
  let audioCtx = null;

  const ensureAudioCtx = () => {
    if (!AudioCtx) return null;
    if (!audioCtx) {
      try {
        audioCtx = new AudioCtx();
      } catch (err) {
        audioCtx = null;
      }
    }
    if (audioCtx && audioCtx.state === 'suspended') {
      audioCtx.resume().catch(() => { /* ignore */ });
    }
    return audioCtx;
  };

  const playBlip = (frequency, durationMs, gainValue) => {
    try {
      const ctx = ensureAudioCtx();
      if (!ctx) return;
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = 'sine';
      osc.frequency.value = frequency;
      const now = ctx.currentTime;
      const durationSec = Math.max(durationMs || 0, 1) / 1000;
      const volume = Math.max(0.0001, gainValue ?? 0.04);
      gain.gain.setValueAtTime(volume, now);
      gain.gain.exponentialRampToValueAtTime(0.0001, now + durationSec);
      osc.connect(gain).connect(ctx.destination);
      osc.start(now);
      osc.stop(now + durationSec);
    } catch (_) {
      /* ignore autoplay or audio errors */
    }
  };

  const playMoveBlip = () => playBlip(12000, 50, 0.03);
  const playResizeBlip = () => playBlip(200, 100, 0.05);

  return {
    playBlip,
    playMoveBlip,
    playResizeBlip
  };
}

export { createAudioHelpers };
