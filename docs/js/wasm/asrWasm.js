import { pipeline } from '@xenova/transformers';
import { ensureOrtEnv, normalizeThreads } from './common.js';

const DEFAULT_MODEL = 'Xenova/whisper-tiny';
const PIPELINES = new Map();

async function loadWhisper(modelId, { threads, progressCb } = {}) {
  const normThreads = normalizeThreads(threads);
  ensureOrtEnv({ threads: normThreads });
  const key = `${modelId || DEFAULT_MODEL}::${normThreads}`;
  if (!PIPELINES.has(key)) {
    const task = pipeline('automatic-speech-recognition', modelId || DEFAULT_MODEL, {
      quantized: true,
      progress_callback: progressCb
    });
    PIPELINES.set(key, task);
  }
  return PIPELINES.get(key);
}

async function transcribe(samples, sampleRate = 16000, { model, threads, progressCb } = {}) {
  if (!samples || !samples.length) return { text: '' };
  const pipe = await loadWhisper(model, { threads, progressCb });
  const audio = { array: samples, sampling_rate: sampleRate };
  const result = await pipe(audio, { chunk_length_s: 30, stride_length_s: 6 });
  const text = result?.text || '';
  return { text, result };
}

function resetPipelines() {
  PIPELINES.clear();
}

export { DEFAULT_MODEL, loadWhisper, resetPipelines, transcribe };
