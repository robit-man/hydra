import * as ort from 'onnxruntime-web';
import { phonemize } from 'phonemizer';
import { cachedFetch, ensureOrtEnv, normalizeThreads } from './common.js';

const DEFAULT_PIPER_MODEL_URL =
  'https://huggingface.co/rhasspy/piper-voices/resolve/main/en/en_US/libritts_r/medium/en_US-libritts_r-medium.onnx';
const DEFAULT_PIPER_CONFIG_URL =
  'https://huggingface.co/rhasspy/piper-voices/resolve/main/en/en_US/libritts_r/medium/en_US-libritts_r-medium.onnx.json';

class PiperTTS {
  constructor(voiceConfig, session) {
    this.voiceConfig = voiceConfig;
    this.session = session;
  }

  static async fromPretrained(modelUrl, configUrl, { threads } = {}) {
    const threadCount = normalizeThreads(threads);
    ensureOrtEnv({ threads: threadCount });
    const [modelResponse, configResponse] = await Promise.all([cachedFetch(modelUrl), cachedFetch(configUrl)]);
    const [modelBuffer, voiceConfig] = await Promise.all([modelResponse.arrayBuffer(), configResponse.json()]);
    const session = await ort.InferenceSession.create(new Uint8Array(modelBuffer), {
      executionProviders: ['wasm'],
      graphOptimizationLevel: 'all',
      intraOpNumThreads: threadCount
    });
    return new PiperTTS(voiceConfig, session);
  }

  async textToPhonemes(text) {
    if (!this.voiceConfig) return [];
    if (this.voiceConfig.phoneme_type === 'text') {
      return [Array.from(text.normalize('NFD'))];
    }

    const voice = this.voiceConfig.espeak?.voice || 'en-us';
    const phonemes = await phonemize(text, voice);
    let phonemeText;
    if (typeof phonemes === 'string') phonemeText = phonemes;
    else if (Array.isArray(phonemes)) phonemeText = phonemes.join(' ');
    else if (phonemes && typeof phonemes === 'object') phonemeText = phonemes.text || phonemes.phonemes || String(phonemes);
    else phonemeText = String(phonemes || text);

    const sentences = phonemeText
      .split(/[.!?]+/)
      .filter((s) => s.trim().length > 0);
    return sentences.map((sentence) => Array.from(sentence.trim().normalize('NFD')));
  }

  phonemesToIds(textPhonemes) {
    if (!this.voiceConfig || !this.voiceConfig.phoneme_id_map) {
      throw new Error('Phoneme ID map not available');
    }
    const idMap = this.voiceConfig.phoneme_id_map;
    const BOS = '^';
    const EOS = '$';
    const PAD = '_';
    const ids = [];
    for (const sentencePhonemes of textPhonemes) {
      ids.push(idMap[BOS]);
      ids.push(idMap[PAD]);
      for (const phoneme of sentencePhonemes) {
        if (phoneme in idMap) {
          ids.push(idMap[phoneme]);
          ids.push(idMap[PAD]);
        }
      }
      ids.push(idMap[EOS]);
    }
    return ids;
  }

  getSpeakers() {
    if (!this.voiceConfig || !this.voiceConfig.num_speakers || this.voiceConfig.num_speakers <= 1) {
      return [{ id: 0, name: 'Voice 1' }];
    }
    const speakerIdMap = this.voiceConfig.speaker_id_map || {};
    return Object.entries(speakerIdMap)
      .sort(([, a], [, b]) => a - b)
      .map(([originalId, id]) => ({
        id,
        name: `Voice ${id + 1}`,
        originalId
      }));
  }

  async synthesize(text, speakerId = 0) {
    if (!this.session || !this.voiceConfig) throw new Error('TTS not initialized');
    const textPhonemes = await this.textToPhonemes(text);
    const phonemeIds = this.phonemesToIds(textPhonemes);
    if (!phonemeIds.length) throw new Error('No valid phonemes generated');

    const inputs = {
      input: new ort.Tensor('int64', new BigInt64Array(phonemeIds.map((id) => BigInt(id))), [1, phonemeIds.length]),
      input_lengths: new ort.Tensor('int64', BigInt64Array.from([BigInt(phonemeIds.length)]), [1]),
      scales: new ort.Tensor('float32', Float32Array.from([0.667, 1.0, 0.8]), [3])
    };

    if (this.voiceConfig.num_speakers > 1) {
      inputs.sid = new ort.Tensor('int64', BigInt64Array.from([BigInt(speakerId)]), [1]);
    }

    const results = await this.session.run(inputs);
    const audioOutput = results.output;
    const audioData = new Float32Array(audioOutput.data);
    const sampleRate = this.voiceConfig.audio?.sample_rate || 22050;
    return { audioData, sampleRate };
  }
}

const INSTANCES = new Map();

const normalizeUrl = (val, fallback) => {
  const text = typeof val === 'string' ? val.trim() : '';
  return text || fallback;
};

async function ensurePiper(modelUrl, configUrl, options = {}) {
  const model = normalizeUrl(modelUrl, DEFAULT_PIPER_MODEL_URL);
  const config = normalizeUrl(configUrl, DEFAULT_PIPER_CONFIG_URL);
  const threads = normalizeThreads(options.threads);
  const key = `${model}|${config}|${threads}`;
  if (!INSTANCES.has(key)) {
    INSTANCES.set(key, PiperTTS.fromPretrained(model, config, { threads }));
  }
  return INSTANCES.get(key);
}

async function synthesize(text, { modelUrl, configUrl, speakerId = 0, threads } = {}) {
  const tts = await ensurePiper(modelUrl, configUrl, { threads });
  const result = await tts.synthesize(text, speakerId);
  return { ...result, speakerId };
}

async function listSpeakers({ modelUrl, configUrl, threads } = {}) {
  const tts = await ensurePiper(modelUrl, configUrl, { threads });
  return tts.getSpeakers();
}

function resetPiper() {
  INSTANCES.clear();
}

export {
  DEFAULT_PIPER_CONFIG_URL,
  DEFAULT_PIPER_MODEL_URL,
  listSpeakers,
  resetPiper,
  synthesize
};
