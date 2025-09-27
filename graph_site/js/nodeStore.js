import { LS, ASR_DEFAULT_PROMPT } from './utils.js';

const NodeStore = {
  key: (id) => `graph.node.${id}`,
  defaultsByType: {
    ASR: {
      base: 'http://localhost:8126',
      relay: '',
      api: '',
      model: '',
      mode: 'fast',
      rate: 16000,
      chunk: 120,
      live: true,
      rms: 0.2,
      hold: 250,
      emaMs: 120,
      phraseOn: true,
      phraseMin: 3,
      phraseStable: 350,
      silence: 900,
      prompt: ASR_DEFAULT_PROMPT,
      prevWin: '',
      prevStep: '',
      prevModel: ''
    },
    LLM: {
      base: 'http://127.0.0.1:11434',
      relay: '',
      api: '',
      model: '',
      stream: true,
      useSystem: false,
      system: '',
      memoryOn: false,
      persistMemory: false,
      maxTurns: 16,
      memory: [],
      capabilities: [],
      think: false
    },
    TTS: {
      base: 'http://localhost:8123',
      relay: '',
      api: '',
      model: '',
      mode: 'stream'
    },
    TextInput: {
      placeholder: '',
      text: '',
      lastSent: ''
    },
    Template: {
      template: 'Hello {name}',
      variables: {}
    },
    TextDisplay: {
      text: ''
    },
    NknDM: {
      address: '',
      chunkBytes: 1800,
      heartbeatInterval: 15,
      componentId: '',
      handshake: { status: 'idle', peer: '', direction: 'idle', remoteId: '', graphId: '' },
      peer: null,
      allowedPeers: [],
      autoAccept: false
    },
    MCP: {
      base: 'http://127.0.0.1:9003',
      relay: '',
      api: '',
      autoConnect: true,
      connectOnQuery: true,
      protocolVersion: '2024-05-31',
      clientName: 'hydra-graph',
      clientVersion: '0.1.0',
      resourceFilters: '',
      toolFilters: '',
      emitSystem: true,
      emitContext: true,
      timeoutMs: 20000,
      augmentSystem: '',
      lastStatus: null,
      lastSystem: '',
      lastPrompt: '',
      lastContext: '',
      lastTs: 0
    },
    MediaStream: {
      includeVideo: true,
      includeAudio: false,
      frameRate: 8,
      videoWidth: 640,
      videoHeight: 480,
      compression: 60,
      audioSampleRate: 48000,
      audioChannels: 1,
      audioFormat: 'opus',
      audioBitsPerSecond: 32000,
      running: false,
      lastRemoteFrame: '',
      lastRemoteFrom: ''
    },
    Orientation: {
      format: 'raw',
      lastEventTs: 0,
      running: false
    },
    Location: {
      format: 'raw',
      precision: 6,
      running: false
    }
  },

  ensure(id, type) {
    let obj = LS.get(this.key(id), null);
    if (!obj || obj.type !== type) {
      obj = { id, type, config: { ...this.defaultsByType[type] } };
      this.saveObj(id, obj);
    }
    return obj;
  },

  load(id) {
    return LS.get(this.key(id), null);
  },

  saveObj(id, obj) {
    LS.set(this.key(id), obj);
  },

  saveCfg(id, type, cfg) {
    this.saveObj(id, { id, type, config: cfg });
  },

  update(id, patch) {
    const current = this.load(id) || this.ensure(id, patch.type);
    const cfg = { ...(current.config || {}), ...patch };
    this.saveCfg(id, current.type, cfg);
    return cfg;
  },

  erase(id) {
    LS.del(this.key(id));
  },

  setRelay(id, type, relay) {
    const current = this.ensure(id, type);
    const cfg = { ...(current.config || {}), relay };
    this.saveCfg(id, type, cfg);
    return cfg;
  }
};

export { NodeStore };
