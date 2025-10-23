import { LS, ASR_DEFAULT_PROMPT } from './utils.js';

const NodeStore = {
  key: (id) => `graph.node.${id}`,
  defaultsByType: {
    ASR: {
      base: 'http://localhost:8126',
      relay: '',
      api: '',
      model: '',
      endpointMode: 'auto',
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
      prevModel: '',
      muteSignalMode: 'true/false',
      activeSignalMode: 'true/false'
    },
    LLM: {
      base: 'http://127.0.0.1:11434',
      relay: '',
      api: '',
      model: '',
      endpointMode: 'auto',
      stream: true,
      useSystem: false,
      system: '',
      memoryOn: false,
      persistMemory: false,
      maxTurns: 16,
      memory: [],
      capabilities: [],
      think: false,
      tools: []
    },
    FaceLandmarks: {
      delegate: 'GPU',
      numFaces: 1,
      outputBlendshapes: true,
      outputWorld: false,
      outputHeadPose: true,
      runningMode: 'VIDEO',
      modelAssetPath:
        'https://storage.googleapis.com/mediapipe-models/face_landmarker/face_landmarker/float16/1/face_landmarker.task',
      maxFPS: 30,
      smoothing: true,
      minFaceDetectionConfidence: 0.5,
      minFacePresenceConfidence: 0.5,
      minTrackingConfidence: 0.5
    },
    PoseLandmarks: {
      delegate: 'GPU',
      runningMode: 'VIDEO',
      modelAssetPath:
        'https://storage.googleapis.com/mediapipe-models/pose_landmarker/pose_landmarker_lite/float16/1/pose_landmarker_lite.task',
      minPoseDetectionConfidence: 0.5,
      minPoseTrackingConfidence: 0.5,
      segmentation: false,
      outputWorld: true,
      maxFPS: 30,
      smoothing: true
    },
    TTS: {
      base: 'http://localhost:8123',
      relay: '',
      api: '',
      model: '',
      endpointMode: 'auto',
      mode: 'stream',
      volume: 1,
      filterTokens: ['#'],
      muteSignalMode: 'true/false',
      activeSignalMode: 'true/false'
    },
    WebScraper: {
      base: 'http://127.0.0.1:8130',
      relay: '',
      service: 'web_scrape',
      api: '',
      headless: 'true',
      autoScreenshot: 'false',
      sid: '',
      lastSid: '',
      lastFrame: '',
      lastDom: '',
      autoCapture: 'false',
      frameRate: 1,
      frameOutputMode: 'wrapped'
    },
    ImageInput: {
      image: '',
      b64: '',
      mime: '',
      width: 0,
      height: 0,
      updatedAt: 0
    },
    TextInput: {
      placeholder: '',
      text: '',
      lastSent: '',
      emitActionKey: '(none)',
      actionValue: 'type',
      outputMode: 'object',
      includeNodeId: true,
      nodeIdKey: 'nodeId',
      typeKey: 'type',
      typeValue: 'text',
      typeBackupKey: 'messageType',
      includeText: true,
      textKey: 'text',
      autoSendIncoming: true,
      incomingMode: 'replace',
      previewMode: 'pretty',
      customFields: [{ key: 'intent', mode: 'action', value: '' }],
      lastPreview: '',
      lastPreviewCopy: '',
      lastPreviewOrigin: '',
      lastIncomingText: ''
    },
    Template: {
      template: 'Hello {name}',
      variables: {}
    },
    LogicGate: {
      rules: [
        {
          id: '',
          label: 'Trigger',
          input: 'trigger',
          path: '',
          operator: 'truthy',
          compareValue: '',
          outputTrue: 'true',
          outputFalse: 'false',
          trueMode: 'message',
          falseMode: 'message'
        }
      ]
    },
    FileTransfer: {
      chunkSize: 1024,
      autoAccept: true,
      defaultKey: '',
      preferRoute: ''
    },
    TextDisplay: {
      text: ''
    },
    NknDM: {
      address: '',
      chunkBytes: 64000,
      heartbeatInterval: 15,
      componentId: '',
      handshake: { status: 'idle', peer: '', direction: 'idle', remoteId: '', graphId: '' },
      peer: null,
      allowedPeers: [],
      autoAccept: false,
      autoChunk: false
    },
    NoClipBridge: {
      targetPub: '',
      targetAddr: '',
      room: 'hybrid-bridge',
      autoConnect: true
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
      audioFormat: 'pcm16',
      audioBitsPerSecond: 32000,
      running: false,
      lastRemoteFrame: '',
      lastRemoteFrom: '',
      componentId: '',
      targets: [],
      pendingAddress: '',
      lastFacingMode: 'user',
      torchEnabled: false,
      activeTargets: [],
      peerMeta: {}
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
    },
    Meshtastic: {
      autoConnect: true,
      channel: 0,
      publicPortName: 'public',
      defaultJson: false,
      peers: {},
      rememberPort: true,
      rememberDevice: true,
      lastPortInfo: null,
      lastDeviceName: ''
    },
    Payments: {
      mode: 'seller',
      amount: '1.0',
      asset: '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913',
      chainId: 8453,
      receiver: '',
      memo: '',
      unlockTTL: 900
    },
    WebSerial: {
      autoConnect: true,
      autoReconnect: true,
      rememberPort: true,
      rememberDevice: true,
      lastPortInfo: null,
      lastDeviceName: '',
      baudRate: 115200,
      customBaud: '',
      encoding: 'utf-8',
      appendNewline: false,
      newline: '\\n',
      writeMode: 'text',
      readMode: 'text',
      echoSent: false,
      maxLogLines: 500
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
