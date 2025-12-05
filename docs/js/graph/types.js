const GraphTypes = {
  ASR: {
    title: 'ASR',
    supportsNkn: true,
    relayKey: 'relay',
    inputs: [
      { name: 'mute', label: 'Mute' },
      { name: 'audio', label: 'Audio Input', type: 'audio' }
    ],
    outputs: [{ name: 'partial' }, { name: 'phrase' }, { name: 'final' }, { name: 'active', label: 'Active' }],
    schema: [
      { key: 'wasm', label: 'Run In Browser (WASM)', type: 'select', options: ['false', 'true'], def: 'false' },
      {
        key: 'wasmWhisperModel',
        label: 'WASM Whisper Model',
        type: 'select',
        options: ['Xenova/whisper-tiny', 'Xenova/whisper-base', 'Xenova/whisper-small', 'distil-whisper/distil-medium.en'],
        def: 'Xenova/whisper-tiny'
      },
      { key: 'wasmThreads', label: 'WASM Threads', type: 'number', def: 1, min: 1, max: 8, step: 1 },
      { key: 'base', label: 'Base URL', type: 'text', placeholder: 'http://localhost:8126' },
      { key: 'relay', label: 'NKN Relay', type: 'text' },
      { key: 'api', label: 'API Key', type: 'text' },
      { key: 'model', label: 'Model', type: 'select', options: [] },
      { key: 'mode', label: 'Mode', type: 'select', options: ['fast', 'accurate'], def: 'fast' },
      { key: 'rate', label: 'Sample Rate', type: 'number', def: 16000 },
      { key: 'chunk', label: 'Chunk (ms)', type: 'number', def: 120 },
      { key: 'live', label: 'Live Mode', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'rms', label: 'RMS Threshold', type: 'text', def: 0.015 },
      { key: 'hold', label: 'Hold (ms)', type: 'number', def: 250 },
      { key: 'emaMs', label: 'EMA (ms)', type: 'number', def: 120 },
      { key: 'phraseOn', label: 'Phrase Mode', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'phraseMin', label: 'Min Words', type: 'number', def: 3 },
      { key: 'phraseStable', label: 'Stable (ms)', type: 'number', def: 350 },
      { key: 'silence', label: 'Silence End (ms)', type: 'number', def: 900 },
      { key: 'prevWin', label: 'Preview Window (s)', type: 'text', placeholder: '(server default)' },
      { key: 'prevStep', label: 'Preview Step (s)', type: 'text', placeholder: '(server default)' },
      { key: 'prevModel', label: 'Preview Model', type: 'select', options: [] },
      { key: 'prompt', label: 'Prompt', type: 'textarea', placeholder: 'Bias decoding, names, spellings…' },
      { key: 'muteSignalMode', label: 'Mute Signal', type: 'select', options: ['true/false', 'true/empty'], def: 'true/false' },
      { key: 'activeSignalMode', label: 'Active Signal', type: 'select', options: ['true/false', 'true/empty'], def: 'true/false' }
    ]
  },
  LLM: {
    title: 'LLM',
    supportsNkn: true,
    relayKey: 'relay',
    inputs: [{ name: 'prompt' }, { name: 'image' }, { name: 'system' }],
    outputs: [{ name: 'delta' }, { name: 'final' }, { name: 'memory' }],
    schema: [
      { key: 'base', label: 'Base URL', type: 'text', placeholder: 'http://127.0.0.1:11434' },
      { key: 'relay', label: 'NKN Relay', type: 'text' },
      { key: 'api', label: 'API Key', type: 'text' },
      { key: 'model', label: 'Model', type: 'select', options: [] },
      { key: 'stream', label: 'Stream', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'useSystem', label: 'Use System Message', type: 'select', options: ['false', 'true'], def: 'false' },
      { key: 'system', label: 'System Prompt', type: 'textarea' },
      { key: 'memoryOn', label: 'Use Chat Memory', type: 'select', options: ['false', 'true'], def: 'false' },
      { key: 'persistMemory', label: 'Persist Memory', type: 'select', options: ['false', 'true'], def: 'false' },
      { key: 'maxTurns', label: 'Max Turns', type: 'number', def: 16 }
    ]
  },
  TTS: {
    title: 'TTS',
    supportsNkn: true,
    relayKey: 'relay',
    inputs: [{ name: 'text' }, { name: 'mute', label: 'Mute' }],
    outputs: [
      { name: 'active', label: 'Active' },
      { name: 'audio', label: 'Audio Stream', type: 'audio' }
    ],
    schema: [
      { key: 'wasm', label: 'Run In Browser (WASM)', type: 'select', options: ['false', 'true'], def: 'false' },
      { key: 'wasmVoicePreset', label: 'WASM Voice Model', type: 'select', options: [] },
      { key: 'wasmPiperModelUrl', label: 'WASM Piper Model URL', type: 'text', placeholder: 'https://…/en_US-libritts_r-medium.onnx' },
      { key: 'wasmPiperConfigUrl', label: 'WASM Piper Config URL', type: 'text', placeholder: 'https://…/en_US-libritts_r-medium.onnx.json' },
      { key: 'wasmSpeakerId', label: 'WASM Speaker', type: 'select', options: [] },
      { key: 'wasmThreads', label: 'WASM Threads', type: 'number', def: 1, min: 1, max: 8, step: 1 },
      { key: 'base', label: 'Base URL', type: 'text', placeholder: 'http://localhost:8123' },
      { key: 'relay', label: 'NKN Relay', type: 'text' },
      { key: 'api', label: 'API Key', type: 'text' },
      { key: 'model', label: 'Voice/Model', type: 'select', options: [] },
      { key: 'mode', label: 'Mode', type: 'select', options: ['stream', 'file'], def: 'stream' },
      { key: 'filterTokens', label: 'Filter List', type: 'filterList', def: ['#'], placeholder: 'Character or phrase to strip', note: 'Emoji are always removed before synthesis.' },
      { key: 'muteSignalMode', label: 'Mute Signal', type: 'select', options: ['true/false', 'true/empty'], def: 'true/false' },
      { key: 'activeSignalMode', label: 'Active Signal', type: 'select', options: ['true/false', 'true/empty'], def: 'true/false' }
    ]
  },
  WebScraper: {
    title: 'Web Scraper',
    supportsNkn: true,
    relayKey: 'relay',
    inputs: [
      {
        name: 'url',
        label: 'URL',
        tooltip: 'Target URL for navigation.\nAccepts plain strings or objects with a text field (Text Input wiring works automatically).\nUsed by nav/open actions and the on-card address bar.'
      },
      {
        name: 'action',
        label: 'Action',
        tooltip: 'Drive browser actions.\nSend a string like "connect" or an object with action/type/kind/mode keys (Text Input emit setting helps).\nUnderstands: connect/open, close, nav/back/forward, click, type, enter, scroll, scroll_up, scroll_down, drag, screenshot, dom, events.'
      },
      {
        name: 'selector',
        label: 'Selector',
        tooltip: 'CSS selector for element actions.\nSend strings or objects with a text field.\nUsed by click, type, drag, and scroll point operations.'
      },
      {
        name: 'text',
        label: 'Text',
        tooltip: 'Typing payload or default field value.\nAccepts strings or objects with text/value.\nConsumed by type/enter actions and stored as the active input buffer.'
      },
      {
        name: 'amount',
        label: 'Amount',
        tooltip: 'Scroll distance in pixels.\nProvide numbers or numeric strings.\nConsumed by scroll, scroll_up, and scroll_down actions.'
      },
      {
        name: 'xy',
        label: 'XY',
        tooltip: 'Viewport coordinates for click/drag helpers.\nSend objects such as { x, y, viewportW, viewportH, naturalW, naturalH }.'
      },
      {
        name: 'sid',
        label: 'Session',
        tooltip: 'Manual session id override.\nSend a string to attach to an existing browser session and resume streaming events.'
      }
    ],
    outputs: [
      {
        name: 'status',
        label: 'Status',
        tooltip: 'Status text describing the latest action.\nForward downstream for logging or user feedback.'
      },
      {
        name: 'frame',
        label: 'Frame',
        tooltip: 'Screenshot output.\nIn "wrapped" mode emits metadata (blobUrl, b64, size); in "raw" mode emits the base64 image string.'
      },
      {
        name: 'dom',
        label: 'DOM',
        tooltip: 'DOM snapshot payload ({ dom, length, sid }).\nProduced by the DOM action and useful for parsing or inspection.'
      },
      {
        name: 'log',
        label: 'Log',
        tooltip: 'On-card log stream.\nShows detailed progress in the Web Scraper UI (no router payload emitted).'
      },
      {
        name: 'rawFrame',
        label: 'Raw Frame',
        tooltip: 'Base64 screenshot string.\nAlways emits the raw image data regardless of the configured frame output mode.'
      }
    ],
    schema: [
      { key: 'base', label: 'Base URL', type: 'text', placeholder: 'http://127.0.0.1:8130' },
      { key: 'relay', label: 'NKN Relay', type: 'text', placeholder: 'hydra.router' },
      { key: 'service', label: 'Service Alias', type: 'text', placeholder: 'web_scrape' },
      { key: 'api', label: 'API Key / Bearer', type: 'text', placeholder: '(optional)' },
      { key: 'headless', label: 'Headless', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'autoScreenshot', label: 'Auto Screenshot After Actions', type: 'select', options: ['false', 'true'], def: 'false' },
      { key: 'autoCapture', label: 'Continuous Capture', type: 'select', options: ['false', 'true'], def: 'false' },
      { key: 'frameRate', label: 'Capture FPS', type: 'number', def: 1 },
      { key: 'frameOutputMode', label: 'Frame Output Mode', type: 'select', options: ['wrapped', 'raw'], def: 'wrapped' },
      { key: 'sid', label: 'Session Override', type: 'text', placeholder: '(external SID)' }
    ]
  },
  ImageInput: {
    title: 'Image Input',
    inputs: [],
    outputs: [{ name: 'image' }],
    schema: []
  },
  TextInput: {
    title: 'Text Input',
    inputs: [{
      name: 'incoming',
      label: 'Incoming',
      tooltip: 'Optional upstream payload to reformat.\nAccepts text, numbers, or objects; Text Input can auto-package it using the configured output schema.'
    }],
    outputs: [{
      name: 'text',
      label: 'Output',
      tooltip: 'Emits the formatted payload.\nConfigure keys and structure below to match downstream expectations.'
    }],
    schema: [
      { key: 'placeholder', label: 'Placeholder', type: 'text', placeholder: 'Type a message…' },
      { key: 'emitActionKey', label: 'Emit Action Key', type: 'select', options: ['(none)', 'action', 'type', 'kind', 'mode'], def: '(none)' },
      { key: 'actionValue', label: 'Action Value', type: 'text', placeholder: 'type | nav | click' },
      { key: 'outputMode', label: 'Output Format', type: 'select', options: ['object', 'text'], def: 'object' },
      { key: 'includeNodeId', label: 'Include Node ID', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'nodeIdKey', label: 'Node ID Key', type: 'text', placeholder: 'nodeId' },
      { key: 'typeKey', label: 'Type Key', type: 'text', placeholder: 'type' },
      { key: 'typeValue', label: 'Type Value', type: 'text', placeholder: 'text' },
      { key: 'typeBackupKey', label: 'Type Backup Key', type: 'text', placeholder: 'messageType', note: 'Used when Emit Action Key is also "type" so the original type is preserved.' },
      { key: 'includeText', label: 'Include Text Field', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'textKey', label: 'Text Key', type: 'text', placeholder: 'text' },
      { key: 'autoSendIncoming', label: 'Auto Send Incoming Payloads', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'incomingMode', label: 'Incoming Text Mode', type: 'select', options: ['replace', 'append', 'ignore'], def: 'replace', note: 'How incoming data updates the composer before emission.' },
      { key: 'previewMode', label: 'Preview Style', type: 'select', options: ['pretty', 'compact'], def: 'pretty' },
      {
        key: 'customFields',
        label: 'Additional Fields',
        type: 'fieldMap',
        note: 'Add extra keys for the outgoing payload. Choose a value type per entry (literal, text, incoming, raw, action, json, number, boolean, nodeId, timestamp, template).'
      }
    ]
  },
  TextDisplay: {
    title: 'Text Display',
    inputs: [{ name: 'text' }],
    outputs: [],
    schema: []
  },
  Template: {
    title: 'Template',
    inputs: [{ name: 'trigger' }],
    outputs: [{ name: 'text' }],
    schema: [
      { key: 'template', label: 'Template Text', type: 'textarea', placeholder: 'Hello {name}, welcome to {place}.' }
    ]
  },
  LogicGate: {
    title: 'Logic Gate',
    inputs: [],
    outputs: [],
    schema: [
      { key: 'rules', label: 'Logic Rules', type: 'logicRules' }
    ]
  },
  FileTransfer: {
    title: 'File Transfer',
    inputs: [{ name: 'incoming', label: 'Incoming DM' }, { name: 'file', label: 'File Input' }],
    outputs: [
      { name: 'packet', label: 'DM Packet' },
      { name: 'outgoing', label: 'DM Packet (legacy)' },
      { name: 'file', label: 'File' },
      { name: 'status', label: 'Status' }
    ],
    schema: [
      { key: 'chunkSize', label: 'Chunk Size (bytes)', type: 'number', def: 1024 },
      { key: 'autoAccept', label: 'Auto-accept incoming', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'defaultKey', label: 'Default Passphrase', type: 'text', placeholder: '(optional shared key)' },
      { key: 'preferRoute', label: 'Default Route Tag', type: 'text', placeholder: 'optional route identifier' }
    ]
  },
  NknDM: {
    title: 'NKN DM',
    supportsNkn: true,
    relayKey: 'address',
    inputs: [{ name: 'text' }, { name: 'packet', label: 'DM Packet' }],
    outputs: [{ name: 'incoming' }, { name: 'status' }, { name: 'raw' }],
    schema: [
      { key: 'address', label: 'Target Address', type: 'text', placeholder: 'nkn...' },
      { key: 'chunkBytes', label: 'Chunk Size (bytes)', type: 'number', def: 50000 },
      { key: 'heartbeatInterval', label: 'Heartbeat (s)', type: 'number', def: 15 }
    ]
  },
  NoClipBridge: {
    title: 'NoClip Bridge',
    supportsNkn: true,
    relayKey: 'targetAddr',
    inputs: [
      {
        name: 'pose',
        label: 'Pose',
        tooltip: 'Pose payload with position/rotation for the remote scene.'
      },
      {
        name: 'resource',
        label: 'Resource',
        tooltip: 'Resource descriptor to inject (GLTF URL, media, etc.).'
      },
      {
        name: 'command',
        label: 'Command',
        tooltip: 'Arbitrary bridge command forwarded to the NoClip peer.'
      },
      {
        name: 'audioOutput',
        label: 'Audio Out',
        type: 'audio',
        tooltip: 'Audio stream to send to NoClip Smart Objects (from TTS).'
      }
    ],
    outputs: [
      {
        name: 'state',
        label: 'State',
        tooltip: 'Normalized state snapshots returned by the NoClip peer.'
      },
      {
        name: 'pose',
        label: 'Pose',
        tooltip: 'Pose updates emitted by the NoClip peer.'
      },
      {
        name: 'peers',
        label: 'Peers',
        tooltip: 'Discovery information for connected NoClip peers.'
      },
      {
        name: 'events',
        label: 'Events',
        tooltip: 'Lifecycle events (friend responses, acknowledgements).'
      },
      {
        name: 'chat',
        label: 'Chat',
        tooltip: 'Chat messages received over the bridge.'
      },
      {
        name: 'audioInput',
        label: 'Audio In',
        type: 'audio',
        tooltip: 'Audio stream from NoClip Smart Objects (for ASR).'
      }
    ],
    schema: [
      { key: 'targetPub', label: 'Target NKN Pub', type: 'text', placeholder: 'hex64 peer id' },
      { key: 'targetAddr', label: 'Target Address', type: 'text', placeholder: 'noclip.<hex64>' },
      { key: 'room', label: 'Discovery Room (override)', type: 'text', placeholder: '(auto from url)', def: 'auto' },
      { key: 'autoConnect', label: 'Auto Connect', type: 'select', options: ['true', 'false'], def: 'true' }
    ]
  },
  Payments: {
    title: 'Payments',
    supportsNkn: false,
    inputs: [{ name: 'input', label: 'Ingress' }, { name: 'control', label: 'Control' }],
    outputs: [{ name: 'output', label: 'Egress' }, { name: 'events', label: 'Events' }],
    schema: [
      { key: 'mode', label: 'Mode', type: 'select', options: ['seller', 'buyer', 'both'], def: 'seller' },
      { key: 'amount', label: 'Amount', type: 'text', placeholder: '1.0', def: '1.0' },
      { key: 'asset', label: 'Asset (ERC-20)', type: 'text', placeholder: '0x8335...' },
      { key: 'chainId', label: 'Chain ID', type: 'number', def: 8453 },
      { key: 'receiver', label: 'Receiver Address', type: 'text', placeholder: '0x...' },
      { key: 'memo', label: 'Memo', type: 'text', placeholder: 'Access description' },
      { key: 'unlockTTL', label: 'Unlock TTL (s)', type: 'number', def: 900 }
    ]
  },
  MediaStream: {
    title: 'Media Stream',
    supportsNkn: true,
    inputs: [{ name: 'media' }],
    outputs: [{ name: 'packet', label: 'DM Packet' }, { name: 'media', label: 'Media' }],
    schema: [
      { key: 'includeVideo', label: 'Include Video', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'includeAudio', label: 'Include Audio', type: 'select', options: ['false', 'true'], def: 'false' },
      { key: 'frameRate', label: 'Frame Rate (fps)', type: 'number', def: 8 },
      { key: 'videoWidth', label: 'Video Width', type: 'number', def: 640 },
      { key: 'videoHeight', label: 'Video Height', type: 'number', def: 480 },
      { key: 'compression', label: 'Compression (%)', type: 'range', def: 60, min: 0, max: 95, step: 5 },
      { key: 'audioSampleRate', label: 'Audio Sample Rate', type: 'select', options: ['16000', '24000', '32000', '44100', '48000'], def: '48000' },
      { key: 'audioChannels', label: 'Audio Channels', type: 'select', options: ['1', '2'], def: '1' },
      { key: 'audioFormat', label: 'Audio Format', type: 'select', options: ['pcm16', 'opus'], def: 'pcm16' },
      { key: 'audioBitsPerSecond', label: 'Audio Bitrate (bps)', type: 'number', def: 32000 }
    ]
  },
  Meshtastic: {
    title: 'Meshtastic',
    supportsNkn: false,
    inputs: [{ name: 'public', label: 'Send Public' }],
    outputs: [{ name: 'public', label: 'Public Messages' }],
    schema: [
      { key: 'channel', label: 'Channel', type: 'number', def: 0 },
      { key: 'autoConnect', label: 'Auto Connect', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'rememberPort', label: 'Remember Port', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'rememberDevice', label: 'Remember Device Name', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'defaultJson', label: 'Default JSON Output', type: 'select', options: ['true', 'false'], def: 'true' }
    ]
  },
  FaceLandmarks: {
    title: 'Face Landmarks',
    supportsNkn: false,
    inputs: [
      { name: 'media' },
      { name: 'image' },
      { name: 'face' },
      { name: 'blendshapes' },
      { name: 'world' },
      { name: 'orientation' },
      { name: 'ts' }
    ],
    outputs: [
      { name: 'face', label: '2D Landmarks' },
      { name: 'blendshapes', label: 'Blendshapes' },
      { name: 'world', label: 'World Landmarks' },
      { name: 'orientation', label: 'Head Pose' },
      { name: 'ts', label: 'Timestamp' }
    ],
    schema: [
      { key: 'delegate', label: 'Delegate', type: 'select', options: ['GPU', 'CPU'], def: 'GPU' },
      { key: 'numFaces', label: 'Max Faces', type: 'number', def: 1 },
      {
        key: 'minFaceDetectionConfidence',
        label: 'Min Detection Confidence',
        type: 'number',
        def: 0.5
      },
      {
        key: 'minFacePresenceConfidence',
        label: 'Min Presence Confidence',
        type: 'number',
        def: 0.5
      },
      {
        key: 'minTrackingConfidence',
        label: 'Min Tracking Confidence',
        type: 'number',
        def: 0.5
      },
      { key: 'outputBlendshapes', label: 'Output Blendshapes', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'outputWorld', label: 'Output World Landmarks', type: 'select', options: ['false', 'true'], def: 'false' },
      { key: 'runningMode', label: 'Running Mode', type: 'select', options: ['VIDEO', 'IMAGE'], def: 'VIDEO' },
      {
        key: 'modelAssetPath',
        label: 'Model URL',
        type: 'text',
        placeholder: 'https://storage.googleapis.com/mediapipe-models/face_landmarker/face_landmarker/float16/1/face_landmarker.task'
      },
      { key: 'maxFPS', label: 'Max FPS', type: 'number', def: 30 },
      { key: 'smoothing', label: 'Smoothing', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'outputHeadPose', label: 'Output Head Pose', type: 'select', options: ['true', 'false'], def: 'true' }
    ]
  },
  PoseLandmarks: {
    title: 'Pose Landmarks',
    supportsNkn: false,
    inputs: [{ name: 'media' }, { name: 'image' }],
    outputs: [
      { name: 'pose', label: '2D Landmarks' },
      { name: 'world', label: 'World Landmarks' },
      { name: 'segmentation', label: 'Segmentation' },
      { name: 'ts', label: 'Timestamp' }
    ],
    schema: [
      { key: 'delegate', label: 'Delegate', type: 'select', options: ['GPU', 'CPU'], def: 'GPU' },
      { key: 'runningMode', label: 'Running Mode', type: 'select', options: ['VIDEO', 'IMAGE'], def: 'VIDEO' },
      {
        key: 'modelAssetPath',
        label: 'Model URL',
        type: 'text',
        placeholder: 'https://storage.googleapis.com/mediapipe-models/pose_landmarker/pose_landmarker_lite/float16/1/pose_landmarker_lite.task'
      },
      {
        key: 'minPoseDetectionConfidence',
        label: 'Min Detection Confidence',
        type: 'number',
        def: 0.5
      },
      {
        key: 'minPoseTrackingConfidence',
        label: 'Min Tracking Confidence',
        type: 'number',
        def: 0.5
      },
      { key: 'segmentation', label: 'Output Segmentation', type: 'select', options: ['false', 'true'], def: 'false' },
      { key: 'outputWorld', label: 'Output World Landmarks', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'maxFPS', label: 'Max FPS', type: 'number', def: 30 },
      { key: 'smoothing', label: 'Smoothing', type: 'select', options: ['true', 'false'], def: 'true' }
    ]
  },
  Pointcloud: {
    title: 'Pointcloud',
    supportsNkn: true,
    relayKey: 'relay',
    inputs: [
      { name: 'image', label: 'Image (Base64)', tooltip: 'Base64 encoded image for depth processing' },
      { name: 'model', label: 'Model ID', tooltip: 'Select model to use for depth estimation' },
      { name: 'download', label: 'Download GLB', tooltip: 'Trigger GLB download' }
    ],
    outputs: [
      { name: 'pointcloud', label: 'Pointcloud Data', tooltip: 'Chunked pointcloud vertices and colors' },
      { name: 'models', label: 'Available Models', tooltip: 'List of available Depth Anything models' },
      { name: 'status', label: 'Status', tooltip: 'Processing status updates' }
    ],
    schema: [
      { key: 'base', label: 'Base URL', type: 'text', placeholder: 'http://127.0.0.1:5000', def: 'http://127.0.0.1:5000' },
      { key: 'relay', label: 'NKN Relay', type: 'text' },
      { key: 'endpointMode', label: 'Endpoint Mode', type: 'select', options: ['auto', 'local', 'remote'], def: 'auto' },
      { key: 'autoLoadModel', label: 'Auto Load Model', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'defaultModel', label: 'Model ID', type: 'select', options: [], def: 'da3nested-giant-large' },
      { key: 'resolution', label: 'Resolution', type: 'number', def: 504, min: 128, max: 2048, step: 8 },
      { key: 'maxPoints', label: 'Max Points', type: 'number', def: 1000000, min: 10000, max: 10000000, step: 10000 },
      { key: 'alignToInputScale', label: 'Align to Input Scale', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'includeConfidence', label: 'Include Confidence', type: 'select', options: ['false', 'true'], def: 'false' },
      { key: 'applyConfidenceFilter', label: 'Apply Confidence Filter', type: 'select', options: ['false', 'true'], def: 'false' },
      { key: 'processResMethod', label: 'Process Res Method', type: 'select', options: ['upper_bound_resize', 'upper_bound_crop', 'lower_bound_resize', 'lower_bound_crop'], def: 'upper_bound_resize' },
      { key: 'inferGs', label: 'Infer GS', type: 'select', options: ['false', 'true'], def: 'false' },
      { key: 'confThreshPercentile', label: 'Confidence Percentile', type: 'number', def: 40, min: 0, max: 100, step: 1 },
      { key: 'showCameras', label: 'Show Cameras', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'featVisFps', label: 'Feature Vis FPS', type: 'number', def: 15, min: 1, max: 60, step: 1 },
      { key: 'chunkSize', label: 'Chunk Size (KB)', type: 'number', def: 800, min: 100, max: 2000, step: 100 },
      { key: 'showViewer', label: 'Show 3D Viewer', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'gridDistance', label: 'Grid Distance', type: 'number', def: 10, min: 3, max: 100, step: 1 }
    ]
  },
  WebSerial: {
    title: 'WebSerial',
    supportsNkn: false,
    inputs: [{ name: 'send', label: 'Send' }],
    outputs: [{ name: 'data', label: 'Data' }],
    schema: [
      { key: 'autoConnect', label: 'Auto Connect', type: 'select', options: ['false', 'true'], def: 'false' },
      { key: 'autoReconnect', label: 'Auto Reconnect', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'rememberPort', label: 'Remember Port', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'rememberDevice', label: 'Remember Device Name', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'baudRate', label: 'Baud Rate', type: 'number', def: 115200 },
      { key: 'encoding', label: 'Encoding', type: 'text', placeholder: 'utf-8', def: 'utf-8' },
      { key: 'writeMode', label: 'Write Mode', type: 'select', options: ['text', 'hex'], def: 'text' },
      { key: 'readMode', label: 'Read Mode', type: 'select', options: ['text', 'hex'], def: 'text' },
      { key: 'appendNewline', label: 'Append Newline', type: 'select', options: ['false', 'true'], def: 'false' },
      { key: 'newline', label: 'Newline', type: 'text', def: '\\n', placeholder: '\\n' },
      { key: 'maxLogLines', label: 'Max Log Lines', type: 'number', def: 500 }
    ]
  },
  Orientation: {
    title: 'Orientation',
    inputs: [],
    outputs: [{ name: 'orientation' }],
    schema: [
      { key: 'format', label: 'Output Format', type: 'select', options: ['raw', 'euler', 'quaternion'], def: 'raw' }
    ]
  },
  Location: {
    title: 'Location',
    inputs: [],
    outputs: [{ name: 'location' }],
    schema: [
      { key: 'format', label: 'Output Format', type: 'select', options: ['raw', 'geohash'], def: 'raw' },
      { key: 'precision', label: 'Precision', type: 'range', min: 1, max: 12, step: 1, def: 6 }
    ]
  },
  MCP: {
    title: 'MCP Server',
    supportsNkn: true,
    relayKey: 'relay',
    inputs: [{ name: 'query' }, { name: 'tool' }, { name: 'refresh' }],
    outputs: [{ name: 'system' }, { name: 'prompt' }, { name: 'context' }, { name: 'resources' }, { name: 'status' }, { name: 'raw' }],
    schema: [
      { key: 'base', label: 'Base URL', type: 'text', placeholder: 'http://127.0.0.1:9003' },
      { key: 'relay', label: 'NKN Relay', type: 'text' },
      { key: 'api', label: 'API Key', type: 'text' },
      { key: 'autoConnect', label: 'Auto Connect', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'connectOnQuery', label: 'Connect on Query', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'protocolVersion', label: 'Protocol Version', type: 'text', def: '2024-05-31' },
      { key: 'clientName', label: 'Client Name', type: 'text', def: 'hydra-graph' },
      { key: 'clientVersion', label: 'Client Version', type: 'text', def: '0.1.0' },
      { key: 'resourceFilters', label: 'Default Resource Tags', type: 'text', placeholder: 'llm,nkn' },
      { key: 'toolFilters', label: 'Default Tools', type: 'text', placeholder: 'hydra.nkn.status' },
      { key: 'augmentSystem', label: 'System Addendum', type: 'textarea', placeholder: 'Supplementary system instructions' },
      { key: 'emitSystem', label: 'Emit System Prompt', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'emitContext', label: 'Emit Context', type: 'select', options: ['true', 'false'], def: 'true' },
      { key: 'timeoutMs', label: 'Timeout (ms)', type: 'number', def: 20000 }
    ]
  }
};

export { GraphTypes };
