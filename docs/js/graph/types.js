const GraphTypes = {
  ASR: {
    title: 'ASR',
    supportsNkn: true,
    relayKey: 'relay',
    inputs: [{ name: 'mute', label: 'Mute' }],
    outputs: [{ name: 'partial' }, { name: 'phrase' }, { name: 'final' }, { name: 'active', label: 'Active' }],
    schema: [
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
    outputs: [{ name: 'active', label: 'Active' }],
    schema: [
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
  ImageInput: {
    title: 'Image Input',
    inputs: [],
    outputs: [{ name: 'image' }],
    schema: []
  },
  TextInput: {
    title: 'Text Input',
    inputs: [],
    outputs: [{ name: 'text' }],
    schema: [
      { key: 'placeholder', label: 'Placeholder', type: 'text', placeholder: 'Type a message…' }
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
      { key: 'defaultJson', label: 'Default JSON Output', type: 'select', options: ['true', 'false'], def: 'true' }
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
