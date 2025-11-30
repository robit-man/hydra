# Routing & Data Guide

Technical overview of how node data flows, payload shapes, and compatibility expectations across Hydra.

## Payload Conventions
- **Text payloads**: Most text-carrying ports use `{ type: 'text', text, final?: boolean, eos?: boolean }`. When sending plain strings, downstream nodes coerce to `text`.  
- **Audio packets**: `format: 'pcm16'`, `sampleRate`, `channels`, `data` (Array of int16) and optional `sequence`, `timestamp`, `samples`. ASR expects mono 16 kHz; TTS emits at its configured sample rate.  
- **Images**: `{ mime, dataUrl, b64, width, height, ts, nodeId }`.  
- **Signals**: `active`/`mute` ports are boolean-ish (string/number/object coercible to boolean). `activeSignalMode`/`muteSignalMode` toggle between `true/false` vs `true/empty`.
- **Transport keys**: `base` (http[s]://host:port), `relay` (NKN address), `api` (bearer key), `endpointMode` (`auto`/`remote`/`local`).

## Typical Pipelines
- **Voice chat**: ASR (`final`) → LLM (`prompt`) → TTS (`text`). Keep ASR/TTS sample rates consistent; LLM expects text, not audio.  
- **Command/control**: TextInput → LogicGate → LLM or NoClip/NknDM. Use structured outputs (`outputMode=object`) when routing actions.  
- **Multimodal**: ImageInput → LLM (`image`) with prompt text; TTS for responses.  
- **Automation**: TextInput → WebScraper (`action`/`url`) → TextDisplay or LogicGate.  
- **Sensors**: Orientation/Location → LogicGate or NoClipBridge for remote control.

## Compatibility Notes
- **Wrapped vs raw text**: LLM/ASR/TTS accept both, but prefer `{ text }` objects for clarity. WebScraper actions and selectors should be objects with `text` when emitted from TextInput using action modes.  
- **Audio**: Only ASR consumes audio input; TTS produces audio output. Do not feed TTS audio into ASR unless it is speech.  
- **Binary/hex**: WebSerial can emit hex/raw; downstream nodes must understand encoding.  
- **NKN/relay-only nodes**: MediaStream, NknDM, NoClipBridge rely on peer addresses; ensure `relay` set when endpointMode is `auto/remote`.  
- **WASM modes**: ASR/TTS WASM bypass transport; relay controls are hidden. Models download via CDN and cache in IndexedDB.

## Endpoint Keys & Nuances
- `base`: Trim trailing slashes; required for model discovery and HTTP routes.  
- `relay`: Used when `endpointMode` allows relay. Empty relay forces direct HTTP.  
- `api`: Adds Authorization headers; leave blank if not required.  
- `endpointMode`: `local` ignores relay even if set; `remote` requires relay; `auto` uses relay when provided.  
- Model selectors (`model`, `prevModel`, `wasm*`): Persist per node; ensure the chosen backend actually serves the name.

## Wiring Examples
- **Inject node from docs**: Use the docs modal “Insert” button to spawn a node near the viewport center.  
- **ASR external audio**: Send PCM16 frames `{ format:'pcm16', sampleRate:16000, data:[...] }` to ASR `audio`; it will reuse VAD and streaming logic.  
- **WebScraper action**: `{ action:'click', selector:'#btn', text:'ok' }` → `action` port; `url` only needed for navigation.  
- **LogicGate path checks**: Use `path` like `data.text` when upstream payloads wrap fields.

## Incompatibilities & Gotchas
- Do not feed audio or binary data into LLM/TTS text ports; sanitize upstream payloads.  
- WebSerial raw/hex outputs are not JSON; route to TextDisplay or custom logic, not LLM, unless converted.  
- Frame outputs from WebScraper can be large; avoid routing to nodes that expect plain text.  
- Payments/MediaStream are transport/UI driven and do not emit standard router outputs beyond status; plan wiring accordingly.
