# TTS (Text-to-Speech)

## Overview
Speech synthesis node that outputs audio streams for playback and routing. Supports remote `/speak` streaming or file fetch, NKN relay, and optional in-browser WASM Piper for offline voices.

## Inputs
- `text` — Text payload or `{ text, eos }` object.  
- `mute` — Boolean-ish signal to silence output.

## Outputs
- `active` — Boolean signal indicating audio production.  
- `audio` — PCM16 audio packets for downstream devices.

## Key Settings
- Transport: `base`, `relay`, `api`, `endpointMode`, `model`, `mode` (stream/file).
- Audio: `volume`, `filterTokens` (strip tokens before synthesis).
- Signals: `muteSignalMode`, `activeSignalMode`.
- WASM: `wasm`, `wasmVoicePreset` (defaults + custom), `wasmPiperModelUrl`, `wasmPiperConfigUrl`, `wasmSpeakerId`, `wasmThreads`, `wasmCustomVoices`.

## Data Contracts
- Inputs: `text` accepts string or `{ text, eos?, type? }`. `mute` accepts boolean-ish values.  
- Outputs: `active` boolean; `audio` packets `{ type:'audio', format:'pcm16', sampleRate, channels, data:int16[], sequence?, timestamp?, samples? }`.  
- Endpoint request (stream): `{ text, mode:'stream', format:'raw', voice/model }`; (file) `{ text, mode:'file', format:'ogg' }`.

## How It Works
- Stream mode: POSTs `/speak` with `mode=stream`, reads PCM16 chunks (HTTP or NKN), feeds an internal ScriptProcessor for playback/visualization, and emits audio packets.  
- File mode: requests an OGG file, plays in the hidden audio element, and sets `active` during playback.  
- WASM mode: loads Piper ONNX + config via cached fetch, phonemizes text, synthesizes locally, and routes Float32 audio through the same output chain.

## Basic Use
1) Set `base`/`model` (or enable `wasm` for offline).  
2) Wire LLM/TextInput `final` → `text`.  
3) Leave `mode=stream` for real-time playback; use `file` when full clips are acceptable.  
4) Adjust `volume` slider on the card; use `filterTokens` to remove prefixes (e.g., “#”).

## Advanced Tips
- Relay-aware: transport badge reflects NKN state; `endpointMode=local` forces HTTP.  
- WASM speakers are discoverable in settings; set `wasmSpeakerId` per voice.  
- The oscilloscope reflects actual output; `active` toggles off when the buffer drains.  
- PCM16 packets include `sequence` and `timestamp` for downstream synchronization.
- Custom WASM voices: add ONNX/model+config URLs in settings; they are stored with the graph. Custom entries appear above default Piper presets. A device-memory check prevents loading very large models on low-memory devices (e.g., phones); fall back to remote or smaller voices if blocked.

## Routing & Compatibility
- Upstream: LLM/TextInput/Template outputs; supply strings or `{ text }`.  
- Downstream: Audio-capable consumers (Smart Objects) that accept PCM16 packets; TextDisplay for status only (no audio playback).  
- Do not feed audio packets back into TTS; it only consumes text.

## Signals & Router
- `active` respects mute state; in PCM stream mode each chunk also emits on `audio`.  
- `mute` input drops gain immediately and deactivates `active`.

## Troubleshooting
- Silence: ensure `text` is non-empty after filters and check relay/API reachability.  
- Choppy audio: lower network latency, prefer HTTP for LAN, or switch to WASM.  
- WASM slow: choose smaller Piper models or reduce `wasmThreads` on low-core devices.
