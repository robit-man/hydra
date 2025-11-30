# ASR (Automatic Speech Recognition)

## Overview
Speech-to-text node that can stream or batch audio into partial/phrase/final transcripts. Supports remote HTTP/NKN backends as well as in-browser WASM Whisper for offline use.

## Inputs
- `mute` — Boolean-ish signal to gate microphone/uplink.
- `audio` — External audio packets (`pcm16` or `float32` with sampleRate) for non-mic ingestion.

## Outputs
- `partial` — Rolling text as words arrive.
- `phrase` — Stable chunks during voice activity.
- `final` — Committed transcription with `eos`.
- `active` — Activity boolean (mode depends on `activeSignalMode`).

## Key Settings
- Transport: `base`, `relay`, `api`, `endpointMode` (auto/remote/local), `model`.
- Mode: `mode` (fast/accurate), `live` (streaming), `chunk`, `rate`, `prevModel/Win/Step`.
- VAD: `rms`, `hold`, `silence`, `emaMs`.
- Output: `phraseOn`, `phraseMin`, `phraseStable`, `prompt`, `muteSignalMode`, `activeSignalMode`.
- WASM: `wasm`, `wasmWhisperModel`, `wasmThreads`.

## How It Works
- Remote: opens `/recognize/stream` session, posts PCM16 chunks, listens for NDJSON events or NKN relay, aggregates partials/phrases, and finalizes `/recognize`.
- Batch: `finalizeOnce` uploads a WAV when `live` is false or on manual finalize.
- External audio: `onAudio` resamples incoming packets and feeds the same VAD/streaming path.
- WASM: captures/resamples audio to 16 kHz, runs Whisper (Transformers.js, ONNX Runtime WASM), and emits partial/final locally.

## Basic Use
1) Set `base` to your ASR service (or enable `wasm` for offline).  
2) Choose `model` and tune `rms/hold/silence` for your mic.  
3) Wire `final` → downstream prompt, `partial` for live UI, `active` for gating.  
4) Press ▶ on the card to start/stop listening.

## Advanced Tips
- Use `endpointMode=local` to disable relay even if `relay` is set.  
- Phrase mode (`phraseOn`) aggregates rolling text for chat-friendly chunks.  
- Preview decoding uses `prevModel`/`prevWin`/`prevStep` when supported by the backend.  
- In WASM mode, model pulls are cached in IndexedDB; `wasmThreads` tunes CPU usage.

## Signals & Router
- `muteSignalMode`/`activeSignalMode` control whether signals send booleans or empties.  
- `active` fires when voice is detected or uplink is open; mute immediately drops it.  
- Outputs include `{ type: 'text', text, final? }` payloads downstream.

## Troubleshooting
- No transcripts: verify mic permission, `base` URL reachability (if remote), and `model` availability.  
- Choppy phrases: increase `chunk` or reduce `rms`.  
- Relay stuck: clear `relay`, set `endpointMode=local`, or flip `wasm` to bypass network.  
- WASM load slow: use `whisper-tiny/base` and lower `wasmThreads` on low-core devices.
