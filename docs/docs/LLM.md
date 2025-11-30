# LLM (Language Model)

## Overview
Text generation and chat node that streams deltas or final completions from a remote LLM endpoint (Ollama/OpenAI-compatible). Handles model discovery, chat memory, and optional system prompts.

## Inputs
- `prompt` — Primary user text or message object.
- `image` — Optional vision payloads where supported.
- `system` — External system override.

## Outputs
- `delta` — Streaming tokens/patches.
- `final` — Completed message with metadata.
- `memory` — Memory update notices.

## Key Settings
- Transport: `base`, `relay`, `api`, `endpointMode`, `model`, `stream`.
- Prompts: `useSystem`, `system`.
- Memory: `memoryOn`, `persistMemory`, `maxTurns`, `memory` (editable in modal).
- Extras: `capabilities` (auto-populated per model), `think`, `tools`.

## Data Contracts
- Inputs: `prompt` accepts string or `{ text, system?, image?, tool_call? }`; non-string inputs are JSON-stringified when needed.  
- Outputs: `delta`/`final` emit `{ type:'text', text, final?:boolean, meta? }`; memory notifications on `memory` port are `{ type:'updated', size }`.  
- Endpoint bodies follow chat/completion schemas used by the configured backend; `model` must match discovery listing.

## How It Works
- Fetches `/api/tags` and `/v1/models` to list models.  
- Sends chat/completion requests (stream or one-shot) via HTTP or NKN relay.  
- Applies system prompt and optional memory stitching based on settings.  
- Emits deltas as they arrive, aggregates final text, and updates memory store.

## Basic Use
1) Set `base` and select a `model`.  
2) Toggle `stream` on for interactive updates.  
3) Wire ASR/TextInput → `prompt`, wire `final` → TTS or display.  
4) Use the settings modal to enable `useSystem` or chat memory if desired.

## Advanced Tips
- `endpointMode=local` forces direct HTTP even if a `relay` is set.  
- Memory manager in settings supports reordering, editing, and clearing turns.  
- `capabilities` determine dynamic ports (e.g., tools/functions).  
- When relaying, watch the transport badge for NKN health.

## Routing & Compatibility
- Upstream: ASR `final`, TextInput, Template outputs. Prefer `{ text }` objects for clarity.  
- Downstream: TTS `text`, TextDisplay, LogicGate.  
- Images: send `{ image: <b64 or payload>, text }` to multimodal models only.  
- Avoid feeding binary or audio packets; ensure payloads are text or objects with `text` keys.

## Signals & Router
- Emits `{ type: 'text', text, final? }` payloads; includes metadata when available.  
- `delta` is suitable for live UIs; `final` should drive downstream actions.

## Troubleshooting
- Empty model list: verify `base`, `api`, and relay reachability.  
- Slow streams: reduce context by trimming memory or disabling `stream`.  
- Tool mismatch: ensure downstream tool schemas match the model’s capability list.
