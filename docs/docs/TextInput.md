# TextInput

## Overview
Composer node for injecting text or structured payloads into the graph. Supports action tagging, schema shaping, and previewing outgoing messages.

## Inputs
- `incoming` — Optional upstream payload to merge or replace in the composer.

## Outputs
- `text` — Formatted payload (object or plain text) based on settings.

## Key Settings
- UX: `placeholder`, `autoSendIncoming`, `incomingMode` (replace/append/ignore), `previewMode`.  
- Structuring: `emitActionKey`, `actionValue`, `outputMode`, `includeNodeId`, `nodeIdKey`, `typeKey`, `typeValue`, `typeBackupKey`, `includeText`, `textKey`.  
- Custom fields: `customFields` map with value modes (literal/text/incoming/raw/action/json/number/boolean/nodeId/timestamp/template).

## Data Contracts
- Outputs: when `outputMode=object`, emits an object composed from configured keys. Example default: `{ type:'text', text, nodeId }`. When `outputMode=text`, emits raw string.  
- `incoming` payloads can be strings or objects; `incomingMode` controls merge vs replace.  
- Action fields: if `emitActionKey=action`, outgoing payload includes `{ action: actionValue }` unless already present.

## How It Works
- Maintains a local composer value (and last incoming text).  
- Shapes outgoing payload according to selected keys and modes, emitting via `Router.sendFrom`.

## Basic Use
1) Type in the card or supply `incoming` from upstream to prefill.  
2) Choose `outputMode=object` for structured prompts, or `text` to forward raw strings.  
3) Set `emitActionKey=action` with `actionValue=type/click/etc.` when driving control nodes.

## Advanced Tips
- Custom fields with `template` allow lightweight templating using `{placeholders}`.  
- `includeNodeId` adds provenance to downstream consumers.  
- `previewMode=compact` keeps the on-card preview tight for complex payloads.

## Routing & Compatibility
- Upstream: feed `incoming` from ASR/LLM outputs to prefill before emitting.  
- Downstream: LLM `prompt`, WebScraper actions, LogicGate, TTS text, or any node expecting `{ text }`.  
- For WebScraper, set `emitActionKey` and add `selector`/`text` custom fields to match scraper expectations.

## Troubleshooting
- Missing text: ensure `includeText=true` and `textKey` set.  
- Wrong merge: check `incomingMode` and `autoSendIncoming` if upstream traffic overwrites your composer.
