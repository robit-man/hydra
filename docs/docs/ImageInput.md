# ImageInput

## Overview
Simple node for injecting images into the graph. Accepts drag/drop, paste, or file pick, converts to base64, and emits image metadata for downstream vision or logging nodes.

## Inputs
None (user-driven within the card).

## Outputs
- `image` — Payload `{ mime, dataUrl, b64, width, height, ts }`.

## Key Settings
No configurable fields.

## How It Works
- Renders a drop/paste zone on the card.  
- Reads the first image file, stores it in local storage for the node, and emits a structured image payload through the router.

## Basic Use
1) Drag an image onto the card or click to browse.  
2) Wire the `image` output to an LLM with vision, a display node, or storage.  
3) Drop a new image anytime to re-emit and update the saved preview.

## Advanced Tips
- Paste from clipboard (Ctrl/Cmd+V) to quickly swap images.  
- The payload includes `nodeId` so downstream logic can trace sources.

## Troubleshooting
- Unsupported file: ensure the file is an image (png/jpg/webp).  
- Large images: resize upstream to reduce payload sizes for relays.
