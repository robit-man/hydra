# Meshtastic

## Overview
Interface node for Meshtastic devices, enabling LoRa mesh messaging through Hydra. Manages ports/devices, channels, and JSON vs default message formats.

## Inputs
- `text` — Message to send over the mesh.

## Outputs
- `text` — Received mesh messages.  
- `status` — Device/connection status.

## Key Settings
- `autoConnect`, `channel`, `publicPortName`, `defaultJson`, `rememberPort`, `rememberDevice`.  
- `peers` metadata, `lastPortInfo`, `lastDeviceName`.

## How It Works
- Connects to a Meshtastic device over serial/BLE, joins the specified channel, sends/receives messages, and emits router payloads.

## Basic Use
1) Set `channel` and enable `autoConnect`.  
2) Wire TextInput → `text`; monitor `status`/`text` outputs.  
3) Use the card UI to pick remembered devices when needed.

## Advanced Tips
- `defaultJson=true` wraps messages for structured downstream handling.  
- Persist `rememberPort`/`rememberDevice` for stable reconnects.

## Troubleshooting
- No device: ensure OS permissions for serial/BLE.  
- Wrong channel: confirm IDs match the remote mesh participants.
