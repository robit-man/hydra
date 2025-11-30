# Orientation

## Overview
Streams device orientation data (mobile/VR capable) into the graph for motion-driven experiences.

## Inputs
None; listens to device motion/orientation events.

## Outputs
- Orientation payloads (raw/quaternion depending on consumer) to downstream nodes.

## Key Settings
- `format` — Output style (`raw` currently).  
- `running` — Runtime flag (toggled from the card).

## Data Contracts
- Outputs: orientation payloads `{ alpha, beta, gamma, absolute?, ts }` (numbers in degrees).  
- No inputs; router receives JSON objects only.

## How It Works
- Subscribes to browser `deviceorientation` events, normalizes values, and emits them through the router at runtime.

## Basic Use
1) Press ▶ on the card to start streaming.  
2) Wire outputs to visualization or control nodes that consume orientation data.

## Advanced Tips
- Some browsers require HTTPS and explicit permission for motion sensors.  
- Combine with LogicGate to threshold tilt/rotation events.

## Troubleshooting
- No data: ensure motion permissions granted and device supports orientation events.
