# TextDisplay

## Overview
Lightweight sink node to show text payloads on the card. Useful for debugging, final outputs, or monitoring upstream nodes.

## Inputs
- `text` — Any payload; stringified for display.

## Outputs
None.

## Key Settings
No configuration fields.

## Data Contracts
- Accepts any payload; non-string values are stringified for display.  
- Does not emit router outputs; display-only.

## How It Works
- Receives payloads, renders them in a monospace block on the card, and persists the last text in local storage.

## Basic Use
1) Wire any text-bearing port into `text`.  
2) Watch the card for updates as data flows through your graph.

## Advanced Tips
- Pair with LogicGate or LLM outputs to confirm routing before wiring actuators.

## Troubleshooting
- Nothing showing: ensure the upstream port is emitting and the wire is connected.
