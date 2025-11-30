# LogicGate

## Overview
Conditional router that evaluates incoming payloads against rule sets and emits boolean/text outputs on configured ports. Useful for branching flows without writing code.

## Inputs
- `trigger` (default) — Payload to test. Custom ports are added per rule definitions.

## Outputs
- User-defined outputs (true/false/message) based on rule evaluation.

## Key Settings
- `rules` — Array of rules with: `input`, `path`, `operator`, `compareValue`, `outputTrue`, `outputFalse`, `trueMode/falseMode` (message/boolean), `label`.

## Data Contracts
- Inputs: any payload; when `path` is set, the gate drills into object paths (dot notation).  
- Outputs: if `trueMode/falseMode` is `message`, emits strings; if `boolean`, emits `true`/`false`. Ports are named per `outputTrue`/`outputFalse`.

## How It Works
- Each rule inspects a selected input, optionally drills into a JSON path, applies the operator (truthy/equals/lt/gt/includes/etc.), and emits on the configured true/false output ports or message bodies.

## Basic Use
1) Add/adjust rules in settings.  
2) Wire the relevant input(s) and connect outputs to downstream nodes.  
3) Send payloads; watch the card to verify which branch fired.

## Advanced Tips
- Use `path` to target nested fields (dot notation).  
- `trueMode=falseMode=message` lets you emit literal strings instead of booleans.  
- Duplicate the node for multi-branch flows rather than overloading a single rule list.

## Troubleshooting
- No output: confirm the input port name matches the rule.  
- Unexpected branch: check operator and compareValue type (numbers vs strings).
