# Template

## Overview
String templating node that replaces `{variable}` placeholders with values from incoming payloads or the configured variable map.

## Inputs
- `text` — Source object providing values for placeholders (merged with configured variables).

## Outputs
- `text` — Resulting templated string.

## Key Settings
- `template` — Base string with `{name}` placeholders.  
- `variables` — Key/value map used when incoming payload lacks a key.

## Data Contracts
- Inputs: strings or objects; objects are used for lookup by key.  
- Outputs: `{ type:'text', text }` with the rendered string.  
- Missing keys remain unchanged in the output.

## How It Works
- Extracts placeholders via a regex, merges incoming payload fields with stored variables, substitutes each occurrence, and emits the final string.

## Basic Use
1) Set `template`, e.g., `Hello {name}, today is {day}.`  
2) Wire an upstream object or text payload to supply `{name}`/`{day}`.  
3) The output becomes the filled string for downstream nodes.

## Advanced Tips
- Combine with LogicGate to branch on missing variables.  
- Use TextInput’s custom fields to emit the right keys for templating.

## Troubleshooting
- Unresolved placeholders: ensure keys exist in `variables` or incoming payload.  
- Extra braces: braces must be balanced; unsupported nesting is left as-is.
