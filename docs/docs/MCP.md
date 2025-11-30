# MCP (Model Context Protocol Client)

## Overview
Connects to MCP servers to query resources/tools and stream responses into the graph. Handles connection lifecycle, filtering, and timeouts.

## Inputs
- `prompt` — Query payload for the MCP server.

## Outputs
- `delta`/`final`-style responses (per server capabilities).  
- Status and context updates on the card.

## Key Settings
- Transport: `base`, `relay`, `api`, `endpointMode`.  
- Connection: `autoConnect`, `connectOnQuery`, `protocolVersion`, `clientName`, `clientVersion`, `timeoutMs`.  
- Filters: `resourceFilters`, `toolFilters`, `emitSystem`, `emitContext`, `augmentSystem`.

## Data Contracts
- Inputs: `prompt` strings or `{ text }`.  
- Outputs: streamed responses shaped by the MCP server; typically `{ type:'text', text, final? }` plus status/context updates.  
- Connection metadata includes resource/tool lists; filter strings accept comma-separated identifiers.

## How It Works
- Connects to the MCP server endpoint, lists available resources/tools, and issues queries.  
- Applies filter strings to narrow the advertised capabilities.  
- Emits responses and context snapshots via router outputs and card UI.

## Basic Use
1) Set `base`/`relay` to your MCP server.  
2) Enable `autoConnect` or let `connectOnQuery` trigger on first prompt.  
3) Wire TextInput/LLM → `prompt`; consume outputs downstream.

## Advanced Tips
- Adjust `timeoutMs` for long-running tool calls.  
- Use `augmentSystem` to prepend context to LLM prompts that wrap MCP results.  
- Keep `resourceFilters`/`toolFilters` narrow for busy servers.

## Troubleshooting
- Connection refused: verify server URL and protocol version.  
- Missing tools: check filters and server-side ACLs.  
- Stalls: increase `timeoutMs` or disable `connectOnQuery` to prewarm sessions.
