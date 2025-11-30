# Payments

## Overview
Handles simple on-chain payment flows (sell/buy) using configured asset, chain, and receiver details. Emits status to downstream nodes or UI.

## Inputs
None (driven from the card controls).

## Outputs
- Status/log updates regarding payment initiation/completion.

## Key Settings
- `mode` — seller/buyer.  
- `amount`, `asset`, `chainId`, `receiver`, `memo`.  
- `unlockTTL` — Lock expiry (seconds).

## Data Contracts
- Inputs: set via UI; no graph payloads required.  
- Outputs: status strings describing payment state; no numeric/binary router payloads.

## How It Works
- Prepares a payment intent with the provided asset and chain, initiates via the Payments helper, and tracks completion/expiry.

## Basic Use
1) Set `mode` and on-chain parameters (`asset`, `chainId`, `receiver`, `amount`).  
2) Initiate from the card; monitor status text.  
3) Connect downstream to notify or gate actions once payment clears.

## Advanced Tips
- Use `memo` to tag sessions.  
- `unlockTTL` protects locked flows; adjust for your settlement speed.  
- Pair with LogicGate to branch on success/failure events.

## Troubleshooting
- Stuck pending: verify chain RPC availability and correct `chainId`.  
- Wrong asset: double-check `asset` address/symbol before sending.
