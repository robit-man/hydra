# NoClipBridgeSync

## Overview
Synchronizes Hydra workspace state across peers using the NoClip bridge. Handles invites, approvals, and session status updates.

## Inputs/Outputs
- Operates via UI controls; does not expose router ports. Workspace updates are transported over the bridge.

## Key Settings
- Managed internally via sync dialogs; no per-node schema fields.

## Data Contracts
- Exchanges workspace snapshots (graph JSON) and status messages over NoClip. No text/audio router payloads are emitted.

## Usage
- Trigger share/request from the sync UI. Invites require approval. The bridge handles identity and session state to merge or replace workspaces.
