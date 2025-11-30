# FaceLandmarks

## Overview
Runs on-device face landmark detection (MediaPipe Tasks) to emit 3D/2D landmarks, blendshapes, and head pose data for AR/analysis flows.

## Inputs
- `image` (implicit from camera or upstream Vision sources).

## Outputs
- Landmarks and metadata routed to connected nodes (e.g., Vision viewer).

## Key Settings
- `delegate` (GPU/CPU), `numFaces`, `outputBlendshapes`, `outputWorld`, `outputHeadPose`, `runningMode` (VIDEO), `modelAssetPath`, `maxFPS`, `smoothing`, confidence thresholds.

## How It Works
- Loads the configured MediaPipe model, processes frames at `maxFPS`, and streams landmark arrays plus optional blendshapes/world coordinates.

## Basic Use
1) Keep defaults for fast GPU inference.  
2) Wire from MediaStream or Vision camera source.  
3) Consume outputs in Vision viewer or downstream analytics.

## Advanced Tips
- Increase `maxFPS` cautiously; GPU delegates perform best.  
- Disable `outputWorld` if you only need 2D to save bandwidth.  
- Use `smoothing` for stable AR overlays.

## Troubleshooting
- No faces: check lighting and confidence thresholds.  
- Slow: lower `maxFPS` or switch to CPU on incompatible GPUs.
