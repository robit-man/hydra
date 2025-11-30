# PoseLandmarks

## Overview
On-device body pose estimator using MediaPipe. Emits landmark coordinates and optional world/segmentation data for motion analysis or control.

## Inputs
- Video frames from camera/MediaStream/Vision (implicit wiring).

## Outputs
- Pose landmarks/world coordinates streamed to connected nodes.

## Key Settings
- `delegate` (GPU/CPU), `runningMode` (VIDEO), `modelAssetPath`, `minPoseDetectionConfidence`, `minPoseTrackingConfidence`, `segmentation`, `outputWorld`, `maxFPS`, `smoothing`.

## How It Works
- Loads the configured pose model, processes frames up to `maxFPS`, and emits landmark sets; optional segmentation mask when enabled.

## Basic Use
1) Provide a video source via MediaStream/Vision.  
2) Leave defaults for quick start; enable `outputWorld` for 3D use cases.  
3) Wire outputs to analytics or animation targets.

## Advanced Tips
- Lower `maxFPS` on low-end devices.  
- Disable `segmentation` if you only need joints to reduce compute.  
- Tweak detection/tracking confidences for challenging scenes.

## Troubleshooting
- Inconsistent tracking: increase lighting or adjust confidence thresholds.  
- High CPU: switch delegate to GPU if available or reduce `maxFPS`.
