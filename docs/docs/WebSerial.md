# WebSerial

## Overview
Serial port bridge for communicating with connected hardware directly from the browser. Supports configurable baud rate, encodings, newline handling, and log limits.

## Inputs
- `text` — Data to send over serial (string or bytes depending on mode).

## Outputs
- `text` — Received serial data (decoded per settings).  
- `status` — Port status/metadata.

## Key Settings
- Connection: `autoConnect`, `autoReconnect`, `rememberPort`, `rememberDevice`, `baudRate`, `customBaud`.  
- Encoding: `encoding`, `appendNewline`, `newline`, `writeMode` (text/hex/raw), `readMode` (text/hex/raw), `echoSent`.  
- Logging: `maxLogLines`.

## Data Contracts
- Inputs: strings or hex/raw buffers depending on `writeMode`; newline appended when enabled.  
- Outputs: decoded strings when `readMode=text`; hex strings or raw ArrayBuffers otherwise.  
- Router emits payloads as text unless `readMode` is raw/hex, in which case downstream nodes must decode accordingly.

## How It Works
- Requests a serial port, applies baud/encoding, streams read data to the router, and writes outbound payloads from the `text` input.  
- Reconnects automatically when enabled and remembers last used devices/ports.

## Basic Use
1) Select/open a port from the card UI.  
2) Set `baudRate` or `customBaud` as needed.  
3) Send payloads via TextInput; watch returned data on the card or downstream.

## Advanced Tips
- Use `writeMode=hex` to send byte sequences; `readMode=hex` to inspect binary protocols.  
- `appendNewline` + configurable `newline` eases line-oriented devices.  
- Limit `maxLogLines` to avoid heavy UIs during long sessions.

## Troubleshooting
- No ports listed: browser must support Web Serial and user must grant permission.  
- Garbled text: verify encoding/baud and newline settings.  
- Disconnect loops: disable `autoReconnect` while debugging.
