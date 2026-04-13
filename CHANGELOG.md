# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- GitHub Actions CI workflow
- GitHub Actions release workflow with cross-platform builds and SHA256 checksums
- Public binary version reporting via `-version`
- Optional RFC 1929 username/password auth for the local SOCKS5 proxy

### Changed
- Project renamed to `jazztun`
- Module path updated to `github.com/Kavun-Sama/jazztun`
- README rewritten for the public release and release downloads

## [0.2.0] - 2026-04-14

### Added
- Per-stream mux acknowledgements, replay, and duplicate suppression
- Replay-on-reconnect integration between the tunnel layer and mux
- End-to-end blackout validation for long-running transfers

### Changed
- Stream shutdown now uses half-close and delayed CLOSE delivery to avoid truncated responses
- WebRTC transport no longer treats transient `Disconnected` states as fatal teardown
- Aggregate throughput with `-peers 6` and eight parallel downloads now reaches about `110 Mbit/s` in live testing

### Notes
- Active transfers now survive the tested blackout/reconnect scenario in live end-to-end runs
- Full `WatchConnection` session teardown and recreation is covered by unit tests; the live reconnect run recovered on the same WebRTC session rather than a full peer rebuild

## [0.1.0] - 2026-04-14

### Added
- Initial public release
- SOCKS5 `CONNECT` proxy over Salute Jazz WebRTC DataChannels
- AES-256-GCM frame encryption
- Credit-based mux flow control
- Multiple transport peers via `-peers N`
- Optional RFC 1929 username/password auth for the local SOCKS5 proxy
