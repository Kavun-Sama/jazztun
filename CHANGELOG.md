# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- GitHub Actions CI workflow
- GitHub Actions release workflow with cross-platform builds and SHA256 checksums
- Public binary version reporting via `-version`

### Changed
- Project renamed to `jazztun`
- Module path updated to `github.com/Kavun-Sama/jazztun`
- README rewritten for the public release and release downloads

## [0.1.0] - 2026-04-14

### Added
- Initial public release
- SOCKS5 `CONNECT` proxy over Salute Jazz WebRTC DataChannels
- AES-256-GCM frame encryption
- Credit-based mux flow control
- Multiple transport peers via `-peers N`
