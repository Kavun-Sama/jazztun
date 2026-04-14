# Security Policy

## Supported Versions

Security fixes are applied to the latest released series and to `main`.

| Version | Supported |
| --- | --- |
| `0.2.x` | Yes |
| older versions | No |

## Reporting A Vulnerability

Please do not open public GitHub issues for security problems.

Preferred order:

1. Open a private GitHub security advisory, if that option is enabled for the repository.
2. If private reporting is not available, contact the maintainer directly at [t.me/kkkavun](https://t.me/kkkavun) and include:
   - affected version or commit
   - impact
   - reproduction steps
   - logs or packet captures if they are safe to share

I will acknowledge the report as quickly as possible and coordinate a fix before public disclosure.

## Practical Security Notes

- Treat the room URL, the tunnel key, and the optional `-session` value as secrets.
- If the local SOCKS5 proxy listens on anything other than `127.0.0.1`, enable `-socks-user` and `-socks-pass`.
- A user who learns the room URL can still enter the Jazz room, but cannot become a valid `jazztun` peer without the shared key and the matching session namespace.
- Use a server outside Russia if you expect the tunnel to reach resources blocked from inside Russia.
