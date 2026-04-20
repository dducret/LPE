# Client Autoconfiguration

### Goal

This document describes the client auto-configuration endpoints published by `LPE` for the MVP.

The guiding principle is strict: publish only what is actually implemented and exposed.

### Published endpoints

- `GET /autoconfig/mail/config-v1.1.xml`
- `GET /.well-known/autoconfig/mail/config-v1.1.xml`
- `GET /autodiscover/autodiscover.xml`
- `POST /autodiscover/autodiscover.xml`
- `GET /Autodiscover/Autodiscover.xml`
- `POST /Autodiscover/Autodiscover.xml`

Without a reverse proxy, these routes are exposed directly by the Rust `LPE` service.

With the documented Debian reverse proxy, those routes are published as-is by `nginx` and should then be re-exposed by `LPE-CT` on the public client hostname.

### Thunderbird

Thunderbird autoconfig publishes:

- `IMAP` against the configured public host
- port `993` by default
- implicit `SSL`
- `password-cleartext` authentication inside TLS
- username `%EMAILADDRESS%`

By default, no client `SMTP` submission endpoint is advertised.

That omission is intentional: the repository currently publishes the internal `LPE -> LPE-CT` relay and a minimal perimeter `SMTP` listener on the `LPE-CT` side, but it does not yet expose an authenticated client-submission endpoint such as `465` or `587`.

An `SMTP` block is included in the XML only when a real client-submission endpoint is explicitly configured through the environment.

### Outlook

Minimal Outlook autodiscovery publishes only:

- `ActiveSync`
- URL `https://<public-host>/Microsoft-Server-ActiveSync`

The MVP does not advertise `EWS`.

That choice stays aligned with the `LPE` architecture: the first priority for native Outlook and mobile compatibility is `ActiveSync`.

### JMAP

`JMAP` remains the primary modern protocol, but the MVP client-autoconfiguration layer only adds a documentation pointer to the published `JMAP` session endpoint:

- `GET /api/jmap/session`

The MVP does not yet publish a dedicated `JMAP` well-known endpoint.

### Environment variables

- `LPE_PUBLIC_SCHEME`, default `https`
- `LPE_PUBLIC_HOSTNAME`, optional; by default the public host is inferred from `Host` or `X-Forwarded-Host`
- `LPE_AUTOCONFIG_IMAP_HOST`, optional
- `LPE_AUTOCONFIG_IMAP_PORT`, default `993`
- `LPE_AUTOCONFIG_SMTP_HOST`, optional; enables the published `SMTP` block
- `LPE_AUTOCONFIG_SMTP_PORT`, default `465`
- `LPE_AUTOCONFIG_SMTP_SOCKET_TYPE`, default `SSL`
- `LPE_AUTODISCOVER_ACTIVESYNC_URL`, optional
- `LPE_AUTOCONFIG_JMAP_SESSION_URL`, optional

### Recommended DNS and HTTP publication

For a domain `example.test`:

- publish `autoconfig.example.test` or `mail.example.test` toward the public `LPE-CT` front end
- publish `autodiscover.example.test` or reuse `mail.example.test` toward the same front end
- re-expose the `/autoconfig/...`, `/.well-known/autoconfig/...`, `/autodiscover/...`, `/Autodiscover/...`, and `/Microsoft-Server-ActiveSync` routes over HTTPS
- publish `IMAPS` on the same hostname when native `IMAP` access is exposed
- do not reuse the internal `LPE -> LPE-CT` relay as a client-submission endpoint


