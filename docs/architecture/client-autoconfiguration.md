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

An `SMTP` block is included in the XML only when a real client-submission endpoint is explicitly configured through the environment.

For the current implementation, that means an authenticated `LPE-CT` submission listener is actually enabled, preferably on implicit `TLS` port `465`, with certificate and key material configured on `LPE-CT`.

For `IMAP`, the public endpoint is `IMAPS` on `LPE-CT` port `993`. `LPE-CT`
terminates the client `TLS` session with the configured public certificate and
proxies the internal stream to the core `LPE` IMAP adapter.

### Outlook

Minimal Outlook autodiscovery publishes only:

- `ActiveSync`
- URL `https://<public-host>/Microsoft-Server-ActiveSync`

The MVP does not advertise `EWS`.

That choice stays aligned with the `LPE` architecture: the first priority for native Outlook and mobile compatibility is `ActiveSync`.

### JMAP

`JMAP` remains the primary modern protocol. Public `JMAP` access is published by
`LPE-CT` over the HTTPS/WSS client hostname and reverse-proxied to the core
`LPE` adapter.

The externally published paths are:

- `GET /api/jmap/session`
- `POST /api/jmap/api`
- `POST /api/jmap/upload/{accountId}`
- `GET /api/jmap/download/{accountId}/{blobId}/{name}`
- `GET /api/jmap/ws`

The MVP client-autoconfiguration layer only embeds a documentation pointer to
the published `JMAP` session endpoint. The MVP does not yet publish a dedicated
`JMAP` well-known endpoint.

`EmailSubmission/set` must continue to call the canonical `LPE` submission
workflow after loading a draft. It must not hand the message directly to
`SMTP` or to an internal relay.

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
- re-expose `/api/jmap/session`, `/api/jmap/api`, `/api/jmap/upload/{accountId}`, `/api/jmap/download/{accountId}/{blobId}/{name}`, and `/api/jmap/ws` from `LPE-CT` to the core `LPE` service
- publish `IMAPS` on the same hostname when native `IMAP` access is exposed
- publish the authenticated `SMTPS` submission listener only when `LPE-CT` really exposes it
- do not reuse the internal `LPE -> LPE-CT` relay as a client-submission endpoint


