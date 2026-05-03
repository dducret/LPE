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
- `OPTIONS /EWS/Exchange.asmx`
- `POST /EWS/Exchange.asmx`
- `OPTIONS /ews/exchange.asmx`
- `POST /ews/exchange.asmx`

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

### Outlook for Windows desktop

Minimal Outlook autodiscovery publishes:

- `IMAP` against the configured public host
- port `993` by default
- `SSL` enabled
- username equal to the discovered email address

An `SMTP` protocol block is included only when a real authenticated client-submission endpoint is explicitly configured through the same environment variables used by Thunderbird autoconfig.

The MVP does not advertise `MAPI` or `MobileSync` for Outlook desktop. Outlook for Windows desktop must not be forced to use `ActiveSync` as an Exchange account.

The `0.1.3` `EWS` endpoint is the Exchange-style compatibility focus for mailbox, contacts, and calendar synchronization. Autodiscovery publishes it only when `LPE_AUTOCONFIG_EWS_ENABLED` is explicitly set to a true value. This keeps `EWS` publication an administrator choice until the deployment accepts the current MVP limits.

When `EWS` autodiscovery is enabled, the POX response publishes the configured `/EWS/Exchange.asmx` URL through a `WEB` protocol block with an `ASUrl`. This gives EWS-aware clients such as Thunderbird a discovery path without advertising top-level `EXCH` or `EXPR` mailbox protocols that Outlook for Windows desktop treats as a full Exchange/MAPI route.

Autodiscover responses include the POX `Response`, `User`, `Account`, and `Protocol` shape expected by Microsoft clients. The request parser accepts both unprefixed and namespace-prefixed request elements, including the `a:EMailAddress` form used by Microsoft connectivity tooling.

Autodiscover also accepts SOAP `GetUserSettings` requests and returns an Exchange-style SOAP response with `ExternalEwsUrl`, `InternalEwsUrl`, mailbox-server identity, user identity, deployment id, and `EwsSupportedSchemas`. The SOAP path publishes the same opt-in `EWS` endpoint and does not advertise `MAPI`, `RPC`, client `SMTP` submission, or any unsupported Exchange surface.

When the request asks for the `mobilesync` response schema, Autodiscover returns an `ActiveSync`-specific `MobileSync` server response that points at `/Microsoft-Server-ActiveSync`. That response is reserved for ActiveSync clients and tests; Outlook desktop autodiscover must continue to avoid advertising `MobileSync` as an Exchange desktop route.

### ActiveSync clients

`ActiveSync` remains exposed at:

- `OPTIONS /Microsoft-Server-ActiveSync`
- `POST /Microsoft-Server-ActiveSync`

This endpoint targets mobile/native clients that actually support `Exchange ActiveSync`, such as Outlook mobile and iOS mail clients. Publishing that route does not make it an Outlook for Windows desktop Exchange endpoint.

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
- `LPE_AUTOCONFIG_EWS_ENABLED`, optional; set to `true`, `1`, `yes`, or `on` to publish the `EWS` endpoint through EWS-aware autodiscover responses
- `LPE_AUTOCONFIG_EWS_URL`, optional; default `{public_scheme}://{public_host}/EWS/Exchange.asmx`
- `LPE_AUTOCONFIG_ACTIVESYNC_URL`, optional; default `{public_scheme}://{public_host}/Microsoft-Server-ActiveSync`
- `LPE_AUTOCONFIG_JMAP_SESSION_URL`, optional

### Recommended DNS and HTTP publication

For a domain `example.test`:

- publish `autoconfig.example.test` or `mail.example.test` toward the public `LPE-CT` front end
- publish `autodiscover.example.test` or reuse `mail.example.test` toward the same front end
- re-expose the `/autoconfig/...`, `/.well-known/autoconfig/...`, `/autodiscover/...`, `/Autodiscover/...`, `/Microsoft-Server-ActiveSync`, `/EWS/Exchange.asmx`, and `/ews/exchange.asmx` routes over HTTPS
- re-expose `/api/jmap/session`, `/api/jmap/api`, `/api/jmap/upload/{accountId}`, `/api/jmap/download/{accountId}/{blobId}/{name}`, and `/api/jmap/ws` from `LPE-CT` to the core `LPE` service
- publish `IMAPS` on the same hostname when native `IMAP` access is exposed
- publish the authenticated `SMTPS` submission listener only when `LPE-CT` really exposes it
- do not reuse the internal `LPE -> LPE-CT` relay as a client-submission endpoint
- do not publish `ActiveSync` as the Outlook for Windows desktop Exchange route
- do not publish `MAPI` until a real `MAPI` service exists


