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
- `OPTIONS /mapi/emsmdb`
- `POST /mapi/emsmdb`
- `OPTIONS /mapi/nspi`
- `POST /mapi/nspi`

Without a reverse proxy, these routes are exposed directly by the Rust `LPE` service.

With the documented Debian reverse proxy, those routes are published as-is by `nginx` and should then be re-exposed by `LPE-CT` on the public client hostname.

The Rust service also mounts the first guarded `MAPI over HTTP` implementation routes at `/mapi/emsmdb` and `/mapi/nspi`. They provide authenticated transport/session handling and early mailbox-folder bootstrap behavior. They are published by autodiscover only when `LPE_AUTOCONFIG_MAPI_ENABLED` is explicitly enabled for Outlook interoperability testing.

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

The default MVP does not advertise `MAPI` or `MobileSync` for Outlook desktop. Outlook for Windows desktop must not be forced to use `ActiveSync` as an Exchange account.

`MAPI over HTTP` implementation has started for the future Outlook desktop Exchange path. It can now be advertised only through the explicit `LPE_AUTOCONFIG_MAPI_ENABLED` interoperability-test switch. When enabled, POX autodiscover distinguishes between the two Outlook setup probes:

- requests that include `X-MapiHttpCapability` receive a `Protocol Type="mapiHttp" Version="1"` block with `MailStore` and `AddressBook` URLs pointing at `/mapi/emsmdb/` and `/mapi/nspi/`
- legacy Outlook and Remote Connectivity Analyzer probes without that header receive the `EXCH` and `EXPR` provider sections that Outlook validates before it moves into the MAPI/HTTP path

SOAP autodiscover returns `MapiHttpEnabled` as `True` when MAPI publication is enabled. This is intentionally not the default because message tables, NSPI address book operations, and full Outlook profile creation are still being implemented.

The `0.1.3` `EWS` endpoint is the Exchange-style compatibility focus for mailbox, contacts, and calendar synchronization. Autodiscovery publishes it only when `LPE_AUTOCONFIG_EWS_ENABLED` is explicitly set to a true value. This keeps `EWS` publication an administrator choice until the deployment accepts the current MVP limits.

When `EWS` autodiscovery is enabled, the POX response publishes the configured `/EWS/Exchange.asmx` URL through a `WEB` protocol block with an `ASUrl`. This gives EWS-aware clients such as Thunderbird a discovery path without advertising top-level `EXCH` or `EXPR` mailbox protocols that Outlook for Windows desktop treats as a full Exchange/MAPI route.

Autodiscover responses include the POX `Response`, `User`, `Account`, and `Protocol` shape expected by Microsoft clients. The `User` block stays limited to the POX fields Microsoft documents for Outlook responses: `DisplayName`, `LegacyDN`, `AutoDiscoverSMTPAddress`, and `DeploymentId`. The request parser accepts both unprefixed and namespace-prefixed request elements, including the `a:EMailAddress` form used by Microsoft connectivity tooling. In MAPI interoperability-test mode, POX `EXCH` / `EXPR` provider sections are compatibility metadata for Outlook setup validation and must still route subsequent mailbox access through the implemented `MAPI over HTTP` and canonical `LPE` mailbox layers; they do not introduce a separate `RPC` or Outlook Anywhere implementation.

Autodiscover also accepts SOAP `GetUserSettings` requests and returns an Exchange-style SOAP response with user identity, mailbox-server identity, mailbox DN, SSL and authentication metadata, `CasVersion`, `ExternalEwsUrl`, `InternalEwsUrl`, and `EwsSupportedSchemas`. The SOAP path publishes the same opt-in `EWS` endpoint and does not advertise `RPC`, client `SMTP` submission, or any unsupported Exchange surface. Unless `LPE_AUTOCONFIG_MAPI_ENABLED` is explicitly enabled, SOAP Autodiscover returns `MapiHttpEnabled` as `False`.

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
- `LPE_AUTOCONFIG_MAPI_ENABLED`, optional; set to `true`, `1`, `yes`, or `on` to publish `MAPI over HTTP` autodiscover for Outlook interoperability testing
- `LPE_AUTOCONFIG_MAPI_EMSMDB_URL`, optional; default `{public_scheme}://{public_host}/mapi/emsmdb/?MailboxId={email}`
- `LPE_AUTOCONFIG_MAPI_NSPI_URL`, optional; default `{public_scheme}://{public_host}/mapi/nspi/?MailboxId={email}`
- `LPE_AUTOCONFIG_ACTIVESYNC_URL`, optional; default `{public_scheme}://{public_host}/Microsoft-Server-ActiveSync`
- `LPE_AUTOCONFIG_JMAP_SESSION_URL`, optional

### Recommended DNS and HTTP publication

For a domain `example.test`:

- publish `autoconfig.example.test` or `mail.example.test` toward the public `LPE-CT` front end
- publish `autodiscover.example.test` or reuse `mail.example.test` toward the same front end
- re-expose the `/autoconfig/...`, `/.well-known/autoconfig/...`, `/autodiscover/...`, `/Autodiscover/...`, `/Microsoft-Server-ActiveSync`, `/EWS/Exchange.asmx`, `/ews/exchange.asmx`, and `/mapi/...` routes over HTTPS
- re-expose `/api/jmap/session`, `/api/jmap/api`, `/api/jmap/upload/{accountId}`, `/api/jmap/download/{accountId}/{blobId}/{name}`, and `/api/jmap/ws` from `LPE-CT` to the core `LPE` service
- publish `IMAPS` on the same hostname when native `IMAP` access is exposed
- publish the authenticated `SMTPS` submission listener only when `LPE-CT` really exposes it
- do not reuse the internal `LPE -> LPE-CT` relay as a client-submission endpoint
- do not publish `ActiveSync` as the Outlook for Windows desktop Exchange route
- publish `MAPI` autodiscover only when `LPE_AUTOCONFIG_MAPI_ENABLED` is explicitly enabled for Outlook interoperability testing


