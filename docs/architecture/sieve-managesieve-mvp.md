# Sieve / ManageSieve MVP

### Goal

This document describes the first `Sieve` and `ManageSieve` support in `LPE`.

The goal is to provide per-account mailbox filtering without reintroducing parallel business logic outside the canonical `LPE` model.

### Architectural placement

- `Sieve` is an end-user mailbox rule, not an `LPE-CT` edge policy
- `ManageSieve` is a script-management protocol adapter, not a new workflow model
- final inbound delivery remains the canonical execution point
- automatic outbound effects from `redirect` and `vacation` reuse canonical `LPE` submission and `outbound_message_queue`

### Canonical storage

- scripts are stored in `PostgreSQL` per `(tenant_id, account_id)`
- only one active script is allowed per account
- minimal `vacation` memory is stored per account and sender to limit repeated replies
- minimal audit coverage exists for script creation, update, rename, activation, deletion, and application

### MVP scope

The MVP supports:

- per-account script storage
- a minimal `ManageSieve` service authenticated with the same mailbox-account login as the other mailbox protocols
- one active script per account
- execution during inbound final delivery on the `LPE` side
- `fileinto`, `discard`, `redirect`, `vacation`, `keep`, and `stop`
- `header`, `address`, `envelope`, `allof`, `anyof`, `not`, `true`, and `false` tests
- automatic creation of the target `fileinto` mailbox when it does not already exist for the account

### Invariants

- multi-tenant resolution still happens per accepted recipient
- `Sieve` is executed only after the target account is resolved
- `Inbox`, `Sent`, and `Drafts` remain canonical views
- `fileinto` changes local delivery placement without creating a parallel copy
- `discard` removes the local copy while keeping the recipient delivery accepted
- `redirect` and `vacation` do not send anything directly to the Internet; they reuse `submit_message` and `LPE-CT`

### MVP protections

- script size is limited to `64 KiB`
- at most `16` scripts per account
- at most `4` redirects per message
- `vacation` applies minimal memory keyed by sender and response content
- the `ManageSieve` MVP supports `AUTHENTICATE PLAIN` and `AUTHENTICATE XOAUTH2`
- the `ManageSieve` MVP supports only non-synchronizing literals `{N+}`

### Explicit limitations

- the supported `Sieve` subset is intentionally bounded and does not cover the full `RFC 5228` surface
- there is no support for `include`, `variables`, `imapflags`, `reject`, `ereject`, `date`, `relational`, `body`, `spamtest`, or vendor-specific extensions
- there is no `:copy` support for `fileinto` and no multi-active-script mode
- the MVP `redirect` path re-submits an `LPE` canonical message and does not guarantee byte-identical replay of the original inbound stream
- `redirect` and `vacation` currently create a canonical outbound copy in `Sent` with a `sieve-*` technical source; this is accepted for the MVP in order to avoid parallel outbound logic
- there is no dedicated web administration UI for scripts in this first iteration


