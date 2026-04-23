# Mail Security and Traceability

### Goal

This document describes mail flow, edge security, traceability, and quarantine between `LPE-CT` and `LPE`.

### Principle

`LPE-CT` receives, filters, traces, routes, and quarantines.

`LPE` persists mailboxes and remains the system of record.

The current `LPE-CT` implementation already executes real `SPF`, `DKIM`, and `DMARC` validation, greylisting, `DNSBL/RBL` lookups, simple local reputation, and a detailed decision trace persisted in the spool.
Outbound transport policy now also covers configured DKIM signing, sender and recipient allow/block rules, internal `LPE`-backed inbound recipient verification with short-lived local caching, and attachment filtering rules keyed by extension, MIME type, and detected file type.

The inbound perimeter pipeline is now explicitly staged as:

1. ingress trace creation
2. SMTP protocol/envelope capture
3. `RBL` / DNS checks plus `SPF` / `DKIM` / `DMARC`
4. local Bayesian scoring plus a configurable antivirus provider chain in `LPE-CT`
5. final score calculation
6. accept, defer, reject, or quarantine

Edge decisions now rely on structured outcomes:

- `DMARC reject` can force SMTP reject
- `DMARC quarantine` can force quarantine
- `SPF fail` can force reject when no aligned `DKIM` pass compensates
- temporary authentication failure (`SPF`/`DKIM`/`DMARC`) can force `defer`
- poor sender/IP reputation can force quarantine or reject

### Separate scores

The model should separate:

- `Spam Score`, probabilistic
- `Security Score`, more deterministic and risk-oriented

### File validation with Magika

Every file entering through an external connection or through a client is validated by `Magika` before normal processing.

This includes in particular:

- inbound attachments
- `JMAP` blobs
- `PST` imports
- future browser or API uploads

If `Magika` identifies an exotic, suspicious, or policy-disallowed file, the default action is quarantine in `LPE-CT`.

A dynamic-analysis sandbox may exist later, but it is not part of the current baseline.

### Encrypted uninspectable messages

If a message is end-to-end encrypted, for example with `PGP` or `S/MIME`, and cannot be inspected, it must be marked `uninspectable`.

### Magika outside the SMTP thread

In the future, `Magika` should be able to run outside the critical `SMTP` receive thread, for example in a separate worker or sidecar.

### Policy propagation

Policy propagation follows a push model from `LPE` to `LPE-CT`.

### Unique trace identity

Each processed message receives a unique trace identifier that must survive until the final outcome.

### Final status return and DSN

If `LPE` rejects final delivery after edge acceptance, it must return that status to `LPE-CT`.

`LPE-CT` must then be able to:

- correlate the error with the original identifier
- keep end-to-end trace search coherent
- generate a consistent bounce or `DSN`

The persisted trace must also stay structured enough to prepare later work on:

- `ARC`
- `MTA-STS`
- `TLS-RPT`

### Internal streaming

When delivery can proceed normally, the internal `LPE-CT -> LPE` exchange should support streaming to avoid unnecessary double disk writes.

### Quarantine

Quarantine is stored in `LPE-CT`.

When `LPE-CT` enables its dedicated local `PostgreSQL` store, quarantined messages are also indexed in a technical `quarantine_messages` table for operational search and release workflows. Spool custody of the message payload remains with `LPE-CT`.

`LPE-CT` may also retain perimeter mail-flow history and generated quarantine digest artifacts for operator search and reporting, provided that those artifacts remain technical sorting-center state and do not become canonical mailbox search or user-visible product data in the core `LPE` store.

That retained technical evidence is now intentionally richer for operator use. The current reporting path preserves and exposes:

- `Message-Id`, sender, recipient, and domain correlation
- peer IP, `HELO`, and route-target correlation
- structured `DNSBL`, `SPF`, `DKIM`, and `DMARC` evidence
- `Magika` summary and final decision when attachment validation influenced the outcome
- latest retained technical status and `DSN` detail for outbound failures or retries
- operator actions such as quarantine release, retry, and deletion in the retained history stream

Retention is also explicit:

- retained transport-audit history is bounded by the configured history-retention window
- mirrored `mail_flow_history` rows follow the same retention boundary
- generated digest artifacts under `policy/digest-reports/` use a separate digest-retention window

These retained artifacts remain non-canonical technical evidence owned by the sorting center.

The current technical store also persists sorting-center-owned policy and coordination metadata needed by the management plane:

- allow/block address rules in `policy_address_rules`
- attachment controls in `attachment_policy_rules`
- digest schedule and recipient targets in `digest_settings` and `digest_recipients`
- short-lived recipient verification cache materialization in `recipient_verification_cache`
- DKIM sender-domain technical references in `dkim_domain_configs`

`LPE` may request the release of a message through a privileged action, but quarantine ownership remains in the sorting center.


