# Mail Security and Traceability

## Current State/Functionality Overview

`LPE-CT` owns perimeter mail security, transport scoring, quarantine decisions, and traceability. `LPE` owns canonical mailbox persistence and user-visible state.

## Implementation/Usage

- Keep transport decisions in `LPE-CT`.
- Keep canonical mailbox state in `LPE`.
- Authenticated client SMTP terminates only at LPE-CT's TLS submission listener.
  It supports the bounded `AUTH PLAIN LOGIN` surface ([MS-OXSMTP] sections
  2.2.1 and 3.2.5.1; [MS-XLOGIN] section 2.2). It accepts only the exact
  `AUTH PLAIN` and `AUTH LOGIN` forms, with at most one initial response
  ([MS-XLOGIN] section 2.2.2), before calling the signed core submission
  bridge. It returns success only after canonical `Sent` and the outbound
  handoff record exist. Public port 25 neither advertises nor accepts `AUTH`;
  the internal `LPE -> LPE-CT` relay is never a client endpoint.
- Maintain separate scores for:
  - spam
  - malware
  - authentication
  - policy
  - reputation
- Validate files with Google `Magika` before normal processing.
- Process expensive inspection outside the SMTP command thread where possible.
- Treat encrypted uninspectable content according to policy.
- Propagate policy outcomes to transport result metadata.
- Assign a unique trace identity and propagate `x-trace-id`.
- Include `message_id` and `internet_message_id` in logs when available.
- Return structured final status for delivery, defer, quarantine, bounce, and failure.
- Produce `DSN` detail for bounce-capable failures.
- Keep quarantine in `LPE-CT` custody until released, rejected, or deleted by policy.
- For an accepted inbound delivery, the signed `LPE-CT -> LPE` bridge owns the
  sole mailbox-safe perimeter projection: the `x-lpe-ct-trace-id` provenance
  link. The bridge strips an Internet-supplied header of that reserved name
  before handoff, including malformed messages that omit a header/body
  separator, and core appends the bridge trace id atomically with the canonical
  mailbox message. The LPE-CT source is the durable queue item/audit trace with
  the same id; signed bridge retries reuse that evidence instead of creating a
  new filtering result. This link supports trace correlation only; it is not a
  score, phishing stamp, quarantine state, or client-mutable policy.
  `PidTagContentFilterSpamConfidenceLevel` and `PidNamePhishingStamp` remain
  absent from canonical messages ([MS-OXCSPAM] section 2.2.1.3;
  [MS-OXPHISH] section 2.2.1.1). The canonical search and AI paths continue to
  exclude protected Bcc metadata.

## Reference Table/List

| Status | Meaning |
| --- | --- |
| `queued` | prepared before handoff |
| `relayed` | relayed toward SMTP target |
| `deferred` | transient failure |
| `quarantined` | retained by policy |
| `bounced` | permanent delivery failure with `DSN` |
| `failed` | permanent failure or incompatible relay configuration |

| Security item | Owner |
| --- | --- |
| SPF / DKIM / DMARC policy | `LPE-CT` |
| DKIM signing | `LPE-CT` |
| quarantine | `LPE-CT` |
| LPE-CT trace provenance link | core `LPE`, sourced only by signed LPE-CT delivery |
| canonical mailbox copy | `LPE` |
| user search | `LPE` |
| protected `Bcc` metadata | `LPE` |

EWS Message Tracking is a bounded projection of canonical submission state and
`LPE-CT` trace events. Per [MS-OXWSMTRK] section 3.1.4.2, LPE can project the
event timeline without making `LPE-CT` mailbox state: when a trace event maps
to a protected canonical recipient, its recipient field and opaque diagnostic
payload are redacted before the EWS response.
