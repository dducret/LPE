# Mail Security and Traceability

## Current State/Functionality Overview

`LPE-CT` owns perimeter mail security, transport scoring, quarantine decisions, and traceability. `LPE` owns canonical mailbox persistence and user-visible state.

## Implementation/Usage

- Keep transport decisions in `LPE-CT`.
- Keep canonical mailbox state in `LPE`.
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
| canonical mailbox copy | `LPE` |
| user search | `LPE` |
| protected `Bcc` metadata | `LPE` |
