# `LPE-CT` Local Data Stores

## Current State/Functionality Overview

`LPE-CT` may use private local data stores only for perimeter-owned operational state. It must never store canonical mailbox, collaboration, rights, or user-visible state.

## Implementation/Usage

- Allowed local `LPE-CT` data:
  - SMTP spool custody
  - quarantine indexes and payload custody
  - Bayesian filtering state
  - reputation data
  - greylisting state
  - throttling state
  - routing policy
  - accepted inbound domains
  - DKIM key configuration
  - TLS profile configuration
  - cluster coordination
  - transport audit events
- Forbidden local `LPE-CT` data:
  - canonical mailbox messages
  - canonical `Sent`
  - contacts
  - calendars
  - tasks
  - mailbox rights
  - collaboration grants
  - tenant administrators
  - user search indexes
  - AI-facing projections
- Network rules:
  - `LPE-CT` must not require direct access to the core `LPE` PostgreSQL database
  - `5432` on `LPE-CT` is private to the sorting center when a local PostgreSQL service is used
  - core integration uses signed HTTP bridge calls
- Rebuild and retention:
  - technical stores may be rebuilt from policy, logs, or retained spool/quarantine state where possible
  - payload custody must be preserved until delivery, bounce, release, rejection, or configured deletion
  - retention policies must distinguish metadata from payload custody
- Management:
  - accepted inbound domains are managed in `System Setup -> Mail relay -> Domains`
  - upstream smart hosts are managed in `System Setup -> Mail relay -> General Settings`
  - public inbound SMTP TLS identity is managed in `System Setup -> Mail relay -> SMTP Settings`

## Reference Table/List

| Path / store | Purpose |
| --- | --- |
| `/var/spool/lpe-ct` | SMTP spool and payload custody |
| `/var/lib/lpe-ct/state.json` | private technical state where configured |
| `/transport-audit.jsonl` | transport audit stream |
| `/digest-reports/` | digest report artifacts |

| Function | Storage assignment |
| --- | --- |
| SMTP spool | `LPE-CT` local custody |
| quarantine payload | `LPE-CT` local custody |
| quarantine metadata | `LPE-CT` technical store |
| greylisting | `LPE-CT` technical store |
| reputation | `LPE-CT` technical store |
| canonical delivery | core `LPE` PostgreSQL |
| canonical mailbox search | core `LPE` PostgreSQL |

| Port / flow | Rule |
| --- | --- |
| `5432` on `LPE-CT` | private sorting-center database only |
| `LPE-CT -> LPE` final delivery | signed HTTP bridge |
| `LPE -> LPE-CT` outbound handoff | signed HTTP bridge |
| public `SMTP` | terminates on `LPE-CT` |
