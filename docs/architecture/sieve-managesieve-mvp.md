# Sieve and ManageSieve

## Current State/Functionality Overview

`ManageSieve` manages per-account scripts stored in `LPE`; mailbox `Sieve` execution runs during final inbound delivery. Filtering must not move perimeter policy into core `LPE` or create a parallel routing engine.

## Implementation/Usage

- Canonical storage:
  - scripts are stored per mailbox account
  - active script state is account-scoped
- ManageSieve supports:
  - `AUTHENTICATE PLAIN`
  - `AUTHENTICATE XOAUTH2`
  - non-synchronizing literals `{N+}`
  - script upload, list, activate, retrieve, and delete operations
- Execution supports:
  - `fileinto`
  - `discard`
  - `redirect`
  - `vacation`
- Submission behavior:
  - `redirect` uses canonical `LPE` submission and outbound relay through `LPE-CT`
  - `vacation` uses canonical `LPE` submission and outbound relay through `LPE-CT`
  - no Sieve-specific transport engine
- Constraints:
  - no edge spam/filtering policy in core mailbox Sieve
  - no byte-identical replay guarantee for `redirect`
  - no dedicated web administration UI for scripts

## Reference Table/List

| Command / action | Status |
| --- | --- |
| `AUTHENTICATE PLAIN` | supported |
| `AUTHENTICATE XOAUTH2` | supported |
| `{N+}` literals | supported |
| `fileinto` | supported |
| `discard` | supported |
| `redirect` | canonical resubmission |
| `vacation` | canonical resubmission |
