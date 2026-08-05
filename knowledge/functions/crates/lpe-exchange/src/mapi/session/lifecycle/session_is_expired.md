---
type: Rust Function
title: session_is_expired
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L339-L344
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/prune_expired_sessions_locked
---

# Signature

`pub(in crate::mapi) fn session_is_expired(session: &MapiSession, now: SystemTime) -> bool`

# Called by

- [prune_expired_sessions_locked](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/prune_expired_sessions_locked.md)