---
type: Rust Function
title: normalize_admin_session_auth_method
resource: crates/lpe-storage/src/util.rs#L23-L29
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/auth/Storage/create_admin_session
---

# Signature

`pub(crate) fn normalize_admin_session_auth_method(value: &str) -> &'static str`

# Called by

- [create_admin_session](../../../../../functions/crates/lpe-storage/src/auth/Storage/create_admin_session.md)