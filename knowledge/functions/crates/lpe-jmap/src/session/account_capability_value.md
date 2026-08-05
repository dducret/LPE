---
type: Rust Function
title: account_capability_value
resource: crates/lpe-jmap/src/session.rs#L274-L289
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/session/session_account_capabilities
---

# Signature

`fn account_capability_value( access: &MailboxAccountAccess, capability: &str, global_value: &Value, ) -> Value`

# Called by

- [session_account_capabilities](../../../../../functions/crates/lpe-jmap/src/session/session_account_capabilities.md)