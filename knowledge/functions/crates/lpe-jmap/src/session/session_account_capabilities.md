---
type: Rust Function
title: session_account_capabilities
resource: crates/lpe-jmap/src/session.rs#L238-L272
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/account_capability_value
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/session/JmapService/session_document
---

# Signature

`fn session_account_capabilities( access: &MailboxAccountAccess, capabilities: &HashMap<String, Value>, ) -> HashMap<String, Value>`

# Calls

- [account_capability_value](../../../../../functions/crates/lpe-jmap/src/session/account_capability_value.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [session_document](../../../../../functions/crates/lpe-jmap/src/session/JmapService/session_document.md)