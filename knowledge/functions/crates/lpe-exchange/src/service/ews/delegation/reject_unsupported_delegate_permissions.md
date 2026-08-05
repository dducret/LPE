---
type: Rust Function
title: reject_unsupported_delegate_permissions
resource: crates/lpe-exchange/src/service/ews/delegation.rs#L401-L416
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_user
---

# Signature

`fn reject_unsupported_delegate_permissions(permissions: &str) -> Result<()>`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [parse_ews_delegate_user](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_user.md)