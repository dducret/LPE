---
type: Rust Function
title: parse_delegate_permission_level
resource: crates/lpe-exchange/src/service/ews/delegation.rs#L390-L399
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/delegation/collaboration_rights
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_user
---

# Signature

`fn parse_delegate_permission_level(permissions: &str, field: &str) -> Result<CollaborationRights>`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [collaboration_rights](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/collaboration_rights.md)

# Called by

- [parse_ews_delegate_user](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/parse_ews_delegate_user.md)