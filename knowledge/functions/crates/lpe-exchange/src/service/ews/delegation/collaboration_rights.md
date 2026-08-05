---
type: Rust Function
title: collaboration_rights
resource: crates/lpe-exchange/src/service/ews/delegation.rs#L418-L430
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/delegation/parse_delegate_permission_level
---

# Signature

`fn collaboration_rights( may_read: bool, may_write: bool, may_delete: bool, may_share: bool, ) -> CollaborationRights`

# Called by

- [parse_delegate_permission_level](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/parse_delegate_permission_level.md)