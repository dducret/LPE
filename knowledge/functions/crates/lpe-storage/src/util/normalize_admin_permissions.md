---
type: Rust Function
title: normalize_admin_permissions
resource: crates/lpe-storage/src/util.rs#L95-L114
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/util/default_permissions_for_role
  - functions/crates/lpe-storage/src/util/split_permissions
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/admin/Storage/create_server_administrator
  - functions/crates/lpe-storage/src/shared/explicit_permissions_are_normalized_and_deduplicated
  - functions/crates/lpe-storage/src/util/permissions_from_storage
---

# Signature

`pub(crate) fn normalize_admin_permissions( role: &str, rights_summary: &str, explicit: &[String], ) -> Vec<String>`

# Calls

- [default_permissions_for_role](../../../../../functions/crates/lpe-storage/src/util/default_permissions_for_role.md)
- [split_permissions](../../../../../functions/crates/lpe-storage/src/util/split_permissions.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [create_server_administrator](../../../../../functions/crates/lpe-storage/src/admin/Storage/create_server_administrator.md)
- [explicit_permissions_are_normalized_and_deduplicated](../../../../../functions/crates/lpe-storage/src/shared/explicit_permissions_are_normalized_and_deduplicated.md)
- [permissions_from_storage](../../../../../functions/crates/lpe-storage/src/util/permissions_from_storage.md)