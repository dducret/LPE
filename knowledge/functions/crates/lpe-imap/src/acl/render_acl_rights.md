---
type: Rust Function
title: render_acl_rights
resource: crates/lpe-imap/src/acl.rs#L454-L473
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-imap/src/acl/Session/handle_getacl
---

# Signature

`fn render_acl_rights(state: AclState, owner: bool) -> String`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_getacl](../../../../../functions/crates/lpe-imap/src/acl/Session/handle_getacl.md)