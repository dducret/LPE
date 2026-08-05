---
type: Rust Function
title: parse_acl_state_update
resource: crates/lpe-imap/src/acl.rs#L372-L415
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/acl/parse_acl_rights
  called_by:
  - functions/crates/lpe-imap/src/acl/Session/apply_acl_update
---

# Signature

`fn parse_acl_state_update(current: Option<AclState>, token: &str) -> Result<AclState>`

# Calls

- [parse_acl_rights](../../../../../functions/crates/lpe-imap/src/acl/parse_acl_rights.md)

# Called by

- [apply_acl_update](../../../../../functions/crates/lpe-imap/src/acl/Session/apply_acl_update.md)