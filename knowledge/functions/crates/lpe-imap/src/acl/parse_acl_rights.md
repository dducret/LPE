---
type: Rust Function
title: parse_acl_rights
resource: crates/lpe-imap/src/acl.rs#L417-L452
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/acl/parse_acl_state_update
---

# Signature

`fn parse_acl_rights(source: &str) -> Result<AclState>`

# Called by

- [parse_acl_state_update](../../../../../functions/crates/lpe-imap/src/acl/parse_acl_state_update.md)