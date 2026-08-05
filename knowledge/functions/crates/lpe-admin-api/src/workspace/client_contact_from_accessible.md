---
type: Rust Function
title: client_contact_from_accessible
resource: crates/lpe-admin-api/src/workspace.rs#L794-L814
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/upsert_client_contact
  - functions/crates/lpe-admin-api/src/workspace/get_client_contact
  - functions/crates/lpe-admin-api/src/workspace/patch_client_contact
---

# Signature

`fn client_contact_from_accessible(contact: AccessibleContact) -> ClientContact`

# Called by

- [upsert_client_contact](../../../../../functions/crates/lpe-admin-api/src/workspace/upsert_client_contact.md)
- [get_client_contact](../../../../../functions/crates/lpe-admin-api/src/workspace/get_client_contact.md)
- [patch_client_contact](../../../../../functions/crates/lpe-admin-api/src/workspace/patch_client_contact.md)