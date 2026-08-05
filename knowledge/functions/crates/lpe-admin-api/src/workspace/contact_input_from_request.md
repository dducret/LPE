---
type: Rust Function
title: contact_input_from_request
resource: crates/lpe-admin-api/src/workspace.rs#L767-L792
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/workspace/upsert_client_contact
---

# Signature

`fn contact_input_from_request( account_id: Uuid, request: UpsertClientContactRequest, ) -> UpsertClientContactInput`

# Called by

- [upsert_client_contact](../../../../../functions/crates/lpe-admin-api/src/workspace/upsert_client_contact.md)