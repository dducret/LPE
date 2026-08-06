---
type: Rust Function
title: create_input
resource: crates/lpe-storage/tests/mapi_contact_create.rs#L272-L290
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/mapi_contact_create/contact_input
---

# Signature

`fn create_input( account_id: Uuid, contact_id: Uuid, identity: Option<MapiContactImportedIdentity>, ) -> MapiContactCreateInput`

# Calls

- [contact_input](../../../../../functions/crates/lpe-storage/tests/mapi_contact_create/contact_input.md)