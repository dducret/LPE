---
type: Rust Function
title: parse_contact_input
resource: crates/lpe-activesync/src/service/application_data.rs#L88-L150
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/application_data/body_text
---

# Signature

`pub(super) fn parse_contact_input( account_id: Uuid, id: Option<Uuid>, existing: Option<&lpe_storage::ClientContact>, application_data: &WbxmlNode, ) -> Result<UpsertClientContactInput>`

# Calls

- [body_text](../../../../../../functions/crates/lpe-activesync/src/service/application_data/body_text.md)