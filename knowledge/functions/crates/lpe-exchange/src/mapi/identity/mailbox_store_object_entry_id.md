---
type: Rust Function
title: mailbox_store_object_entry_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L572-L593
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/principal_mailbox_store_entry_id
  - functions/crates/lpe-exchange/src/mapi/identity/mailbox_store_object_entry_id_matches_outlook_wlink_shape
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly
---

# Signature

`pub(crate) fn mailbox_store_object_entry_id(server_shortname: &str, mailbox_dn: &str) -> Vec<u8>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [principal_mailbox_store_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/principal_mailbox_store_entry_id.md)
- [mailbox_store_object_entry_id_matches_outlook_wlink_shape](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mailbox_store_object_entry_id_matches_outlook_wlink_shape.md)
- [mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly.md)