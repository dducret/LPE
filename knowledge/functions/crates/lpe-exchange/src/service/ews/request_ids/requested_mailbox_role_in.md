---
type: Rust Function
title: requested_mailbox_role_in
resource: crates/lpe-exchange/src/service/ews/request_ids.rs#L143-L148
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/create_folder_path
---

# Signature

`pub(in crate::service) fn requested_mailbox_role_in( request: &str, wrapper: &str, ) -> Option<&'static str>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)

# Called by

- [create_folder_path](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/create_folder_path.md)