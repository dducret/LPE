---
type: Rust Function
title: requested_folder_path_segments
resource: crates/lpe-exchange/src/service/ews/request_ids.rs#L90-L100
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/create_folder_path
---

# Signature

`pub(in crate::service) fn requested_folder_path_segments(request: &str) -> Vec<String>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_contents](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)

# Called by

- [create_folder_path](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/create_folder_path.md)