---
type: Rust Function
title: requested_convert_ids
resource: crates/lpe-exchange/src/service/ews/ids.rs#L75-L87
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/ids/convert_id_sources_for_tag
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ids/ExchangeService/convert_id
---

# Signature

`pub(in crate::service) fn requested_convert_ids(request: &str) -> Vec<ConvertIdSource>`

# Calls

- [convert_id_sources_for_tag](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/convert_id_sources_for_tag.md)

# Called by

- [convert_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/ExchangeService/convert_id.md)