---
type: Rust Function
title: transfer_job_response
resource: crates/lpe-exchange/src/service/ews/bulk_transfer.rs#L69-L119
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/bulk_transfer/ExchangeService/upload_items
  - functions/crates/lpe-exchange/src/service/ews/bulk_transfer/ExchangeService/export_items
---

# Signature

`pub(in crate::service) fn transfer_job_response(operation: &str, job: &EwsTransferJob) -> String`

# Called by

- [upload_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/bulk_transfer/ExchangeService/upload_items.md)
- [export_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/bulk_transfer/ExchangeService/export_items.md)