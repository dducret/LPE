---
type: Rust Method
title: export_items
resource: crates/lpe-exchange/src/service/ews/bulk_transfer.rs#L38-L66
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_ews_transfer_job
  - functions/crates/lpe-exchange/src/service/ews/bulk_transfer/transfer_job_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn export_items( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_item_ids](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [create_ews_transfer_job](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_ews_transfer_job.md)
- [transfer_job_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/bulk_transfer/transfer_job_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)