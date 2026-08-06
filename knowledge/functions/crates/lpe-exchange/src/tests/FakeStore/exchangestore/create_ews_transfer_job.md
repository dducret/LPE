---
type: Rust Method
title: create_ews_transfer_job
resource: crates/lpe-exchange/src/tests/mod.rs#L5557-L5596
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/bulk_transfer/ExchangeService/upload_items
  - functions/crates/lpe-exchange/src/service/ews/bulk_transfer/ExchangeService/export_items
---

# Signature

`fn create_ews_transfer_job<'a>( &'a self, _principal: &'a AccountPrincipal, direction: &'a str, item_ids: &'a [String], _request_json: serde_json::Value, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, EwsTransferJob>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [upload_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/bulk_transfer/ExchangeService/upload_items.md)
- [export_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/bulk_transfer/ExchangeService/export_items.md)