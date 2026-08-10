---
type: Rust Method
title: fetch_outbound_handoff_batch
resource: crates/lpe-storage/src/outbound.rs#L121-L264
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-cli/src/run_outbound_worker
---

# Signature

`pub async fn fetch_outbound_handoff_batch( &self, limit: i64, ) -> Result<Vec<OutboundMessageHandoffRequest>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [run_outbound_worker](../../../../../../functions/crates/lpe-cli/src/run_outbound_worker.md)