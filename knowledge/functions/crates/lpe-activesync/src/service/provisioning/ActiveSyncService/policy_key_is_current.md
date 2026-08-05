---
type: Rust Method
title: policy_key_is_current
resource: crates/lpe-activesync/src/service/provisioning.rs#L129-L154
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
---

# Signature

`pub(super) async fn policy_key_is_current( &self, account_id: Uuid, device_id: &str, request_policy_key: Option<&str>, ) -> Result<bool>`

# Called by

- [handle_parsed_request](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)