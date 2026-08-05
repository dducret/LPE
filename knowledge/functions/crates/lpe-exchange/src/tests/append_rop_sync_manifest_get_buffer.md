---
type: Rust Function
title: append_rop_sync_manifest_get_buffer
resource: crates/lpe-exchange/src/tests/mod.rs#L15060-L15067
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/append_rop_sync_manifest_get_buffer_with_state
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_fast_transfer_get_buffer_resumes_across_execute_requests
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_configure_separates_content_and_hierarchy_manifests
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_manifest_includes_attachment_change_facts_without_bcc
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_manifest_includes_visible_recipient_facts_without_bcc
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_manifest_includes_canonical_read_flag_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_manifest_includes_stable_change_key_facts_without_bcc
---

# Signature

`fn append_rop_sync_manifest_get_buffer( rops: &mut Vec<u8>, input: u8, output: u8, buffer_size: u16, )`

# Calls

- [append_rop_sync_manifest_get_buffer_with_state](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_sync_manifest_get_buffer_with_state.md)

# Called by

- [mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end.md)
- [mapi_over_http_fast_transfer_get_buffer_resumes_across_execute_requests](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_fast_transfer_get_buffer_resumes_across_execute_requests.md)
- [mapi_over_http_sync_configure_separates_content_and_hierarchy_manifests](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_configure_separates_content_and_hierarchy_manifests.md)
- [mapi_over_http_sync_manifest_includes_attachment_change_facts_without_bcc](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_manifest_includes_attachment_change_facts_without_bcc.md)
- [mapi_over_http_sync_manifest_includes_visible_recipient_facts_without_bcc](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_manifest_includes_visible_recipient_facts_without_bcc.md)
- [mapi_over_http_sync_manifest_includes_canonical_read_flag_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_manifest_includes_canonical_read_flag_state.md)
- [mapi_over_http_sync_manifest_includes_stable_change_key_facts_without_bcc](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_manifest_includes_stable_change_key_facts_without_bcc.md)