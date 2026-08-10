---
type: Rust Function
title: append_submit_message_response
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L347-L794
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_submit_attempt_context
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/optimized_send_target
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/optimized_send_replay_email
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/submit_success_response
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/submit_source_is_outgoing
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/mapi_submit_from_existing_email
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/submit_audit_entry
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/mirror_jmap_email_into_mailbox
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/submitted_message_handle_object
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submission_dispatch_response
---

# Signature

`pub(super) async fn append_submit_message_response<S>( store: &S, principal: &AccountPrincipal, mapi_request_id: &str, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], created_emails: &mut Vec<JmapEmail>, responses: &mut Vec<u8>, ) where S: ExchangeStore,`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [record_post_hierarchy_submit_attempt_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_submit_attempt_context.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [optimized_send_target](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/optimized_send_target.md)
- [folder_row_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [optimized_send_replay_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/optimized_send_replay_email.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [submit_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/submit_success_response.md)
- [mapi_submit_from_pending_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message.md)
- [submit_source_is_outgoing](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/submit_source_is_outgoing.md)
- [mapi_submit_from_existing_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/mapi_submit_from_existing_email.md)
- [submit_audit_entry](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/submit_audit_entry.md)
- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [remember_created_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity.md)
- [mirror_jmap_email_into_mailbox](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/mirror_jmap_email_into_mailbox.md)
- [submitted_message_handle_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/submitted_message_handle_object.md)

# Called by

- [append_submission_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submission_dispatch_response.md)