---
type: Rust Module
title: submission
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L1-L862
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/lpe-storage-cancelsubmissionresult
  - external/std-collections-hashmap
  - external/uuid-uuid
  - external/crate-mapi-identity-global-counter-from-store-id-object-ids-from-message-entry-id-source-key-for-object-id-first-dynamic-global-counter-outbox-folder-id-sent-folder-id-properties-mapivalue-pid-tag-target-entry-id
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [OptimizedSendTarget](../../../../../../classes/crates/lpe-exchange/src/mapi/dispatch/submission/OptimizedSendTarget.md)
- [optimized_send_target](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/optimized_send_target.md)
- [optimized_send_replay_email](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/optimized_send_replay_email.md)
- [mapi_submit_from_existing_email](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/mapi_submit_from_existing_email.md)
- [submit_success_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/submit_success_response.md)
- [submit_source_is_outgoing](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/submit_source_is_outgoing.md)
- [submit_audit_entry](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/submit_audit_entry.md)
- [submitted_message_handle_object](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/submitted_message_handle_object.md)
- [transport_folder_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/transport_folder_response.md)
- [options_data_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/options_data_response.md)
- [append_transport_folder_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_transport_folder_response.md)
- [append_options_data_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_options_data_response.md)
- [append_transport_info_dispatch_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_transport_info_dispatch_response.md)
- [is_submission_dispatch_rop](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/is_submission_dispatch_rop.md)
- [append_submission_dispatch_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submission_dispatch_response.md)
- [abort_submit_source_is_sent](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_source_is_sent.md)
- [abort_submit_canonical_message_id](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_canonical_message_id.md)
- [abort_submit_cancel_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_cancel_response.md)
- [spooler_advisory_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/spooler_advisory_response.md)
- [deferred_action_messages_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/deferred_action_messages_response.md)
- [append_spooler_advisory_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_spooler_advisory_response.md)
- [append_spooler_advisory_dispatch_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_spooler_advisory_dispatch_response.md)
- [append_deferred_action_messages_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_deferred_action_messages_response.md)
- [append_deferred_action_messages_dispatch_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_deferred_action_messages_dispatch_response.md)
- [append_submit_message_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response.md)
- [append_abort_submit_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_abort_submit_response.md)
- [abort_submit_audit_entry](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_audit_entry.md)

# Imports

- `super::*`
- `lpe_storage::CancelSubmissionResult`
- `std::collections::HashMap`
- `uuid::Uuid`
- `crate::mapi::{
    identity::{
        global_counter_from_store_id, object_ids_from_message_entry_id, source_key_for_object_id,
        FIRST_DYNAMIC_GLOBAL_COUNTER, OUTBOX_FOLDER_ID, SENT_FOLDER_ID,
    },
    properties::{MapiValue, PID_TAG_TARGET_ENTRY_ID},
}`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)