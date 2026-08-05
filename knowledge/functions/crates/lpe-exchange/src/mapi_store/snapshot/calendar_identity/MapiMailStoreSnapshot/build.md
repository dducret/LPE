---
type: Rust Method
title: build
resource: crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity.rs#L246-L487
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/mapi_message_folder_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/ScopedCalendarIdentities/message_identity
  - functions/crates/lpe-exchange/src/mapi_store/mapi_collaboration_folder_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_store/task_collection_matches
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/fallback_event_version
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests
  called_by:
  - functions/LPE-CT/src/dashboard_config/probe_lpe_core_delivery
  - functions/LPE-CT/src/dashboard_config/probe_lpe_recipient_bridge
  - functions/LPE-CT/src/readiness/check_optional_http_dependency
  - functions/LPE-CT/src/smtp/delivery_bridge/deliver_inbound_message
  - functions/LPE-CT/src/smtp/dns/SystemDnsResolver/new
  - functions/LPE-CT/src/submission/run_submission_listener
  - functions/LPE-CT/src/submission/smtp_xoauth_is_rejected_before_core_auth_request
  - functions/LPE-CT/src/submission/submit_message_posts_trace_header_and_returns_success
  - functions/LPE-CT/src/submission/submit_message_rejects_non_accepted_success_body_before_smtp_final_reply
  - functions/crates/lpe-admin-api/src/readiness/check_optional_http_dependency
  - functions/crates/lpe-cli/src/run_outbound_worker
  - functions/crates/lpe-cli/src/handoff_client_posts_json_and_header
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/new
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/new_with_scoped_calendar_identities
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts
  - functions/crates/lpe-exchange/src/tests/TestSchemaCleanup/drop/drop
  - functions/crates/lpe-storage/tests/mapi_contact_create/TestSchemaCleanup/drop/drop
  - functions/crates/lpe-storage/tests/mapi_event_commit/TestSchemaCleanup/drop/drop
---

# Signature

`fn build( mailboxes: Vec<JmapMailbox>, emails: Vec<JmapEmail>, attachments: Vec<(Uuid, Vec<ActiveSyncAttachment>)>, contact_collections: Vec<CollaborationCollection>, calendar_collections: Vec<CollaborationCollection>, task_collections: Vec<CollaborationCollection>, contacts: Vec<AccessibleContact>, events: Vec<AccessibleEvent>, deleted_events: Vec<AccessibleEvent>, tasks: Vec<ClientTask>, folder_permissions: Vec<MapiFolderPermission>, calendar_identities: Option<&ScopedCalendarIdentities>, ) -> Result<Self>`

# Calls

- [mapi_message_folder_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_message_folder_id.md)
- [message_identity](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/ScopedCalendarIdentities/message_identity.md)
- [mapi_collaboration_folder_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_collaboration_folder_id.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [task_collection_matches](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/task_collection_matches.md)
- [source_key_for_store_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [fallback_event_version](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/fallback_event_version.md)
- [legacy_for_tests](../../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests.md)

# Called by

- [probe_lpe_core_delivery](../../../../../../../../functions/LPE-CT/src/dashboard_config/probe_lpe_core_delivery.md)
- [probe_lpe_recipient_bridge](../../../../../../../../functions/LPE-CT/src/dashboard_config/probe_lpe_recipient_bridge.md)
- [check_optional_http_dependency](../../../../../../../../functions/LPE-CT/src/readiness/check_optional_http_dependency.md)
- [deliver_inbound_message](../../../../../../../../functions/LPE-CT/src/smtp/delivery_bridge/deliver_inbound_message.md)
- [new](../../../../../../../../functions/LPE-CT/src/smtp/dns/SystemDnsResolver/new.md)
- [run_submission_listener](../../../../../../../../functions/LPE-CT/src/submission/run_submission_listener.md)
- [smtp_xoauth_is_rejected_before_core_auth_request](../../../../../../../../functions/LPE-CT/src/submission/smtp_xoauth_is_rejected_before_core_auth_request.md)
- [submit_message_posts_trace_header_and_returns_success](../../../../../../../../functions/LPE-CT/src/submission/submit_message_posts_trace_header_and_returns_success.md)
- [submit_message_rejects_non_accepted_success_body_before_smtp_final_reply](../../../../../../../../functions/LPE-CT/src/submission/submit_message_rejects_non_accepted_success_body_before_smtp_final_reply.md)
- [check_optional_http_dependency](../../../../../../../../functions/crates/lpe-admin-api/src/readiness/check_optional_http_dependency.md)
- [run_outbound_worker](../../../../../../../../functions/crates/lpe-cli/src/run_outbound_worker.md)
- [handoff_client_posts_json_and_header](../../../../../../../../functions/crates/lpe-cli/src/handoff_client_posts_json_and_header.md)
- [new](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/new.md)
- [new_with_scoped_calendar_identities](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/new_with_scoped_calendar_identities.md)
- [mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts](../../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts.md)
- [drop](../../../../../../../../functions/crates/lpe-exchange/src/tests/TestSchemaCleanup/drop/drop.md)
- [drop](../../../../../../../../functions/crates/lpe-storage/tests/mapi_contact_create/TestSchemaCleanup/drop/drop.md)
- [drop](../../../../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/TestSchemaCleanup/drop/drop.md)