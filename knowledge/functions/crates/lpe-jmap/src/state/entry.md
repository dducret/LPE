---
type: Rust Function
title: entry
resource: crates/lpe-jmap/src/state.rs#L754-L759
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/observability/record_outbound_handoff
  - functions/LPE-CT/src/observability/record_inbound_delivery
  - functions/LPE-CT/src/observability/record_smtp_session
  - functions/LPE-CT/src/observability/record_outlook_test_message
  - functions/LPE-CT/src/observability/record_security_event
  - functions/LPE-CT/src/observability/record_http_request
  - functions/LPE-CT/src/reporting/group_history
  - functions/LPE-CT/src/reporting/summarize_digest_counts
  - functions/LPE-CT/src/smtp/bayes/train_bayespam
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_direct_mx
  - functions/crates/lpe-admin-api/src/observability/record_mail_submission
  - functions/crates/lpe-admin-api/src/observability/record_inbound_delivery
  - functions/crates/lpe-admin-api/src/observability/record_outbound_dispatch
  - functions/crates/lpe-admin-api/src/observability/record_security_event
  - functions/crates/lpe-admin-api/src/observability/record_http_request
  - functions/crates/lpe-core/src/outlook_trace/validate_mapi_protocol_request_response_pairs
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_mutation_base_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_named_property_id_reuse
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/format_named_property_id_sources
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/format_named_property_family_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/summarize_named_property_id_duplicates
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/mark_folder_profile_property_tombstones
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/format_nspi_duplicate_entry_keys_for_debug
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/ReplicaCounterSets/local_mut
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/ReplicaCounterSets/insert
  - functions/crates/lpe-exchange/src/service/ews/compliance/get_non_indexable_item_statistics_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/mark_rpc_proxy_out_endpoint_bind_ack
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/queue_pending_rpc_proxy_out_channel_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_channels/mark_rpc_proxy_out_endpoint_rts_connect
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_mapi_contact
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_event_update
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/add_message_attachment
  - functions/crates/lpe-imap/src/acl/combine_acl_state
  - functions/crates/lpe-imap/src/tests/FakeStore/allocate_uid
  - functions/crates/lpe-jmap/src/blob/blob_lookup_index
  - functions/crates/lpe-jmap/src/contacts/contact_array_to_named_object
  - functions/crates/lpe-jmap/src/convert/apply_jmap_property_path
  - functions/crates/lpe-jmap/src/service/JmapService/handle_reminder_set
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state_entries_with_bcc
  - functions/crates/lpe-jmap/src/state/push_state_entries_to_types
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/copy_jmap_email_between_accounts
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/add_calendar_event_attachment
  - functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes
  - functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states
  - functions/crates/lpe-storage/src/attachments/Storage/fetch_calendar_attachments_for_events
  - functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/insert_accounts
  - functions/crates/lpe-storage/src/imap/Storage/fetch_imap_emails
  - functions/crates/lpe-storage/src/mail_items/delete_jmap_email_memberships
  - functions/crates/lpe-storage/src/protocols/Storage/fetch_visible_protected_bcc_recipients
---

# Signature

`fn entry(id: &str, fingerprint: &str) -> StateEntry`

# Called by

- [record_outbound_handoff](../../../../../functions/LPE-CT/src/observability/record_outbound_handoff.md)
- [record_inbound_delivery](../../../../../functions/LPE-CT/src/observability/record_inbound_delivery.md)
- [record_smtp_session](../../../../../functions/LPE-CT/src/observability/record_smtp_session.md)
- [record_outlook_test_message](../../../../../functions/LPE-CT/src/observability/record_outlook_test_message.md)
- [record_security_event](../../../../../functions/LPE-CT/src/observability/record_security_event.md)
- [record_http_request](../../../../../functions/LPE-CT/src/observability/record_http_request.md)
- [group_history](../../../../../functions/LPE-CT/src/reporting/group_history.md)
- [summarize_digest_counts](../../../../../functions/LPE-CT/src/reporting/summarize_digest_counts.md)
- [train_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/train_bayespam.md)
- [relay_message_direct_mx](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_direct_mx.md)
- [record_mail_submission](../../../../../functions/crates/lpe-admin-api/src/observability/record_mail_submission.md)
- [record_inbound_delivery](../../../../../functions/crates/lpe-admin-api/src/observability/record_inbound_delivery.md)
- [record_outbound_dispatch](../../../../../functions/crates/lpe-admin-api/src/observability/record_outbound_dispatch.md)
- [record_security_event](../../../../../functions/crates/lpe-admin-api/src/observability/record_security_event.md)
- [record_http_request](../../../../../functions/crates/lpe-admin-api/src/observability/record_http_request.md)
- [validate_mapi_protocol_request_response_pairs](../../../../../functions/crates/lpe-core/src/outlook_trace/validate_mapi_protocol_request_response_pairs.md)
- [associated_config_mutation_base_properties](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_mutation_base_properties.md)
- [append_delete_attachment_response](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response.md)
- [append_save_changes_attachment_response](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_save_changes_attachment_response.md)
- [format_named_property_id_reuse](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_named_property_id_reuse.md)
- [hydrate_folder_handle_properties_for_request](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request.md)
- [format_named_property_id_sources](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/format_named_property_id_sources.md)
- [format_named_property_family_summary](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/format_named_property_family_summary.md)
- [summarize_named_property_id_duplicates](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/summarize_named_property_id_duplicates.md)
- [mark_folder_profile_property_tombstones](../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/mark_folder_profile_property_tombstones.md)
- [format_nspi_duplicate_entry_keys_for_debug](../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/format_nspi_duplicate_entry_keys_for_debug.md)
- [local_mut](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/ReplicaCounterSets/local_mut.md)
- [insert](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/ReplicaCounterSets/insert.md)
- [get_non_indexable_item_statistics_response](../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/get_non_indexable_item_statistics_response.md)
- [mark_rpc_proxy_out_endpoint_bind_ack](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/mark_rpc_proxy_out_endpoint_bind_ack.md)
- [queue_pending_rpc_proxy_out_channel_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/queue_pending_rpc_proxy_out_channel_response.md)
- [mark_rpc_proxy_out_endpoint_rts_connect](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_channels/mark_rpc_proxy_out_endpoint_rts_connect.md)
- [fetch_or_allocate_mapi_identities](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [create_mapi_contact](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_mapi_contact.md)
- [commit_mapi_event_update](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_event_update.md)
- [add_message_attachment](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/add_message_attachment.md)
- [combine_acl_state](../../../../../functions/crates/lpe-imap/src/acl/combine_acl_state.md)
- [allocate_uid](../../../../../functions/crates/lpe-imap/src/tests/FakeStore/allocate_uid.md)
- [blob_lookup_index](../../../../../functions/crates/lpe-jmap/src/blob/blob_lookup_index.md)
- [contact_array_to_named_object](../../../../../functions/crates/lpe-jmap/src/contacts/contact_array_to_named_object.md)
- [apply_jmap_property_path](../../../../../functions/crates/lpe-jmap/src/convert/apply_jmap_property_path.md)
- [handle_reminder_set](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_reminder_set.md)
- [mail_object_state_entries_with_bcc](../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state_entries_with_bcc.md)
- [push_state_entries_to_types](../../../../../functions/crates/lpe-jmap/src/state/push_state_entries_to_types.md)
- [copy_jmap_email_between_accounts](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/copy_jmap_email_between_accounts.md)
- [add_calendar_event_attachment](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/add_calendar_event_attachment.md)
- [compute_push_changes](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes.md)
- [current_push_states](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states.md)
- [fetch_calendar_attachments_for_events](../../../../../functions/crates/lpe-storage/src/attachments/Storage/fetch_calendar_attachments_for_events.md)
- [insert_accounts](../../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/insert_accounts.md)
- [fetch_imap_emails](../../../../../functions/crates/lpe-storage/src/imap/Storage/fetch_imap_emails.md)
- [delete_jmap_email_memberships](../../../../../functions/crates/lpe-storage/src/mail_items/delete_jmap_email_memberships.md)
- [fetch_visible_protected_bcc_recipients](../../../../../functions/crates/lpe-storage/src/protocols/Storage/fetch_visible_protected_bcc_recipients.md)