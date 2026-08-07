---
type: Rust Module
title: helpers
resource: crates/lpe-jmap/src/service/helpers.rs#L1-L910
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/super-authorization-header
  - external/axum-http-headermap-headervalue
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [api_request_exceeds_call_limit](../../../../../functions/crates/lpe-jmap/src/service/helpers/api_request_exceeds_call_limit.md)
- [requested_account_id_from_arguments](../../../../../functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments.md)
- [string_ids_from_arguments](../../../../../functions/crates/lpe-jmap/src/service/helpers/string_ids_from_arguments.md)
- [property_names_from_arguments](../../../../../functions/crates/lpe-jmap/src/service/helpers/property_names_from_arguments.md)
- [project_get_properties](../../../../../functions/crates/lpe-jmap/src/service/helpers/project_get_properties.md)
- [object_keys](../../../../../functions/crates/lpe-jmap/src/service/helpers/object_keys.md)
- [canonical_create_ids](../../../../../functions/crates/lpe-jmap/src/service/helpers/canonical_create_ids.md)
- [parse_reminder_id](../../../../../functions/crates/lpe-jmap/src/service/helpers/parse_reminder_id.md)
- [parse_share_input](../../../../../functions/crates/lpe-jmap/src/service/helpers/parse_share_input.md)
- [share_audit](../../../../../functions/crates/lpe-jmap/src/service/helpers/share_audit.md)
- [rule_to_value](../../../../../functions/crates/lpe-jmap/src/service/helpers/rule_to_value.md)
- [outlook_profile_state_to_value](../../../../../functions/crates/lpe-jmap/src/service/helpers/outlook_profile_state_to_value.md)
- [canonical_query_state_method](../../../../../functions/crates/lpe-jmap/src/service/helpers/canonical_query_state_method.md)
- [canonical_query_filter](../../../../../functions/crates/lpe-jmap/src/service/helpers/canonical_query_filter.md)
- [search_folder_to_value](../../../../../functions/crates/lpe-jmap/src/service/helpers/search_folder_to_value.md)
- [search_folder_input_from_value](../../../../../functions/crates/lpe-jmap/src/service/helpers/search_folder_input_from_value.md)
- [validate_declared_capabilities](../../../../../functions/crates/lpe-jmap/src/service/helpers/validate_declared_capabilities.md)
- [is_supported_capability](../../../../../functions/crates/lpe-jmap/src/service/helpers/is_supported_capability.md)
- [method_capability](../../../../../functions/crates/lpe-jmap/src/service/helpers/method_capability.md)
- [is_method_error_payload](../../../../../functions/crates/lpe-jmap/src/service/helpers/is_method_error_payload.md)
- [resolve_result_references](../../../../../functions/crates/lpe-jmap/src/service/helpers/resolve_result_references.md)
- [result_reference_error](../../../../../functions/crates/lpe-jmap/src/service/helpers/result_reference_error.md)
- [method_object_limit_error](../../../../../functions/crates/lpe-jmap/src/service/helpers/method_object_limit_error.md)
- [object_array_len](../../../../../functions/crates/lpe-jmap/src/service/helpers/object_array_len.md)
- [object_map_len](../../../../../functions/crates/lpe-jmap/src/service/helpers/object_map_len.md)
- [set_object_count](../../../../../functions/crates/lpe-jmap/src/service/helpers/set_object_count.md)
- [authorization_header](../../../../../functions/crates/lpe-jmap/src/service/helpers/authorization_header.md)
- [bearer_token](../../../../../functions/crates/lpe-jmap/src/service/helpers/bearer_token.md)
- [websocket_authentication_accepts_the_same_origin_mail_session_cookie](../../../../../functions/crates/lpe-jmap/src/service/helpers/websocket_authentication_accepts_the_same_origin_mail_session_cookie.md)
- [collection_state_fingerprint](../../../../../functions/crates/lpe-jmap/src/service/helpers/collection_state_fingerprint.md)
- [email_submission_state_fingerprint](../../../../../functions/crates/lpe-jmap/src/service/helpers/email_submission_state_fingerprint.md)
- [identity_state_fingerprint](../../../../../functions/crates/lpe-jmap/src/service/helpers/identity_state_fingerprint.md)
- [mailbox_state_fingerprint](../../../../../functions/crates/lpe-jmap/src/service/helpers/mailbox_state_fingerprint.md)
- [contact_state_fingerprint](../../../../../functions/crates/lpe-jmap/src/service/helpers/contact_state_fingerprint.md)
- [event_state_fingerprint](../../../../../functions/crates/lpe-jmap/src/service/helpers/event_state_fingerprint.md)
- [task_state_fingerprint](../../../../../functions/crates/lpe-jmap/src/service/helpers/task_state_fingerprint.md)
- [task_list_state_fingerprint](../../../../../functions/crates/lpe-jmap/src/service/helpers/task_list_state_fingerprint.md)
- [email_state_fingerprint](../../../../../functions/crates/lpe-jmap/src/service/helpers/email_state_fingerprint.md)
- [format_mailbox_ids](../../../../../functions/crates/lpe-jmap/src/service/helpers/format_mailbox_ids.md)
- [format_mailbox_states](../../../../../functions/crates/lpe-jmap/src/service/helpers/format_mailbox_states.md)
- [opaque_state_fingerprint](../../../../../functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint.md)
- [trim_snippet](../../../../../functions/crates/lpe-jmap/src/service/helpers/trim_snippet.md)

# Imports

- `super::*`
- `super::authorization_header`
- `axum::http::{HeaderMap, HeaderValue}`

# Member of

- [lpe-jmap](../../../../../packages/crates/lpe-jmap.md)