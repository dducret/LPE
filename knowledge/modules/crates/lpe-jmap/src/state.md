---
type: Rust Module
title: state
resource: crates/lpe-jmap/src/state.rs#L1-L1011
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/base64-engine-general-purpose-url-safe-no-pad-engine-as
  - external/serde-deserialize-serialize
  - external/serde-json-json-value
  - external/std-collections-hashmap
  - external/uuid-uuid
  - external/super
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [QueryStateToken](../../../../classes/crates/lpe-jmap/src/state/QueryStateToken.md)
- [QueryDiff](../../../../classes/crates/lpe-jmap/src/state/QueryDiff.md)
- [StateToken](../../../../classes/crates/lpe-jmap/src/state/StateToken.md)
- [PushStateToken](../../../../classes/crates/lpe-jmap/src/state/PushStateToken.md)
- [StateEntry](../../../../classes/crates/lpe-jmap/src/state/StateEntry.md)
- [DurableObjectChange](../../../../classes/crates/lpe-jmap/src/state/DurableObjectChange.md)
- [changes_response](../../../../functions/crates/lpe-jmap/src/state/changes_response.md)
- [state_cursor](../../../../functions/crates/lpe-jmap/src/state/state_cursor.md)
- [changes_response_from_durable_with_cursor](../../../../functions/crates/lpe-jmap/src/state/changes_response_from_durable_with_cursor.md)
- [changes_response_with_cursor](../../../../functions/crates/lpe-jmap/src/state/changes_response_with_cursor.md)
- [finish_changes_response](../../../../functions/crates/lpe-jmap/src/state/finish_changes_response.md)
- [apply_state_changes](../../../../functions/crates/lpe-jmap/src/state/apply_state_changes.md)
- [encode_state](../../../../functions/crates/lpe-jmap/src/state/encode_state.md)
- [encode_state_with_cursor](../../../../functions/crates/lpe-jmap/src/state/encode_state_with_cursor.md)
- [decode_state](../../../../functions/crates/lpe-jmap/src/state/decode_state.md)
- [encode_push_state](../../../../functions/crates/lpe-jmap/src/state/encode_push_state.md)
- [decode_push_state](../../../../functions/crates/lpe-jmap/src/state/decode_push_state.md)
- [push_state_entries_to_types](../../../../functions/crates/lpe-jmap/src/state/push_state_entries_to_types.md)
- [encode_query_state](../../../../functions/crates/lpe-jmap/src/state/encode_query_state.md)
- [encode_query_state_reference](../../../../functions/crates/lpe-jmap/src/state/encode_query_state_reference.md)
- [encode_query_state_parts](../../../../functions/crates/lpe-jmap/src/state/encode_query_state_parts.md)
- [decode_query_state](../../../../functions/crates/lpe-jmap/src/state/decode_query_state.md)
- [query_changes_response](../../../../functions/crates/lpe-jmap/src/state/query_changes_response.md)
- [query_changes_response_from_diff](../../../../functions/crates/lpe-jmap/src/state/query_changes_response_from_diff.md)
- [validate_query_state_token](../../../../functions/crates/lpe-jmap/src/state/validate_query_state_token.md)
- [query_diff_for_kind](../../../../functions/crates/lpe-jmap/src/state/query_diff_for_kind.md)
- [query_position](../../../../functions/crates/lpe-jmap/src/state/query_position.md)
- [compute_query_diff](../../../../functions/crates/lpe-jmap/src/state/compute_query_diff.md)
- [truncate_query_diff](../../../../functions/crates/lpe-jmap/src/state/truncate_query_diff.md)
- [apply_query_changes](../../../../functions/crates/lpe-jmap/src/state/apply_query_changes.md)
- [compute_query_diff_with_reorders](../../../../functions/crates/lpe-jmap/src/state/compute_query_diff_with_reorders.md)
- [entry](../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [changes_response_returns_intermediate_state_when_truncated](../../../../functions/crates/lpe-jmap/src/state/changes_response_returns_intermediate_state_when_truncated.md)
- [changes_response_rejects_invalid_or_mismatched_state_tokens](../../../../functions/crates/lpe-jmap/src/state/changes_response_rejects_invalid_or_mismatched_state_tokens.md)
- [state_tokens_preserve_optional_change_log_cursor](../../../../functions/crates/lpe-jmap/src/state/state_tokens_preserve_optional_change_log_cursor.md)
- [truncated_changes_do_not_advance_change_log_cursor](../../../../functions/crates/lpe-jmap/src/state/truncated_changes_do_not_advance_change_log_cursor.md)
- [query_changes_response_returns_intermediate_query_state_when_truncated](../../../../functions/crates/lpe-jmap/src/state/query_changes_response_returns_intermediate_query_state_when_truncated.md)
- [email_query_changes_reports_reorders_and_paginates_to_current_order](../../../../functions/crates/lpe-jmap/src/state/email_query_changes_reports_reorders_and_paginates_to_current_order.md)
- [query_position_supports_anchors_and_negative_offsets](../../../../functions/crates/lpe-jmap/src/state/query_position_supports_anchors_and_negative_offsets.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _}`
- `serde::{Deserialize, Serialize}`
- `serde_json::{json, Value}`
- `std::collections::HashMap`
- `uuid::Uuid`
- `super::*`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)