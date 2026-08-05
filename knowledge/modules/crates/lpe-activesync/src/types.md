---
type: Rust Module
title: types
resource: crates/lpe-activesync/src/types.rs#L1-L286
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/base64-engine-general-purpose-standard-as-base64-engine-as
  - external/serde-deserialize-serialize
  - external/serde-json-value
  - external/uuid-uuid
  - external/crate-constants-active-sync-version-protocol-activesynccommand
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [ActiveSyncQuery](../../../../classes/crates/lpe-activesync/src/types/ActiveSyncQuery.md)
- [ParsedActiveSyncQuery](../../../../classes/crates/lpe-activesync/src/types/ParsedActiveSyncQuery.md)
- [from_raw_query](../../../../functions/crates/lpe-activesync/src/types/ParsedActiveSyncQuery/from_raw_query.md)
- [looks_like_plain_query](../../../../functions/crates/lpe-activesync/src/types/looks_like_plain_query.md)
- [parse_plain_query](../../../../functions/crates/lpe-activesync/src/types/parse_plain_query.md)
- [parse_base64_query](../../../../functions/crates/lpe-activesync/src/types/parse_base64_query.md)
- [decode_parameter_value](../../../../functions/crates/lpe-activesync/src/types/decode_parameter_value.md)
- [decode_protocol_version](../../../../functions/crates/lpe-activesync/src/types/decode_protocol_version.md)
- [decode_query_component](../../../../functions/crates/lpe-activesync/src/types/decode_query_component.md)
- [hex_value](../../../../functions/crates/lpe-activesync/src/types/hex_value.md)
- [save_in_sent_options](../../../../functions/crates/lpe-activesync/src/types/save_in_sent_options.md)
- [ByteCursor](../../../../classes/crates/lpe-activesync/src/types/ByteCursor.md)
- [new](../../../../functions/crates/lpe-activesync/src/types/ByteCursor/new.md)
- [has_remaining](../../../../functions/crates/lpe-activesync/src/types/ByteCursor/has_remaining.md)
- [take_u8](../../../../functions/crates/lpe-activesync/src/types/ByteCursor/take_u8.md)
- [take_array](../../../../functions/crates/lpe-activesync/src/types/ByteCursor/take_array.md)
- [take_string](../../../../functions/crates/lpe-activesync/src/types/ByteCursor/take_string.md)
- [take_exact](../../../../functions/crates/lpe-activesync/src/types/ByteCursor/take_exact.md)
- [CollectionDefinition](../../../../classes/crates/lpe-activesync/src/types/CollectionDefinition.md)
- [SnapshotEntry](../../../../classes/crates/lpe-activesync/src/types/SnapshotEntry.md)
- [SnapshotChange](../../../../classes/crates/lpe-activesync/src/types/SnapshotChange.md)
- [CollectionStateEntry](../../../../classes/crates/lpe-activesync/src/types/CollectionStateEntry.md)
- [StoredSyncState](../../../../classes/crates/lpe-activesync/src/types/StoredSyncState.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `base64::{engine::general_purpose::STANDARD as BASE64, Engine as _}`
- `serde::{Deserialize, Serialize}`
- `serde_json::Value`
- `uuid::Uuid`
- `crate::{constants::ACTIVE_SYNC_VERSION, protocol::ActiveSyncCommand}`

# Member of

- [lpe-activesync](../../../../packages/crates/lpe-activesync.md)