---
type: Rust Module
title: outlook_trace
resource: crates/lpe-core/src/outlook_trace.rs#L1-L831
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/base64-engine-general-purpose-standard-as-base64-standard-engine-as
  - external/std-collections-hash-map-defaulthasher-env-fs-self-openoptions-hash-hash-hasher-io-bufread-bufreader-write-path-path-pathbuf-time-systemtime-unix-epoch
  - external/uuid-uuid
  - external/std-os-unix-fs-permissionsext
  - external/std-os-unix-fs-openoptionsext
  - external/super
  - external/std-collections-hashmap-fs
  member_of:
  - packages/crates/lpe-core
---

# Contains

- [OutlookTraceConfig](../../../../classes/crates/lpe-core/src/outlook_trace/OutlookTraceConfig.md)
- [from_env](../../../../functions/crates/lpe-core/src/outlook_trace/OutlookTraceConfig/from_env.md)
- [OutlookTraceDirection](../../../../classes/crates/lpe-core/src/outlook_trace/OutlookTraceDirection.md)
- [as_str](../../../../functions/crates/lpe-core/src/outlook_trace/OutlookTraceDirection/as_str.md)
- [OutlookTraceEvent](../../../../classes/crates/lpe-core/src/outlook_trace/OutlookTraceEvent.md)
- [write_outlook_trace](../../../../functions/crates/lpe-core/src/outlook_trace/write_outlook_trace.md)
- [write_outlook_trace_with_config](../../../../functions/crates/lpe-core/src/outlook_trace/write_outlook_trace_with_config.md)
- [write_event](../../../../functions/crates/lpe-core/src/outlook_trace/write_event.md)
- [create_trace_dir](../../../../functions/crates/lpe-core/src/outlook_trace/create_trace_dir.md)
- [set_restrictive_dir_permissions](../../../../functions/crates/lpe-core/src/outlook_trace/set_restrictive_dir_permissions.md)
- [set_restrictive_dir_permissions](../../../../functions/crates/lpe-core/src/outlook_trace/set_restrictive_dir_permissions-2.md)
- [open_trace_file](../../../../functions/crates/lpe-core/src/outlook_trace/open_trace_file.md)
- [open_trace_file_with_mode](../../../../functions/crates/lpe-core/src/outlook_trace/open_trace_file_with_mode.md)
- [open_trace_file_with_mode](../../../../functions/crates/lpe-core/src/outlook_trace/open_trace_file_with_mode-2.md)
- [TraceFilePaths](../../../../classes/crates/lpe-core/src/outlook_trace/TraceFilePaths.md)
- [trace_file_paths](../../../../functions/crates/lpe-core/src/outlook_trace/trace_file_paths.md)
- [next_trace_sequence](../../../../functions/crates/lpe-core/src/outlook_trace/next_trace_sequence.md)
- [TraceRenderContext](../../../../classes/crates/lpe-core/src/outlook_trace/TraceRenderContext.md)
- [new](../../../../functions/crates/lpe-core/src/outlook_trace/TraceRenderContext/new.md)
- [render_event](../../../../functions/crates/lpe-core/src/outlook_trace/render_event.md)
- [render_replay_event](../../../../functions/crates/lpe-core/src/outlook_trace/render_replay_event.md)
- [render_request_response_event](../../../../functions/crates/lpe-core/src/outlook_trace/render_request_response_event.md)
- [redacted_metadata](../../../../functions/crates/lpe-core/src/outlook_trace/redacted_metadata.md)
- [is_protocol_event](../../../../functions/crates/lpe-core/src/outlook_trace/is_protocol_event.md)
- [sanitized_payload_summary](../../../../functions/crates/lpe-core/src/outlook_trace/sanitized_payload_summary.md)
- [redact_metadata_value](../../../../functions/crates/lpe-core/src/outlook_trace/redact_metadata_value.md)
- [redact_sensitive_text](../../../../functions/crates/lpe-core/src/outlook_trace/redact_sensitive_text.md)
- [redact_named_text](../../../../functions/crates/lpe-core/src/outlook_trace/redact_named_text.md)
- [is_sensitive_name](../../../../functions/crates/lpe-core/src/outlook_trace/is_sensitive_name.md)
- [json_pair](../../../../functions/crates/lpe-core/src/outlook_trace/json_pair.md)
- [json_object_pair](../../../../functions/crates/lpe-core/src/outlook_trace/json_object_pair.md)
- [escape_json](../../../../functions/crates/lpe-core/src/outlook_trace/escape_json.md)
- [safe_component](../../../../functions/crates/lpe-core/src/outlook_trace/safe_component.md)
- [stable_hash](../../../../functions/crates/lpe-core/src/outlook_trace/stable_hash.md)
- [unix_timestamp_millis](../../../../functions/crates/lpe-core/src/outlook_trace/unix_timestamp_millis.md)
- [env_flag](../../../../functions/crates/lpe-core/src/outlook_trace/env_flag.md)
- [temp_trace_dir](../../../../functions/crates/lpe-core/src/outlook_trace/temp_trace_dir.md)
- [trace_file_names](../../../../functions/crates/lpe-core/src/outlook_trace/trace_file_names.md)
- [trace_file_with_suffix](../../../../functions/crates/lpe-core/src/outlook_trace/trace_file_with_suffix.md)
- [legacy_trace_file](../../../../functions/crates/lpe-core/src/outlook_trace/legacy_trace_file.md)
- [json_string_value](../../../../functions/crates/lpe-core/src/outlook_trace/json_string_value.md)
- [disabled_trace_does_not_create_files](../../../../functions/crates/lpe-core/src/outlook_trace/disabled_trace_does_not_create_files.md)
- [trace_file_name_uses_generated_safe_session_hash](../../../../functions/crates/lpe-core/src/outlook_trace/trace_file_name_uses_generated_safe_session_hash.md)
- [trace_events_append_in_order_with_matching_replay_and_rr_steps](../../../../functions/crates/lpe-core/src/outlook_trace/trace_events_append_in_order_with_matching_replay_and_rr_steps.md)
- [mapi_protocol_exports_ignore_non_protocol_diagnostics_for_request_pairing](../../../../functions/crates/lpe-core/src/outlook_trace/mapi_protocol_exports_ignore_non_protocol_diagnostics_for_request_pairing.md)
- [sanitized_mode_redacts_secrets_without_raw_payload](../../../../functions/crates/lpe-core/src/outlook_trace/sanitized_mode_redacts_secrets_without_raw_payload.md)
- [raw_mode_writes_payload_only_when_explicitly_enabled](../../../../functions/crates/lpe-core/src/outlook_trace/raw_mode_writes_payload_only_when_explicitly_enabled.md)
- [rr_trace_names_outbound_payload_as_response_body](../../../../functions/crates/lpe-core/src/outlook_trace/rr_trace_names_outbound_payload_as_response_body.md)
- [validate_mapi_protocol_request_response_pairs](../../../../functions/crates/lpe-core/src/outlook_trace/validate_mapi_protocol_request_response_pairs.md)
- [sample_event](../../../../functions/crates/lpe-core/src/outlook_trace/sample_event.md)

# Imports

- `base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _}`
- `std::{
    collections::hash_map::DefaultHasher,
    env,
    fs::{self, OpenOptions},
    hash::{Hash, Hasher},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
}`
- `uuid::Uuid`
- `std::os::unix::fs::PermissionsExt`
- `std::os::unix::fs::OpenOptionsExt`
- `super::*`
- `std::{collections::HashMap, fs}`

# Member of

- [lpe-core](../../../../packages/crates/lpe-core.md)