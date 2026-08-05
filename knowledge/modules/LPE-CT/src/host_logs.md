---
type: Rust Module
title: host_logs
resource: LPE-CT/src/host_logs.rs#L1-L296
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-http-statuscode
  - external/serde-serialize
  - external/std-env-fs-io-path-path-pathbuf-time-unix-epoch
  member_of:
  - packages/LPE-CT
---

# Contains

- [HostLogList](../../../classes/LPE-CT/src/host_logs/HostLogList.md)
- [HostLogItem](../../../classes/LPE-CT/src/host_logs/HostLogItem.md)
- [HostLogContent](../../../classes/LPE-CT/src/host_logs/HostLogContent.md)
- [HostLogDownload](../../../classes/LPE-CT/src/host_logs/HostLogDownload.md)
- [HostLogError](../../../classes/LPE-CT/src/host_logs/HostLogError.md)
- [new](../../../functions/LPE-CT/src/host_logs/HostLogError/new.md)
- [status](../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [message](../../../functions/LPE-CT/src/host_logs/HostLogError/message.md)
- [list](../../../functions/LPE-CT/src/host_logs/list.md)
- [read_content](../../../functions/LPE-CT/src/host_logs/read_content.md)
- [download](../../../functions/LPE-CT/src/host_logs/download.md)
- [delete](../../../functions/LPE-CT/src/host_logs/delete.md)
- [host_log_dir](../../../functions/LPE-CT/src/host_logs/host_log_dir.md)
- [category_definition](../../../functions/LPE-CT/src/host_logs/category_definition.md)
- [discover_log_names](../../../functions/LPE-CT/src/host_logs/discover_log_names.md)
- [item_for_name](../../../functions/LPE-CT/src/host_logs/item_for_name.md)
- [virtual_item](../../../functions/LPE-CT/src/host_logs/virtual_item.md)
- [resolve_log](../../../functions/LPE-CT/src/host_logs/resolve_log.md)
- [is_allowed_log_name](../../../functions/LPE-CT/src/host_logs/is_allowed_log_name.md)
- [is_previewable](../../../functions/LPE-CT/src/host_logs/is_previewable.md)
- [io_error](../../../functions/LPE-CT/src/host_logs/io_error.md)
- [LogCategory](../../../classes/LPE-CT/src/host_logs/LogCategory.md)
- [ResolvedLog](../../../classes/LPE-CT/src/host_logs/ResolvedLog.md)

# Imports

- `axum::http::StatusCode`
- `serde::Serialize`
- `std::{
    env, fs, io,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
}`

# Member of

- [lpe-ct](../../../packages/LPE-CT.md)