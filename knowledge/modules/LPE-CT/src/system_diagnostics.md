---
type: Rust Module
title: system_diagnostics
resource: LPE-CT/src/system_diagnostics.rs#L1-L475
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-context-result
  - external/base64-engine-general-purpose-standard-as-base64-engine-as
  - external/serde-deserialize-serialize
  - external/std-env-net-ipv4addr-path-pathbuf-time-duration
  - external/tokio-fs-process-command-time
  - external/uuid-uuid
  - external/super
  member_of:
  - packages/LPE-CT
---

# Contains

- [ServiceStatusList](../../../classes/LPE-CT/src/system_diagnostics/ServiceStatusList.md)
- [ServiceStatus](../../../classes/LPE-CT/src/system_diagnostics/ServiceStatus.md)
- [DiagnosticReport](../../../classes/LPE-CT/src/system_diagnostics/DiagnosticReport.md)
- [ToolRunRequest](../../../classes/LPE-CT/src/system_diagnostics/ToolRunRequest.md)
- [SpamTestRequest](../../../classes/LPE-CT/src/system_diagnostics/SpamTestRequest.md)
- [service_statuses](../../../functions/LPE-CT/src/system_diagnostics/service_statuses.md)
- [service_action](../../../functions/LPE-CT/src/system_diagnostics/service_action.md)
- [command_diagnostic](../../../functions/LPE-CT/src/system_diagnostics/command_diagnostic.md)
- [routing_table_report](../../../functions/LPE-CT/src/system_diagnostics/routing_table_report.md)
- [routing_table_from_proc](../../../functions/LPE-CT/src/system_diagnostics/routing_table_from_proc.md)
- [format_proc_ipv4_routes](../../../functions/LPE-CT/src/system_diagnostics/format_proc_ipv4_routes.md)
- [format_proc_ipv4_route](../../../functions/LPE-CT/src/system_diagnostics/format_proc_ipv4_route.md)
- [ipv4_from_proc_hex](../../../functions/LPE-CT/src/system_diagnostics/ipv4_from_proc_hex.md)
- [run_tool](../../../functions/LPE-CT/src/system_diagnostics/run_tool.md)
- [support_connect](../../../functions/LPE-CT/src/system_diagnostics/support_connect.md)
- [spam_test](../../../functions/LPE-CT/src/system_diagnostics/spam_test.md)
- [flush_mail_queue](../../../functions/LPE-CT/src/system_diagnostics/flush_mail_queue.md)
- [service_status](../../../functions/LPE-CT/src/system_diagnostics/service_status.md)
- [service_definition](../../../functions/LPE-CT/src/system_diagnostics/service_definition.md)
- [command_report](../../../functions/LPE-CT/src/system_diagnostics/command_report.md)
- [run_command](../../../functions/LPE-CT/src/system_diagnostics/run_command.md)
- [output_text](../../../functions/LPE-CT/src/system_diagnostics/output_text.md)
- [validate_target](../../../functions/LPE-CT/src/system_diagnostics/validate_target.md)
- [ConfiguredCommand](../../../classes/LPE-CT/src/system_diagnostics/ConfiguredCommand.md)
- [configured_command](../../../functions/LPE-CT/src/system_diagnostics/configured_command.md)
- [env_value](../../../functions/LPE-CT/src/system_diagnostics/env_value.md)
- [formats_proc_default_route](../../../functions/LPE-CT/src/system_diagnostics/formats_proc_default_route.md)
- [formats_proc_network_route](../../../functions/LPE-CT/src/system_diagnostics/formats_proc_network_route.md)

# Imports

- `anyhow::{anyhow, bail, Context, Result}`
- `base64::{engine::general_purpose::STANDARD as BASE64, Engine as _}`
- `serde::{Deserialize, Serialize}`
- `std::{env, net::Ipv4Addr, path::PathBuf, time::Duration}`
- `tokio::{fs, process::Command, time}`
- `uuid::Uuid`
- `super::*`

# Member of

- [lpe-ct](../../../packages/LPE-CT.md)