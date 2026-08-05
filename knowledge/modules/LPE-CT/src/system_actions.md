---
type: Rust Module
title: system_actions
resource: LPE-CT/src/system_actions.rs#L1-L191
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-context-result
  - external/serde-deserialize-serialize
  - external/std-process-output-time-duration
  - external/tokio-io-asyncwriteext-process-command-time
  member_of:
  - packages/LPE-CT
---

# Contains

- [NtpUpdateRequest](../../../classes/LPE-CT/src/system_actions/NtpUpdateRequest.md)
- [SystemActionResponse](../../../classes/LPE-CT/src/system_actions/SystemActionResponse.md)
- [update_ntp](../../../functions/LPE-CT/src/system_actions/update_ntp.md)
- [sync_ntp](../../../functions/LPE-CT/src/system_actions/sync_ntp.md)
- [apt_update_upgrade](../../../functions/LPE-CT/src/system_actions/apt_update_upgrade.md)
- [power_action](../../../functions/LPE-CT/src/system_actions/power_action.md)
- [normalize_servers](../../../functions/LPE-CT/src/system_actions/normalize_servers.md)
- [run_host_action](../../../functions/LPE-CT/src/system_actions/run_host_action.md)
- [output_text](../../../functions/LPE-CT/src/system_actions/output_text.md)

# Imports

- `anyhow::{bail, Context, Result}`
- `serde::{Deserialize, Serialize}`
- `std::{process::Output, time::Duration}`
- `tokio::{io::AsyncWriteExt, process::Command, time}`

# Member of

- [lpe-ct](../../../packages/LPE-CT.md)