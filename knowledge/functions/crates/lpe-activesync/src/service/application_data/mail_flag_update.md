---
type: Rust Function
title: mail_flag_update
resource: crates/lpe-activesync/src/service/application_data.rs#L11-L66
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/application_data/active_sync_datetime_to_rfc3339
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_mail_sync_commands
---

# Signature

`pub(super) fn mail_flag_update(flag: &WbxmlNode) -> Result<JmapEmailFollowupUpdate>`

# Calls

- [active_sync_datetime_to_rfc3339](../../../../../../functions/crates/lpe-activesync/src/service/application_data/active_sync_datetime_to_rfc3339.md)

# Called by

- [apply_mail_sync_commands](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_mail_sync_commands.md)