---
type: Rust Method
title: into_followup_update
resource: crates/lpe-storage/src/mail_items.rs#L19-L32
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mail_items/update_message_flags
  - functions/crates/lpe-storage/src/mail_items/message_flag_update_projects_followup_flag_status
---

# Signature

`pub fn into_followup_update(self) -> JmapEmailFollowupUpdate`

# Called by

- [update_message_flags](../../../../../../functions/crates/lpe-storage/src/mail_items/update_message_flags.md)
- [message_flag_update_projects_followup_flag_status](../../../../../../functions/crates/lpe-storage/src/mail_items/message_flag_update_projects_followup_flag_status.md)