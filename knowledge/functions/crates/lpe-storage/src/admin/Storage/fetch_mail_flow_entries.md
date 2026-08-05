---
type: Rust Method
title: fetch_mail_flow_entries
resource: crates/lpe-storage/src/admin.rs#L958-L1018
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/console/mail_flow
---

# Signature

`pub async fn fetch_mail_flow_entries(&self) -> Result<Vec<MailFlowEntry>>`

# Called by

- [mail_flow](../../../../../../functions/crates/lpe-admin-api/src/console/mail_flow.md)