---
type: Rust Function
title: log_default_folder_discovery_contract
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/default_folders.rs#L68-L98
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_default_folder_discovery_contract( principal: &AccountPrincipal, request_id: &str, stage: &str, request_rop_id: &str, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, )`

# Called by

- [append_logon_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_response.md)