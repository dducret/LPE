---
type: Rust Function
title: append_rules_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/rules.rs#L152-L192
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_get_rules_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) async fn append_rules_dispatch_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, ) where S: ExchangeStore,`

# Calls

- [append_get_rules_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_get_rules_table_response.md)
- [append_modify_rules_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/rules/append_modify_rules_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)