---
type: Rust Function
title: imported_message_move_destination_global_counter
resource: crates/lpe-storage/src/message_ops.rs#L1470-L1491
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/message_ops/rekey_mapi_message_identity_in_tx
---

# Signature

`fn imported_message_move_destination_global_counter( identity: &MapiMessageImportedMoveIdentity, ) -> Result<u64>`

# Called by

- [rekey_mapi_message_identity_in_tx](../../../../../functions/crates/lpe-storage/src/message_ops/rekey_mapi_message_identity_in_tx.md)