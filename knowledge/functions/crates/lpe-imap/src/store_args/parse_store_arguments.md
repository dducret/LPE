---
type: Rust Function
title: parse_store_arguments
resource: crates/lpe-imap/src/store_args.rs#L17-L47
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/store_args/parse_store_condstore
  called_by:
  - functions/crates/lpe-imap/src/messages/Session/handle_store
---

# Signature

`pub(crate) fn parse_store_arguments( input: &str, ) -> Result<(String, StoreCondstore, String, String)>`

# Calls

- [parse_store_condstore](../../../../../functions/crates/lpe-imap/src/store_args/parse_store_condstore.md)

# Called by

- [handle_store](../../../../../functions/crates/lpe-imap/src/messages/Session/handle_store.md)