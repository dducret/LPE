---
type: Rust Function
title: smtp_starttls_upgrades_to_tls_after_ready_reply
resource: LPE-CT/src/smtp/tests.rs#L1614-L1748
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/env_test_lock
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tls/smtp_starttls_acceptor_for_paths
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/LPE-CT/src/smtp/tests/spawn_dummy_core
  - functions/LPE-CT/src/smtp/tests/plaintext_inbound_store
  - functions/LPE-CT/src/smtp/session/handle_smtp_session
  - functions/crates/lpe-storage/src/core/Storage/connect
  - functions/LPE-CT/src/smtp/tests/read_test_smtp_reply
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/LPE-CT/web/app/smoke/test/MockClassList/add
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
---

# Signature

`async fn smtp_starttls_upgrades_to_tls_after_ready_reply()`

# Calls

- [env_test_lock](../../../../../functions/LPE-CT/src/env_test_lock.md)
- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [smtp_starttls_acceptor_for_paths](../../../../../functions/LPE-CT/src/smtp/tls/smtp_starttls_acceptor_for_paths.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [spawn_dummy_core](../../../../../functions/LPE-CT/src/smtp/tests/spawn_dummy_core.md)
- [plaintext_inbound_store](../../../../../functions/LPE-CT/src/smtp/tests/plaintext_inbound_store.md)
- [handle_smtp_session](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_session.md)
- [connect](../../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [read_test_smtp_reply](../../../../../functions/LPE-CT/src/smtp/tests/read_test_smtp_reply.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [empty](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [add](../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/add.md)
- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)