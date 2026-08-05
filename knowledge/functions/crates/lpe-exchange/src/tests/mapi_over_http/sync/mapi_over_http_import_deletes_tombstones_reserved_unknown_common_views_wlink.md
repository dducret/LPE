---
type: Rust Function
title: mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink
resource: crates/lpe-exchange/src/tests/mapi_over_http/sync.rs#L14640-L14808
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/reserve_mapi_local_replica_ids
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/mapi_private_logon_rops
  - functions/crates/lpe-exchange/src/tests/renew_mapi_request_id
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/append_rop_sync_import_deletes
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_import
  - functions/crates/lpe-exchange/src/tests/test_filetime
---

# Signature

`async fn mapi_over_http_import_deletes_tombstones_reserved_unknown_common_views_wlink( ) -> anyhow::Result<()>`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [reserve_mapi_local_replica_ids](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/reserve_mapi_local_replica_ids.md)
- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [mapi_private_logon_rops](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_private_logon_rops.md)
- [renew_mapi_request_id](../../../../../../../functions/crates/lpe-exchange/src/tests/renew_mapi_request_id.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [append_rop_sync_import_deletes](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/append_rop_sync_import_deletes.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)
- [pool](../../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [commit_mapi_navigation_shortcut_import](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_import.md)
- [test_filetime](../../../../../../../functions/crates/lpe-exchange/src/tests/test_filetime.md)