---
type: Python Function
title: jmap_inbox_mailbox_id
resource: tools/operations_benchmark.py#L697-L705
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/operations_benchmark/method_response
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/tools/operations_benchmark/benchmark_activesync
---

# Signature

`def jmap_inbox_mailbox_id(account: AccountLogin) -> str | None:`

# Calls

- [method_response](../../../functions/tools/operations_benchmark/method_response.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [next](../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [benchmark_activesync](../../../functions/tools/operations_benchmark/benchmark_activesync.md)