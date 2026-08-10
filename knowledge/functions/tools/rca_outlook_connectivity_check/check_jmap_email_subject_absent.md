---
type: Python Function
title: check_jmap_email_subject_absent
resource: tools/rca_outlook_connectivity_check.py#L629-L667
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_connectivity_check/check_jmap_session
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/http/join_url
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture
---

# Signature

`def check_jmap_email_subject_absent( base_url: str, email: str, password: str, subject: str, insecure_tls: bool, timeout: int, ) -> None:`

# Calls

- [check_jmap_session](../../../functions/tools/rca_outlook_connectivity_check/check_jmap_session.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [next](../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [require](../../../functions/tools/rca_outlook/http/require.md)
- [join_url](../../../functions/tools/rca_outlook/http/join_url.md)

# Called by

- [check_mapi_empty_deleted_items_fixture](../../../functions/tools/rca_outlook_connectivity_check/check_mapi_empty_deleted_items_fixture.md)