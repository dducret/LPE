---
type: Python Function
title: check_jmap_session
resource: tools/rca_outlook_connectivity_check.py#L350-L377
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/join_url
  - functions/tools/rca_outlook/http/require
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook/http/content_type
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_jmap_email_subject_absent
  - functions/tools/rca_outlook_connectivity_check/main
---

# Signature

`def check_jmap_session(base_url: str, email: str, password: str, insecure_tls: bool, timeout: int) -> dict:`

# Calls

- [join_url](../../../functions/tools/rca_outlook/http/join_url.md)
- [require](../../../functions/tools/rca_outlook/http/require.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [content_type](../../../functions/tools/rca_outlook/http/content_type.md)

# Called by

- [check_jmap_email_subject_absent](../../../functions/tools/rca_outlook_connectivity_check/check_jmap_email_subject_absent.md)
- [main](../../../functions/tools/rca_outlook_connectivity_check/main.md)