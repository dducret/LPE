---
type: Python Function
title: check_options
resource: tools/activesync_mobile_lab_preflight.py#L68-L93
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/activesync_mobile_lab_preflight/basic_auth
  - functions/tools/activesync_mobile_lab_preflight/check
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/tools/activesync_mobile_lab_preflight/main
---

# Signature

`def check_options(args, failures: list[str]) -> None:`

# Calls

- [basic_auth](../../../functions/tools/activesync_mobile_lab_preflight/basic_auth.md)
- [check](../../../functions/tools/activesync_mobile_lab_preflight/check.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [main](../../../functions/tools/activesync_mobile_lab_preflight/main.md)