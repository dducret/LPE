---
type: Python Function
title: check_autodiscover_json
resource: tools/activesync_mobile_lab_preflight.py#L96-L117
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/activesync_mobile_lab_preflight/autodiscover_url
  - functions/tools/activesync_mobile_lab_preflight/check
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/tools/activesync_mobile_lab_preflight/main
---

# Signature

`def check_autodiscover_json(args, protocol: str, failures: list[str]) -> None:`

# Calls

- [autodiscover_url](../../../functions/tools/activesync_mobile_lab_preflight/autodiscover_url.md)
- [check](../../../functions/tools/activesync_mobile_lab_preflight/check.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [main](../../../functions/tools/activesync_mobile_lab_preflight/main.md)