---
type: Python Function
title: check_mobilesync_pox
resource: tools/activesync_mobile_lab_preflight.py#L129-L154
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/activesync_mobile_lab_preflight/autodiscover_url
  - functions/tools/activesync_mobile_lab_preflight/check
  called_by:
  - functions/tools/activesync_mobile_lab_preflight/main
---

# Signature

`def check_mobilesync_pox(args, failures: list[str]) -> None:`

# Calls

- [autodiscover_url](../../../functions/tools/activesync_mobile_lab_preflight/autodiscover_url.md)
- [check](../../../functions/tools/activesync_mobile_lab_preflight/check.md)

# Called by

- [main](../../../functions/tools/activesync_mobile_lab_preflight/main.md)