---
type: Python Function
title: check_desktop_pox
resource: tools/activesync_mobile_lab_preflight.py#L120-L126
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

`def check_desktop_pox(args, failures: list[str]) -> None:`

# Calls

- [autodiscover_url](../../../functions/tools/activesync_mobile_lab_preflight/autodiscover_url.md)
- [check](../../../functions/tools/activesync_mobile_lab_preflight/check.md)

# Called by

- [main](../../../functions/tools/activesync_mobile_lab_preflight/main.md)