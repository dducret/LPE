---
type: Python Function
title: check
resource: tools/activesync_mobile_lab_preflight.py#L60-L65
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/activesync_mobile_lab_preflight/check_options
  - functions/tools/activesync_mobile_lab_preflight/check_autodiscover_json
  - functions/tools/activesync_mobile_lab_preflight/check_desktop_pox
  - functions/tools/activesync_mobile_lab_preflight/check_mobilesync_pox
  - functions/tools/ews_live_smoke_check/main
---

# Signature

`def check(condition: bool, label: str, failures: list[str]) -> None:`

# Called by

- [check_options](../../../functions/tools/activesync_mobile_lab_preflight/check_options.md)
- [check_autodiscover_json](../../../functions/tools/activesync_mobile_lab_preflight/check_autodiscover_json.md)
- [check_desktop_pox](../../../functions/tools/activesync_mobile_lab_preflight/check_desktop_pox.md)
- [check_mobilesync_pox](../../../functions/tools/activesync_mobile_lab_preflight/check_mobilesync_pox.md)
- [main](../../../functions/tools/ews_live_smoke_check/main.md)