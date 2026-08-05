---
type: Python Method
title: call
resource: tools/ews_live_smoke_check.py#L35-L63
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/web/app/setPageTab
  - functions/LPE-CT/web/i18n/getMessage
  - functions/tools/check_repository/main
  - functions/tools/ews_live_smoke_check/check_get_server_time_zones
  - functions/tools/ews_live_smoke_check/check_find_folder
  - functions/tools/ews_live_smoke_check/check_get_user_oof_settings
  - functions/tools/ews_live_smoke_check/check_resolve_names
  - functions/tools/ews_live_smoke_check/check_get_user_availability
  - functions/tools/ews_live_smoke_check/check_task_mutation
  - functions/tools/operations_benchmark/timed
  - functions/tools/operations_benchmark/run_section
---

# Signature

`def call(self, operation: str, body: str) -> str:`

# Called by

- [setPageTab](../../../../functions/LPE-CT/web/app/setPageTab.md)
- [getMessage](../../../../functions/LPE-CT/web/i18n/getMessage.md)
- [main](../../../../functions/tools/check_repository/main.md)
- [check_get_server_time_zones](../../../../functions/tools/ews_live_smoke_check/check_get_server_time_zones.md)
- [check_find_folder](../../../../functions/tools/ews_live_smoke_check/check_find_folder.md)
- [check_get_user_oof_settings](../../../../functions/tools/ews_live_smoke_check/check_get_user_oof_settings.md)
- [check_resolve_names](../../../../functions/tools/ews_live_smoke_check/check_resolve_names.md)
- [check_get_user_availability](../../../../functions/tools/ews_live_smoke_check/check_get_user_availability.md)
- [check_task_mutation](../../../../functions/tools/ews_live_smoke_check/check_task_mutation.md)
- [timed](../../../../functions/tools/operations_benchmark/timed.md)
- [run_section](../../../../functions/tools/operations_benchmark/run_section.md)