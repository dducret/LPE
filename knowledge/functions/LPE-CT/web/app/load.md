---
type: JavaScript Function
title: load
resource: LPE-CT/web/app.js#L656-L691
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/ui/setButtonBusy
  - functions/LPE-CT/web/app/syncLoadingState
  - functions/LPE-CT/web/modules/app/api/fetchDashboard
  - functions/LPE-CT/web/app/loadOps
  - functions/LPE-CT/web/modules/app/ui/setAuthenticated
  - functions/LPE-CT/web/modules/app/ui/hideFeedback
  - functions/LPE-CT/web/modules/app/ui/showLoginFeedback
  - functions/LPE-CT/web/modules/app/ui/showFeedback
  called_by:
  - functions/LPE-CT/src/smtp/run_smtp_listener
  - functions/LPE-CT/web/app/refreshDashboardOnSchedule
  - functions/LPE-CT/web/app/loginAdmin
  - functions/crates/lpe-exchange/src/mapi/mapi_folder_purge_metrics
  - functions/crates/lpe-exchange/src/mapi/mapi_outlook_view_metrics
  - functions/crates/lpe-exchange/src/mapi/event_metrics/mapi_calendar_event_save_metrics
  - functions/crates/lpe-exchange/src/mapi/notification_metrics/mapi_notification_metrics
  - functions/web/admin/src/App
  - functions/web/admin/src/runPstJobs
  - functions/web/admin/src/restoreSnapshot
---

# Signature

`async function load({ silent = false } = {})`

# Calls

- [setButtonBusy](../../../../functions/LPE-CT/web/modules/app/ui/setButtonBusy.md)
- [syncLoadingState](../../../../functions/LPE-CT/web/app/syncLoadingState.md)
- [fetchDashboard](../../../../functions/LPE-CT/web/modules/app/api/fetchDashboard.md)
- [loadOps](../../../../functions/LPE-CT/web/app/loadOps.md)
- [setAuthenticated](../../../../functions/LPE-CT/web/modules/app/ui/setAuthenticated.md)
- [hideFeedback](../../../../functions/LPE-CT/web/modules/app/ui/hideFeedback.md)
- [showLoginFeedback](../../../../functions/LPE-CT/web/modules/app/ui/showLoginFeedback.md)
- [showFeedback](../../../../functions/LPE-CT/web/modules/app/ui/showFeedback.md)

# Called by

- [run_smtp_listener](../../../../functions/LPE-CT/src/smtp/run_smtp_listener.md)
- [refreshDashboardOnSchedule](../../../../functions/LPE-CT/web/app/refreshDashboardOnSchedule.md)
- [loginAdmin](../../../../functions/LPE-CT/web/app/loginAdmin.md)
- [mapi_folder_purge_metrics](../../../../functions/crates/lpe-exchange/src/mapi/mapi_folder_purge_metrics.md)
- [mapi_outlook_view_metrics](../../../../functions/crates/lpe-exchange/src/mapi/mapi_outlook_view_metrics.md)
- [mapi_calendar_event_save_metrics](../../../../functions/crates/lpe-exchange/src/mapi/event_metrics/mapi_calendar_event_save_metrics.md)
- [mapi_notification_metrics](../../../../functions/crates/lpe-exchange/src/mapi/notification_metrics/mapi_notification_metrics.md)
- [App](../../../../functions/web/admin/src/App.md)
- [runPstJobs](../../../../functions/web/admin/src/runPstJobs.md)
- [restoreSnapshot](../../../../functions/web/admin/src/restoreSnapshot.md)