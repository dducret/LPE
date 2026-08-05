---
type: JavaScript Function
title: loadOps
resource: LPE-CT/web/app.js#L611-L651
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/ui/setButtonBusy
  - functions/LPE-CT/web/modules/app/ui/setListLoading
  - functions/LPE-CT/web/modules/app/api/fetchOptionalJson
  - functions/LPE-CT/web/modules/app/lists/pruneQuarantineSelection
  - functions/LPE-CT/web/app/renderDashboard
  called_by:
  - functions/LPE-CT/web/app/savePolicies
  - functions/LPE-CT/web/app/syncNtp
  - functions/LPE-CT/web/app/runAptUpgrade
  - functions/LPE-CT/web/app/openPlatformDrawer
  - functions/LPE-CT/web/app/openPublicTlsUploadDrawer
  - functions/LPE-CT/web/app/selectPublicTlsProfile
  - functions/LPE-CT/web/app/disablePublicTlsProfile
  - functions/LPE-CT/web/app/deletePublicTlsProfile
  - functions/LPE-CT/web/app/load
  - functions/LPE-CT/web/app/getActionHandlers
  - functions/LPE-CT/web/modules/app/trace-actions/triggerTraceAction
  - functions/LPE-CT/web/modules/app/trace-actions/triggerSelectedTraceAction
  - functions/LPE-CT/web/modules/app/trace-actions/flushMailQueue
---

# Signature

`async function loadOps({ silent = false } = {})`

# Calls

- [setButtonBusy](../../../../functions/LPE-CT/web/modules/app/ui/setButtonBusy.md)
- [setListLoading](../../../../functions/LPE-CT/web/modules/app/ui/setListLoading.md)
- [fetchOptionalJson](../../../../functions/LPE-CT/web/modules/app/api/fetchOptionalJson.md)
- [pruneQuarantineSelection](../../../../functions/LPE-CT/web/modules/app/lists/pruneQuarantineSelection.md)
- [renderDashboard](../../../../functions/LPE-CT/web/app/renderDashboard.md)

# Called by

- [savePolicies](../../../../functions/LPE-CT/web/app/savePolicies.md)
- [syncNtp](../../../../functions/LPE-CT/web/app/syncNtp.md)
- [runAptUpgrade](../../../../functions/LPE-CT/web/app/runAptUpgrade.md)
- [openPlatformDrawer](../../../../functions/LPE-CT/web/app/openPlatformDrawer.md)
- [openPublicTlsUploadDrawer](../../../../functions/LPE-CT/web/app/openPublicTlsUploadDrawer.md)
- [selectPublicTlsProfile](../../../../functions/LPE-CT/web/app/selectPublicTlsProfile.md)
- [disablePublicTlsProfile](../../../../functions/LPE-CT/web/app/disablePublicTlsProfile.md)
- [deletePublicTlsProfile](../../../../functions/LPE-CT/web/app/deletePublicTlsProfile.md)
- [load](../../../../functions/LPE-CT/web/app/load.md)
- [getActionHandlers](../../../../functions/LPE-CT/web/app/getActionHandlers.md)
- [triggerTraceAction](../../../../functions/LPE-CT/web/modules/app/trace-actions/triggerTraceAction.md)
- [triggerSelectedTraceAction](../../../../functions/LPE-CT/web/modules/app/trace-actions/triggerSelectedTraceAction.md)
- [flushMailQueue](../../../../functions/LPE-CT/web/modules/app/trace-actions/flushMailQueue.md)