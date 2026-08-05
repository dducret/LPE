---
type: JavaScript Function
title: getActionHandlers
resource: LPE-CT/web/app.js#L791-L849
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/ui/closeDrawer
  - functions/LPE-CT/web/app/runAction
  - functions/LPE-CT/web/modules/app/trace-actions/loadTrace
  - functions/LPE-CT/web/modules/app/trace-actions/loadQuarantineTrace
  - functions/LPE-CT/web/modules/app/trace-actions/setQuarantineDialogTab
  - functions/LPE-CT/web/modules/app/trace-actions/triggerTraceAction
  - functions/LPE-CT/web/modules/app/policy-drawers/openAddressRuleDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/deleteAddressRule
  - functions/LPE-CT/web/modules/app/policy-drawers/openAttachmentRuleDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/deleteAttachmentRule
  - functions/LPE-CT/web/modules/app/policy-drawers/openDkimDomainDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/deleteDkimDomain
  - functions/LPE-CT/web/modules/app/policy-drawers/openDigestDefaultDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/deleteDigestDefault
  - functions/LPE-CT/web/modules/app/policy-drawers/openDigestOverrideDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/deleteDigestOverride
  - functions/LPE-CT/web/modules/app/trace-actions/openDigestReport
  - functions/LPE-CT/web/app/openAcceptedDomainDrawer
  - functions/LPE-CT/web/app/deleteAcceptedDomain
  - functions/LPE-CT/web/app/testAcceptedDomain
  - functions/LPE-CT/web/app/openAcceptedDomainImportDrawer
  - functions/LPE-CT/web/app/openPublicTlsUploadDrawer
  - functions/LPE-CT/web/app/selectPublicTlsProfile
  - functions/LPE-CT/web/app/disablePublicTlsProfile
  - functions/LPE-CT/web/app/deletePublicTlsProfile
  - functions/LPE-CT/web/app/openPlatformDrawer
  - functions/LPE-CT/web/app/syncNtp
  - functions/LPE-CT/web/app/runAptUpgrade
  - functions/LPE-CT/web/app/runPowerAction
  - functions/LPE-CT/web/modules/app/trace-actions/runQuarantineBulkAction
  - functions/LPE-CT/web/modules/app/format/setQuarantineSort
  - functions/LPE-CT/web/modules/app/format/setHistorySort
  - functions/LPE-CT/web/modules/app/format/setLogSort
  - functions/LPE-CT/web/modules/app/trace-actions/openHostLog
  - functions/LPE-CT/web/modules/app/trace-actions/downloadHostLog
  - functions/LPE-CT/web/modules/app/trace-actions/deleteHostLog
  - functions/LPE-CT/web/modules/app/trace-actions/openDiagnostic
  - functions/LPE-CT/web/modules/app/trace-actions/runSpamTest
  - functions/LPE-CT/web/modules/app/trace-actions/runDiagnosticTool
  - functions/LPE-CT/web/modules/app/trace-actions/connectSupport
  - functions/LPE-CT/web/modules/app/trace-actions/runHealthCheck
  - functions/LPE-CT/web/modules/app/trace-actions/flushMailQueue
  - functions/LPE-CT/web/modules/app/trace-actions/runServiceAction
  - functions/LPE-CT/web/app/setPageTab
  - functions/LPE-CT/web/app/setSystemSetupTab
  - functions/LPE-CT/web/app/loadOps
  called_by:
  - functions/LPE-CT/web/app/handleBodyClick
---

# Signature

`function getActionHandlers(actionTarget)`

# Calls

- [closeDrawer](../../../../functions/LPE-CT/web/modules/app/ui/closeDrawer.md)
- [runAction](../../../../functions/LPE-CT/web/app/runAction.md)
- [loadTrace](../../../../functions/LPE-CT/web/modules/app/trace-actions/loadTrace.md)
- [loadQuarantineTrace](../../../../functions/LPE-CT/web/modules/app/trace-actions/loadQuarantineTrace.md)
- [setQuarantineDialogTab](../../../../functions/LPE-CT/web/modules/app/trace-actions/setQuarantineDialogTab.md)
- [triggerTraceAction](../../../../functions/LPE-CT/web/modules/app/trace-actions/triggerTraceAction.md)
- [openAddressRuleDrawer](../../../../functions/LPE-CT/web/modules/app/policy-drawers/openAddressRuleDrawer.md)
- [deleteAddressRule](../../../../functions/LPE-CT/web/modules/app/policy-drawers/deleteAddressRule.md)
- [openAttachmentRuleDrawer](../../../../functions/LPE-CT/web/modules/app/policy-drawers/openAttachmentRuleDrawer.md)
- [deleteAttachmentRule](../../../../functions/LPE-CT/web/modules/app/policy-drawers/deleteAttachmentRule.md)
- [openDkimDomainDrawer](../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDkimDomainDrawer.md)
- [deleteDkimDomain](../../../../functions/LPE-CT/web/modules/app/policy-drawers/deleteDkimDomain.md)
- [openDigestDefaultDrawer](../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDigestDefaultDrawer.md)
- [deleteDigestDefault](../../../../functions/LPE-CT/web/modules/app/policy-drawers/deleteDigestDefault.md)
- [openDigestOverrideDrawer](../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDigestOverrideDrawer.md)
- [deleteDigestOverride](../../../../functions/LPE-CT/web/modules/app/policy-drawers/deleteDigestOverride.md)
- [openDigestReport](../../../../functions/LPE-CT/web/modules/app/trace-actions/openDigestReport.md)
- [openAcceptedDomainDrawer](../../../../functions/LPE-CT/web/app/openAcceptedDomainDrawer.md)
- [deleteAcceptedDomain](../../../../functions/LPE-CT/web/app/deleteAcceptedDomain.md)
- [testAcceptedDomain](../../../../functions/LPE-CT/web/app/testAcceptedDomain.md)
- [openAcceptedDomainImportDrawer](../../../../functions/LPE-CT/web/app/openAcceptedDomainImportDrawer.md)
- [openPublicTlsUploadDrawer](../../../../functions/LPE-CT/web/app/openPublicTlsUploadDrawer.md)
- [selectPublicTlsProfile](../../../../functions/LPE-CT/web/app/selectPublicTlsProfile.md)
- [disablePublicTlsProfile](../../../../functions/LPE-CT/web/app/disablePublicTlsProfile.md)
- [deletePublicTlsProfile](../../../../functions/LPE-CT/web/app/deletePublicTlsProfile.md)
- [openPlatformDrawer](../../../../functions/LPE-CT/web/app/openPlatformDrawer.md)
- [syncNtp](../../../../functions/LPE-CT/web/app/syncNtp.md)
- [runAptUpgrade](../../../../functions/LPE-CT/web/app/runAptUpgrade.md)
- [runPowerAction](../../../../functions/LPE-CT/web/app/runPowerAction.md)
- [runQuarantineBulkAction](../../../../functions/LPE-CT/web/modules/app/trace-actions/runQuarantineBulkAction.md)
- [setQuarantineSort](../../../../functions/LPE-CT/web/modules/app/format/setQuarantineSort.md)
- [setHistorySort](../../../../functions/LPE-CT/web/modules/app/format/setHistorySort.md)
- [setLogSort](../../../../functions/LPE-CT/web/modules/app/format/setLogSort.md)
- [openHostLog](../../../../functions/LPE-CT/web/modules/app/trace-actions/openHostLog.md)
- [downloadHostLog](../../../../functions/LPE-CT/web/modules/app/trace-actions/downloadHostLog.md)
- [deleteHostLog](../../../../functions/LPE-CT/web/modules/app/trace-actions/deleteHostLog.md)
- [openDiagnostic](../../../../functions/LPE-CT/web/modules/app/trace-actions/openDiagnostic.md)
- [runSpamTest](../../../../functions/LPE-CT/web/modules/app/trace-actions/runSpamTest.md)
- [runDiagnosticTool](../../../../functions/LPE-CT/web/modules/app/trace-actions/runDiagnosticTool.md)
- [connectSupport](../../../../functions/LPE-CT/web/modules/app/trace-actions/connectSupport.md)
- [runHealthCheck](../../../../functions/LPE-CT/web/modules/app/trace-actions/runHealthCheck.md)
- [flushMailQueue](../../../../functions/LPE-CT/web/modules/app/trace-actions/flushMailQueue.md)
- [runServiceAction](../../../../functions/LPE-CT/web/modules/app/trace-actions/runServiceAction.md)
- [setPageTab](../../../../functions/LPE-CT/web/app/setPageTab.md)
- [setSystemSetupTab](../../../../functions/LPE-CT/web/app/setSystemSetupTab.md)
- [loadOps](../../../../functions/LPE-CT/web/app/loadOps.md)

# Called by

- [handleBodyClick](../../../../functions/LPE-CT/web/app/handleBodyClick.md)