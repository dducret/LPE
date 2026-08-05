---
type: JavaScript Function
title: showFeedback
resource: LPE-CT/web/modules/app/ui.js#L5-L8
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/web/app/syncNtp
  - functions/LPE-CT/web/app/runAptUpgrade
  - functions/LPE-CT/web/app/runPowerAction
  - functions/LPE-CT/web/app/openAcceptedDomainDrawer
  - functions/LPE-CT/web/app/openAcceptedDomainImportDrawer
  - functions/LPE-CT/web/app/deleteAcceptedDomain
  - functions/LPE-CT/web/app/testAcceptedDomain
  - functions/LPE-CT/web/app/openPlatformDrawer
  - functions/LPE-CT/web/app/openPublicTlsUploadDrawer
  - functions/LPE-CT/web/app/selectPublicTlsProfile
  - functions/LPE-CT/web/app/disablePublicTlsProfile
  - functions/LPE-CT/web/app/deletePublicTlsProfile
  - functions/LPE-CT/web/app/load
  - functions/LPE-CT/web/app/runAction
  - functions/LPE-CT/web/modules/app/policy-drawers/openAddressRuleDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/deleteAddressRule
  - functions/LPE-CT/web/modules/app/policy-drawers/openAttachmentRuleDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/deleteAttachmentRule
  - functions/LPE-CT/web/modules/app/policy-drawers/openFilteringPolicyDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/openVirusFilteringDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/openRecipientVerificationDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/openDkimSettingsDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/openDkimDomainDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/deleteDkimDomain
  - functions/LPE-CT/web/modules/app/policy-drawers/openDigestSettingsDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/openDigestDefaultDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/deleteDigestDefault
  - functions/LPE-CT/web/modules/app/policy-drawers/openDigestOverrideDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/deleteDigestOverride
  - functions/LPE-CT/web/modules/app/trace-actions/deleteHostLog
  - functions/LPE-CT/web/modules/app/trace-actions/triggerTraceAction
  - functions/LPE-CT/web/modules/app/trace-actions/triggerSelectedTraceAction
  - functions/LPE-CT/web/modules/app/trace-actions/updateSelectedSenderPolicy
  - functions/LPE-CT/web/modules/app/trace-actions/runDiagnosticTool
  - functions/LPE-CT/web/modules/app/trace-actions/runSpamTest
  - functions/LPE-CT/web/modules/app/trace-actions/runServiceAction
---

# Signature

`function showFeedback(message, type = "success")`

# Called by

- [syncNtp](../../../../../../functions/LPE-CT/web/app/syncNtp.md)
- [runAptUpgrade](../../../../../../functions/LPE-CT/web/app/runAptUpgrade.md)
- [runPowerAction](../../../../../../functions/LPE-CT/web/app/runPowerAction.md)
- [openAcceptedDomainDrawer](../../../../../../functions/LPE-CT/web/app/openAcceptedDomainDrawer.md)
- [openAcceptedDomainImportDrawer](../../../../../../functions/LPE-CT/web/app/openAcceptedDomainImportDrawer.md)
- [deleteAcceptedDomain](../../../../../../functions/LPE-CT/web/app/deleteAcceptedDomain.md)
- [testAcceptedDomain](../../../../../../functions/LPE-CT/web/app/testAcceptedDomain.md)
- [openPlatformDrawer](../../../../../../functions/LPE-CT/web/app/openPlatformDrawer.md)
- [openPublicTlsUploadDrawer](../../../../../../functions/LPE-CT/web/app/openPublicTlsUploadDrawer.md)
- [selectPublicTlsProfile](../../../../../../functions/LPE-CT/web/app/selectPublicTlsProfile.md)
- [disablePublicTlsProfile](../../../../../../functions/LPE-CT/web/app/disablePublicTlsProfile.md)
- [deletePublicTlsProfile](../../../../../../functions/LPE-CT/web/app/deletePublicTlsProfile.md)
- [load](../../../../../../functions/LPE-CT/web/app/load.md)
- [runAction](../../../../../../functions/LPE-CT/web/app/runAction.md)
- [openAddressRuleDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openAddressRuleDrawer.md)
- [deleteAddressRule](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/deleteAddressRule.md)
- [openAttachmentRuleDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openAttachmentRuleDrawer.md)
- [deleteAttachmentRule](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/deleteAttachmentRule.md)
- [openFilteringPolicyDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openFilteringPolicyDrawer.md)
- [openVirusFilteringDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openVirusFilteringDrawer.md)
- [openRecipientVerificationDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openRecipientVerificationDrawer.md)
- [openDkimSettingsDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDkimSettingsDrawer.md)
- [openDkimDomainDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDkimDomainDrawer.md)
- [deleteDkimDomain](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/deleteDkimDomain.md)
- [openDigestSettingsDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDigestSettingsDrawer.md)
- [openDigestDefaultDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDigestDefaultDrawer.md)
- [deleteDigestDefault](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/deleteDigestDefault.md)
- [openDigestOverrideDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDigestOverrideDrawer.md)
- [deleteDigestOverride](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/deleteDigestOverride.md)
- [deleteHostLog](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/deleteHostLog.md)
- [triggerTraceAction](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/triggerTraceAction.md)
- [triggerSelectedTraceAction](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/triggerSelectedTraceAction.md)
- [updateSelectedSenderPolicy](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/updateSelectedSenderPolicy.md)
- [runDiagnosticTool](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/runDiagnosticTool.md)
- [runSpamTest](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/runSpamTest.md)
- [runServiceAction](../../../../../../functions/LPE-CT/web/modules/app/trace-actions/runServiceAction.md)