---
type: JavaScript Function
title: renderDrawerForm
resource: LPE-CT/web/modules/app/policy-drawers.js#L64-L109
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/ui/renderDrawerContent
  - functions/LPE-CT/web/app/smoke/test/MockElement/querySelector
  - functions/LPE-CT/web/app/smoke/test/MockElement/addEventListener
  - functions/LPE-CT/web/modules/app/ui/clearInvalidFields
  - functions/LPE-CT/web/modules/app/ui/setButtonBusy
  - functions/LPE-CT/web/modules/app/policy-drawers/buildFormError
  - functions/LPE-CT/web/modules/app/ui/markInvalid
  called_by:
  - functions/LPE-CT/web/app/openAcceptedDomainDrawer
  - functions/LPE-CT/web/app/openAcceptedDomainImportDrawer
  - functions/LPE-CT/web/app/openPlatformDrawer
  - functions/LPE-CT/web/app/openPublicTlsUploadDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/openAddressRuleDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/openAttachmentRuleDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/openFilteringPolicyDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/openVirusFilteringDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/openRecipientVerificationDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/openDkimSettingsDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/openDkimDomainDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/openDigestSettingsDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/openDigestDefaultDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/openDigestOverrideDrawer
---

# Signature

`function renderDrawerForm({ title, summary, formId, content, onSubmit, opener })`

# Calls

- [renderDrawerContent](../../../../../../functions/LPE-CT/web/modules/app/ui/renderDrawerContent.md)
- [querySelector](../../../../../../functions/LPE-CT/web/app/smoke/test/MockElement/querySelector.md)
- [addEventListener](../../../../../../functions/LPE-CT/web/app/smoke/test/MockElement/addEventListener.md)
- [clearInvalidFields](../../../../../../functions/LPE-CT/web/modules/app/ui/clearInvalidFields.md)
- [setButtonBusy](../../../../../../functions/LPE-CT/web/modules/app/ui/setButtonBusy.md)
- [buildFormError](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/buildFormError.md)
- [markInvalid](../../../../../../functions/LPE-CT/web/modules/app/ui/markInvalid.md)

# Called by

- [openAcceptedDomainDrawer](../../../../../../functions/LPE-CT/web/app/openAcceptedDomainDrawer.md)
- [openAcceptedDomainImportDrawer](../../../../../../functions/LPE-CT/web/app/openAcceptedDomainImportDrawer.md)
- [openPlatformDrawer](../../../../../../functions/LPE-CT/web/app/openPlatformDrawer.md)
- [openPublicTlsUploadDrawer](../../../../../../functions/LPE-CT/web/app/openPublicTlsUploadDrawer.md)
- [openAddressRuleDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openAddressRuleDrawer.md)
- [openAttachmentRuleDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openAttachmentRuleDrawer.md)
- [openFilteringPolicyDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openFilteringPolicyDrawer.md)
- [openVirusFilteringDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openVirusFilteringDrawer.md)
- [openRecipientVerificationDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openRecipientVerificationDrawer.md)
- [openDkimSettingsDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDkimSettingsDrawer.md)
- [openDkimDomainDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDkimDomainDrawer.md)
- [openDigestSettingsDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDigestSettingsDrawer.md)
- [openDigestDefaultDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDigestDefaultDrawer.md)
- [openDigestOverrideDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDigestOverrideDrawer.md)