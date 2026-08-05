---
type: JavaScript Function
title: savePolicies
resource: LPE-CT/web/app.js#L133-L136
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/api/putJson
  - functions/LPE-CT/web/app/loadOps
  called_by:
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
  - functions/LPE-CT/web/modules/app/trace-actions/updateSelectedSenderPolicy
---

# Signature

`async function savePolicies(policies)`

# Calls

- [putJson](../../../../functions/LPE-CT/web/modules/app/api/putJson.md)
- [loadOps](../../../../functions/LPE-CT/web/app/loadOps.md)

# Called by

- [openAddressRuleDrawer](../../../../functions/LPE-CT/web/modules/app/policy-drawers/openAddressRuleDrawer.md)
- [deleteAddressRule](../../../../functions/LPE-CT/web/modules/app/policy-drawers/deleteAddressRule.md)
- [openAttachmentRuleDrawer](../../../../functions/LPE-CT/web/modules/app/policy-drawers/openAttachmentRuleDrawer.md)
- [deleteAttachmentRule](../../../../functions/LPE-CT/web/modules/app/policy-drawers/deleteAttachmentRule.md)
- [openFilteringPolicyDrawer](../../../../functions/LPE-CT/web/modules/app/policy-drawers/openFilteringPolicyDrawer.md)
- [openVirusFilteringDrawer](../../../../functions/LPE-CT/web/modules/app/policy-drawers/openVirusFilteringDrawer.md)
- [openRecipientVerificationDrawer](../../../../functions/LPE-CT/web/modules/app/policy-drawers/openRecipientVerificationDrawer.md)
- [openDkimSettingsDrawer](../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDkimSettingsDrawer.md)
- [openDkimDomainDrawer](../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDkimDomainDrawer.md)
- [deleteDkimDomain](../../../../functions/LPE-CT/web/modules/app/policy-drawers/deleteDkimDomain.md)
- [updateSelectedSenderPolicy](../../../../functions/LPE-CT/web/modules/app/trace-actions/updateSelectedSenderPolicy.md)