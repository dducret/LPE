---
type: JavaScript Function
title: openAddressRuleDrawer
resource: LPE-CT/web/modules/app/policy-drawers.js#L111-L175
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/lists/findAddressRule
  - functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/modules/app/policy-drawers/normalizeDomain
  - functions/LPE-CT/web/modules/app/policy-drawers/isValidAddressRule
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/web/modules/app/format/currentPolicies
  - functions/LPE-CT/web/modules/app/lists/getAddressRules
  - functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes
  - functions/LPE-CT/web/modules/app/lists/routeToPolicies
  - functions/LPE-CT/web/app/savePolicies
  - functions/LPE-CT/web/modules/app/ui/closeDrawer
  - functions/LPE-CT/web/modules/app/ui/showFeedback
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`function openAddressRuleDrawer(ruleId = null, opener = document.activeElement)`

# Calls

- [findAddressRule](../../../../../../functions/LPE-CT/web/modules/app/lists/findAddressRule.md)
- [renderDrawerForm](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [normalizeDomain](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/normalizeDomain.md)
- [isValidAddressRule](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/isValidAddressRule.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [currentPolicies](../../../../../../functions/LPE-CT/web/modules/app/format/currentPolicies.md)
- [getAddressRules](../../../../../../functions/LPE-CT/web/modules/app/lists/getAddressRules.md)
- [includes](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes.md)
- [routeToPolicies](../../../../../../functions/LPE-CT/web/modules/app/lists/routeToPolicies.md)
- [savePolicies](../../../../../../functions/LPE-CT/web/app/savePolicies.md)
- [closeDrawer](../../../../../../functions/LPE-CT/web/modules/app/ui/closeDrawer.md)
- [showFeedback](../../../../../../functions/LPE-CT/web/modules/app/ui/showFeedback.md)

# Called by

- [getActionHandlers](../../../../../../functions/LPE-CT/web/app/getActionHandlers.md)