---
type: JavaScript Function
title: getAddressRules
resource: LPE-CT/web/modules/app/lists.js#L22-L41
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/web/modules/app/lists/findAddressRule
  - functions/LPE-CT/web/modules/app/lists/renderAddressRules
  - functions/LPE-CT/web/modules/app/policy-drawers/openAddressRuleDrawer
---

# Signature

`function getAddressRules(policies = state.dashboard?.policies)`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [findAddressRule](../../../../../../functions/LPE-CT/web/modules/app/lists/findAddressRule.md)
- [renderAddressRules](../../../../../../functions/LPE-CT/web/modules/app/lists/renderAddressRules.md)
- [openAddressRuleDrawer](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/openAddressRuleDrawer.md)