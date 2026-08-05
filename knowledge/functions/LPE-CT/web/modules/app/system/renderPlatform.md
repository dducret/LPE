---
type: JavaScript Function
title: renderPlatform
resource: LPE-CT/web/modules/app/system.js#L668-L729
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/ui/buildLoadingRows
  - functions/LPE-CT/web/modules/app/system/renderNetworkSetup
  - functions/LPE-CT/web/modules/app/system/renderSystemSetupPanel
  - functions/LPE-CT/web/modules/app/system/renderSystemSetupSummary
  - functions/LPE-CT/web/modules/app/format/formatDateTime
  - functions/LPE-CT/web/modules/app/system/getHostClockDate
  - functions/LPE-CT/web/modules/app/format/formatUptime
  - functions/LPE-CT/web/modules/app/format/formatList
  - functions/LPE-CT/web/modules/app/system/renderMailRelaySetup
  - functions/LPE-CT/web/modules/app/system/renderMailAuthenticationSetup
  - functions/LPE-CT/web/modules/app/system/renderSystemSetupTabs
  called_by:
  - functions/LPE-CT/web/app/syncNtp
  - functions/LPE-CT/web/app/runAptUpgrade
  - functions/LPE-CT/web/app/openAcceptedDomainDrawer
  - functions/LPE-CT/web/app/openAcceptedDomainImportDrawer
  - functions/LPE-CT/web/app/deleteAcceptedDomain
  - functions/LPE-CT/web/app/testAcceptedDomain
  - functions/LPE-CT/web/app/setSystemSetupTab
---

# Signature

`function renderPlatform()`

# Calls

- [buildLoadingRows](../../../../../../functions/LPE-CT/web/modules/app/ui/buildLoadingRows.md)
- [renderNetworkSetup](../../../../../../functions/LPE-CT/web/modules/app/system/renderNetworkSetup.md)
- [renderSystemSetupPanel](../../../../../../functions/LPE-CT/web/modules/app/system/renderSystemSetupPanel.md)
- [renderSystemSetupSummary](../../../../../../functions/LPE-CT/web/modules/app/system/renderSystemSetupSummary.md)
- [formatDateTime](../../../../../../functions/LPE-CT/web/modules/app/format/formatDateTime.md)
- [getHostClockDate](../../../../../../functions/LPE-CT/web/modules/app/system/getHostClockDate.md)
- [formatUptime](../../../../../../functions/LPE-CT/web/modules/app/format/formatUptime.md)
- [formatList](../../../../../../functions/LPE-CT/web/modules/app/format/formatList.md)
- [renderMailRelaySetup](../../../../../../functions/LPE-CT/web/modules/app/system/renderMailRelaySetup.md)
- [renderMailAuthenticationSetup](../../../../../../functions/LPE-CT/web/modules/app/system/renderMailAuthenticationSetup.md)
- [renderSystemSetupTabs](../../../../../../functions/LPE-CT/web/modules/app/system/renderSystemSetupTabs.md)

# Called by

- [syncNtp](../../../../../../functions/LPE-CT/web/app/syncNtp.md)
- [runAptUpgrade](../../../../../../functions/LPE-CT/web/app/runAptUpgrade.md)
- [openAcceptedDomainDrawer](../../../../../../functions/LPE-CT/web/app/openAcceptedDomainDrawer.md)
- [openAcceptedDomainImportDrawer](../../../../../../functions/LPE-CT/web/app/openAcceptedDomainImportDrawer.md)
- [deleteAcceptedDomain](../../../../../../functions/LPE-CT/web/app/deleteAcceptedDomain.md)
- [testAcceptedDomain](../../../../../../functions/LPE-CT/web/app/testAcceptedDomain.md)
- [setSystemSetupTab](../../../../../../functions/LPE-CT/web/app/setSystemSetupTab.md)