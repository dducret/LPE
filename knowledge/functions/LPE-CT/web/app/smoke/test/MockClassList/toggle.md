---
type: JavaScript Method
title: toggle
resource: LPE-CT/web/app.smoke.test.cjs#L20-L35
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/host_logs/delete
  - functions/LPE-CT/web/app/smoke/test/MockClassList/add
  called_by:
  - functions/LPE-CT/web/app/syncPageTabs
  - functions/LPE-CT/web/modules/app/ui/setSidebarOpen
  - functions/LPE-CT/web/modules/app/ui/setSidebarCollapsed
  - functions/LPE-CT/web/modules/app/ui/renderDrawerContent
  - functions/LPE-CT/web/modules/app/ui/setAuthenticated
  - functions/LPE-CT/web/modules/pages/activatePageView
---

# Signature

`toggle(name, force)`

# Calls

- [delete](../../../../../../../functions/LPE-CT/src/host_logs/delete.md)
- [add](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/add.md)

# Called by

- [syncPageTabs](../../../../../../../functions/LPE-CT/web/app/syncPageTabs.md)
- [setSidebarOpen](../../../../../../../functions/LPE-CT/web/modules/app/ui/setSidebarOpen.md)
- [setSidebarCollapsed](../../../../../../../functions/LPE-CT/web/modules/app/ui/setSidebarCollapsed.md)
- [renderDrawerContent](../../../../../../../functions/LPE-CT/web/modules/app/ui/renderDrawerContent.md)
- [setAuthenticated](../../../../../../../functions/LPE-CT/web/modules/app/ui/setAuthenticated.md)
- [activatePageView](../../../../../../../functions/LPE-CT/web/modules/pages/activatePageView.md)