---
type: JavaScript Module
title: app
resource: LPE-CT/web/app.js#L1-L1120
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/modules-i18n-index-js-v-20260502-outbound-ehlo
  - external/modules-pages-index-js-v-20260501-health-check-output
  - external/modules-app-context-js-v-20260502-outbound-ehlo
  - external/modules-app-format-js-v-20260502-outbound-ehlo
  - external/modules-app-ui-js-v-20260502-outbound-ehlo
  - external/modules-app-api-js-v-20260502-outbound-ehlo
  - external/modules-app-system-js-v-20260502-outbound-ehlo
  - external/modules-app-lists-js-v-20260502-outbound-ehlo
  - external/modules-app-dashboard-js-v-20260502-outbound-ehlo
  - external/modules-app-trace-actions-js-v-20260502-outbound-ehlo
  - external/modules-app-policy-drawers-js-v-20260502-outbound-ehlo
  member_of:
  - packages/LPE-CT
---

# Contains

- [syncLoadingState](../../../functions/LPE-CT/web/app/syncLoadingState.md)
- [pageFromHash](../../../functions/LPE-CT/web/app/pageFromHash.md)
- [syncPageTabs](../../../functions/LPE-CT/web/app/syncPageTabs.md)
- [setActivePage](../../../functions/LPE-CT/web/app/setActivePage.md)
- [updateNavState](../../../functions/LPE-CT/web/app/updateNavState.md)
- [registerSectionObserver](../../../functions/LPE-CT/web/app/registerSectionObserver.md)
- [renderDashboard](../../../functions/LPE-CT/web/app/renderDashboard.md)
- [savePolicies](../../../functions/LPE-CT/web/app/savePolicies.md)
- [saveReporting](../../../functions/LPE-CT/web/app/saveReporting.md)
- [syncNtp](../../../functions/LPE-CT/web/app/syncNtp.md)
- [runAptUpgrade](../../../functions/LPE-CT/web/app/runAptUpgrade.md)
- [runPowerAction](../../../functions/LPE-CT/web/app/runPowerAction.md)
- [currentAcceptedDomains](../../../functions/LPE-CT/web/app/currentAcceptedDomains.md)
- [findAcceptedDomain](../../../functions/LPE-CT/web/app/findAcceptedDomain.md)
- [acceptedDomainPayloadFromForm](../../../functions/LPE-CT/web/app/acceptedDomainPayloadFromForm.md)
- [validateAcceptedDomainPayload](../../../functions/LPE-CT/web/app/validateAcceptedDomainPayload.md)
- [openAcceptedDomainDrawer](../../../functions/LPE-CT/web/app/openAcceptedDomainDrawer.md)
- [openAcceptedDomainImportDrawer](../../../functions/LPE-CT/web/app/openAcceptedDomainImportDrawer.md)
- [deleteAcceptedDomain](../../../functions/LPE-CT/web/app/deleteAcceptedDomain.md)
- [testAcceptedDomain](../../../functions/LPE-CT/web/app/testAcceptedDomain.md)
- [getPlatformDrawerConfigs](../../../functions/LPE-CT/web/app/getPlatformDrawerConfigs.md)
- [openPlatformDrawer](../../../functions/LPE-CT/web/app/openPlatformDrawer.md)
- [readSelectedTextFile](../../../functions/LPE-CT/web/app/readSelectedTextFile.md)
- [openPublicTlsUploadDrawer](../../../functions/LPE-CT/web/app/openPublicTlsUploadDrawer.md)
- [selectPublicTlsProfile](../../../functions/LPE-CT/web/app/selectPublicTlsProfile.md)
- [disablePublicTlsProfile](../../../functions/LPE-CT/web/app/disablePublicTlsProfile.md)
- [deletePublicTlsProfile](../../../functions/LPE-CT/web/app/deletePublicTlsProfile.md)
- [loadOps](../../../functions/LPE-CT/web/app/loadOps.md)
- [load](../../../functions/LPE-CT/web/app/load.md)
- [refreshDashboardOnSchedule](../../../functions/LPE-CT/web/app/refreshDashboardOnSchedule.md)
- [loginAdmin](../../../functions/LPE-CT/web/app/loginAdmin.md)
- [hydrateLoginForm](../../../functions/LPE-CT/web/app/hydrateLoginForm.md)
- [runAction](../../../functions/LPE-CT/web/app/runAction.md)
- [setPageTab](../../../functions/LPE-CT/web/app/setPageTab.md)
- [setSystemSetupTab](../../../functions/LPE-CT/web/app/setSystemSetupTab.md)
- [getActionHandlers](../../../functions/LPE-CT/web/app/getActionHandlers.md)
- [handleBodyClick](../../../functions/LPE-CT/web/app/handleBodyClick.md)
- [handleBodyChange](../../../functions/LPE-CT/web/app/handleBodyChange.md)
- [startHistoryColumnResize](../../../functions/LPE-CT/web/app/startHistoryColumnResize.md)
- [startLogColumnResize](../../../functions/LPE-CT/web/app/startLogColumnResize.md)
- [trapDrawerFocus](../../../functions/LPE-CT/web/app/trapDrawerFocus.md)
- [setLocale](../../../functions/LPE-CT/web/app/setLocale.md)
- [hydrateLocaleSpecificState](../../../functions/LPE-CT/web/app/hydrateLocaleSpecificState.md)

# Imports

- `./modules/i18n/index.js?v=20260502-outbound-ehlo`
- `./modules/pages/index.js?v=20260501-health-check-output`
- `./modules/app/context.js?v=20260502-outbound-ehlo`
- `./modules/app/format.js?v=20260502-outbound-ehlo`
- `./modules/app/ui.js?v=20260502-outbound-ehlo`
- `./modules/app/api.js?v=20260502-outbound-ehlo`
- `./modules/app/system.js?v=20260502-outbound-ehlo`
- `./modules/app/lists.js?v=20260502-outbound-ehlo`
- `./modules/app/dashboard.js?v=20260502-outbound-ehlo`
- `./modules/app/trace-actions.js?v=20260502-outbound-ehlo`
- `./modules/app/policy-drawers.js?v=20260502-outbound-ehlo`

# Member of

- [lpe-ct](../../../packages/LPE-CT.md)