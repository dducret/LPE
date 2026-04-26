import { createPageModule } from "./page-module.js";

export const dashboardPage = createPageModule({
  id: "dashboard",
  labelKey: "navDashboard",
  captionKey: "navDashboardCaption",
  sectionIds: ["overview-section"],
  rendererKeys: ["overview"],
});
