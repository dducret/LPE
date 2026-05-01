import { createPageModule } from "./page-module.js";

export const logsPage = createPageModule({
  id: "logs",
  labelKey: "navLogs",
  captionKey: "navLogsCaption",
  sectionIds: ["audit-section"],
  rendererKeys: ["mailLog", "audit", "messageLog", "emailAlertLog"],
});
