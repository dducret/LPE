import { createPageModule } from "./page-module.js";

export const reportingPage = createPageModule({
  id: "reporting",
  labelKey: "navReporting",
  captionKey: "navReportingCaption",
  sectionIds: ["history-section", "digest-section", "reporting-system-information"],
  rendererKeys: ["systemInformation", "history", "digestReporting"],
});
