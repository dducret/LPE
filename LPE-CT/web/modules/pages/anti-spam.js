import { createPageModule } from "./page-module.js";

export const antiSpamPage = createPageModule({
  id: "anti-spam",
  labelKey: "navAntiSpam",
  captionKey: "navAntiSpamCaption",
  sectionIds: ["anti-spam-section"],
  rendererKeys: ["overview"],
});
