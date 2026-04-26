import { antiSpamPage } from "./anti-spam.js";
import { dashboardPage } from "./dashboard.js";
import { filteringPage } from "./filtering.js";
import { logsPage } from "./logs.js";
import { quarantinePage } from "./quarantine.js";
import { reportingPage } from "./reporting.js";
import { systemSetupPage } from "./system-setup.js";

export const PAGE_MODULES = Object.freeze([
  dashboardPage,
  systemSetupPage,
  filteringPage,
  antiSpamPage,
  quarantinePage,
  reportingPage,
  logsPage,
]);

export const DEFAULT_PAGE_ID = dashboardPage.id;

const PAGE_BY_ID = new Map(PAGE_MODULES.map((page) => [page.id, page]));

export function resolvePageId(pageId) {
  return PAGE_BY_ID.has(pageId) ? pageId : DEFAULT_PAGE_ID;
}

export function pageIdFromHash(hash) {
  return resolvePageId(String(hash ?? "").replace(/^#/, ""));
}

export function activatePageView(pageId, { pageViews, navButtons }) {
  const targetPage = resolvePageId(pageId);
  const page = PAGE_BY_ID.get(targetPage);
  pageViews.forEach((view) => {
    const isActive = page.ownsView(view);
    view.classList.toggle("hidden", !isActive);
    view.classList.toggle("page-view-active", isActive);
    view.setAttribute("aria-hidden", String(!isActive));
  });
  navButtons.forEach((button) => {
    const isActive = button.dataset.pageTarget === targetPage;
    button.setAttribute("aria-current", isActive ? "true" : "false");
  });
  return targetPage;
}

export function renderPageModules(renderers, pages = PAGE_MODULES) {
  pages.forEach((page) => {
    page.render(renderers);
  });
}
