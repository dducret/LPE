import { getCopy } from "../i18n/index.js?v=20260502-outbound-ehlo";
import { elements, state } from "./context.js?v=20260502-outbound-ehlo";
import { escapeHtml } from "./format.js?v=20260502-outbound-ehlo";

export function showFeedback(message, type = "success") {
  elements.feedback.textContent = message;
  elements.feedback.className = type === "error" ? "feedback error" : type === "warning" ? "feedback warning" : "feedback";
}

export function showLoginFeedback(message, type = "error") {
  elements.loginFeedback.textContent = message;
  elements.loginFeedback.className = type === "error" ? "feedback error" : "feedback";
}

export function hideFeedback(target = elements.feedback) {
  target.className = "feedback hidden";
  target.textContent = "";
}

export function setButtonBusy(button, busy, busyLabel, idleLabel) {
  if (!button) {
    return;
  }
  button.disabled = busy;
  button.dataset.idleLabel = button.dataset.idleLabel || idleLabel || button.textContent;
  button.textContent = busy ? busyLabel : button.dataset.idleLabel;
}

export function setSidebarOpen(open) {
  document.body.classList.toggle("sidebar-open", open);
  elements.sidebarBackdrop.classList.toggle("hidden", !open);
  elements.mobileSidebarToggle?.setAttribute("aria-expanded", String(open));
  if (elements.mobileSidebarToggle) {
    elements.mobileSidebarToggle.textContent = open ? getCopy().closeNavigation : getCopy().openNavigation;
  }
}

export function setSidebarCollapsed(collapsed) {
  document.body.classList.toggle("sidebar-collapsed", collapsed);
  try {
    window.localStorage.setItem("lpeCtSidebarCollapsed", collapsed ? "true" : "false");
  } catch {}
}

export function toggleSidebarState() {
  if (window.innerWidth <= 1024) {
    setSidebarOpen(!document.body.classList.contains("sidebar-open"));
    return;
  }
  setSidebarCollapsed(!document.body.classList.contains("sidebar-collapsed"));
}

export function buildEmptyState(title, description, actionHtml = "") {
  return `
    <article class="empty-state">
      <div>
        <strong>${escapeHtml(title)}</strong>
        <p>${escapeHtml(description)}</p>
      </div>
      ${actionHtml ? `<div class="empty-state-actions">${actionHtml}</div>` : ""}
    </article>
  `;
}

export function buildLoadingRows(count = 2) {
  return Array.from({ length: count })
    .map(
      () => `
        <article class="loading-row" aria-hidden="true">
          <div class="loading-line short"></div>
          <div class="loading-line long"></div>
          <div class="loading-line medium"></div>
        </article>
      `,
    )
    .join("");
}

export function setListLoading(container, count = 2) {
  container.innerHTML = buildLoadingRows(count);
}

export function clearInvalidFields(form) {
  form.querySelectorAll(".invalid").forEach((field) => field.classList.remove("invalid"));
}

export function markInvalid(form, names) {
  names.forEach((name) => {
    const field = form.elements.namedItem(name);
    if (field) {
      field.classList.add("invalid");
      field.setAttribute("aria-invalid", "true");
    }
  });
}

export function renderDrawerContent(title, summary, content, opener = document.activeElement, onClose = null, variant = "") {
  state.drawer.previousFocus = opener instanceof HTMLElement ? opener : null;
  state.drawer.onClose = onClose;
  state.drawer.open = true;
  elements.drawer.classList.toggle("drawer-wide", variant === "wide");
  elements.drawerTitle.textContent = title;
  elements.drawerSummary.textContent = summary;
  elements.drawerContent.innerHTML = content;
  elements.drawerBackdrop.classList.remove("hidden");
  document.body.classList.add("drawer-open");
  requestAnimationFrame(() => {
    const focusable = elements.drawer.querySelector("input, select, textarea, button, [href], [tabindex]:not([tabindex='-1'])");
    (focusable || elements.drawer).focus();
  });
}

export function closeDrawer() {
  if (!state.drawer.open) {
    return;
  }
  state.drawer.open = false;
  elements.drawer.classList.remove("drawer-wide");
  elements.drawerBackdrop.classList.add("hidden");
  document.body.classList.remove("drawer-open");
  if (typeof state.drawer.onClose === "function") {
    state.drawer.onClose();
  }
  if (state.drawer.previousFocus instanceof HTMLElement) {
    state.drawer.previousFocus.focus();
  }
  state.drawer.onClose = null;
}

export function renderMetric(element, value) {
  if (element) {
    element.textContent = formatMetric(value);
  }
}

export function setText(element, value) {
  if (element) {
    element.textContent = value;
  }
}

export function setClassName(element, value) {
  if (element) {
    element.className = value;
  }
}

export function setAuthenticated(authenticated) {
  elements.consoleShell.classList.toggle("hidden", !authenticated);
  elements.loginShell.classList.toggle("hidden", authenticated);
}

