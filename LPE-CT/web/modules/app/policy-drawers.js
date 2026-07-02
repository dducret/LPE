import { getCopy } from "../i18n/index.js?v=20260502-outbound-ehlo";
import { state } from "./context.js?v=20260502-outbound-ehlo";
import { clearInvalidFields, markInvalid, renderDrawerContent, setButtonBusy, showFeedback } from "./ui.js?v=20260502-outbound-ehlo";
import { currentPolicies, currentReporting, dedupeList, escapeHtml, formatNumber, isValidHostname, labelForAction, labelForAddressRole, labelForAntivirusProvider, labelForAttachmentScope, parseProviderChain } from "./format.js?v=20260502-outbound-ehlo";
import { findAddressRule, findAttachmentRule, getAddressRules, getAttachmentRules } from "./lists.js?v=20260502-outbound-ehlo";

const callbacks = {
  savePolicies: async () => {},
  saveReporting: async () => {},
};

export function configurePolicyDrawers(options) {
  callbacks.savePolicies = options.savePolicies;
  callbacks.saveReporting = options.saveReporting;
}

export function normalizeDomain(value) {
  return String(value ?? "").trim().toLowerCase();
}

export function normalizeEmail(value) {
  return String(value ?? "").trim().toLowerCase();
}

export function parseLines(value) {
  return String(value ?? "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

export function isValidEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

export function isValidDomain(value) {
  return /^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$/i.test(value);
}

export function isValidAddressRule(value) {
  return isValidEmail(value) || isValidDomain(value);
}

export function isValidMimeType(value) {
  return /^[a-z0-9!#$&^_.+-]+\/[a-z0-9!#$&^_.+-]+$/i.test(value);
}

export function isValidSelector(value) {
  return /^[a-z0-9][a-z0-9._-]{0,62}$/i.test(value);
}

export function buildFormError(errors) {
  if (!errors.length) {
    return "";
  }
  return `
    <div class="form-error-list" role="alert">
      <p>${escapeHtml(getCopy().drawerValidationHeading)}</p>
      <ul>${errors.map((error) => `<li>${escapeHtml(error.message)}</li>`).join("")}</ul>
    </div>
  `;
}

export function renderDrawerForm({ title, summary, formId, content, onSubmit, opener }) {
  const copy = getCopy();
  renderDrawerContent(
    title,
    summary,
    `
      <form id="${formId}" class="drawer-form" novalidate>
        <div id="${formId}-errors"></div>
        ${content}
      </form>
    `,
    opener,
  );

  const form = document.getElementById(formId);
  const submitButton = form.querySelector('[type="submit"]');
  const errorContainer = document.getElementById(`${formId}-errors`);

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    clearInvalidFields(form);
    errorContainer.innerHTML = "";

    try {
      setButtonBusy(submitButton, true, copy.saving, submitButton.textContent);
      await onSubmit(form, {
        fail(errors) {
          errorContainer.innerHTML = buildFormError(errors);
          markInvalid(
            form,
            errors
              .map((error) => error.field)
              .filter(Boolean),
          );
          throw new Error("__validation__");
        },
      });
    } catch (error) {
      if (!(error instanceof Error && error.message === "__validation__")) {
        errorContainer.innerHTML = buildFormError([{ message: error instanceof Error ? error.message : copy.unknownError }]);
      }
    } finally {
      setButtonBusy(submitButton, false, copy.saving, submitButton.textContent);
    }
  });
}

export function openAddressRuleDrawer(ruleId = null, opener = document.activeElement) {
  const copy = getCopy();
  const rule = ruleId ? findAddressRule(ruleId) : { role: "sender", action: "allow", value: "" };
  renderDrawerForm({
    title: ruleId ? copy.edit : copy.createRule,
    summary: copy.addressRulesSummary,
    formId: "address-rule-form",
    opener,
    content: `
      <div class="field-grid">
        <label>
          <span>${copy.addressRuleRoleLabel}</span>
          <select name="role">
            <option value="sender"${rule.role === "sender" ? " selected" : ""}>${copy.policyRoleSender}</option>
            <option value="recipient"${rule.role === "recipient" ? " selected" : ""}>${copy.policyRoleRecipient}</option>
          </select>
        </label>
        <label>
          <span>${copy.addressRuleActionLabel}</span>
          <select name="action">
            <option value="allow"${rule.action === "allow" ? " selected" : ""}>${copy.policyActionAllow}</option>
            <option value="block"${rule.action === "block" ? " selected" : ""}>${copy.policyActionBlock}</option>
          </select>
        </label>
        <label class="field-span-full">
          <span>${copy.addressRuleValueLabel}</span>
          <input name="value" required value="${escapeHtml(rule.value)}" />
        </label>
      </div>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${ruleId ? copy.save : copy.create}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const nextRule = {
        role: form.elements.namedItem("role").value,
        action: form.elements.namedItem("action").value,
        value: normalizeDomain(form.elements.namedItem("value").value),
      };
      const errors = [];
      if (!isValidAddressRule(nextRule.value)) {
        errors.push({ field: "value", message: copy.validationAddressRule });
      }
      const policies = currentPolicies();
      const existingRules = getAddressRules(policies)
        .filter((item) => item.id !== ruleId)
        .map((item) => `${item.role}:${item.action}:${normalizeDomain(item.value)}`);
      if (existingRules.includes(`${nextRule.role}:${nextRule.action}:${nextRule.value}`)) {
        errors.push({ field: "value", message: copy.validationDuplicateAddressRule });
      }
      if (errors.length) {
        context.fail(errors);
      }
      if (ruleId) {
        const existing = findAddressRule(ruleId);
        policies.address_policy[routeToPolicies(existing.role, existing.action)].splice(existing.index, 1);
      }
      policies.address_policy[routeToPolicies(nextRule.role, nextRule.action)].push(nextRule.value);
      await callbacks.savePolicies(policies);
      closeDrawer();
      showFeedback(ruleId ? copy.recordSaved : copy.recordCreated);
    },
  });
}

export async function deleteAddressRule(ruleId) {
  const copy = getCopy();
  const rule = findAddressRule(ruleId);
  if (!rule) {
    return;
  }
  const policies = currentPolicies();
  policies.address_policy[routeToPolicies(rule.role, rule.action)].splice(rule.index, 1);
  await callbacks.savePolicies(policies);
  showFeedback(copy.recordDeleted);
}

export function openAttachmentRuleDrawer(ruleId = null, opener = document.activeElement) {
  const copy = getCopy();
  const rule = ruleId ? findAttachmentRule(ruleId) : { scope: "extension", action: "block", value: "" };
  renderDrawerForm({
    title: ruleId ? copy.edit : copy.createRule,
    summary: copy.attachmentRulesSummary,
    formId: "attachment-rule-form",
    opener,
    content: `
      <div class="field-grid">
        <label>
          <span>${copy.attachmentRuleScopeLabel}</span>
          <select name="scope">
            <option value="extension"${rule.scope === "extension" ? " selected" : ""}>${copy.attachmentScopeExtension}</option>
            <option value="mime"${rule.scope === "mime" ? " selected" : ""}>${copy.attachmentScopeMime}</option>
            <option value="detected"${rule.scope === "detected" ? " selected" : ""}>${copy.attachmentScopeDetected}</option>
          </select>
        </label>
        <label>
          <span>${copy.attachmentRuleActionLabel}</span>
          <select name="action">
            <option value="allow"${rule.action === "allow" ? " selected" : ""}>${copy.policyActionAllow}</option>
            <option value="block"${rule.action === "block" ? " selected" : ""}>${copy.policyActionBlock}</option>
          </select>
        </label>
        <label class="field-span-full">
          <span>${copy.attachmentRuleValueLabel}</span>
          <input name="value" required value="${escapeHtml(rule.value)}" />
        </label>
      </div>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${ruleId ? copy.save : copy.create}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const nextRule = {
        scope: form.elements.namedItem("scope").value,
        action: form.elements.namedItem("action").value,
        value: String(form.elements.namedItem("value").value).trim().toLowerCase(),
      };
      const errors = [];
      if (!nextRule.value) {
        errors.push({ field: "value", message: copy.validationAttachmentRule });
      } else if (nextRule.scope === "extension" && !/^[.]?[a-z0-9]+$/i.test(nextRule.value)) {
        errors.push({ field: "value", message: copy.validationAttachmentExtension });
      } else if (nextRule.scope === "mime" && !isValidMimeType(nextRule.value)) {
        errors.push({ field: "value", message: copy.validationAttachmentMime });
      } else if (nextRule.scope === "detected" && !/^[a-z0-9._-]+$/i.test(nextRule.value)) {
        errors.push({ field: "value", message: copy.validationDetectedType });
      }
      if (errors.length) {
        context.fail(errors);
      }
      const policies = currentPolicies();
      if (ruleId) {
        const existing = findAttachmentRule(ruleId);
        policies.attachment_policy[routeToAttachmentPolicies(existing.scope, existing.action)].splice(existing.index, 1);
      }
      policies.attachment_policy[routeToAttachmentPolicies(nextRule.scope, nextRule.action)].push(nextRule.value);
      await callbacks.savePolicies(policies);
      closeDrawer();
      showFeedback(ruleId ? copy.recordSaved : copy.recordCreated);
    },
  });
}

export async function deleteAttachmentRule(ruleId) {
  const copy = getCopy();
  const rule = findAttachmentRule(ruleId);
  if (!rule) {
    return;
  }
  const policies = currentPolicies();
  policies.attachment_policy[routeToAttachmentPolicies(rule.scope, rule.action)].splice(rule.index, 1);
  await callbacks.savePolicies(policies);
  showFeedback(copy.recordDeleted);
}

export function openFilteringPolicyDrawer(opener = document.activeElement) {
  const copy = getCopy();
  const policies = currentPolicies();
  const numberValue = (value) => (value === null || value === undefined ? "" : escapeHtml(value));
  renderDrawerForm({
    title: copy.editSettings,
    summary: copy.filteringPolicySummary,
    formId: "filtering-policy-form",
    opener,
    content: `
      <div class="field-grid">
        <label class="toggle-field">
          <span>${copy.requireSpfLabel}</span>
          <input name="require_spf" type="checkbox"${policies.require_spf ? " checked" : ""} />
        </label>
        <label class="toggle-field">
          <span>${copy.requireDmarcLabel}</span>
          <input name="require_dmarc_enforcement" type="checkbox"${policies.require_dmarc_enforcement ? " checked" : ""} />
        </label>
        <label class="toggle-field">
          <span>${copy.requireDkimAlignmentLabel}</span>
          <input name="require_dkim_alignment" type="checkbox"${policies.require_dkim_alignment ? " checked" : ""} />
        </label>
        <label class="toggle-field">
          <span>${copy.deferOnAuthTempfailLabel}</span>
          <input name="defer_on_auth_tempfail" type="checkbox"${policies.defer_on_auth_tempfail ? " checked" : ""} />
        </label>
        <label class="toggle-field">
          <span>${copy.bayespamEnabledLabel}</span>
          <input name="bayespam_enabled" type="checkbox"${policies.bayespam_enabled ? " checked" : ""} />
        </label>
        <label class="toggle-field">
          <span>${copy.reputationEnabledLabel}</span>
          <input name="reputation_enabled" type="checkbox"${policies.reputation_enabled ? " checked" : ""} />
        </label>
        <label>
          <span>${copy.spamQuarantineThresholdLabel}</span>
          <input name="spam_quarantine_threshold" type="number" min="0" step="0.1" value="${numberValue(policies.spam_quarantine_threshold)}" />
        </label>
        <label>
          <span>${copy.spamRejectThresholdLabel}</span>
          <input name="spam_reject_threshold" type="number" min="0" step="0.1" value="${numberValue(policies.spam_reject_threshold)}" />
        </label>
        <label>
          <span>${copy.reputationQuarantineThresholdLabel}</span>
          <input name="reputation_quarantine_threshold" type="number" step="1" value="${numberValue(policies.reputation_quarantine_threshold)}" />
        </label>
        <label>
          <span>${copy.reputationRejectThresholdLabel}</span>
          <input name="reputation_reject_threshold" type="number" step="1" value="${numberValue(policies.reputation_reject_threshold)}" />
        </label>
      </div>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${copy.save}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const spamQuarantineThreshold = Number(form.elements.namedItem("spam_quarantine_threshold").value);
      const spamRejectThreshold = Number(form.elements.namedItem("spam_reject_threshold").value);
      const reputationQuarantineThreshold = Number(form.elements.namedItem("reputation_quarantine_threshold").value);
      const reputationRejectThreshold = Number(form.elements.namedItem("reputation_reject_threshold").value);
      const errors = [];
      if (!Number.isFinite(spamQuarantineThreshold) || spamQuarantineThreshold < 0) {
        errors.push({ field: "spam_quarantine_threshold", message: copy.validationNonNegativeNumber });
      }
      if (!Number.isFinite(spamRejectThreshold) || spamRejectThreshold < 0) {
        errors.push({ field: "spam_reject_threshold", message: copy.validationNonNegativeNumber });
      }
      if (Number.isFinite(spamQuarantineThreshold) && Number.isFinite(spamRejectThreshold) && spamQuarantineThreshold > spamRejectThreshold) {
        errors.push({ field: "spam_reject_threshold", message: copy.validationSpamThresholdOrder });
      }
      if (!Number.isInteger(reputationQuarantineThreshold)) {
        errors.push({ field: "reputation_quarantine_threshold", message: copy.validationInteger });
      }
      if (!Number.isInteger(reputationRejectThreshold)) {
        errors.push({ field: "reputation_reject_threshold", message: copy.validationInteger });
      }
      if (
        Number.isInteger(reputationQuarantineThreshold) &&
        Number.isInteger(reputationRejectThreshold) &&
        reputationRejectThreshold > reputationQuarantineThreshold
      ) {
        errors.push({ field: "reputation_reject_threshold", message: copy.validationReputationThresholdOrder });
      }
      if (errors.length) {
        context.fail(errors);
      }
      policies.require_spf = form.elements.namedItem("require_spf").checked;
      policies.require_dmarc_enforcement = form.elements.namedItem("require_dmarc_enforcement").checked;
      policies.require_dkim_alignment = form.elements.namedItem("require_dkim_alignment").checked;
      policies.defer_on_auth_tempfail = form.elements.namedItem("defer_on_auth_tempfail").checked;
      policies.bayespam_enabled = form.elements.namedItem("bayespam_enabled").checked;
      policies.reputation_enabled = form.elements.namedItem("reputation_enabled").checked;
      policies.spam_quarantine_threshold = spamQuarantineThreshold;
      policies.spam_reject_threshold = spamRejectThreshold;
      policies.reputation_quarantine_threshold = reputationQuarantineThreshold;
      policies.reputation_reject_threshold = reputationRejectThreshold;
      await callbacks.savePolicies(policies);
      closeDrawer();
      showFeedback(copy.recordSaved);
    },
  });
}

export function openVirusFilteringDrawer(opener = document.activeElement) {
  const copy = getCopy();
  const policies = currentPolicies();
  const providerChain = antivirusProviderChain(policies);
  renderDrawerForm({
    title: copy.editSettings,
    summary: copy.virusFilteringSummary,
    formId: "virus-filtering-form",
    opener,
    content: `
      <label class="toggle-field">
        <span>${copy.virusFilteringEnabledLabel}</span>
        <input name="antivirus_enabled" type="checkbox"${policies.antivirus_enabled ? " checked" : ""} />
      </label>
      <label class="toggle-field">
        <span>${copy.virusFailClosedLabel}</span>
        <input name="antivirus_fail_closed" type="checkbox"${policies.antivirus_fail_closed ? " checked" : ""} />
      </label>
      <label>
        <span>${copy.virusProviderChainLabel}</span>
        <textarea name="antivirus_provider_chain" rows="4">${escapeHtml(providerChain.join("\n"))}</textarea>
        <small>${copy.virusProviderChainHelp}</small>
      </label>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${copy.save}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const antivirusEnabled = form.elements.namedItem("antivirus_enabled").checked;
      const providerChainInput = parseProviderChain(form.elements.namedItem("antivirus_provider_chain").value);
      if (antivirusEnabled && !providerChainInput.length) {
        context.fail([{ field: "antivirus_provider_chain", message: copy.validationProviderChain }]);
      }
      policies.antivirus_enabled = antivirusEnabled;
      policies.antivirus_fail_closed = form.elements.namedItem("antivirus_fail_closed").checked;
      policies.antivirus_provider_chain = providerChainInput;
      await callbacks.savePolicies(policies);
      closeDrawer();
      showFeedback(copy.recordSaved);
    },
  });
}

export function openRecipientVerificationDrawer(opener = document.activeElement) {
  const copy = getCopy();
  const settings = currentPolicies().recipient_verification;
  renderDrawerForm({
    title: copy.editSettings,
    summary: copy.verificationSummary,
    formId: "recipient-verification-form",
    opener,
    content: `
      <label class="toggle-field">
        <span>${copy.recipientVerificationEnabledLabel}</span>
        <input name="enabled" type="checkbox"${settings.enabled ? " checked" : ""} />
      </label>
      <label class="toggle-field">
        <span>${copy.recipientVerificationFailClosedLabel}</span>
        <input name="fail_closed" type="checkbox"${settings.fail_closed ? " checked" : ""} />
      </label>
      <label>
        <span>${copy.recipientVerificationTtlLabel}</span>
        <input name="cache_ttl_seconds" type="number" min="1" value="${escapeHtml(settings.cache_ttl_seconds)}" />
      </label>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${copy.save}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const ttl = Number(form.elements.namedItem("cache_ttl_seconds").value);
      if (!Number.isInteger(ttl) || ttl < 1) {
        context.fail([{ field: "cache_ttl_seconds", message: copy.validationPositiveInteger }]);
      }
      const policies = currentPolicies();
      policies.recipient_verification.enabled = form.elements.namedItem("enabled").checked;
      policies.recipient_verification.fail_closed = form.elements.namedItem("fail_closed").checked;
      policies.recipient_verification.cache_ttl_seconds = ttl;
      await callbacks.savePolicies(policies);
      closeDrawer();
      showFeedback(copy.recordSaved);
    },
  });
}

export function openDkimSettingsDrawer(opener = document.activeElement) {
  const copy = getCopy();
  const settings = currentPolicies().dkim;
  renderDrawerForm({
    title: copy.editSigningProfile,
    summary: copy.dkimSummary,
    formId: "dkim-settings-form",
    opener,
    content: `
      <label class="toggle-field">
        <span>${copy.dkimSigningEnabledLabel}</span>
        <input name="enabled" type="checkbox"${settings.enabled ? " checked" : ""} />
      </label>
      <label class="toggle-field">
        <span>${copy.dkimOverSignLabel}</span>
        <input name="over_sign" type="checkbox"${settings.over_sign ? " checked" : ""} />
      </label>
      <label>
        <span>${copy.dkimHeadersLabel}</span>
        <textarea name="headers" rows="4">${escapeHtml((settings.headers ?? []).join("\n"))}</textarea>
      </label>
      <label>
        <span>${copy.dkimExpirationLabel}</span>
        <input name="expiration_seconds" type="number" min="0" value="${escapeHtml(settings.expiration_seconds ?? "")}" />
      </label>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${copy.save}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const expirationValue = String(form.elements.namedItem("expiration_seconds").value).trim();
      const headers = dedupeList(parseLines(form.elements.namedItem("headers").value).map((value) => value.toLowerCase()));
      const errors = [];
      if (form.elements.namedItem("enabled").checked && !headers.length) {
        errors.push({ field: "headers", message: copy.validationHeaders });
      }
      if (expirationValue && (!Number.isInteger(Number(expirationValue)) || Number(expirationValue) < 0)) {
        errors.push({ field: "expiration_seconds", message: copy.validationPositiveInteger });
      }
      if (errors.length) {
        context.fail(errors);
      }
      const policies = currentPolicies();
      policies.dkim.enabled = form.elements.namedItem("enabled").checked;
      policies.dkim.over_sign = form.elements.namedItem("over_sign").checked;
      policies.dkim.headers = headers;
      policies.dkim.expiration_seconds = expirationValue ? Number(expirationValue) : null;
      await callbacks.savePolicies(policies);
      closeDrawer();
      showFeedback(copy.recordSaved);
    },
  });
}

export function openDkimDomainDrawer(index = null, opener = document.activeElement) {
  const copy = getCopy();
  const settings = currentPolicies().dkim;
  const domain = index === null ? { domain: "", selector: "", private_key_path: "", enabled: true } : settings.domains[index];
  renderDrawerForm({
    title: index === null ? copy.createDomain : copy.edit,
    summary: copy.dkimSummary,
    formId: "dkim-domain-form",
    opener,
    content: `
      <label>
        <span>${copy.dkimDomainLabel}</span>
        <input name="domain" required value="${escapeHtml(domain.domain)}" />
      </label>
      <label>
        <span>${copy.dkimSelectorLabel}</span>
        <input name="selector" required value="${escapeHtml(domain.selector)}" />
      </label>
      <label>
        <span>${copy.dkimKeyPathLabel}</span>
        <input name="private_key_path" required value="${escapeHtml(domain.private_key_path)}" />
      </label>
      <label class="toggle-field">
        <span>${copy.enabled}</span>
        <input name="enabled" type="checkbox"${domain.enabled ? " checked" : ""} />
      </label>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${index === null ? copy.create : copy.save}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const nextDomain = {
        domain: normalizeDomain(form.elements.namedItem("domain").value),
        selector: String(form.elements.namedItem("selector").value).trim().toLowerCase(),
        private_key_path: String(form.elements.namedItem("private_key_path").value).trim(),
        enabled: form.elements.namedItem("enabled").checked,
      };
      const errors = [];
      if (!isValidDomain(nextDomain.domain)) {
        errors.push({ field: "domain", message: copy.validationDomain });
      }
      if (!isValidSelector(nextDomain.selector)) {
        errors.push({ field: "selector", message: copy.validationSelector });
      }
      if (!nextDomain.private_key_path) {
        errors.push({ field: "private_key_path", message: copy.validationKeyPath });
      }
      if (errors.length) {
        context.fail(errors);
      }
      const policies = currentPolicies();
      if (index === null) {
        policies.dkim.domains.push(nextDomain);
      } else {
        policies.dkim.domains[index] = nextDomain;
      }
      await callbacks.savePolicies(policies);
      closeDrawer();
      showFeedback(index === null ? copy.recordCreated : copy.recordSaved);
    },
  });
}

export async function deleteDkimDomain(index) {
  const copy = getCopy();
  const policies = currentPolicies();
  policies.dkim.domains.splice(index, 1);
  await callbacks.savePolicies(policies);
  showFeedback(copy.recordDeleted);
}

export function openDigestSettingsDrawer(opener = document.activeElement) {
  const copy = getCopy();
  const settings = currentReporting();
  renderDrawerForm({
    title: copy.editSettings,
    summary: copy.digestSummary,
    formId: "digest-settings-form",
    opener,
    content: `
      <label class="toggle-field">
        <span>${copy.reportingEnabledLabel}</span>
        <input name="digest_enabled" type="checkbox"${settings.digest_enabled ? " checked" : ""} />
      </label>
      <div class="field-grid">
        <label>
          <span>${copy.digestIntervalLabel}</span>
          <input name="digest_interval_minutes" type="number" min="1" value="${escapeHtml(settings.digest_interval_minutes)}" />
        </label>
        <label>
          <span>${copy.digestMaxItemsLabel}</span>
          <input name="digest_max_items" type="number" min="1" value="${escapeHtml(settings.digest_max_items)}" />
        </label>
        <label>
          <span>${copy.digestRetentionLabel}</span>
          <input name="history_retention_days" type="number" min="1" value="${escapeHtml(settings.history_retention_days)}" />
        </label>
      </div>
      <p class="helper-text">${escapeHtml(copy.reportingSettingsNote)}</p>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${copy.save}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const interval = Number(form.elements.namedItem("digest_interval_minutes").value);
      const maxItems = Number(form.elements.namedItem("digest_max_items").value);
      const retention = Number(form.elements.namedItem("history_retention_days").value);
      const errors = [];
      if (!Number.isInteger(interval) || interval < 1) errors.push({ field: "digest_interval_minutes", message: copy.validationPositiveInteger });
      if (!Number.isInteger(maxItems) || maxItems < 1) errors.push({ field: "digest_max_items", message: copy.validationPositiveInteger });
      if (!Number.isInteger(retention) || retention < 1) errors.push({ field: "history_retention_days", message: copy.validationPositiveInteger });
      if (errors.length) {
        context.fail(errors);
      }
      const settingsToSave = currentReporting();
      settingsToSave.digest_enabled = form.elements.namedItem("digest_enabled").checked;
      settingsToSave.digest_interval_minutes = interval;
      settingsToSave.digest_max_items = maxItems;
      settingsToSave.history_retention_days = retention;
      await callbacks.saveReporting(settingsToSave);
      closeDrawer();
      showFeedback(copy.recordSaved);
    },
  });
}

export function openDigestDefaultDrawer(index = null, opener = document.activeElement) {
  const copy = getCopy();
  const settings = currentReporting();
  const item = index === null ? { domain: "", recipients: [] } : settings.domain_defaults[index];
  renderDrawerForm({
    title: index === null ? copy.createDomainDefault : copy.edit,
    summary: copy.digestDefaultsTitle,
    formId: "digest-default-form",
    opener,
    content: `
      <label>
        <span>${copy.dkimDomainLabel}</span>
        <input name="domain" required value="${escapeHtml(item.domain)}" />
      </label>
      <label>
        <span>${copy.domainDefaultsLabel}</span>
        <textarea name="recipients" rows="4">${escapeHtml((item.recipients ?? []).join("\n"))}</textarea>
      </label>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${index === null ? copy.create : copy.save}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const domain = normalizeDomain(form.elements.namedItem("domain").value);
      const recipients = dedupeList(parseLines(form.elements.namedItem("recipients").value).map(normalizeEmail));
      const errors = [];
      if (!isValidDomain(domain)) {
        errors.push({ field: "domain", message: copy.validationDomain });
      }
      if (!recipients.length) {
        errors.push({ field: "recipients", message: copy.validationRecipients });
      }
      recipients.forEach((recipient) => {
        if (!isValidEmail(recipient)) {
          errors.push({ field: "recipients", message: copy.validationEmail });
        }
      });
      if (errors.length) {
        context.fail(errors);
      }
      const settingsToSave = currentReporting();
      const nextItem = { domain, recipients };
      if (index === null) {
        settingsToSave.domain_defaults.push(nextItem);
      } else {
        settingsToSave.domain_defaults[index] = nextItem;
      }
      await callbacks.saveReporting(settingsToSave);
      closeDrawer();
      showFeedback(index === null ? copy.recordCreated : copy.recordSaved);
    },
  });
}

export async function deleteDigestDefault(index) {
  const copy = getCopy();
  const settings = currentReporting();
  settings.domain_defaults.splice(index, 1);
  await callbacks.saveReporting(settings);
  showFeedback(copy.recordDeleted);
}

export function openDigestOverrideDrawer(index = null, opener = document.activeElement) {
  const copy = getCopy();
  const settings = currentReporting();
  const item = index === null ? { mailbox: "", recipient: "", enabled: true } : settings.user_overrides[index];
  renderDrawerForm({
    title: index === null ? copy.createOverride : copy.edit,
    summary: copy.digestOverridesTitle,
    formId: "digest-override-form",
    opener,
    content: `
      <label>
        <span>${copy.overrideMailboxLabel}</span>
        <input name="mailbox" required value="${escapeHtml(item.mailbox)}" />
      </label>
      <label>
        <span>${copy.overrideRecipientLabel}</span>
        <input name="recipient" required value="${escapeHtml(item.recipient)}" />
      </label>
      <label class="toggle-field">
        <span>${copy.enabled}</span>
        <input name="enabled" type="checkbox"${item.enabled ? " checked" : ""} />
      </label>
      <div class="record-actions">
        <button class="primary-button compact-button" type="submit">${index === null ? copy.create : copy.save}</button>
        <button class="secondary-button compact-button" type="button" data-action="drawer-close">${copy.cancel}</button>
      </div>
    `,
    onSubmit: async (form, context) => {
      const mailbox = normalizeEmail(form.elements.namedItem("mailbox").value);
      const recipient = normalizeEmail(form.elements.namedItem("recipient").value);
      const errors = [];
      if (!isValidEmail(mailbox)) {
        errors.push({ field: "mailbox", message: copy.validationMailbox });
      }
      if (!isValidEmail(recipient)) {
        errors.push({ field: "recipient", message: copy.validationEmail });
      }
      if (errors.length) {
        context.fail(errors);
      }
      const settingsToSave = currentReporting();
      const nextItem = {
        mailbox,
        recipient,
        enabled: form.elements.namedItem("enabled").checked,
      };
      if (index === null) {
        settingsToSave.user_overrides.push(nextItem);
      } else {
        settingsToSave.user_overrides[index] = nextItem;
      }
      await callbacks.saveReporting(settingsToSave);
      closeDrawer();
      showFeedback(index === null ? copy.recordCreated : copy.recordSaved);
    },
  });
}

export async function deleteDigestOverride(index) {
  const copy = getCopy();
  const settings = currentReporting();
  settings.user_overrides.splice(index, 1);
  await callbacks.saveReporting(settings);
  showFeedback(copy.recordDeleted);
}
