import { getCopy } from "../i18n/index.js?v=20260502-outbound-ehlo";
import { AUTH_TOKEN_KEY } from "./context.js?v=20260502-outbound-ehlo";

export function authHeaders() {
  const token = window.localStorage.getItem(AUTH_TOKEN_KEY);
  return token ? { Authorization: `Bearer ${token}` } : {};
}

export async function parseError(response) {
  let detail = "";
  try {
    const contentType = response.headers.get("content-type") || "";
    if (contentType.includes("application/json")) {
      const body = await response.json();
      detail = body.error || body.message || body.detail || "";
    } else {
      detail = (await response.text()).trim();
    }
  } catch {}
  const prefix = getCopy().backendErrorPrefix || "Backend error";
  const suffix = detail ? `: ${detail}` : "";
  throw new Error(`${prefix} (${response.status})${suffix}`);
}

export async function fetchJson(path, init = {}) {
  const response = await fetch(path, {
    ...init,
    headers: { ...authHeaders(), ...(init.headers ?? {}) },
  });
  if (response.status === 401) {
    throw new Error("401");
  }
  if (!response.ok) {
    await parseError(response);
  }
  return response.status === 204 ? null : response.json();
}

export async function fetchOptionalJson(path, fallback) {
  try {
    return await fetchJson(path);
  } catch {
    return fallback;
  }
}

export async function fetchBlob(path, init = {}) {
  const response = await fetch(path, {
    ...init,
    headers: { ...authHeaders(), ...(init.headers ?? {}) },
  });
  if (response.status === 401) {
    throw new Error("401");
  }
  if (!response.ok) {
    await parseError(response);
  }
  return response.blob();
}

export async function fetchDashboard() {
  return fetchJson("/api/dashboard");
}

export async function putJson(path, payload) {
  return fetchJson(path, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function postJson(path, payload = null) {
  return fetchJson(path, {
    method: "POST",
    headers: payload ? { "Content-Type": "application/json" } : {},
    body: payload ? JSON.stringify(payload) : undefined,
  });
}
