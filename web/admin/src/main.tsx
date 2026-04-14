import React from "react";
import ReactDOM from "react-dom/client";
import { getInitialLocale, localeLabels, messages, supportedLocales, type Locale } from "./i18n";
import "./styles.css";

type HealthResponse = {
  service: string;
  status: string;
};

type BootstrapResponse = {
  email: string;
  display_name: string;
};

type LocalAiHealthResponse = {
  provider: string;
  models: string[];
  bootstrap_summary_payload: string;
};

type AttachmentSupportResponse = {
  formats: string[];
};

type DashboardState = {
  health: HealthResponse;
  bootstrap: BootstrapResponse;
  localAi: LocalAiHealthResponse;
  attachments: AttachmentSupportResponse;
};

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`/api/${path}`);
  if (!response.ok) {
    throw new Error(`Request failed for ${path}: ${response.status}`);
  }

  return (await response.json()) as T;
}

function App() {
  const [locale, setLocale] = React.useState<Locale>(getInitialLocale);
  const [state, setState] = React.useState<DashboardState | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const copy = messages[locale];

  React.useEffect(() => {
    document.documentElement.lang = locale;
    window.localStorage.setItem("lpe.locale", locale);
  }, [locale]);

  React.useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const [health, bootstrap, localAi, attachments] = await Promise.all([
          fetchJson<HealthResponse>("health"),
          fetchJson<BootstrapResponse>("bootstrap/admin"),
          fetchJson<LocalAiHealthResponse>("health/local-ai"),
          fetchJson<AttachmentSupportResponse>("capabilities/attachments")
        ]);

        if (!cancelled) {
          setState({ health, bootstrap, localAi, attachments });
          setError(null);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Unknown error");
        }
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <main className="shell">
      <section className="panel">
        <div className="toolbar">
          <div>
            <p className="eyebrow">{copy.eyebrow}</p>
            <h1>{copy.title}</h1>
          </div>
          <label className="locale-picker">
            <span>{copy.languageLabel}</span>
            <select value={locale} onChange={(event) => setLocale(event.target.value as Locale)}>
              {supportedLocales.map((entry) => (
                <option key={entry} value={entry}>
                  {localeLabels[entry]}
                </option>
              ))}
            </select>
          </label>
        </div>

        <p className="intro">{copy.body}</p>

        <div className="status-strip">
          <strong>{state?.health.status === "ok" ? copy.serviceHealthy : copy.serviceUnhealthy}</strong>
          <a href="/api/health" target="_blank" rel="noreferrer">
            {copy.openApiLabel}
          </a>
        </div>

        {error ? <p className="error">{copy.failed} {error}</p> : null}
        {!state && !error ? <p className="loading">{copy.loading}</p> : null}

        {state ? (
          <div className="grid">
            <article className="card">
              <h2>{copy.apiStatusTitle}</h2>
              <p>{copy.apiStatusBody}</p>
              <dl>
                <div>
                  <dt>{copy.endpointLabel}</dt>
                  <dd>{state.health.service}</dd>
                </div>
                <div>
                  <dt>Status</dt>
                  <dd>{state.health.status}</dd>
                </div>
              </dl>
            </article>

            <article className="card">
              <h2>{copy.adminAccountTitle}</h2>
              <p>{copy.adminAccountBody}</p>
              <dl>
                <div>
                  <dt>Email</dt>
                  <dd>{state.bootstrap.email}</dd>
                </div>
                <div>
                  <dt>Name</dt>
                  <dd>{state.bootstrap.display_name}</dd>
                </div>
              </dl>
            </article>

            <article className="card">
              <h2>{copy.localAiTitle}</h2>
              <p>{copy.localAiBody}</p>
              <dl>
                <div>
                  <dt>{copy.providerLabel}</dt>
                  <dd>{state.localAi.provider}</dd>
                </div>
                <div>
                  <dt>{copy.modelsLabel}</dt>
                  <dd>{state.localAi.models.join(", ")}</dd>
                </div>
              </dl>
            </article>

            <article className="card">
              <h2>{copy.attachmentTitle}</h2>
              <p>{copy.attachmentBody}</p>
              <ul className="formats">
                {state.attachments.formats.map((format) => (
                  <li key={format}>{format}</li>
                ))}
              </ul>
            </article>
          </div>
        ) : null}
      </section>
    </main>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);

