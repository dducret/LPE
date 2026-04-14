import React from "react";
import ReactDOM from "react-dom/client";
import { getInitialLocale, localeLabels, messages, supportedLocales, type Locale } from "./i18n";
import "./styles.css";

function App() {
  const [locale, setLocale] = React.useState<Locale>(getInitialLocale);
  const copy = messages[locale];

  React.useEffect(() => {
    document.documentElement.lang = locale;
    window.localStorage.setItem("lpe.locale", locale);
  }, [locale]);

  return (
    <main className="shell">
      <section className="panel">
        <div className="toolbar">
          <p className="eyebrow">{copy.eyebrow}</p>
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
        <h1>{copy.title}</h1>
        <p>{copy.body}</p>
      </section>
    </main>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);

