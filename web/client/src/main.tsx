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
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand">LPE</div>
        <nav>
          <a href="/">{copy.navInbox}</a>
          <a href="/">{copy.navCalendar}</a>
          <a href="/">{copy.navContacts}</a>
        </nav>
      </aside>
      <section className="content">
        <header className="content-header">
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
        </header>
        <article className="message-card">
          <h2>{copy.cardTitle}</h2>
          <p>{copy.cardBody}</p>
        </article>
      </section>
    </main>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);

