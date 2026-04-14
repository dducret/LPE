import React from "react";
import ReactDOM from "react-dom/client";
import "./styles.css";

function App() {
  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand">LPE</div>
        <nav>
          <a href="/">Boite de reception</a>
          <a href="/">Calendrier</a>
          <a href="/">Contacts</a>
        </nav>
      </aside>
      <section className="content">
        <header>
          <p className="eyebrow">Client web</p>
          <h1>Interface moderne orientee JMAP</h1>
        </header>
        <article className="message-card">
          <h2>Bienvenue dans LPE</h2>
          <p>
            Ce client web servira de base pour la consultation mail, les
            calendriers et les contacts via une API moderne.
          </p>
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

