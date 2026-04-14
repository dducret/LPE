import React from "react";
import ReactDOM from "react-dom/client";
import "./styles.css";

function App() {
  return (
    <main className="shell">
      <section className="panel">
        <p className="eyebrow">La Poste ELectronique</p>
        <h1>Console d'administration</h1>
        <p>
          Point d'entree initial du back office pour piloter comptes, domaines,
          quotas et politiques.
        </p>
      </section>
    </main>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);

