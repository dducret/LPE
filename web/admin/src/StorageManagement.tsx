import React from "react";
import { Drawer, Input } from "../../ui/src/components/primitives";
import type { Locale } from "./i18n";

type AdminIdentity = { email: string; role: string; permissions: string[] };
type StoragePoolSummary = {
  id: string;
  name: string;
  poolKind: string;
  status: string;
  assignable: boolean;
  isPlatformDefault: boolean;
  createdAt: string;
  updatedAt: string;
};
type StoragePoolReference = Pick<StoragePoolSummary, "id" | "name" | "poolKind" | "status">;
type StoragePolicySummary = {
  scope: {
    kind: "platform" | "tenant" | "domain" | "account";
    tenantId: string | null;
    tenantName: string | null;
    domainId: string | null;
    domainName: string | null;
    accountId: string | null;
    accountEmail: string | null;
    name: string;
  };
  configuredPool: StoragePoolReference | null;
  effectivePool: StoragePoolReference;
  inheritedFrom: string | null;
  updatedAt: string | null;
  updatedBy: string | null;
};
type StoragePolicyOverview = { allowedPools: StoragePoolSummary[]; policies: StoragePolicySummary[] };
type StoragePoolHealth = {
  pool: StoragePoolReference;
  health: string;
  activePlacements: number;
  retiringPlacements: number;
  failedPlacements: number;
  cleanupFailedPlacements: number;
};
type StoragePlacementCounts = {
  active: number;
  copying: number;
  verified: number;
  retiring: number;
  failed: number;
  cleaning: number;
  cleanupFailed: number;
  deleted: number;
  missingActive: number;
  degraded: number;
};
type StorageMigrationCounts = {
  pending: number;
  running: number;
  verified: number;
  switched: number;
  failed: number;
  cancelled: number;
  expiredLeases: number;
};
type StorageCleanupCounts = {
  due: number;
  retiring: number;
  cleaning: number;
  cleanupFailed: number;
  deleted: number;
  blockedByRollback: number;
  blockedByMissingActiveReplacement: number;
  blockedByRetentionOrLegalHold: number;
};
type StorageHealthResponse = {
  status: string;
  pools: StoragePoolHealth[];
  placements: StoragePlacementCounts;
  migrations: StorageMigrationCounts;
  cleanup: StorageCleanupCounts;
};
type StorageMigrationJobSummary = {
  id: string;
  tenantId: string;
  domainId: string;
  blobKind: string;
  sourcePool: StoragePoolReference;
  targetPool: StoragePoolReference;
  status: string;
  attempts: number;
  nextAttemptAt: string;
  lastErrorSummary: string | null;
  startedAt: string | null;
  verifiedAt: string | null;
  switchedAt: string | null;
  rollbackUntil: string | null;
};
type StorageMigrationVisibilityResponse = {
  summary: StorageMigrationCounts;
  items: StorageMigrationJobSummary[];
};
type StorageCleanupPlacementSummary = {
  tenantId: string;
  domainId: string;
  blobKind: string;
  pool: StoragePoolReference;
  status: string;
  cleanupAttempts: number;
  rollbackUntil: string | null;
  nextCleanupAttemptAt: string | null;
  cleanedAt: string | null;
  cleanupErrorSummary: string | null;
  blockers: string[];
};
type StorageCleanupVisibilityResponse = {
  summary: StorageCleanupCounts;
  items: StorageCleanupPlacementSummary[];
};
type StorageData = {
  pools: StoragePoolSummary[];
  policies: StoragePolicyOverview;
  health: StorageHealthResponse;
  migrations: StorageMigrationVisibilityResponse;
  cleanup: StorageCleanupVisibilityResponse;
};
type Selection =
  | { kind: "new-pool" }
  | { kind: "pool"; id: string }
  | { kind: "policy"; key: string }
  | { kind: "migration"; id: string }
  | { kind: "cleanup"; index: number };

type StorageCopy = {
  title: string;
  summary: string;
  newPool: string;
  editTenantPolicy: string;
  refresh: string;
  pool: string;
  policy: string;
  migration: string;
  cleanup: string;
  health: string;
  placements: string;
  missingActive: string;
  expiredLeases: string;
  blocked: string;
  failed: string;
  degraded: string;
  retiring: string;
  active: string;
  allowedPools: string;
  effectivePool: string;
  configuredPool: string;
  inheritedFrom: string;
  clearInheritance: string;
  save: string;
  create: string;
  close: string;
  open: string;
  name: string;
  kind: string;
  status: string;
  attempts: string;
  nextAttempt: string;
  lastError: string;
  sourcePool: string;
  targetPool: string;
  blockers: string;
  noJobs: string;
  noCleanup: string;
  loading: string;
  noData: string;
  saved: string;
  poolKindPostgres: string;
};

const storageMessages: Record<Locale, StorageCopy> = {
  en: {
    title: "Storage pools and policy",
    summary: "Pool health, policy inheritance, migration status, and cleanup visibility.",
    newPool: "New pool",
    editTenantPolicy: "Edit tenant policy",
    refresh: "Refresh",
    pool: "Pool",
    policy: "Policy",
    migration: "Migration",
    cleanup: "Cleanup",
    health: "Health",
    placements: "Placements",
    missingActive: "Missing active",
    expiredLeases: "Expired leases",
    blocked: "Blocked",
    failed: "Failed",
    degraded: "Degraded",
    retiring: "Retiring",
    active: "Active",
    allowedPools: "Allowed pools",
    effectivePool: "Effective pool",
    configuredPool: "Configured pool",
    inheritedFrom: "Inherited from",
    clearInheritance: "Inherit",
    save: "Save",
    create: "Create",
    close: "Close",
    open: "Open",
    name: "Name",
    kind: "Kind",
    status: "Status",
    attempts: "Attempts",
    nextAttempt: "Next attempt",
    lastError: "Last error",
    sourcePool: "Source pool",
    targetPool: "Target pool",
    blockers: "Blockers",
    noJobs: "No visible migration jobs.",
    noCleanup: "No visible cleanup work.",
    loading: "Loading storage status...",
    noData: "Storage visibility is not available.",
    saved: "Saved.",
    poolKindPostgres: "PostgreSQL",
  },
  fr: {
    title: "Pools et politiques de stockage",
    summary: "Sante des pools, heritage des politiques, migrations et nettoyage.",
    newPool: "Nouveau pool",
    editTenantPolicy: "Modifier la politique locataire",
    refresh: "Actualiser",
    pool: "Pool",
    policy: "Politique",
    migration: "Migration",
    cleanup: "Nettoyage",
    health: "Sante",
    placements: "Placements",
    missingActive: "Actif manquant",
    expiredLeases: "Baux expires",
    blocked: "Bloque",
    failed: "Echec",
    degraded: "Degrade",
    retiring: "Retrait",
    active: "Actif",
    allowedPools: "Pools autorises",
    effectivePool: "Pool effectif",
    configuredPool: "Pool configure",
    inheritedFrom: "Herite de",
    clearInheritance: "Heriter",
    save: "Enregistrer",
    create: "Creer",
    close: "Fermer",
    open: "Ouvrir",
    name: "Nom",
    kind: "Type",
    status: "Statut",
    attempts: "Tentatives",
    nextAttempt: "Prochaine tentative",
    lastError: "Derniere erreur",
    sourcePool: "Pool source",
    targetPool: "Pool cible",
    blockers: "Blocages",
    noJobs: "Aucune migration visible.",
    noCleanup: "Aucun nettoyage visible.",
    loading: "Chargement du stockage...",
    noData: "Visibilite stockage indisponible.",
    saved: "Enregistre.",
    poolKindPostgres: "PostgreSQL",
  },
  de: {
    title: "Speicherpools und Richtlinien",
    summary: "Poolzustand, Richtlinienvererbung, Migrationen und Bereinigung.",
    newPool: "Neuer Pool",
    editTenantPolicy: "Tenant-Richtlinie bearbeiten",
    refresh: "Aktualisieren",
    pool: "Pool",
    policy: "Richtlinie",
    migration: "Migration",
    cleanup: "Bereinigung",
    health: "Zustand",
    placements: "Placements",
    missingActive: "Aktiv fehlt",
    expiredLeases: "Abgelaufene Leases",
    blocked: "Blockiert",
    failed: "Fehlgeschlagen",
    degraded: "Degradiert",
    retiring: "Auslaufend",
    active: "Aktiv",
    allowedPools: "Erlaubte Pools",
    effectivePool: "Wirksamer Pool",
    configuredPool: "Konfigurierter Pool",
    inheritedFrom: "Geerbt von",
    clearInheritance: "Erben",
    save: "Speichern",
    create: "Erstellen",
    close: "Schliessen",
    open: "Offnen",
    name: "Name",
    kind: "Art",
    status: "Status",
    attempts: "Versuche",
    nextAttempt: "Nachster Versuch",
    lastError: "Letzter Fehler",
    sourcePool: "Quellpool",
    targetPool: "Zielpool",
    blockers: "Blocker",
    noJobs: "Keine sichtbaren Migrationen.",
    noCleanup: "Keine sichtbare Bereinigung.",
    loading: "Speicherstatus wird geladen...",
    noData: "Speichersichtbarkeit ist nicht verfugbar.",
    saved: "Gespeichert.",
    poolKindPostgres: "PostgreSQL",
  },
  it: {
    title: "Pool e politiche storage",
    summary: "Salute pool, ereditarieta politiche, migrazioni e pulizia.",
    newPool: "Nuovo pool",
    editTenantPolicy: "Modifica politica tenant",
    refresh: "Aggiorna",
    pool: "Pool",
    policy: "Politica",
    migration: "Migrazione",
    cleanup: "Pulizia",
    health: "Salute",
    placements: "Placement",
    missingActive: "Attivo mancante",
    expiredLeases: "Lease scaduti",
    blocked: "Bloccato",
    failed: "Fallito",
    degraded: "Degradato",
    retiring: "In ritiro",
    active: "Attivo",
    allowedPools: "Pool consentiti",
    effectivePool: "Pool effettivo",
    configuredPool: "Pool configurato",
    inheritedFrom: "Ereditato da",
    clearInheritance: "Eredita",
    save: "Salva",
    create: "Crea",
    close: "Chiudi",
    open: "Apri",
    name: "Nome",
    kind: "Tipo",
    status: "Stato",
    attempts: "Tentativi",
    nextAttempt: "Prossimo tentativo",
    lastError: "Ultimo errore",
    sourcePool: "Pool sorgente",
    targetPool: "Pool destinazione",
    blockers: "Blocchi",
    noJobs: "Nessuna migrazione visibile.",
    noCleanup: "Nessuna pulizia visibile.",
    loading: "Caricamento storage...",
    noData: "Visibilita storage non disponibile.",
    saved: "Salvato.",
    poolKindPostgres: "PostgreSQL",
  },
  es: {
    title: "Pools y politicas de almacenamiento",
    summary: "Salud de pools, herencia de politicas, migraciones y limpieza.",
    newPool: "Nuevo pool",
    editTenantPolicy: "Editar politica tenant",
    refresh: "Actualizar",
    pool: "Pool",
    policy: "Politica",
    migration: "Migracion",
    cleanup: "Limpieza",
    health: "Salud",
    placements: "Ubicaciones",
    missingActive: "Activo faltante",
    expiredLeases: "Leases vencidos",
    blocked: "Bloqueado",
    failed: "Fallido",
    degraded: "Degradado",
    retiring: "En retirada",
    active: "Activo",
    allowedPools: "Pools permitidos",
    effectivePool: "Pool efectivo",
    configuredPool: "Pool configurado",
    inheritedFrom: "Heredado de",
    clearInheritance: "Heredar",
    save: "Guardar",
    create: "Crear",
    close: "Cerrar",
    open: "Abrir",
    name: "Nombre",
    kind: "Tipo",
    status: "Estado",
    attempts: "Intentos",
    nextAttempt: "Proximo intento",
    lastError: "Ultimo error",
    sourcePool: "Pool origen",
    targetPool: "Pool destino",
    blockers: "Bloqueos",
    noJobs: "No hay migraciones visibles.",
    noCleanup: "No hay limpieza visible.",
    loading: "Cargando almacenamiento...",
    noData: "Visibilidad de almacenamiento no disponible.",
    saved: "Guardado.",
    poolKindPostgres: "PostgreSQL",
  },
};

function authHeaders(token: string | null): Record<string, string> {
  return token ? { Authorization: `Bearer ${token}` } : {};
}

async function fetchStorageJson<T>(path: string, token: string | null): Promise<T> {
  const response = await fetch(`/api/${path}`, { headers: authHeaders(token), credentials: "same-origin" });
  if (!response.ok) throw new Error((await response.text()).trim() || `Request failed for ${path}: ${response.status}`);
  return (await response.json()) as T;
}

async function sendStorageJson<T>(path: string, method: "POST" | "PUT", payload: unknown, token: string | null): Promise<T> {
  const response = await fetch(`/api/${path}`, {
    method,
    headers: { "Content-Type": "application/json", ...authHeaders(token) },
    body: JSON.stringify(payload),
    credentials: "same-origin",
  });
  if (!response.ok) throw new Error((await response.text()).trim() || `Request failed for ${path}: ${response.status}`);
  return (await response.json()) as T;
}

function isGlobalAdmin(admin: AdminIdentity | null) {
  return Boolean(admin?.permissions.includes("*") || ["server-admin", "super-admin", "global_admin"].includes(admin?.role ?? ""));
}

function statusClass(status: string) {
  if (["ok", "active", "verified", "switched"].includes(status)) return "pill ok";
  if (["failed", "cleanup_failed", "degraded"].includes(status)) return "pill danger";
  if (["retiring", "pending", "running", "cleaning", "disabled"].includes(status)) return "pill warn";
  return "pill";
}

function policyKey(policy: StoragePolicySummary) {
  return [
    policy.scope.kind,
    policy.scope.tenantId ?? "",
    policy.scope.domainId ?? "",
    policy.scope.accountId ?? "",
  ].join(":");
}

function policyPath(policy: StoragePolicySummary) {
  if (policy.scope.kind === "platform") return "console/storage/policies/platform";
  if (policy.scope.kind === "tenant" && policy.scope.tenantId) return `console/storage/policies/tenants/${policy.scope.tenantId}`;
  if (policy.scope.kind === "domain" && policy.scope.domainId) return `console/storage/policies/domains/${policy.scope.domainId}`;
  if (policy.scope.kind === "account" && policy.scope.accountId) return `console/storage/policies/accounts/${policy.scope.accountId}`;
  return null;
}

function countBlockedCleanup(cleanup: StorageCleanupCounts) {
  return cleanup.blockedByRollback + cleanup.blockedByMissingActiveReplacement + cleanup.blockedByRetentionOrLegalHold;
}

function DetailRow(props: { label: string; value: React.ReactNode }) {
  return <div className="row"><strong>{props.label}</strong><span>{props.value}</span></div>;
}

export function StorageManagement(props: { token: string | null; admin: AdminIdentity | null; locale: Locale }) {
  const copy = storageMessages[props.locale];
  const [data, setData] = React.useState<StorageData | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [notice, setNotice] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState<string | null>(null);
  const [selection, setSelection] = React.useState<Selection | null>(null);
  const [poolForm, setPoolForm] = React.useState({ name: "", poolKind: "postgres", status: "active" });
  const [policyPoolId, setPolicyPoolId] = React.useState("");
  const globalAdmin = isGlobalAdmin(props.admin);

  const loadStorage = React.useCallback(async () => {
    if (!props.token) return;
    setBusy("storage-load");
    try {
      const [pools, policies, health, migrations, cleanup] = await Promise.all([
        fetchStorageJson<StoragePoolSummary[]>("console/storage/pools", props.token),
        fetchStorageJson<StoragePolicyOverview>("console/storage/policies", props.token),
        fetchStorageJson<StorageHealthResponse>("console/storage/health", props.token),
        fetchStorageJson<StorageMigrationVisibilityResponse>("console/storage/migrations", props.token),
        fetchStorageJson<StorageCleanupVisibilityResponse>("console/storage/cleanup", props.token),
      ]);
      setData({ pools, policies, health, migrations, cleanup });
      setError(null);
    } catch (event) {
      setError(event instanceof Error ? event.message : copy.noData);
    } finally {
      setBusy(null);
    }
  }, [copy.noData, props.token]);

  React.useEffect(() => { void loadStorage(); }, [loadStorage]);

  const selectedPool = selection?.kind === "pool" ? data?.pools.find((pool) => pool.id === selection.id) ?? null : null;
  const selectedPolicy = selection?.kind === "policy" ? data?.policies.policies.find((policy) => policyKey(policy) === selection.key) ?? null : null;
  const selectedMigration = selection?.kind === "migration" ? data?.migrations.items.find((job) => job.id === selection.id) ?? null : null;
  const selectedCleanup = selection?.kind === "cleanup" ? data?.cleanup.items[selection.index] ?? null : null;
  const tenantPolicy = data?.policies.policies.find((policy) => policy.scope.kind === "tenant") ?? null;

  function openNewPool() {
    setPoolForm({ name: "", poolKind: "postgres", status: "active" });
    setSelection({ kind: "new-pool" });
  }

  function openPool(pool: StoragePoolSummary) {
    setPoolForm({ name: pool.name, poolKind: pool.poolKind, status: pool.status });
    setSelection({ kind: "pool", id: pool.id });
  }

  function openPolicy(policy: StoragePolicySummary) {
    setPolicyPoolId(policy.configuredPool?.id ?? "");
    setSelection({ kind: "policy", key: policyKey(policy) });
  }

  async function savePool() {
    if (!globalAdmin) return;
    const editingPool = selectedPool;
    const action = editingPool ? `pool-${editingPool.id}` : "pool-create";
    setBusy(action);
    try {
      if (editingPool) {
        await sendStorageJson(`console/storage/pools/${editingPool.id}`, "PUT", { name: poolForm.name, status: poolForm.status }, props.token);
      } else {
        await sendStorageJson("console/storage/pools", "POST", poolForm, props.token);
      }
      setNotice(copy.saved);
      await loadStorage();
      if (!editingPool) setSelection(null);
    } catch (event) {
      setError(event instanceof Error ? event.message : copy.noData);
    } finally {
      setBusy(null);
    }
  }

  async function savePolicy() {
    if (!selectedPolicy) return;
    const path = policyPath(selectedPolicy);
    if (!path) return;
    setBusy(`policy-${policyKey(selectedPolicy)}`);
    try {
      await sendStorageJson(path, "PUT", { storagePoolId: policyPoolId || null }, props.token);
      setNotice(copy.saved);
      await loadStorage();
    } catch (event) {
      setError(event instanceof Error ? event.message : copy.noData);
    } finally {
      setBusy(null);
    }
  }

  return <div className="storage-management">
    {error ? <p className="feedback error">{error}</p> : null}
    {notice ? <p className="feedback notice">{notice}</p> : null}
    {!data ? <p className="feedback muted">{busy === "storage-load" ? copy.loading : copy.noData}</p> : null}
    {data ? <>
      <div className="storage-summary-grid">
        <div className="metric"><span>{copy.health}</span><strong>{data.health.status}</strong></div>
        <div className="metric"><span>{copy.placements}</span><strong>{data.health.placements.active}</strong><span>{data.health.placements.missingActive} {copy.missingActive.toLowerCase()}</span></div>
        <div className="metric"><span>{copy.failed}</span><strong>{data.health.placements.failed + data.health.migrations.failed}</strong><span>{data.health.migrations.expiredLeases} {copy.expiredLeases.toLowerCase()}</span></div>
        <div className="metric"><span>{copy.blocked}</span><strong>{countBlockedCleanup(data.cleanup.summary)}</strong></div>
      </div>
      <article className="card management-list-card">
        <div className="section-title-row">
          <div><h3>{copy.title}</h3><p className="muted">{copy.summary}</p></div>
          <div className="management-actions">
            <button className="secondary-button" type="button" disabled={busy === "storage-load"} onClick={() => void loadStorage()}>{copy.refresh}</button>
            {globalAdmin ? <button className="primary-button" type="button" onClick={openNewPool}>{copy.newPool}</button> : tenantPolicy ? <button className="primary-button" type="button" onClick={() => openPolicy(tenantPolicy)}>{copy.editTenantPolicy}</button> : null}
          </div>
        </div>
        <div className="management-list full-width">
          {data.pools.map((pool) => {
            const poolHealth = data.health.pools.find((entry) => entry.pool.id === pool.id);
            return <button key={pool.id} type="button" className={selectedPool?.id === pool.id ? "management-list-item is-active" : "management-list-item"} onClick={() => openPool(pool)}>
              <span className="management-main"><strong>{pool.name}</strong><span>{copy.pool} · {pool.poolKind}</span></span>
              <span className="management-meta"><span>{poolHealth?.activePlacements ?? 0} {copy.active}</span><span className={statusClass(poolHealth?.health ?? pool.status)}>{poolHealth?.health ?? pool.status}</span><span className={statusClass(pool.status)}>{pool.status}</span></span>
              <span className="management-actions">{copy.open}</span>
            </button>;
          })}
          {data.policies.policies.map((policy) => <button key={policyKey(policy)} type="button" className={selectedPolicy && policyKey(selectedPolicy) === policyKey(policy) ? "management-list-item is-active" : "management-list-item"} onClick={() => openPolicy(policy)}>
            <span className="management-main"><strong>{policy.scope.name}</strong><span>{copy.policy} · {policy.scope.kind}</span></span>
            <span className="management-meta"><span>{policy.effectivePool.name}</span><span className="pill">{policy.inheritedFrom ? `${copy.inheritedFrom} ${policy.inheritedFrom}` : copy.configuredPool}</span></span>
            <span className="management-actions">{copy.open}</span>
          </button>)}
          {data.migrations.items.map((job) => <button key={job.id} type="button" className={selectedMigration?.id === job.id ? "management-list-item is-active" : "management-list-item"} onClick={() => setSelection({ kind: "migration", id: job.id })}>
            <span className="management-main"><strong>{job.targetPool.name}</strong><span>{copy.migration} · {job.blobKind}</span></span>
            <span className="management-meta"><span>{job.attempts} {copy.attempts.toLowerCase()}</span><span className={statusClass(job.status)}>{job.status}</span>{job.lastErrorSummary ? <span className="pill danger">{copy.failed}</span> : null}</span>
            <span className="management-actions">{copy.open}</span>
          </button>)}
          {data.cleanup.items.map((item, index) => <button key={`${item.tenantId}-${item.domainId}-${item.pool.id}-${index}`} type="button" className={selection?.kind === "cleanup" && selection.index === index ? "management-list-item is-active" : "management-list-item"} onClick={() => setSelection({ kind: "cleanup", index })}>
            <span className="management-main"><strong>{item.pool.name}</strong><span>{copy.cleanup} · {item.blobKind}</span></span>
            <span className="management-meta"><span>{item.cleanupAttempts} {copy.attempts.toLowerCase()}</span><span className={statusClass(item.status)}>{item.status}</span>{item.blockers.length ? <span className="pill warn">{item.blockers.length} {copy.blocked.toLowerCase()}</span> : null}</span>
            <span className="management-actions">{copy.open}</span>
          </button>)}
        </div>
        {!data.migrations.items.length ? <p className="feedback muted">{copy.noJobs}</p> : null}
        {!data.cleanup.items.length ? <p className="feedback muted">{copy.noCleanup}</p> : null}
      </article>
    </> : null}
    <Drawer open={selection !== null} onClose={() => setSelection(null)} title={copy.title} className="storage-drawer">
      <div className="form-stack">
        <div className="side-panel-header">
          <div><h3>{selection?.kind === "new-pool" ? copy.newPool : selectedPool?.name ?? selectedPolicy?.scope.name ?? selectedMigration?.targetPool.name ?? selectedCleanup?.pool.name ?? copy.title}</h3><p className="muted">{selection?.kind ?? ""}</p></div>
          <button className="icon-button" type="button" aria-label={copy.close} onClick={() => setSelection(null)}>x</button>
        </div>
        {selection?.kind === "new-pool" || selectedPool ? <form className="form-stack" onSubmit={(event) => { event.preventDefault(); void savePool(); }}>
          <label className="field"><span>{copy.name}</span><Input value={poolForm.name} disabled={!globalAdmin} onChange={(event) => setPoolForm((current) => ({ ...current, name: event.target.value }))} /></label>
          <label className="field"><span>{copy.kind}</span><select value={poolForm.poolKind} disabled><option value="postgres">{copy.poolKindPostgres}</option></select></label>
          <label className="field"><span>{copy.status}</span><select value={poolForm.status} disabled={!globalAdmin} onChange={(event) => setPoolForm((current) => ({ ...current, status: event.target.value }))}><option value="active">active</option><option value="disabled">disabled</option></select></label>
          {selectedPool ? <div className="list"><DetailRow label={copy.health} value={<span className={statusClass(data?.health.pools.find((entry) => entry.pool.id === selectedPool.id)?.health ?? selectedPool.status)}>{data?.health.pools.find((entry) => entry.pool.id === selectedPool.id)?.health ?? selectedPool.status}</span>} /><DetailRow label={copy.placements} value={data?.health.pools.find((entry) => entry.pool.id === selectedPool.id)?.activePlacements ?? 0} /><DetailRow label={copy.retiring} value={data?.health.pools.find((entry) => entry.pool.id === selectedPool.id)?.retiringPlacements ?? 0} /><DetailRow label={copy.failed} value={(data?.health.pools.find((entry) => entry.pool.id === selectedPool.id)?.failedPlacements ?? 0) + (data?.health.pools.find((entry) => entry.pool.id === selectedPool.id)?.cleanupFailedPlacements ?? 0)} /></div> : null}
          {globalAdmin ? <button className="primary-button" type="submit" disabled={busy === "pool-create" || (selectedPool ? busy === `pool-${selectedPool.id}` : false)}>{selectedPool ? copy.save : copy.create}</button> : null}
        </form> : null}
        {selectedPolicy ? <form className="form-stack" onSubmit={(event) => { event.preventDefault(); void savePolicy(); }}>
          <div className="list"><DetailRow label={copy.effectivePool} value={selectedPolicy.effectivePool.name} /><DetailRow label={copy.configuredPool} value={selectedPolicy.configuredPool?.name ?? copy.clearInheritance} /><DetailRow label={copy.inheritedFrom} value={selectedPolicy.inheritedFrom ?? "-"} /></div>
          <label className="field"><span>{copy.allowedPools}</span><select value={policyPoolId} onChange={(event) => setPolicyPoolId(event.target.value)}>{selectedPolicy.scope.kind !== "platform" ? <option value="">{copy.clearInheritance}</option> : null}{data?.policies.allowedPools.map((pool) => <option key={pool.id} value={pool.id}>{pool.name}</option>)}</select></label>
          <button className="primary-button" type="submit" disabled={busy === `policy-${policyKey(selectedPolicy)}`}>{copy.save}</button>
        </form> : null}
        {selectedMigration ? <div className="list"><DetailRow label={copy.status} value={<span className={statusClass(selectedMigration.status)}>{selectedMigration.status}</span>} /><DetailRow label={copy.sourcePool} value={selectedMigration.sourcePool.name} /><DetailRow label={copy.targetPool} value={selectedMigration.targetPool.name} /><DetailRow label={copy.attempts} value={selectedMigration.attempts} /><DetailRow label={copy.nextAttempt} value={selectedMigration.nextAttemptAt} /><DetailRow label={copy.lastError} value={selectedMigration.lastErrorSummary ?? "-"} /></div> : null}
        {selectedCleanup ? <div className="list"><DetailRow label={copy.status} value={<span className={statusClass(selectedCleanup.status)}>{selectedCleanup.status}</span>} /><DetailRow label={copy.pool} value={selectedCleanup.pool.name} /><DetailRow label={copy.attempts} value={selectedCleanup.cleanupAttempts} /><DetailRow label={copy.lastError} value={selectedCleanup.cleanupErrorSummary ?? "-"} /><DetailRow label={copy.blockers} value={selectedCleanup.blockers.length ? selectedCleanup.blockers.join(", ") : "-"} /></div> : null}
      </div>
    </Drawer>
  </div>;
}
