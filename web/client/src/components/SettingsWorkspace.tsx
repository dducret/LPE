import React from "react";
import type { ClientCopy } from "../i18n";
import type {
  CollaborationOverview,
  MailboxDelegationOverview,
  SieveOverview
} from "../client-types";

type Props = {
  copy: ClientCopy;
  collaboration: CollaborationOverview | null;
  mailboxDelegation: MailboxDelegationOverview | null;
  sieve: SieveOverview | null;
  shareForm: { kind: "contacts" | "calendar"; granteeEmail: string; mayRead: boolean; mayWrite: boolean; mayDelete: boolean; mayShare: boolean };
  setShareForm: React.Dispatch<React.SetStateAction<Props["shareForm"]>>;
  mailboxForm: { granteeEmail: string; senderRight: "send_as" | "send_on_behalf" };
  setMailboxForm: React.Dispatch<React.SetStateAction<Props["mailboxForm"]>>;
  sieveForm: { name: string; content: string; activate: boolean };
  setSieveForm: React.Dispatch<React.SetStateAction<Props["sieveForm"]>>;
  onSaveShare: () => void;
  onDeleteShare: (kind: string, granteeAccountId: string) => void;
  onSaveMailboxDelegation: () => void;
  onDeleteMailboxDelegation: (granteeAccountId: string) => void;
  onSaveSenderDelegation: () => void;
  onDeleteSenderDelegation: (senderRight: string, granteeAccountId: string) => void;
  onSaveSieve: () => void;
  onLoadSieve: (name: string) => void;
  onDeleteSieve: (name: string) => void;
  onSetActiveSieve: (name: string | null) => void;
};

function rightsLabel(rights: { mayRead: boolean; mayWrite: boolean; mayDelete: boolean; mayShare: boolean }) {
  const labels = [];
  if (rights.mayRead) labels.push("read");
  if (rights.mayWrite) labels.push("write");
  if (rights.mayDelete) labels.push("delete");
  if (rights.mayShare) labels.push("share");
  return labels.join(", ");
}

export function SettingsWorkspace(props: Props) {
  return (
    <section className="settings-shell">
      <article className="settings-card">
        <div className="detail-header">
          <div>
            <p className="detail-label">Delegation</p>
            <h3>Mailbox and collection access</h3>
          </div>
          <div className="detail-actions">
            <button className="primary-button" type="button" onClick={props.onSaveShare}>Save share</button>
            <button className="secondary-button" type="button" onClick={props.onSaveMailboxDelegation}>Share inbox</button>
            <button className="secondary-button" type="button" onClick={props.onSaveSenderDelegation}>Grant sender right</button>
          </div>
        </div>

        <div className="form-grid">
          <label className="field">
            <span>Collection</span>
            <select value={props.shareForm.kind} onChange={(event) => props.setShareForm((value) => ({ ...value, kind: event.target.value as "contacts" | "calendar" }))}>
              <option value="contacts">Contacts</option>
              <option value="calendar">Calendar</option>
            </select>
          </label>
          <label className="field">
            <span>Grantee email</span>
            <input value={props.shareForm.granteeEmail} onChange={(event) => props.setShareForm((value) => ({ ...value, granteeEmail: event.target.value }))} />
          </label>
          <label className="toggle-field"><span>Read</span><input type="checkbox" checked={props.shareForm.mayRead} onChange={(event) => props.setShareForm((value) => ({ ...value, mayRead: event.target.checked }))} /></label>
          <label className="toggle-field"><span>Write</span><input type="checkbox" checked={props.shareForm.mayWrite} onChange={(event) => props.setShareForm((value) => ({ ...value, mayWrite: event.target.checked, mayRead: event.target.checked || value.mayRead }))} /></label>
          <label className="toggle-field"><span>Delete</span><input type="checkbox" checked={props.shareForm.mayDelete} onChange={(event) => props.setShareForm((value) => ({ ...value, mayDelete: event.target.checked, mayWrite: event.target.checked || value.mayWrite, mayRead: event.target.checked || value.mayRead }))} /></label>
          <label className="toggle-field"><span>Share</span><input type="checkbox" checked={props.shareForm.mayShare} onChange={(event) => props.setShareForm((value) => ({ ...value, mayShare: event.target.checked, mayWrite: event.target.checked || value.mayWrite, mayRead: event.target.checked || value.mayRead }))} /></label>
        </div>

        <div className="settings-grid">
          <div className="settings-list">
            <h4>Outgoing shares</h4>
            {[...(props.collaboration?.outgoingContacts ?? []), ...(props.collaboration?.outgoingCalendars ?? [])].map((grant) => (
              <div className="settings-item" key={grant.id}>
                <div>
                  <strong>{grant.granteeEmail}</strong>
                  <p>{grant.kind} · {rightsLabel(grant.rights)}</p>
                </div>
                <button className="ghost-button" type="button" onClick={() => props.onDeleteShare(grant.kind, grant.granteeAccountId)}>Remove</button>
              </div>
            ))}
          </div>
          <div className="settings-list">
            <h4>Mailbox delegation</h4>
            <div className="form-grid">
              <label className="field">
                <span>Mailbox grantee</span>
                <input value={props.mailboxForm.granteeEmail} onChange={(event) => props.setMailboxForm((value) => ({ ...value, granteeEmail: event.target.value }))} />
              </label>
              <label className="field">
                <span>Sender right</span>
                <select value={props.mailboxForm.senderRight} onChange={(event) => props.setMailboxForm((value) => ({ ...value, senderRight: event.target.value as "send_as" | "send_on_behalf" }))}>
                  <option value="send_as">Send As</option>
                  <option value="send_on_behalf">Send on Behalf</option>
                </select>
              </label>
            </div>
            {(props.mailboxDelegation?.outgoingMailboxes ?? []).map((grant) => (
              <div className="settings-item" key={grant.id}>
                <div>
                  <strong>{grant.granteeEmail}</strong>
                  <p>Inbox access</p>
                </div>
                <button className="ghost-button" type="button" onClick={() => props.onDeleteMailboxDelegation(grant.granteeAccountId)}>Remove</button>
              </div>
            ))}
            {(props.mailboxDelegation?.outgoingSenderRights ?? []).map((grant) => (
              <div className="settings-item" key={grant.id}>
                <div>
                  <strong>{grant.granteeEmail}</strong>
                  <p>{grant.senderRight}</p>
                </div>
                <button className="ghost-button" type="button" onClick={() => props.onDeleteSenderDelegation(grant.senderRight, grant.granteeAccountId)}>Remove</button>
              </div>
            ))}
          </div>
        </div>
      </article>

      <article className="settings-card">
        <div className="detail-header">
          <div>
            <p className="detail-label">Sieve</p>
            <h3>Vacation and filtering</h3>
          </div>
          <div className="detail-actions">
            <button className="primary-button" type="button" onClick={props.onSaveSieve}>Save script</button>
            <button className="ghost-button" type="button" onClick={() => props.onSetActiveSieve(null)}>Disable active</button>
          </div>
        </div>

        <div className="form-grid">
          <label className="field">
            <span>Script name</span>
            <input value={props.sieveForm.name} onChange={(event) => props.setSieveForm((value) => ({ ...value, name: event.target.value }))} />
          </label>
          <label className="toggle-field">
            <span>Activate after save</span>
            <input type="checkbox" checked={props.sieveForm.activate} onChange={(event) => props.setSieveForm((value) => ({ ...value, activate: event.target.checked }))} />
          </label>
          <label className="field field-wide">
            <span>Sieve script</span>
            <textarea rows={16} value={props.sieveForm.content} onChange={(event) => props.setSieveForm((value) => ({ ...value, content: event.target.value }))} />
          </label>
        </div>

        <div className="settings-list">
          <h4>Stored scripts</h4>
          {(props.sieve?.scripts ?? []).map((script) => (
            <div className="settings-item" key={script.name}>
              <div>
                <strong>{script.name}</strong>
                <p>{script.isActive ? "active" : "inactive"} · {script.sizeOctets} bytes</p>
              </div>
              <div className="detail-actions">
                <button className="ghost-button" type="button" onClick={() => props.onLoadSieve(script.name)}>Load</button>
                <button className="ghost-button" type="button" onClick={() => props.onSetActiveSieve(script.name)}>Activate</button>
                <button className="danger-button" type="button" onClick={() => props.onDeleteSieve(script.name)}>Delete</button>
              </div>
            </div>
          ))}
        </div>
      </article>
    </section>
  );
}
