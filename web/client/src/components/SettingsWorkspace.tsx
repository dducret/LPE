import React from "react";
import type { ClientCopy } from "../i18n";
import type {
  ClientTaskList,
  CollaborationOverview,
  MailboxDelegationOverview,
  SieveOverview,
} from "../client-types";
import { Button, Card, Input, Select, Textarea } from "../../../ui/src/components/primitives";

type Props = {
  copy: ClientCopy;
  collaboration: CollaborationOverview | null;
  taskLists: ClientTaskList[];
  mailboxDelegation: MailboxDelegationOverview | null;
  sieve: SieveOverview | null;
  shareForm: { kind: "contacts" | "calendar" | "tasks"; taskListId: string; granteeEmail: string; mayRead: boolean; mayWrite: boolean; mayDelete: boolean; mayShare: boolean };
  setShareForm: React.Dispatch<React.SetStateAction<Props["shareForm"]>>;
  mailboxForm: { granteeEmail: string; senderRight: "send_as" | "send_on_behalf" };
  setMailboxForm: React.Dispatch<React.SetStateAction<Props["mailboxForm"]>>;
  sieveForm: { name: string; content: string; activate: boolean };
  setSieveForm: React.Dispatch<React.SetStateAction<Props["sieveForm"]>>;
  onSaveShare: () => void;
  onDeleteShare: (kind: string, granteeAccountId: string, taskListId?: string) => void;
  onSaveMailboxDelegation: () => void;
  onDeleteMailboxDelegation: (granteeAccountId: string) => void;
  onSaveSenderDelegation: () => void;
  onDeleteSenderDelegation: (senderRight: string, granteeAccountId: string) => void;
  onSaveSieve: () => void;
  onLoadSieve: (name: string) => void;
  onDeleteSieve: (name: string) => void;
  onSetActiveSieve: (name: string | null) => void;
};

function rightsLabel(rights: { mayRead: boolean; mayWrite: boolean; mayDelete: boolean; mayShare: boolean }, copy: ClientCopy) {
  const labels = [];
  if (rights.mayRead) labels.push(copy.settings.rights.read.toLowerCase());
  if (rights.mayWrite) labels.push(copy.settings.rights.write.toLowerCase());
  if (rights.mayDelete) labels.push(copy.settings.rights.delete.toLowerCase());
  if (rights.mayShare) labels.push(copy.settings.rights.share.toLowerCase());
  return labels.join(", ");
}

function collectionLabel(kind: string, copy: ClientCopy) {
  if (kind === "contacts") return copy.settings.collectionKinds.contacts;
  if (kind === "calendar") return copy.settings.collectionKinds.calendar;
  return copy.settings.collectionKinds.tasks;
}

export function SettingsWorkspace(props: Props) {
  const outgoingShares = [
    ...(props.collaboration?.outgoingContacts ?? []).map((grant) => ({ type: "collection" as const, kind: grant.kind, grant })),
    ...(props.collaboration?.outgoingCalendars ?? []).map((grant) => ({ type: "collection" as const, kind: grant.kind, grant })),
    ...(props.collaboration?.outgoingTaskLists ?? []).map((grant) => ({ type: "task-list" as const, kind: "tasks", grant })),
  ];
  const incomingCollections = [
    ...(props.collaboration?.incomingContactCollections ?? []),
    ...(props.collaboration?.incomingCalendarCollections ?? []),
    ...(props.collaboration?.incomingTaskListCollections ?? []),
  ];

  return (
    <section className="settings-shell">
      <Card as="article" className="settings-card">
        <div className="detail-header">
          <div>
            <p className="detail-label">{props.copy.settings.delegationLabel}</p>
            <h3>{props.copy.settings.delegationTitle}</h3>
          </div>
          <div className="detail-actions">
            <Button variant="primary" type="button" onClick={props.onSaveShare}>{props.copy.settings.actions.saveShare}</Button>
            <Button variant="secondary" type="button" onClick={props.onSaveMailboxDelegation}>{props.copy.settings.actions.shareInbox}</Button>
            <Button variant="secondary" type="button" onClick={props.onSaveSenderDelegation}>{props.copy.settings.actions.grantSenderRight}</Button>
          </div>
        </div>

        <div className="form-grid">
          <label className="field">
            <span>{props.copy.settings.collectionField}</span>
            <Select value={props.shareForm.kind} onChange={(event) => props.setShareForm((value) => ({ ...value, kind: event.target.value as "contacts" | "calendar" | "tasks" }))}>
              <option value="contacts">{props.copy.settings.collectionKinds.contacts}</option>
              <option value="calendar">{props.copy.settings.collectionKinds.calendar}</option>
              <option value="tasks">{props.copy.settings.collectionKinds.tasks}</option>
            </Select>
          </label>
          {props.shareForm.kind === "tasks" ? (
            <label className="field">
              <span>{props.copy.settings.taskListField}</span>
              <Select value={props.shareForm.taskListId} onChange={(event) => props.setShareForm((value) => ({ ...value, taskListId: event.target.value }))}>
                {props.taskLists.map((taskList) => (
                  <option key={taskList.id} value={taskList.id}>{taskList.name}</option>
                ))}
              </Select>
            </label>
          ) : null}
          <label className="field">
            <span>{props.copy.settings.granteeEmailField}</span>
            <Input value={props.shareForm.granteeEmail} onChange={(event) => props.setShareForm((value) => ({ ...value, granteeEmail: event.target.value }))} />
          </label>
          <label className="toggle-field"><span>{props.copy.settings.rights.read}</span><input type="checkbox" checked={props.shareForm.mayRead} onChange={(event) => props.setShareForm((value) => ({ ...value, mayRead: event.target.checked }))} /></label>
          <label className="toggle-field"><span>{props.copy.settings.rights.write}</span><input type="checkbox" checked={props.shareForm.mayWrite} onChange={(event) => props.setShareForm((value) => ({ ...value, mayWrite: event.target.checked, mayRead: event.target.checked || value.mayRead }))} /></label>
          <label className="toggle-field"><span>{props.copy.settings.rights.delete}</span><input type="checkbox" checked={props.shareForm.mayDelete} onChange={(event) => props.setShareForm((value) => ({ ...value, mayDelete: event.target.checked, mayWrite: event.target.checked || value.mayWrite, mayRead: event.target.checked || value.mayRead }))} /></label>
          <label className="toggle-field"><span>{props.copy.settings.rights.share}</span><input type="checkbox" checked={props.shareForm.mayShare} onChange={(event) => props.setShareForm((value) => ({ ...value, mayShare: event.target.checked, mayWrite: event.target.checked || value.mayWrite, mayRead: event.target.checked || value.mayRead }))} /></label>
        </div>

        <div className="settings-grid">
          <div className="settings-list">
            <h4>{props.copy.settings.outgoingSharesTitle}</h4>
            {outgoingShares.map((entry) => (
              <div className="settings-item" key={entry.grant.id}>
                <div>
                  <strong>{entry.grant.granteeEmail}</strong>
                  <p>
                    {entry.type === "task-list"
                      ? props.copy.settings.taskListPrefix.replace("{name}", entry.grant.taskListName)
                      : collectionLabel(entry.kind, props.copy)}
                    {" · "}
                    {rightsLabel(entry.grant.rights, props.copy)}
                  </p>
                </div>
                <Button variant="ghost" size="sm" type="button" onClick={() => props.onDeleteShare(entry.kind, entry.grant.granteeAccountId, entry.type === "task-list" ? entry.grant.taskListId : undefined)}>{props.copy.settings.remove}</Button>
              </div>
            ))}
          </div>

          <div className="settings-list">
            <h4>{props.copy.settings.incomingSharesTitle}</h4>
            {incomingCollections.map((collection) => (
              <div className="settings-item" key={collection.id}>
                <div>
                  <strong>{collection.displayName}</strong>
                  <p>{collectionLabel(collection.kind, props.copy)} · {collection.ownerEmail} · {rightsLabel(collection.rights, props.copy)}</p>
                </div>
              </div>
            ))}
          </div>

          <div className="settings-list">
            <h4>{props.copy.settings.mailboxDelegationTitle}</h4>
            <div className="form-grid">
              <label className="field">
                <span>{props.copy.settings.mailboxGranteeField}</span>
                <Input value={props.mailboxForm.granteeEmail} onChange={(event) => props.setMailboxForm((value) => ({ ...value, granteeEmail: event.target.value }))} />
              </label>
              <label className="field">
                <span>{props.copy.settings.senderRightField}</span>
                <Select value={props.mailboxForm.senderRight} onChange={(event) => props.setMailboxForm((value) => ({ ...value, senderRight: event.target.value as "send_as" | "send_on_behalf" }))}>
                  <option value="send_as">{props.copy.settings.senderRights.sendAs}</option>
                  <option value="send_on_behalf">{props.copy.settings.senderRights.sendOnBehalf}</option>
                </Select>
              </label>
            </div>
            {(props.mailboxDelegation?.outgoingMailboxes ?? []).map((grant) => (
              <div className="settings-item" key={grant.id}>
                <div>
                  <strong>{grant.granteeEmail}</strong>
                  <p>{props.copy.settings.inboxAccess}</p>
                </div>
                <Button variant="ghost" size="sm" type="button" onClick={() => props.onDeleteMailboxDelegation(grant.granteeAccountId)}>{props.copy.settings.remove}</Button>
              </div>
            ))}
            {(props.mailboxDelegation?.outgoingSenderRights ?? []).map((grant) => (
              <div className="settings-item" key={grant.id}>
                <div>
                  <strong>{grant.granteeEmail}</strong>
                  <p>{grant.senderRight === "send_as" ? props.copy.settings.senderRights.sendAs : props.copy.settings.senderRights.sendOnBehalf}</p>
                </div>
                <Button variant="ghost" size="sm" type="button" onClick={() => props.onDeleteSenderDelegation(grant.senderRight, grant.granteeAccountId)}>{props.copy.settings.remove}</Button>
              </div>
            ))}
          </div>
        </div>
      </Card>

      <Card as="article" className="settings-card">
        <div className="detail-header">
          <div>
            <p className="detail-label">{props.copy.settings.sieveLabel}</p>
            <h3>{props.copy.settings.sieveTitle}</h3>
          </div>
          <div className="detail-actions">
            <Button variant="primary" type="button" onClick={props.onSaveSieve}>{props.copy.settings.sieveActions.saveScript}</Button>
            <Button variant="ghost" type="button" onClick={() => props.onSetActiveSieve(null)}>{props.copy.settings.sieveActions.disableActive}</Button>
          </div>
        </div>

        <div className="form-grid">
          <label className="field">
            <span>{props.copy.settings.scriptNameField}</span>
            <Input value={props.sieveForm.name} onChange={(event) => props.setSieveForm((value) => ({ ...value, name: event.target.value }))} />
          </label>
          <label className="toggle-field">
            <span>{props.copy.settings.activateAfterSaveField}</span>
            <input type="checkbox" checked={props.sieveForm.activate} onChange={(event) => props.setSieveForm((value) => ({ ...value, activate: event.target.checked }))} />
          </label>
          <label className="field field-wide">
            <span>{props.copy.settings.sieveScriptField}</span>
            <Textarea rows={16} value={props.sieveForm.content} onChange={(event) => props.setSieveForm((value) => ({ ...value, content: event.target.value }))} />
          </label>
        </div>

        <div className="settings-list">
          <h4>{props.copy.settings.storedScriptsTitle}</h4>
          {(props.sieve?.scripts ?? []).map((script) => (
            <div className="settings-item" key={script.name}>
              <div>
                <strong>{script.name}</strong>
                <p>{script.isActive ? props.copy.settings.scriptState.active : props.copy.settings.scriptState.inactive} · {script.sizeOctets} {props.copy.settings.bytesLabel}</p>
              </div>
              <div className="detail-actions">
                <Button variant="ghost" size="sm" type="button" onClick={() => props.onLoadSieve(script.name)}>{props.copy.settings.sieveActions.load}</Button>
                <Button variant="ghost" size="sm" type="button" onClick={() => props.onSetActiveSieve(script.name)}>{props.copy.settings.sieveActions.activate}</Button>
                <Button variant="danger" size="sm" type="button" onClick={() => props.onDeleteSieve(script.name)}>{props.copy.settings.sieveActions.delete}</Button>
              </div>
            </div>
          ))}
        </div>
      </Card>
    </section>
  );
}
