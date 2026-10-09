import { useCallback, useEffect, useRef, useState } from "react";

import {
  HOST_PROTOCOL_VERSION,
  HostContractValidationError,
  type StreamSkopeHost,
} from "../contracts";
import {
  parseKafkaTopicAnnotation,
  type KafkaTopicAnnotation,
  type KafkaTopicAnnotationSnapshot,
  type KafkaTopicCatalogSnapshot,
} from "../contracts/topic-catalog";
import { sameKafkaTopicIdentity } from "../contracts/topic-identity";
import { parseKafkaRunbookUrl } from "../contracts/operational-preference-validation";

type Authority = {
  readonly host: StreamSkopeHost;
  readonly key: string;
  readonly connected: boolean;
};
type Selection = {
  readonly draft: KafkaTopicAnnotation;
  readonly expected: KafkaTopicAnnotation | null;
  readonly authority: Authority | null;
};
interface LocalTopicNotesController {
  readonly catalog: KafkaTopicCatalogSnapshot | undefined;
  readonly selection: Selection | undefined;
  readonly busy: boolean;
  readonly error: string | undefined;
  readonly status: string;
  readonly verified: boolean;
  readonly dirty: boolean;
  refresh(this: void): Promise<boolean>;
  load(this: void, topic: string): Promise<boolean>;
  save(this: void): Promise<boolean>;
  remove(this: void): Promise<boolean>;
  openLink(this: void, url: string): Promise<boolean>;
  select(this: void, annotation: KafkaTopicAnnotation): void;
  update(this: void, patch: Partial<Omit<KafkaTopicAnnotation, "identity">>): void;
}

/** Local drafts retain their identity; only a fresh metadata read grants edit authority. */
export function useLocalTopicNotes({
  host,
  connected,
  authorityKey,
  initialTopic,
}: {
  readonly host: StreamSkopeHost;
  readonly connected: boolean;
  readonly authorityKey: string;
  readonly initialTopic: string | null;
}): LocalTopicNotesController {
  const authority = useRef<Authority>({ host, key: authorityKey, connected });
  if (
    authority.current.host !== host ||
    authority.current.key !== authorityKey ||
    authority.current.connected !== connected
  )
    authority.current = { host, key: authorityKey, connected };
  const [, setRevision] = useState(0);
  const [catalog, setCatalog] = useState<KafkaTopicCatalogSnapshot>();
  const [selection, setSelection] = useState<Selection>();
  const currentSelection = useRef(selection);
  currentSelection.current = selection;
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const [status, setStatus] = useState("");
  const owner = useRef({});
  const pending = useRef<object | null>(null);
  const choose = useCallback((next: Selection | undefined): void => {
    currentSelection.current = next;
    setSelection(next);
  }, []);
  const operate = useCallback(
    async (work: (current: () => boolean) => Promise<boolean>): Promise<boolean> => {
      if (pending.current !== null) return false;
      const token = {};
      const capturedOwner = owner.current;
      pending.current = token;
      const current = (): boolean => owner.current === capturedOwner && pending.current === token;
      setBusy(true);
      setError(undefined);
      setStatus("");
      try {
        return await work(current);
      } catch (failure) {
        if (current())
          setError(
            failure instanceof HostContractValidationError
              ? failure.message
              : "The host did not confirm the notes operation. Your draft is retained; refresh saved notes before retrying.",
          );
        return false;
      } finally {
        if (current()) {
          pending.current = null;
          setBusy(false);
        }
      }
    },
    [],
  );
  const refresh = useCallback(
    (): Promise<boolean> =>
      operate(async (current) => {
        const response = await host.execute({
          command: "catalog.list",
          payload: {},
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
        });
        if (!current()) return false;
        if (!response.ok) {
          setError(`${response.error.summary} ${response.error.recovery}`);
          return false;
        }
        setCatalog(response.result.snapshot);
        return true;
      }),
    [host, operate],
  );
  const load = useCallback(
    (topic: string): Promise<boolean> =>
      operate(async (current) => {
        const captured = authority.current;
        if (!captured.connected) {
          setError(
            "Connect to Kafka before verifying the current topic. Saved notes remain available offline.",
          );
          return false;
        }
        const response = await host.execute({
          command: "catalog.load",
          payload: { topic },
          id: crypto.randomUUID(),
          version: HOST_PROTOCOL_VERSION,
        });
        if (!current()) return false;
        if (captured !== authority.current) {
          setError(
            "The connection changed while verifying the topic. Your draft is retained; verify the current topic again.",
          );
          return false;
        }
        if (!response.ok) {
          setError(`${response.error.summary} ${response.error.recovery}`);
          return false;
        }
        const snapshot = response.result.snapshot;
        choose({
          draft: snapshot.annotation ?? {
            identity: snapshot.identity,
            description: "",
            owner: "",
            labels: [],
            links: [],
          },
          expected: snapshot.annotation,
          authority: captured,
        });
        setStatus(
          snapshot.annotation === null
            ? "No notes are saved for this topic identity."
            : "Current topic identity verified.",
        );
        return true;
      }),
    [choose, host, operate],
  );

  useEffect(() => {
    const currentOwner = {};
    owner.current = currentOwner;
    pending.current = null;
    setCatalog(undefined);
    choose(undefined);
    const unsubscribe = host.subscribe((event) => {
      if (event.event !== "connection.state" && event.event !== "backend.availability") return;
      authority.current = { ...authority.current };
      setRevision((revision) => revision + 1);
    });
    void refresh().then((loaded) => {
      if (loaded && owner.current === currentOwner && initialTopic !== null)
        void load(initialTopic);
    });
    return (): void => {
      owner.current = {};
      pending.current = null;
      unsubscribe();
    };
  }, [choose, host, initialTopic, load, refresh]);

  const acceptSnapshot = (snapshot: KafkaTopicAnnotationSnapshot): void => {
    setCatalog((previous) => ({
      durability: snapshot.durability,
      topics: [
        ...(previous?.topics ?? []).filter(
          (entry) => !sameKafkaTopicIdentity(entry.identity, snapshot.identity),
        ),
        ...(snapshot.annotation === null ? [] : [snapshot.annotation]),
      ],
    }));
  };
  const save = (): Promise<boolean> =>
    operate(async (current) => {
      const selected = currentSelection.current;
      if (
        selected === undefined ||
        selected.authority !== authority.current ||
        !authority.current.connected
      ) {
        setError(
          "Verify the current topic before saving. The draft still belongs to its original topic identity.",
        );
        return false;
      }
      const annotation = parseKafkaTopicAnnotation({
        ...selected.draft,
        labels: selected.draft.labels.map((label) => label.trim()).filter(Boolean),
      });
      const response = await host.execute({
        command: "catalog.put",
        payload: { annotation, expected: selected.expected },
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
      });
      if (!current()) return false;
      if (!response.ok) {
        setError(`${response.error.summary} ${response.error.recovery}`);
        return false;
      }
      const snapshot = response.result.snapshot;
      if (
        snapshot.annotation === null ||
        !sameKafkaTopicIdentity(snapshot.identity, annotation.identity)
      )
        throw new Error("The host did not confirm notes for the original topic identity.");
      acceptSnapshot(snapshot);
      choose({ ...selected, draft: snapshot.annotation, expected: snapshot.annotation });
      setStatus(
        selected.authority === authority.current
          ? "Topic notes saved."
          : "Topic notes saved for the original topic identity. Verify the current topic before further edits.",
      );
      return true;
    });
  const remove = (): Promise<boolean> =>
    operate(async (current) => {
      const selected = currentSelection.current;
      if (selected?.expected === undefined || selected.expected === null) return false;
      const response = await host.execute({
        command: "catalog.delete",
        payload: { identity: selected.expected.identity, expected: selected.expected },
        id: crypto.randomUUID(),
        version: HOST_PROTOCOL_VERSION,
      });
      if (!current()) return false;
      if (!response.ok) {
        setError(`${response.error.summary} ${response.error.recovery}`);
        return false;
      }
      const snapshot = response.result.snapshot;
      if (
        snapshot.annotation !== null ||
        !sameKafkaTopicIdentity(snapshot.identity, selected.expected.identity)
      )
        throw new Error("The host did not confirm removal for the original topic identity.");
      acceptSnapshot(snapshot);
      choose(undefined);
      setStatus("Local topic notes removed. Kafka was not changed.");
      return true;
    });
  const openLink = (url: string): Promise<boolean> =>
    operate(async (current) => {
      const validated = parseKafkaRunbookUrl(url, "Runbook URL");
      try {
        await host.openExternalUrl(validated);
      } catch {
        if (current())
          setError(
            "The platform did not accept the runbook request. Check browser popup or operating-system policy, then retry.",
          );
        return false;
      }
      if (!current()) return false;
      setStatus("Runbook request accepted by the platform.");
      return true;
    });
  const select = (annotation: KafkaTopicAnnotation): void => {
    if (pending.current !== null) return;
    choose({ draft: annotation, expected: annotation, authority: null });
    setError(undefined);
    setStatus("");
  };
  const update = (patch: Partial<Omit<KafkaTopicAnnotation, "identity">>): void => {
    const selected = currentSelection.current;
    if (
      pending.current !== null ||
      selected === undefined ||
      selected.authority !== authority.current
    )
      return;
    choose({
      ...selected,
      draft: { ...selected.draft, ...patch, identity: selected.draft.identity },
    });
    setStatus("");
  };
  return {
    catalog,
    busy,
    error,
    status,
    selection,
    verified: connected && selection?.authority === authority.current,
    dirty:
      selection !== undefined &&
      JSON.stringify(selection.draft) !==
        JSON.stringify(
          selection.expected ?? {
            identity: selection.draft.identity,
            description: "",
            owner: "",
            labels: [],
            links: [],
          },
        ),
    refresh,
    load,
    save,
    remove,
    openLink,
    select,
    update,
  };
}
