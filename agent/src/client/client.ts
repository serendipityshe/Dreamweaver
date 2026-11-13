import { HarnessError, type CallOptions, type HarnessClient, type RunView } from "../../../harness/src/index.js";
import type { AgentClient, RunHandle, WaitOptions, WaitResult } from "../contract/types.js";
import { AgentClientError } from "../contract/errors.js";
import { copy, fail, fields, string } from "../contract/common.js";
import { validateWaitOptions } from "./wait-options.js";

const aborted = () => new HarnessError({ code: "CALL_ABORTED", message: "Waiting was aborted", disposition: "rejected", details: null, retryAfterMs: null });
export function createAgentClient(client: HarnessClient): AgentClient {
  function run(id: string, requestKey: string | null = null): RunHandle {
    string(id);
    const wrap = async <T>(operation: "get" | "cancel", phase: "query" | "cancel", fn: () => Promise<T>): Promise<T> => { try { return await fn(); } catch (e) { throw new AgentClientError(operation, phase, requestKey, id, e); } };
    return Object.freeze({
      id, requestKey,
      get: (options?: CallOptions) => wrap("get", "query", () => client.getRun({ runID: id }, options)),
      cancel: (request: { requestKey: string; reason?: string }, options?: CallOptions) => wrap("cancel", "cancel", () => {
        fields(request, ["requestKey", "reason"], ["requestKey"]); string(request.requestKey);
        if (request.requestKey === requestKey) fail("Cancellation requires a distinct requestKey");
        return client.requestCancel({ requestKey: request.requestKey, runID: id, reason: request.reason ?? null }, options);
      }),
      async wait(options: WaitOptions = {}): Promise<WaitResult> {
        let timer: ReturnType<typeof setTimeout> | undefined, lastObserved: RunView | null = null;
        let waitingDeadline: number | null = null;
        const controller = new AbortController(), abort = () => controller.abort();
        let expired = false;
        const timeoutResult = (): WaitResult => ({ kind: "timeout", runID: id, lastObserved });
        let signal: AbortSignal | undefined;
        try {
          const normalized = validateWaitOptions(options);
          signal = normalized.signal;
          const timeout = normalized.timeoutMs!, initial = normalized.poll!.initialMs!, max = normalized.poll!.maxMs!;
          if (signal?.aborted) throw aborted(); signal?.addEventListener("abort", abort, { once: true });
          const deadline = performance.now() + timeout;
          if (timeout > 0) waitingDeadline = deadline;
          // Chunk very long timers instead of Node's >2^31 truncation to 1ms.
          const arm = () => { const remaining = deadline - performance.now(); if (remaining <= 0) { expired = true; controller.abort(); } else timer = setTimeout(arm, Math.min(remaining, 2147483647)); };
          if (timeout > 0) arm();
          const budget = (): CallOptions => {
            if (signal?.aborted) throw aborted();
            if (timeout > 0 && performance.now() >= deadline) { expired = true; controller.abort(); throw aborted(); }
            return { signal: controller.signal, timeoutMs: timeout === 0 ? 5000 : Math.max(1, Math.ceil(deadline - performance.now())) };
          };
          let interval = initial, observation = "";
          const pause = () => new Promise<void>((resolve, reject) => {
            const finish = () => { controller.signal.removeEventListener("abort", stop); resolve(); };
            const sleep = setTimeout(finish, Math.min(interval, Math.max(1, deadline - performance.now()), 2147483647));
            const stop = () => { clearTimeout(sleep); controller.signal.removeEventListener("abort", stop); reject(aborted()); };
            controller.signal.addEventListener("abort", stop, { once: true }); if (controller.signal.aborted) stop();
          });
          const backoff = async () => { await pause(); interval = Math.min(max, Math.ceil(interval * 1.5)); };
          const reread = async (error: unknown) => {
            const uncertain = error instanceof HarnessError && error.info.disposition === "uncertain";
            if (!(timeout > 0 && uncertain && !signal?.aborted && performance.now() < deadline)) {
              if (timeout > 0 && uncertain) { if (signal?.aborted) throw aborted(); expired = true; controller.abort(); throw aborted(); }
              throw error;
            }
            await backoff();
          };
          for (;;) {
            try { lastObserved = await client.getRun({ runID: id }, budget()); }
            catch (error) { await reread(error); continue; }
            if (lastObserved.outcome) return { kind: "completed", run: lastObserved, outcome: lastObserved.outcome };
            if (lastObserved.blocked) return { kind: "blocked", run: lastObserved, issues: [lastObserved.blocked], actionIDs: [] };
            if (timeout === 0) return timeoutResult();
            const identity = `${lastObserved.revision}:${lastObserved.wait?.id ?? ""}`;
            if (identity !== observation) { observation = identity; interval = initial; }
            if (lastObserved.wait && lastObserved.wait.winner === null) {
              const current = lastObserved, keys = new Set(current.wait!.actionKeys), blocked = [];
              let cursor: string | undefined;
              try {
                do {
                  const page = await client.listActions({ runID: id, ...(cursor ? { cursor } : {}) }, budget());
                  for (const a of page.items) if (keys.has(a.request.key) && !a.outcome && a.blocked) blocked.push(a);
                  cursor = page.nextCursor ?? undefined;
                } while (cursor);
              } catch (error) { await reread(error); continue; }
              if (blocked.length) {
                let review: RunView;
                try { review = await client.getRun({ runID: id }, budget()); }
                catch (error) { await reread(error); continue; }
                lastObserved = review;
                if (lastObserved.outcome) return { kind: "completed", run: lastObserved, outcome: lastObserved.outcome };
                if (lastObserved.blocked) return { kind: "blocked", run: lastObserved, issues: [lastObserved.blocked], actionIDs: [] };
                if (lastObserved.revision !== current.revision || lastObserved.wait?.id !== current.wait!.id) continue;
                return { kind: "blocked", run: lastObserved, issues: blocked.map(a => a.blocked!), actionIDs: blocked.map(a => a.id) };
              }
            }
            budget();
            await backoff();
          }
        } catch (e) {
          if ((expired || waitingDeadline !== null && performance.now() >= waitingDeadline) && !signal?.aborted) return timeoutResult();
          throw new AgentClientError("wait", "wait", requestKey, id, signal?.aborted ? aborted() : e);
        } finally { if (timer) clearTimeout(timer); signal?.removeEventListener("abort", abort); }
      },
    });
  }
  return Object.freeze({ run, agent(selector: import("../../../harness/src/index.js").VersionSelector) {
    fields(selector, ["id", "version"], ["id"]); string(selector.id); if (selector.version !== undefined) string(selector.version);
    const agent = copy(selector);
    const handle = {
      async start(request: Parameters<import("../contract/types.js").AgentHandle["start"]>[0], options?: CallOptions) {
        let requestKey: string | null = null;
        try {
          const submitted = copy(request);
          fields(submitted, ["requestKey", "input", "limits"], ["requestKey", "input"]); string(submitted.requestKey);
          requestKey = submitted.requestKey;
          const receipt = await client.submit({ ...submitted, agent, config: null }, options); return run(receipt.runID, requestKey);
        } catch (e) { throw new AgentClientError("start", "submit", requestKey, null, e); }
      },
      async invoke(request: Parameters<import("../contract/types.js").AgentHandle["start"]>[0], options?: { submit?: CallOptions; wait?: WaitOptions }) {
        try { return await (await handle.start(request, options?.submit)).wait(options?.wait); }
        catch (e) { if (e instanceof AgentClientError) throw new AgentClientError("invoke", e.phase, e.requestKey, e.runID, e.cause); throw e; }
      },
    };
    return Object.freeze(handle);
  } });
}
