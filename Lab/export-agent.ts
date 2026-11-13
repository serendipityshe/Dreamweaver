import type { HarnessClient, ActionView, HistoryRecord, RunView } from "../harness/src/index.js";

/** Public authorized facts only; observations remain distinct from adopted results. */
export async function exportAgentRun(client: HarnessClient, runID: string, taskSetVersion: string) {
  const run: RunView = await client.getRun({ runID });
  const actions: ActionView[] = [], history: HistoryRecord[] = [];
  let cursor: string | undefined;
  do { const p = await client.listActions({ runID, ...(cursor ? { cursor } : {}) }); actions.push(...p.items); cursor = p.nextCursor ?? undefined; } while (cursor);
  do { const p = await client.getHistory({ subject: { kind: "run", runID }, ...(cursor ? { cursor } : {}) }); history.push(...p.items); cursor = p.nextCursor ?? undefined; } while (cursor);
  const actionHistory: Record<string, HistoryRecord[]> = {};
  for (const action of actions) {
    const rows: HistoryRecord[] = [];
    do { const p = await client.getHistory({ subject: { kind: "action", actionID: action.id }, ...(cursor ? { cursor } : {}) }); rows.push(...p.items); cursor = p.nextCursor ?? undefined; } while (cursor);
    actionHistory[action.id] = rows;
  }
  return { taskSetVersion, run, actions, history, actionHistory, actualCharges: null, note: "Usage may be missing for failed, unknown or late requests; observed usage is not a billing total." };
}
