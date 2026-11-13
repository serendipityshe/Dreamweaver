import { HarnessError, type ErrorInfo, type Issue } from "../../../harness/src/index.js";
import { copy } from "./common.js";
export class AgentExecutionError extends Error {
  readonly issue: Issue;
  constructor(issue: Issue) { super(issue.message); this.name = "AgentExecutionError"; this.issue = copy(issue); }
}
export class AgentClientError extends Error {
  readonly harness: ErrorInfo | null;
  constructor(readonly operation: "start" | "invoke" | "get" | "wait" | "cancel", readonly phase: "submit" | "query" | "wait" | "cancel", readonly requestKey: string | null, readonly runID: string | null, readonly cause: unknown) {
    super(`${operation} failed during ${phase}`, { cause }); this.name = "AgentClientError";
    this.harness = cause instanceof HarnessError ? copy(cause.info) : null;
  }
}
