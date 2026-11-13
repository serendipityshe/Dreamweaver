import { CoreError } from "./errors.js"
import type {
  CompletedSnapshot,
  CompletionKind,
  CoreSnapshot,
  ExecutionToken,
  ResumedSnapshot,
  RunID,
  StartedSnapshot,
  SuspendedSnapshot,
} from "./types.js"

/** 快照仅允许这五个自有数据字段，包括不可枚举字段在内都必须严格匹配。 */
const snapshotFields = [
  "runID",
  "state",
  "executionToken",
  "suspendRequested",
  "outcome",
] as const

/** 身份必须是非空白字符串；校验不改变原始身份中的空格。 */
function isNonBlankString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0
}

/** 完成类别由固定字符串组成，不对外部值执行隐式类型转换。 */
function isCompletionKind(value: unknown): value is CompletionKind {
  return value === "succeeded" || value === "failed" || value === "cancelled"
}

/** 仅活动快照可以请求挂起、交回令牌或提交执行结果。 */
function requireActiveSnapshot(
  current: CoreSnapshot | null,
): StartedSnapshot | ResumedSnapshot {
  if (current === null || (current.state !== "started" && current.state !== "resumed")) {
    throw new CoreError("INVALID_TRANSITION", "当前状态不是活动执行状态。")
  }
  return current
}

/** 校验调用入口提供的 Run 身份，并保留其原始字符串值。 */
export function validateRunID(value: unknown): RunID {
  if (!isNonBlankString(value)) {
    throw new CoreError("INVALID_ARGUMENT", "RunID 必须是非空白字符串。")
  }
  return value as RunID
}

/**
 * 从外部数据重建挂起或完成快照，不读取属性 getter，也不保留外部对象引用。
 * 原型为 Object.prototype 或 null 的普通对象均可接受；额外字段及访问器均被拒绝。
 */
export function restoreSnapshot(
  value: unknown,
): SuspendedSnapshot | CompletedSnapshot {
  if (typeof value !== "object" || value === null) {
    throw new CoreError("INVALID_SNAPSHOT", "恢复快照必须是普通对象。")
  }

  let descriptors: PropertyDescriptorMap | undefined
  try {
    const prototype: unknown = Object.getPrototypeOf(value)
    if (!Array.isArray(value) && (prototype === Object.prototype || prototype === null)) {
      // 一次捕获描述符，后续只使用捕获的值，避免校验后再次读取外部属性。
      descriptors = Object.getOwnPropertyDescriptors(value)
    }
  } catch (cause) {
    // Proxy 的反射陷阱也可能抛出 CoreError，仍作为外部异常保留原始 cause。
    throw new CoreError("INVALID_SNAPSHOT", "无法读取恢复快照的数据字段。", { cause })
  }

  if (
    descriptors === undefined ||
    Reflect.ownKeys(descriptors).length !== snapshotFields.length ||
    !snapshotFields.every((field) => {
      const descriptor = descriptors[field]
      return Object.hasOwn(descriptors, field) &&
        descriptor !== undefined && Object.hasOwn(descriptor, "value")
    })
  ) {
    throw new CoreError("INVALID_SNAPSHOT", "恢复快照必须只包含五个自有数据字段。")
  }

  // 描述符由内建反射函数创建；读取其 value 不会调用输入对象的 getter。
  const runID: unknown = descriptors.runID?.value
  const state: unknown = descriptors.state?.value
  const executionToken: unknown = descriptors.executionToken?.value
  const suspendRequested: unknown = descriptors.suspendRequested?.value
  const outcome: unknown = descriptors.outcome?.value

  if (!isNonBlankString(runID) || executionToken !== null || suspendRequested !== false) {
    throw new CoreError("INVALID_SNAPSHOT", "恢复快照的身份、令牌或挂起标志不合法。")
  }

  if (state === "suspended" && outcome === null) {
    return {
      runID: runID as RunID,
      state,
      executionToken: null,
      suspendRequested: false,
      outcome: null,
    }
  }
  if (state === "completed" && isCompletionKind(outcome)) {
    return {
      runID: runID as RunID,
      state,
      executionToken: null,
      suspendRequested: false,
      outcome,
    }
  }
  throw new CoreError("INVALID_SNAPSHOT", "只能恢复合法的挂起或完成快照。")
}

/** 首次开始只接受尚无快照的句柄；令牌由运行句柄生成后传入。 */
export function startSnapshot(
  runID: RunID,
  current: CoreSnapshot | null,
  newToken: ExecutionToken,
): StartedSnapshot {
  if (current !== null) {
    throw new CoreError("INVALID_TRANSITION", "已经开始的 Run 不能再次开始。")
  }
  return {
    runID,
    state: "started",
    executionToken: newToken,
    suspendRequested: false,
    outcome: null,
  }
}

/** 请求挂起不改变生命周期状态；已有请求时返回原快照表达幂等。 */
export function requestSuspendSnapshot(
  current: CoreSnapshot | null,
): StartedSnapshot | ResumedSnapshot {
  const active = requireActiveSnapshot(current)
  return active.suspendRequested ? active : { ...active, suspendRequested: true }
}

/** 只有持有当前令牌的活动执行者可以交回执行资格。 */
export function suspendSnapshot(
  current: CoreSnapshot | null,
  executionToken: unknown,
): SuspendedSnapshot {
  const active = requireActiveSnapshot(current)
  if (executionToken !== active.executionToken) {
    throw new CoreError("INVALID_EXECUTION_TOKEN", "挂起操作必须提供当前执行令牌。")
  }
  return {
    runID: active.runID,
    state: "suspended",
    executionToken: null,
    suspendRequested: false,
    outcome: null,
  }
}

/** 仅挂起快照可以恢复，并使用运行句柄传入的新令牌。 */
export function resumeSnapshot(
  current: CoreSnapshot | null,
  newToken: ExecutionToken,
): ResumedSnapshot {
  if (current === null || current.state !== "suspended") {
    throw new CoreError("INVALID_TRANSITION", "只有挂起的 Run 可以恢复。")
  }
  return {
    runID: current.runID,
    state: "resumed",
    executionToken: newToken,
    suspendRequested: false,
    outcome: null,
  }
}

/**
 * 活动执行必须携带当前令牌完成；挂起执行只能以 null 令牌取消。
 * 这里只生成完成候选，不安装状态、不调用外部确认，也不保存业务返回值。
 */
export function completeSnapshot(
  current: CoreSnapshot | null,
  executionToken: unknown,
  outcome: unknown,
): CompletedSnapshot {
  if (current === null || current.state === "completed") {
    throw new CoreError("INVALID_TRANSITION", "尚未开始或已经完成的 Run 不能完成。")
  }
  if (!isCompletionKind(outcome)) {
    throw new CoreError("INVALID_ARGUMENT", "完成类别必须是 succeeded、failed 或 cancelled。")
  }
  if (current.state === "suspended" && outcome !== "cancelled") {
    throw new CoreError("INVALID_TRANSITION", "挂起的 Run 只能通过取消结束。")
  }
  if (executionToken !== current.executionToken) {
    throw new CoreError("INVALID_EXECUTION_TOKEN", "完成操作必须提供当前执行令牌。")
  }
  return {
    runID: current.runID,
    state: "completed",
    executionToken: null,
    suspendRequested: false,
    outcome,
  }
}
