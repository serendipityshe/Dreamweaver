import { randomUUID } from "node:crypto"
import {
  assertResolutionDecision,
  confirmTransition,
  type ConfirmTransition,
  type CoreAction,
  type ResolutionDecision,
  type TransitionProposal,
} from "./confirmation.js"
import { CoreError } from "./errors.js"
import {
  completeSnapshot,
  requestSuspendSnapshot,
  restoreSnapshot,
  resumeSnapshot,
  startSnapshot,
  suspendSnapshot,
  validateRunID,
} from "./transitions.js"
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

/** 完成请求；活动态需要当前令牌，挂起态仅允许 null 配合 cancelled。 */
export interface CompleteInput {
  /** 当前执行资格；挂起后的取消不再持有执行令牌。 */
  readonly executionToken: ExecutionToken | null
  /** 完成类别，业务结果与错误详情由 Harness 保存。 */
  readonly outcome: CompletionKind
}

/** 单个 Run 的内存句柄；方法使用闭包，不依赖调用时的 this。 */
export interface RunCore {
  /** 返回最后确认的快照副本；首次开始尚未确认时为 null。 */
  getSnapshot(): CoreSnapshot | null
  /** 首次开始，确认后返回携带新令牌的快照。 */
  start(confirm: ConfirmTransition): Promise<StartedSnapshot>
  /** 请求在安全边界挂起，不直接改变活动状态或令牌。 */
  requestSuspend(confirm: ConfirmTransition): Promise<StartedSnapshot | ResumedSnapshot>
  /** 校验活动执行者的令牌，确认挂起并交回执行资格。 */
  suspend(executionToken: ExecutionToken, confirm: ConfirmTransition): Promise<SuspendedSnapshot>
  /** 从挂起状态恢复，确认后返回新令牌。 */
  resume(confirm: ConfirmTransition): Promise<ResumedSnapshot>
  /** 确认完成并撤销令牌，完成后不能再次迁移。 */
  complete(input: CompleteInput, confirm: ConfirmTransition): Promise<CompletedSnapshot>
  /** 只解决当前结果不明的交接，不再次确认或重新生成令牌。 */
  resolveTransition(operationID: string, decision: ResolutionDecision): CoreSnapshot | null
}

/**
 * 唯一的交接占用标记，同时承担互斥与待确认操作的保存。
 * phase 仅是内存中的交接阶段，不进入 Core 的四态五字段快照。
 */
type TransitionSlot =
  | null
  | { readonly phase: "preparing" }
  | { readonly phase: "confirming"; readonly proposal: TransitionProposal }
  | { readonly phase: "unknown"; readonly proposal: TransitionProposal }

/** 创建尚未开始的句柄；不执行任务，也不注册全局 Run 实例。 */
export function createRunCore(runID: string): RunCore {
  return makeRunCore(validateRunID(runID), null)
}

/** 校验并复制可重建快照；加载数据和隔离旧执行者由 Harness 负责。 */
export function restoreRunCore(snapshot: unknown): RunCore {
  const restored = restoreSnapshot(snapshot)
  return makeRunCore(restored.runID, restored)
}

/** 共享所有句柄的实现，每个句柄独立拥有快照和交接标记。 */
function makeRunCore(runID: RunID, initial: CoreSnapshot | null): RunCore {
  let snapshot = initial
  let slot: TransitionSlot = null

  /** 返回独立副本，外部修改不能影响当前已确认状态。 */
  function getSnapshot(): CoreSnapshot | null {
    return snapshot === null ? null : { ...snapshot }
  }

  /** 正在准备时尚无交接 ID；其余阶段携带现有 ID 供调用方定位。 */
  function busyError(occupied: Exclude<TransitionSlot, null>): CoreError {
    return new CoreError(
      "TRANSITION_BUSY",
      "当前 Run 有正在处理或等待解决的交接。",
      occupied.phase === "preparing"
        ? undefined
        : { operationID: occupied.proposal.operationID },
    )
  }

  /**
   * 所有普通写操作共用的交接流程；prepare 只能同步计算候选快照。
   * 在读取外部 input 的 getter 前抢占标记，防止同步重入绕过互斥。
   */
  async function applyTransition<S extends CoreSnapshot>(
    action: CoreAction,
    prepare: (current: CoreSnapshot | null) => S,
    confirm: ConfirmTransition,
  ): Promise<S> {
    if (slot !== null) throw busyError(slot)
    slot = { phase: "preparing" }

    let candidate: S
    let proposal: TransitionProposal
    try {
      if (typeof confirm !== "function") {
        throw new CoreError("INVALID_ARGUMENT", "交接确认函数必须是可调用的函数。")
      }

      candidate = prepare(snapshot)
      // 已确认过的重复挂起请求是幂等操作，不产生另一笔外部交接。
      if (candidate === snapshot) {
        slot = null
        return { ...candidate }
      }

      proposal = {
        operationID: randomUUID(),
        action,
        before: snapshot,
        after: candidate,
      }
    } catch (error) {
      // 尚未调用外部确认函数，可以确定本次交接未提交。
      slot = null
      throw error
    }

    // 必须先保存提案，再调用可能同步重入句柄的外部确认函数。
    slot = { phase: "confirming", proposal }
    try {
      await confirmTransition(proposal, confirm)
    } catch (error) {
      if (error instanceof CoreError && error.code === "CONFIRMATION_REJECTED") {
        slot = null
        throw error
      }

      // 进入外部确认后，不能因异常而无条件解锁或自动重试。
      slot = { phase: "unknown", proposal }
      if (error instanceof CoreError && error.code === "CONFIRMATION_UNKNOWN") {
        throw error
      }
      throw new CoreError("CONFIRMATION_UNKNOWN", "无法确定交接结果，等待显式解决。", {
        operationID: proposal.operationID,
        cause: error,
      })
    }

    // 普通迁移唯一的安装点，外部接受后才使候选状态及令牌生效。
    snapshot = candidate
    slot = null
    return { ...candidate }
  }

  /** 只有结果已归类为 unknown 后，才允许同步地解决当前交接。 */
  function resolveTransition(
    operationID: string,
    decision: ResolutionDecision,
  ): CoreSnapshot | null {
    if (slot === null) {
      throw new CoreError("INVALID_TRANSITION", "当前 Run 没有等待解决的交接。")
    }
    if (slot.phase !== "unknown") throw busyError(slot)

    // 校验失败时保留原标记，错误 ID 或非法结果都不能意外解锁。
    assertResolutionDecision(decision)
    if (operationID !== slot.proposal.operationID) {
      throw new CoreError("INVALID_ARGUMENT", "操作 ID 与当前待解决交接不匹配。")
    }

    // 使用原候选快照，不重新生成 token，不再调用外部确认函数。
    if (decision === "accepted") snapshot = slot.proposal.after
    slot = null
    return getSnapshot()
  }

  return {
    getSnapshot,
    start: confirm => applyTransition(
      "start",
      current => startSnapshot(runID, current, randomUUID() as ExecutionToken),
      confirm,
    ),
    requestSuspend: confirm => applyTransition(
      "requestSuspend",
      requestSuspendSnapshot,
      confirm,
    ),
    suspend: (executionToken, confirm) => applyTransition(
      "suspend",
      current => suspendSnapshot(current, executionToken),
      confirm,
    ),
    resume: confirm => applyTransition(
      "resume",
      current => resumeSnapshot(current, randomUUID() as ExecutionToken),
      confirm,
    ),
    complete: (input, confirm) => applyTransition(
      "complete",
      current => {
        let executionToken: unknown
        let outcome: unknown
        try {
          if (typeof input !== "object" || input === null || Array.isArray(input)) {
            throw new TypeError("完成请求必须是有效对象。")
          }
          // 只读取一次外部字段，且读取全过程已处于 preparing 互斥阶段。
          executionToken = input.executionToken
          outcome = input.outcome
        } catch (cause) {
          // 外部 getter/Proxy 的异常仅表示参数读取失败，不能伪装成结果不明。
          throw new CoreError("INVALID_ARGUMENT", "无法读取有效的完成请求。", { cause })
        }
        // 迁移规则在读取边界的 catch 外执行，保留真实的状态或令牌错误码。
        return completeSnapshot(current, executionToken, outcome)
      },
      confirm,
    ),
    resolveTransition,
  }
}
