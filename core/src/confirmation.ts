import { CoreError } from "./errors.js"
import type { CoreSnapshot } from "./types.js"

/** 可产生交接的动作；requestSuspend 只修改控制标志，不增加生命周期状态。 */
export type CoreAction =
  | "start"
  | "requestSuspend"
  | "suspend"
  | "resume"
  | "complete"

/** 一次尚未由运行句柄安装的候选状态变化。 */
export interface TransitionProposal {
  /** 本次交接的身份；结果不明时沿用此 ID 解决，不生成另一笔交接。 */
  readonly operationID: string
  /** 产生候选快照的动作。 */
  readonly action: CoreAction
  /** 交接前的快照；首次开始前没有快照，以 null 表示。 */
  readonly before: CoreSnapshot | null
  /** 本次交接准备采用的快照，确认成功后才由运行句柄安装。 */
  readonly after: CoreSnapshot
}

/** Harness 对一次交接的确认；不是 Run 的生命周期状态。 */
export type ConfirmationDecision =
  /** 已确认接受候选状态及本次交接需要协调的外部数据。 */
  | "accepted"
  /** 明确保证未提交且不会迟到提交，可以保留旧状态并解锁。 */
  | "rejected"
  /** 无法确定是否提交，需要保留待确认操作并等待显式解决。 */
  | "unknown"

/** 显式解决只能给出确定结果，不允许用 unknown 结束待确认操作。 */
export type ResolutionDecision = Exclude<ConfirmationDecision, "unknown">

/**
 * 由 Harness 提供的交接确认函数，可在其中协调检查点和持久化。
 * 明确拒绝必须返回 rejected；抛出异常无法证明交接未提交。
 */
export type ConfirmTransition = (
  proposal: TransitionProposal,
) => Promise<ConfirmationDecision>

/**
 * 调用外部确认函数并统一解释结果：只有 accepted 正常返回。
 * 候选快照的合法性由迁移规则与运行句柄保证；此处只隔离回调参数并校验结果。
 * 本函数不安装状态、不维护待确认操作，也不释放运行句柄的锁。
 */
export async function confirmTransition(
  proposal: TransitionProposal,
  confirm: ConfirmTransition,
): Promise<void> {
  // 在调用外部代码前保存身份，避免回调修改副本后污染错误关联的操作 ID。
  const operationID = proposal.operationID

  if (typeof confirm !== "function") {
    throw new CoreError("INVALID_ARGUMENT", "交接确认函数必须是可调用的函数。", {
      operationID,
    })
  }

  // 快照的五个字段均为原始值，复制每份快照即可隔离所有可变引用。
  // 即使 JavaScript 回调绕过 readonly 修改或保留参数，也不会修改内部候选状态。
  const isolatedProposal: TransitionProposal = {
    operationID,
    action: proposal.action,
    before: proposal.before === null ? null : { ...proposal.before },
    after: { ...proposal.after },
  }

  // 外部调用可能不遵守 TypeScript 声明，必须按 unknown 校验实际返回值。
  let decision: unknown
  try {
    decision = await confirm(isolatedProposal)
  } catch (cause) {
    // 只捕获外部调用阶段的异常；即使异常自称明确拒绝，也不能据此判断提交结果。
    throw new CoreError("CONFIRMATION_UNKNOWN", "交接确认异常，提交结果不明。", {
      operationID,
      cause,
    })
  }

  // 明确结果在 catch 外处理，避免把本函数产生的拒绝错误再次包装为结果不明。
  switch (decision) {
    case "accepted":
      return
    case "rejected":
      throw new CoreError("CONFIRMATION_REJECTED", "外部已明确拒绝本次交接。", {
        operationID,
      })
    case "unknown":
      throw new CoreError("CONFIRMATION_UNKNOWN", "交接提交结果不明，等待显式解决。", {
        operationID,
      })
    default:
      // 原样保存非法返回值，不调用其自定义字符串转换，避免丢失原始失败信息。
      throw new CoreError("CONFIRMATION_UNKNOWN", "交接确认返回值非法，无法确定提交结果。", {
        operationID,
        cause: decision,
      })
  }
}

/**
 * 校验显式解决的决定；非法参数不能被当成确认或拒绝使用。
 * 操作 ID 匹配、只解决一次及解锁条件，均由运行句柄负责。
 */
export function assertResolutionDecision(
  value: unknown,
): asserts value is ResolutionDecision {
  if (value !== "accepted" && value !== "rejected") {
    throw new CoreError("INVALID_ARGUMENT", "交接解决结果必须是 accepted 或 rejected。")
  }
}
