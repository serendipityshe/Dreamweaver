/** 品牌标记只参与类型检查，编译后不会生成运行时变量。 */
declare const runIDBrand: unique symbol
declare const executionTokenBrand: unique symbol

/**
 * 单次执行的身份，在开始、挂起、恢复和完成之间保持不变。
 * 品牌用于避免与执行令牌混用；实际字符串仍需在运行入口校验。
 */
export type RunID = string & { readonly [runIDBrand]: "RunID" }

/**
 * 一次活动执行的资格标识，由 Core 在开始或恢复时生成。
 * 挂起或完成后失效，旧执行者不能凭旧令牌继续提交结果。
 */
export type ExecutionToken = string & {
  readonly [executionTokenBrand]: "ExecutionToken"
}

/** 生命周期完成的类别；业务返回值和错误详情由 Harness 管理。 */
export type CompletionKind = "succeeded" | "failed" | "cancelled"

/** 首次开始后、尚未挂起或完成的执行快照。 */
export interface StartedSnapshot {
  /** 标识本次执行。 */
  readonly runID: RunID
  /** 首次开始后处于活动状态。 */
  readonly state: "started"
  /** 当前执行者必须持有的有效令牌。 */
  readonly executionToken: ExecutionToken
  /** 只表示已请求挂起，实际挂起需在安全交接点确认。 */
  readonly suspendRequested: boolean
  /** 活动执行尚无完成类别。 */
  readonly outcome: null
}

/** 从挂起状态恢复后、再次取得执行资格的快照。 */
export interface ResumedSnapshot {
  /** 沿用原执行身份，恢复不会创建新的 Run。 */
  readonly runID: RunID
  /** 恢复后再次处于活动状态。 */
  readonly state: "resumed"
  /** 本次恢复新生成的令牌，先前的令牌不能复用。 */
  readonly executionToken: ExecutionToken
  /** 恢复后仍允许再次请求挂起。 */
  readonly suspendRequested: boolean
  /** 恢复表示继续执行，尚无完成类别。 */
  readonly outcome: null
}

/** 已确认交回执行资格、可以在之后恢复的快照。 */
export interface SuspendedSnapshot {
  /** 保留原执行身份，供后续恢复使用。 */
  readonly runID: RunID
  /** 已完成挂起交接。 */
  readonly state: "suspended"
  /** 挂起后不再保留有效执行资格。 */
  readonly executionToken: null
  /** 挂起已得到确认，对应请求同时清除。 */
  readonly suspendRequested: false
  /** 挂起尚未终结生命周期。 */
  readonly outcome: null
}

/** 生命周期已终结的快照，后续不能恢复或继续推进。 */
export interface CompletedSnapshot {
  /** 保留执行身份，供调用方识别最终结果。 */
  readonly runID: RunID
  /** 成功、失败和取消均终结本次生命周期。 */
  readonly state: "completed"
  /** 完成后撤销执行资格，拒绝迟到的执行结果。 */
  readonly executionToken: null
  /** 已完成的执行不再保留挂起请求。 */
  readonly suspendRequested: false
  /** 完成快照必须说明成功、失败或取消。 */
  readonly outcome: CompletionKind
}

/**
 * Core 的只读执行快照，以 state 为判别字段约束合法的字段组合。
 * 尚未开始的句柄由后续 API 返回 null 表示，不增加第五种状态。
 * readonly 只提供编译期约束；外部数据校验与运行时快照隔离由后续实现负责。
 */
export type CoreSnapshot =
  | StartedSnapshot
  | ResumedSnapshot
  | SuspendedSnapshot
  | CompletedSnapshot

/** 从快照派生四种生命周期状态，避免另行维护一份状态列表。 */
export type CoreState = CoreSnapshot["state"]
