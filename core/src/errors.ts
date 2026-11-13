/**
 * Core 的稳定错误码，调用方应按 code 分支处理，不解析 message。
 * 这些错误描述内核操作失败，不代表 Run 已进入 completed 状态。
 */
export type CoreErrorCode =
  /** 调用参数不合法，例如空的 RunID 或不受支持的完成类别。 */
  | "INVALID_ARGUMENT"
  /** 外部快照不符合四态五字段约束，或不能用于重建句柄。 */
  | "INVALID_SNAPSHOT"
  /** 当前生命周期状态不允许执行请求的动作。 */
  | "INVALID_TRANSITION"
  /** 提交方的执行令牌缺失、失效，或不属于当前 Run 的活动执行。 */
  | "INVALID_EXECUTION_TOKEN"
  /** 当前句柄正在确认交接，或仍有结果不明的交接等待解决。 */
  | "TRANSITION_BUSY"
  /** 外部明确保证交接未提交且不会迟到提交，可保留原状态并解锁。 */
  | "CONFIRMATION_REJECTED"
  /** 无法确定交接是否提交，必须保留待确认操作，等待显式解决。 */
  | "CONFIRMATION_UNKNOWN"

/** 内核错误的可选上下文；原始异常沿用标准 ErrorOptions.cause。 */
export interface CoreErrorOptions extends ErrorOptions {
  /**
   * 错误关联的交接操作 ID，不是 RunID 或执行令牌。
   * 抛出 CONFIRMATION_UNKNOWN 的调用处必须提供此 ID，供 resolveTransition 使用。
   */
  readonly operationID?: string
}

/**
 * 内核操作的统一错误，保留标准 Error 的 message、stack 和 cause。
 * 错误对象只携带失败信息；状态安装、互斥释放与未知结果处理由运行句柄负责。
 */
export class CoreError extends Error {
  /** 稳定的程序判别字段；message 仅用于解释具体失败原因。 */
  readonly code: CoreErrorCode

  /** 关联具体交接时提供；不涉及交接的错误可为 undefined。 */
  readonly operationID: string | undefined

  /** 创建错误并原样保留 cause，不根据异常内容猜测交接是否已提交。 */
  constructor(code: CoreErrorCode, message: string, options?: CoreErrorOptions) {
    super(message, options)
    this.name = "CoreError"
    this.code = code
    this.operationID = options?.operationID
  }
}
