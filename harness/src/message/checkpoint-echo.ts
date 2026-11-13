import { canonicalJsonDigest, type Checkpoint, type SessionBinding } from "../contract/index.js";

/** Bind a compact receipt to the original response and exact save identity. */
export function checkpointDigest(requestID: string, binding: SessionBinding, operationID: string, checkpoint: Checkpoint): string {
  return canonicalJsonDigest({ protocolVersion: "harness/1", requestID, binding, operationID, checkpoint });
}
