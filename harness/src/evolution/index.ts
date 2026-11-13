// Coordinator-private query seam; never re-export through the SDK facade.
export { getVersionPointer, getVersionPointerAsync } from "./queries.js";
export { getDefinitionAvailability } from "./directory.js";
