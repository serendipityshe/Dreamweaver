# Agent dependency and source notices

No OpenCode or Provider implementation source was copied into this package.

| Source | Fixed version | Use | License |
| --- | --- | --- | --- |
| [Ajv](https://github.com/ajv-validator/ajv) | 8.20.0 | Dependency; JSON Schema 2020-12 validator, with a local keyword allowlist; no coercion/default insertion | MIT |
| [OpenAI Node SDK](https://github.com/openai/openai-node) | 7.53.0 | Dependency; raw Responses request/response, cancellation; maxRetries=0 and redirects rejected | Apache-2.0 |
| [Anthropic TypeScript SDK](https://github.com/anthropics/anthropic-sdk-typescript) | 1.6.0 | Dependency; raw Messages request/response, cancellation; maxRetries=0 and redirects rejected | Apache-2.0 |
| [OpenCode](https://github.com/anomalyco/opencode/tree/21eaa626a37f77d7a8d80fa01997fa7d52506c97) | 21eaa626a37f77d7a8d80fa01997fa7d52506c97 | Reference only: packages/core/src/provider.ts and aisdk.ts; no application runtime, storage, retry loop or source copied | MIT |

The dependencies retain their own copyright/license files in their distributed packages. Exact direct and transitive versions/integrities are in package-lock.json. Local protocol conversion is original implementation in capacities/model/managed.ts, not a copied SDK implementation. Re-evaluate protocol conversion and fixed adapter versions when upgrading.
