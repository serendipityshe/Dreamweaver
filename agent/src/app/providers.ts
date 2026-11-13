import type { ProviderConfiguration } from "../contract/types.js";
import type { AppProviderConfiguration } from "./types.js";
import { copy, fail, fields } from "../contract/common.js";

export function appProviders(providers: Record<string, AppProviderConfiguration> = {}): Record<string, ProviderConfiguration> {
  fields(providers, Object.keys(providers));
  return Object.fromEntries(Object.entries(providers).map(([id, provider]) => {
    fields(provider, ["revision", "protocol", "baseURL", "allowInsecureHTTP", "accountIdentity", "apiKey", "models"], ["protocol", "accountIdentity", "apiKey", "models"]);
    if (!["openai-responses", "anthropic-messages"].includes(provider.protocol)) fail("App provider protocol is not implemented", "VERSION_UNAVAILABLE");
    fields(provider.models, Object.keys(provider.models));
    const { apiKey, ...data } = provider;
    const models = Object.fromEntries(Object.entries(provider.models).map(([name, model]) => {
      fields(model, ["features", "parameters", "providerOptions"]);
      fields(model.features ?? {}, ["text", "tools", "image", "jsonSchema", "streaming"]);
      return [name, { ...model, features: { text: "supported" as const, tools: "unsupported" as const,
        image: "unsupported" as const, jsonSchema: "unsupported" as const, streaming: "unsupported" as const, ...model.features } }];
    }));
    return [id, { ...copy({ ...data, revision: provider.revision ?? "1", models }), apiKey }];
  }));
}
