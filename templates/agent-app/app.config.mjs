import agent from "./agent.mjs";

for (const key of ["MODEL_API_KEY", "MODEL_NAME", "MODEL_ACCOUNT_ID"])
  if (!process.env[key]) throw new Error(`请在 .env 中设置 ${key}`);

export default {
  agent,
  providers: {
    default: {
      protocol: "openai-responses", // Or "anthropic-messages"; choose the actual service protocol.
      apiKey: () => process.env.MODEL_API_KEY,
      accountIdentity: process.env.MODEL_ACCOUNT_ID,
      ...(process.env.MODEL_BASE_URL ? { baseURL: process.env.MODEL_BASE_URL } : {}),
      allowInsecureHTTP: process.env.MODEL_ALLOW_INSECURE_HTTP === "1", // Only for an explicit local HTTP service.
      models: { [process.env.MODEL_NAME]: {} }, // Text by default. Tools: { features: { tools: "supported" } }.
    },
  },
};
