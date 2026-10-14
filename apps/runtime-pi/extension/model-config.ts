import { readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { ModelRuntime, type ProviderConfig, type ExtensionAPI } from "@earendil-works/pi-coding-agent";

export interface ModelConfig {
    provider: string;
    model: string;
    apiBase?: string;
    apiKey?: string;
    api?: "openai-completions" | "openai-responses" | "anthropic-messages";
    capabilities?: {
        reasoning?: boolean | null;
        input?: ("text" | "image")[] | null;
        context_window?: number | null;
        max_tokens?: number | null;
    } | null;
}
const apis = ["openai-completions", "openai-responses", "anthropic-messages"];

export function decodeLlmConfig(encoded: string): ModelConfig {
    return validateConfig(JSON.parse(Buffer.from(encoded, "base64url").toString("utf8")));
}

function validateConfig(parsed: unknown): ModelConfig {
    if (!parsed || typeof parsed !== "object") throw new Error("LLM configuration must be an object");
    const config = parsed as ModelConfig;
    if (typeof config.provider !== "string" || !config.provider.trim()) throw new Error("provider is required");
    if (typeof config.model !== "string" || !config.model.trim()) throw new Error("model is required");
    if (config.api != null && !apis.includes(config.api)) throw new Error("Unsupported model protocol");
    return config;
}

export function launchConfig(): ModelConfig | undefined {
    const file = process.env.XGOVERNOR_PI_LLM_CONFIG;
    if (!file) return undefined;
    const config = validateConfig(JSON.parse(readFileSync(file, "utf8")));
    // Rust deliberately persists env references without their resolved secret.
    config.apiKey ??= process.env.XGOVERNOR_PI_MODEL_API_KEY;
    return config;
}

// Resolve against the unmodified built-in catalogue, never against a previous
// turn's provider overrides or the minimal startup models.json bootstrap.
let catalog: ReturnType<typeof ModelRuntime.create> | undefined;
export async function resolveModelConfig(config: ModelConfig) {
    validateConfig(config);
    catalog ??= ModelRuntime.create({modelsPath:null, refreshOnCreate:false, allowModelNetwork:false});
    const builtin = (await catalog).getModel(config.provider, config.model);
    if (!config.apiBase && !builtin) throw new Error("Model not found in Pi catalogue");
    if (!config.apiBase && config.api != null && config.api !== builtin?.api) {
        throw new Error("Protocol conflicts with built-in model; supply api_base for a custom endpoint");
    }
    const caps = config.capabilities;
    const model = {
        ...builtin,
        id: config.model,
        name: builtin?.name ?? config.model,
        api: config.apiBase ? (config.api ?? "openai-completions") : builtin!.api,
        baseUrl: config.apiBase ?? builtin!.baseUrl,
        reasoning: caps?.reasoning ?? builtin?.reasoning ?? false,
        input: caps?.input ?? builtin?.input ?? ["text" as const],
        cost: builtin?.cost ?? {input:0,output:0,cacheRead:0,cacheWrite:0},
        contextWindow: caps?.context_window ?? builtin?.contextWindow ?? 128_000,
        maxTokens: caps?.max_tokens ?? builtin?.maxTokens ?? 32_000,
    };
    if (!Array.isArray(model.input) || !model.input.length || model.input.some(input => input !== "text" && input !== "image")) {
        throw new Error("Model input must contain text or image");
    }
    if (typeof model.reasoning !== "boolean" || !Number.isInteger(model.contextWindow) || !Number.isInteger(model.maxTokens)
        || model.contextWindow <= 0 || model.maxTokens <= 0 || model.maxTokens > model.contextWindow) {
        throw new Error("Invalid model capabilities or token limits");
    }
    const provider: ProviderConfig = {
        baseUrl:model.baseUrl, api:model.api, models:[model],
        ...(config.apiKey ? {apiKey:config.apiKey} : config.apiBase ? {apiKey:"xgovernor-keyless"} : {}),
    };
    return {model,provider};
}

export async function registerModel(pi: ExtensionAPI, config: ModelConfig, persist = false) {
    const resolved = await resolveModelConfig(config);
    pi.registerProvider(config.provider, resolved.provider);
    if (persist && process.env.PI_CODING_AGENT_DIR) {
        const provider = {...resolved.provider, apiKey: config.apiKey || config.apiBase ? "${XGOVERNOR_PI_MODEL_API_KEY}" : undefined};
        writeFileSync(path.join(process.env.PI_CODING_AGENT_DIR,"models.json"),
            JSON.stringify({providers:{[config.provider]:provider}}), {mode:0o600});
    }
    return resolved.model;
}

export function safeModelError(error: unknown): {code:string;message:string} {
    const text = error instanceof Error ? error.message : String(error);
    // Never return provider response bodies, endpoint URLs or credential values.
    if (/401|403|auth|api.?key|credential|expired/i.test(text)) return {code:"llm_auth",message:"Model authentication failed"};
    if (/not found|404|unknown model/i.test(text)) return {code:"llm_model",message:"Model not found or unavailable"};
    if (/timeout|timed out|abort/i.test(text)) return {code:"llm_timeout",message:"Model request timed out"};
    if (/connect|fetch|network|ECONN/i.test(text)) return {code:"llm_connection",message:"Model endpoint connection failed"};
    if (/protocol|capabilit|token limits|input must|configuration|required/i.test(text)) return {code:"llm_config",message:"Invalid model protocol or capabilities"};
    return {code:"llm_provider",message:"Model provider request failed"};
}
