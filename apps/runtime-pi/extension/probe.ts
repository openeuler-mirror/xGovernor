import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { launchConfig, registerModel, safeModelError } from "./model-config.ts";

export default async function (pi: ExtensionAPI) {
    const config = launchConfig();
    let failure: unknown;
    try {
        if (!config) throw new Error("LLM configuration required");
        await registerModel(pi, config);
    } catch (error) { failure = error; }
    pi.on("session_start", async (_event, ctx) => {
        try {
            if (failure) throw failure;
            const model = ctx.modelRegistry.find(config!.provider,config!.model);
            if (!model) throw new Error("Model not found");
            const result = await ctx.modelRegistry.complete(model, {
                messages:[{role:"user",content:"Reply OK.",timestamp:Date.now()}],
            }, {maxTokens:Math.min(16,model.maxTokens),signal:AbortSignal.timeout(10000),maxRetries:0});
            if (result.stopReason === "error" || result.stopReason === "aborted") {
                throw new Error(result.errorMessage ?? "Model provider request failed");
            }
            ctx.ui.notify(JSON.stringify({type:"xgovernor_probe",success:true}), "info");
        } catch (error) {
            ctx.ui.notify(JSON.stringify({type:"xgovernor_probe",success:false,...safeModelError(error)}), "error");
        }
    });
}
