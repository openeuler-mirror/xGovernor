const tools = ["shell", "read", "write", "edit", "glob", "grep"];

export default {
  id: "xgovernor.operation-bridge",
  async setup(ctx) {
    const bindings = new Map();
    const url = process.env.XG_OPERATION_URL;
    const token = process.env.XG_OPERATION_TOKEN;
    delete process.env.XG_OPERATION_TOKEN;
    delete process.env.XG_OPERATION_URL;
    delete process.env.OPENCODE_PASSWORD;
    const call = async (path, body, signal) => {
      const response = await fetch(`${url}${path}`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${token}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(body),
        signal,
      });
      if (!response.ok) throw new Error("Operation bridge rejected request");
      return response.status === 204 ? null : response.json();
    };
    await ctx.session.hook("prompt", async (event) => {
      const binding = await call("/binding", { session: event.sessionID });
      bindings.set(event.sessionID, binding.turn);
      if (event.prompt.files?.length || event.prompt.agents?.length || event.prompt.skills?.length) {
        throw new Error("Only text prompts are supported");
      }
    });
    await ctx.session.hook("http.request", (event) => {
      if (!event.request.url.startsWith(`${url}/model/`)) throw new Error("Model endpoint is disabled");
      event.request.headers.set("authorization", `Bearer ${token}`);
    });
    await ctx.session.hook("title", (event) => { event.result = "Workspace session"; });
    await ctx.session.hook("context", (event) => {
      for (const id of Object.keys(event.tools)) {
        if (!tools.includes(id)) delete event.tools[id];
      }
    });
    await ctx.tool.hook("execute.before", (event) => {
      if (!tools.includes(event.tool) || !bindings.has(event.sessionID)) {
        throw new Error("Tool is disabled in controlled mode");
      }
    });
    await ctx.tool.transform((editor) => {
      for (const tool of editor.list()) {
        if (!tools.includes(tool.id)) { editor.remove(tool.id); continue; }
        editor.update(tool.id, (entry) => {
          entry.output = undefined;
          entry.execute = async (args, context) => {
            const result = await call("/tool", {
              session: context.sessionID,
              turn: bindings.get(context.sessionID),
              operation: context.id,
              tool: tool.id,
              args,
            }, context.signal);
            if (result.error) throw new Error(result.error);
            return { content: result.output };
          };
        });
      }
    });
    await call("/ready", { tools: (await ctx.tool.list()).map((tool) => tool.id) });
  },
};
