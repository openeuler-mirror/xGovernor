// OpenCode loads this entry as a local plugin. Runs belong to the MCP application.
const command = ["mcts", "mcp"];

export default {
  id: "xgovernor.mcts",
  async setup(ctx) {
    await ctx.mcp.transform((editor) => {
      if (editor.get("mcts")) throw new Error("MCP server mcts is already configured");
      editor.set("mcts", { type: "local", command, codemode: false });
    });
  },
  async server() {
    return {
      async config(config) {
        config.mcp ??= {};
        if (Object.hasOwn(config.mcp, "mcts")) {
          throw new Error("MCP server mcts is already configured");
        }
        config.mcp.mcts = { type: "local", command, enabled: true };
      },
    };
  },
};
