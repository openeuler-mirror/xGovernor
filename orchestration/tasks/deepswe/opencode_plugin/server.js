const solverSystemPrompt = "You are a helpful assistant that can interact with a computer."

export default {
  id: "mcts.deepswe",
  async setup(ctx) {
    await ctx.session.hook("context", (event) => {
      const latestUser = event.messages.findLast((message) => message.role === "user")
      if (latestUser?.metadata?.mcts_deepswe_solver === true) {
        event.system = [{ type: "text", text: solverSystemPrompt }]
      }
    })
  },
}
