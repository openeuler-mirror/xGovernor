import { readFileSync, writeFileSync } from 'node:fs';
import { inspect } from 'node:util';

export default {
  id: 'mcts.entry-check',
  async setup(ctx) {
    let started = false;
    const timer = setInterval(async () => {
      if (started) return;
      const tools = await ctx.tool.list();
      if (!tools.some(t => t.id === 'mcts_mcts_start')) return;
      started = true;
      clearInterval(timer);
      const directory = process.env.MCTS_ENTRY_DIRECTORY;
      const transcript = [];
      try {
        const session = await ctx.session.create({ title: 'MCTS task delivery', permissions: [{ action: '*', resource: '*', effect: 'allow' }] });
        async function call(name, input) {
          const tool = tools.find(t => t.id === 'mcts_' + name);
          const output = await tool.execute(input, { sessionID: session.id, messageID: 'msg_entry', id: 'call_entry', agent: 'build', signal: new AbortController().signal, progress: () => {} });
          transcript.push({ name, input, output });
          writeFileSync(directory + '/transcript.json', JSON.stringify(transcript));
          const value = output.output ?? output;
          if (value.structuredContent) return value.structuredContent.result ?? value.structuredContent;
          const content = value.content ?? output.content;
          if (content) return JSON.parse(content.find(c => c.type === 'text').text);
          return typeof value === 'string' ? JSON.parse(value) : value;
        }
        const request = JSON.parse(readFileSync(process.env.MCTS_ENTRY_REQUEST, 'utf8'));
        const start = await call('mcts_start', request);
        const deadline = Date.now() + Number(process.env.MCTS_E2E_TIMEOUT ?? 3000) * 1000;
        while (Date.now() < deadline) {
          await call('mcts_status', { run_id: start.run_id, after_sequence: 0, limit: 1000 });
          const result = await call('mcts_result', { run_id: start.run_id });
          if (['completed', 'cancelled', 'failed', 'interrupted'].includes(result.run.status)) {
            writeFileSync(directory + '/result.json', JSON.stringify(result));
            return;
          }
          await new Promise(resolve => setTimeout(resolve, 5000));
        }
        throw Error('Task did not terminate');
      } catch (error) {
        writeFileSync(directory + '/failure.txt', inspect(error, { depth: 8 }));
      }
    }, 1000);
    return () => clearInterval(timer);
  },
};
