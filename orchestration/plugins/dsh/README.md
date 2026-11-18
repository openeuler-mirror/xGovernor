# dsh MCTS entry

Run `mcts install --target dsh` with Python 3.11+ and `mcts` on PATH.
The installer appends its marked MCP insertion to `$DSH_HOME/cordis.patch.yml`
(default `~/.dsh`) and places the shared Skill at `$DSH_HOME/skills/mcts/SKILL.md`.
`--config-dir DIR` selects this dsh home; launch dsh with `DSH_HOME=DIR` too.
The home patch applies over each profile's own layer.

The entry uses the shipped `@deepseek-ai/dsh-mcp-client` plugin with stdio
command `mcts mcp`. Use a base-backed profile with MCP and filesystem Skill
support, such as `headless` or `sdk`. Tools are named
`mcp__mcts__mcts_start`, `mcp__mcts__mcts_status`, `mcp__mcts__mcts_cancel`,
`mcp__mcts__mcts_result` and `mcp__mcts__mcts_list`. Restart dsh after installation.
No model credentials are stored in the entry.

Use `mcts install --target dsh --uninstall` to remove the marked insertion and
Skill. Unrelated patch bytes survive, including changes after installation.
A modified marked insertion or Skill is preserved with an error. Keep the
ownership manifest beside the patch. The patch must use dsh's documented
block sequence format; convert flow sequences before installing.
