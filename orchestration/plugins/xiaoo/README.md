# xiaoO MCTS entry

Run `mcts install --target xiaoo` with Python 3.11+ and `mcts` on PATH.
The installer merges only `mcpServers.mcts` into `~/.config/xiaoo/mcp.json`
and places the shared Skill at `~/.xiaoo/skills/mcts/SKILL.md`.
`--config-dir DIR` selects the directory containing `mcp.json`; the Skill
remains in the current user's xiaoO Skill directory.

The MCP command is `mcts mcp`. xiaoO exposes the tools as
`mcp__mcts__mcts_start`, `mcp__mcts__mcts_status`, `mcp__mcts__mcts_cancel`,
`mcp__mcts__mcts_result` and `mcp__mcts__mcts_list`. Restart xiaoO after installation.
An explicit `--mcp-config`, `XIAOO_MCP_CONFIG` or workspace `.mcp.json` takes
precedence over the user MCP file; select the installed file explicitly when
using such a workspace. A TOML server also named `mcts` conflicts and must be
resolved before installation. No hooker is required.

Use `mcts install --target xiaoo --uninstall` to remove this entry. Existing
servers and settings survive, and modified owned entries or Skills are
preserved with an error. An ownership manifest beside `mcp.json` supports
repeatable installation and removal. Keep it with the installation.
