# OpenCode MCTS entry

Install the local plugin and [MCTS Skill](../../skills/mcts/SKILL.md) after
installing the `mcts` executable on PATH:

```sh
python3 orchestration/plugins/opencode/install.py
python3 orchestration/plugins/opencode/install.py --uninstall
```

Both commands accept `--config-dir PATH`. The default is
`$XDG_CONFIG_HOME/opencode`, or `~/.config/opencode`. The installer can also be
called as `install(config_dir, uninstall=False)` by the CLI's
`mcts install --target opencode` action. Release bundles must preserve the
`orchestration/plugins/opencode` and `orchestration/skills/mcts` relative layout.

The installer places `plugins/mcts.js` and `skills/mcts/SKILL.md` in OpenCode's
automatically discovered directories. It leaves JSON/JSONC configuration intact.
Repeated installs update only files recorded in its installation manifest;
uninstall removes only those files. A pre-existing or modified destination is
preserved and reported as an error. Close OpenCode while installing or removing
files, then restart it. An existing MCP server named `mcts` must be renamed or
removed before enabling this plugin.

The plugin supports the documented OpenCode V1 object entry (`server`, available
from 1.18.29) and V2 entry (`id`, `setup`). V1 adds an in-memory `config.mcp.mcts`
entry; V2 uses the MCP transform API with direct tool exposure. Both launch
`["mcts", "mcp"]`. The MCP catalog contains five tools. OpenCode prefixes their
names with the server name, for example `mcts_mcts_start`. The Skill describes
budget confirmation, progress, cancellation and manual patch application.
There is no solver or run lifecycle inside the plugin.

Interfaces: [plugins](https://opencode.ai/docs/build/plugins/server/),
[V1 plugins](https://opencode.ai/v1/docs/plugins/),
[MCP](https://opencode.ai/docs/mcp-servers/),
[Skills](https://opencode.ai/docs/skills/).
