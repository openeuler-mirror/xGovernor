---
name: mcts
description: Use MCTS to explore alternative solutions to a difficult coding task with executable scoring, then inspect progress and retrieve the selected patch.
---

Use MCTS when several solution paths are worth comparing and the task has executable tests or another explicit scoring method. For a small, direct edit, use ordinary editing tools.

Before starting, agree with the user on the task, code source, budget limits and expected duration. Explain that sandbox execution and model use consume the configured budget. The initial entry supports tasks with their own code source, such as SWE-bench; it cannot upload the current local checkout.

Call `mcts_start` with `start_key`, `goal`, `content`, task `code_source`, `config` and `approval`. Put the agreed limits in both `config.budget` and `approval.budget`; they must match exactly. Reuse that key if acceptance is uncertain. Save the returned run ID. Acceptance means the background run was received; it does not mean the task is solved. The run uses its own model configuration and continues after the entry session closes.

Use `mcts_status` with the run ID to report progress, budget use, selected backend and any cleanup issues. Pass `next_sequence` as `after_sequence` with an explicit event `limit`; do not repeatedly fetch all logs. Use `mcts_list` to find previous runs. If the user asks to stop, call `mcts_cancel` with the run ID and a reason, and report whether cancellation has taken effect; continue checking until the run reaches a terminal state when needed.

Use `mcts_result` to retrieve the summary and artifact references. Report the selection reason, test or scoring evidence, unresolved failures, and the patch/workspace location. Distinguish completed, cancelled, interrupted and failed runs. If a selected Agent session can be continued, present its continuation instructions and retention choice.

Do not apply the patch automatically. Present it for user review and obtain confirmation before applying it with the entry Agent's editing tools. The entry platform may qualify MCP tool names with a server namespace.
