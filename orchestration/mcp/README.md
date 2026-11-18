# Run MCP adapter

`xgovernor-mcp` exposes `mcts_start`, `mcts_status`, `mcts_cancel`, `mcts_result`
and `mcts_list` over classic MCP JSON-RPC stdio. It has no run engine or storage.
The CLI's `mcts mcp` entry constructs `McpServer::new(client)` and calls
`serve` with buffered stdin and stdout. Diagnostic output belongs on stderr.

Implement the five typed `RunClient` methods using the same application entry
as the CLI. Requests and responses use `orchestration_contract::run` directly:
`StartRequest`/`StartResponse`, `StatusRequest`/`StatusResponse`,
`CancelRequest`/`CancelResponse`, `ResultRequest`/`ResultResponse`, and
`ListRequest`/`ListResponse`. MCP arguments are decoded into these types before
forwarding. Status uses `after_sequence` and `limit`; the returned
`next_sequence` is the cursor for the next query. The MCP result includes JSON
text and `structuredContent.result`; application failures set `isError: true`.

The application must enforce budget confirmations and admission, deduplicate
start keys, persist acceptance before returning the run ID, own background
execution, confirm cancellation, and return summaries with controlled artifact
references. Closing MCP input only stops this adapter. It does not cancel runs.
There is no production run client in this package; callers supply one. The test
client records calls and returns receipts without executing a task.

Messages are newline delimited and capped at 1 MiB. Initialization supports
protocol revisions 2024-11-05 through 2025-11-25. Local workspace upload is not
supported by this entry; use tasks with their own code source.
