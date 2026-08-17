/**
 * xGovernor Bridge Extension
 *
 * Overrides pi's built-in file/exec tools (read, write, edit, bash, ls,
 * find, grep) so that every operation is proxied over a local HTTP bridge
 * (`apps/runtime-pi`'s `Bridge`, see its module doc) instead of touching
 * this `pi` process's own local filesystem. `pi` runs unsandboxed on the
 * daemon host, but the sandboxed backend it must actually operate against
 * (a local directory or a remote e2b sandbox) lives on the other side of
 * that bridge.
 *
 * Loaded via `pi -e <this-dir> --mode rpc`. Not meant to be used standalone
 * — see README.md.
 *
 * Modeled closely on Pi's own official "Gondolin" example extension
 * (`packages/coding-agent/examples/extensions/gondolin/index.ts` in
 * badlogic/pi-mono), which does the analogous thing for a local micro-VM
 * sandbox instead of an HTTP bridge.
 */

import path from "node:path";
import {
	type BashOperations,
	createBashTool,
	createEditTool,
	createFindTool,
	createLsTool,
	createReadTool,
	createWriteTool,
	type EditOperations,
	type ExtensionAPI,
	type FindOperations,
	type LsOperations,
	type ReadOperations,
	type WriteOperations,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

/**
 * The three env vars `apps/runtime-pi::PiRuntime` sets on the spawned `pi`
 * child process (see `CONTRACT.md`). All three are required — this
 * extension is useless without a bridge to talk to.
 */
interface BridgeEnv {
	bridgeUrl: string;
	bridgeToken: string;
	workspaceRoot: string;
}

function readBridgeEnv(): BridgeEnv {
	const bridgeUrl = process.env.XGOVERNOR_BRIDGE_URL;
	const bridgeToken = process.env.XGOVERNOR_BRIDGE_TOKEN;
	const workspaceRoot = process.env.XGOVERNOR_WORKSPACE_ROOT;

	const missing: string[] = [];
	if (!bridgeUrl) missing.push("XGOVERNOR_BRIDGE_URL");
	if (!bridgeToken) missing.push("XGOVERNOR_BRIDGE_TOKEN");
	if (!workspaceRoot) missing.push("XGOVERNOR_WORKSPACE_ROOT");

	if (missing.length > 0) {
		throw new Error(
			`xgovernor-bridge-extension: missing required environment variable(s) [${missing.join(
				", ",
			)}]. This extension only works when launched by apps/runtime-pi's PiRuntime, which sets these on the spawned 'pi' process — see CONTRACT.md.`,
		);
	}

	return { bridgeUrl: bridgeUrl as string, bridgeToken: bridgeToken as string, workspaceRoot: workspaceRoot as string };
}

// ---------------------------------------------------------------------------
// Bridge wire types (see CONTRACT.md for the authoritative, documented copy
// of this contract; kept in sync by hand since the Rust side of this bridge,
// apps/runtime-pi/src/bridge.rs, is implemented independently from the same
// contract document).
// ---------------------------------------------------------------------------

interface BridgeErrorBody {
	error?: { kind?: string; message?: string };
}

type StatKind = "file" | "directory" | "symlink" | "other" | null;

interface StatResponse {
	exists: boolean;
	kind: StatKind;
	size_bytes: number | null;
	modified_at_ms: number | null;
}

interface ReadResponse {
	content_base64: string;
}

type WriteMode = "create" | "overwrite" | "atomic_overwrite";

interface WriteResponse {
	path: string;
}

interface ExecRequestBody {
	command: string;
	args: string[];
	cwd: string | null;
	env: Record<string, string> | null;
	timeout_ms: number | null;
	shell: string | null;
}

interface ExecResponse {
	stdout_base64: string;
	stderr_base64: string;
	exit_code: number | null;
	timed_out: boolean;
}

interface GlobResponse {
	paths: string[];
}

type GrepMode = "files_with_matches" | "content" | "count";

interface GrepRequestBody {
	query: string;
	base_dir: string;
	include: string | null;
	mode: GrepMode;
	head_limit: number | null;
}

interface GrepResponse {
	entries: string[];
}

interface WorkspaceRootResponse {
	path: string;
}

// ---------------------------------------------------------------------------
// BridgeClient: thin fetch() wrapper around the 8 bridge endpoints.
// ---------------------------------------------------------------------------

class BridgeClient {
	constructor(
		private readonly baseUrl: string,
		private readonly token: string,
	) {}

	private async post<TResponse>(endpoint: string, body: unknown, signal?: AbortSignal): Promise<TResponse> {
		const url = `${this.baseUrl}${endpoint}`;
		let response: Response;
		try {
			response = await fetch(url, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					Authorization: `Bearer ${this.token}`,
				},
				body: JSON.stringify(body ?? {}),
				signal,
			});
		} catch (error) {
			if (signal?.aborted) throw error;
			throw new Error(
				`xgovernor bridge request to ${endpoint} failed: could not reach ${url} (${(error as Error).message})`,
			);
		}

		if (!response.ok) {
			let kind = "unknown";
			let message = response.statusText || `HTTP ${response.status}`;
			try {
				const parsed = (await response.json()) as BridgeErrorBody;
				if (parsed?.error?.kind) kind = parsed.error.kind;
				if (parsed?.error?.message) message = parsed.error.message;
			} catch {
				// Non-JSON error body: fall back to statusText above.
			}
			throw new Error(`xgovernor bridge request to ${endpoint} failed (${response.status} ${kind}): ${message}`);
		}

		return (await response.json()) as TResponse;
	}

	workspaceRoot(): Promise<WorkspaceRootResponse> {
		return this.post<WorkspaceRootResponse>("/v1/workspace-root", {});
	}

	stat(path: string): Promise<StatResponse> {
		return this.post<StatResponse>("/v1/stat", { path });
	}

	read(path: string): Promise<ReadResponse> {
		return this.post<ReadResponse>("/v1/read", { path });
	}

	write(path: string, contentBase64: string, mode: WriteMode): Promise<WriteResponse> {
		return this.post<WriteResponse>("/v1/write", { path, content_base64: contentBase64, mode });
	}

	mkdir(path: string): Promise<Record<string, never>> {
		return this.post<Record<string, never>>("/v1/mkdir", { path });
	}

	exec(request: ExecRequestBody, signal?: AbortSignal): Promise<ExecResponse> {
		return this.post<ExecResponse>("/v1/exec", request, signal);
	}

	glob(pattern: string, baseDir: string | null, limit: number | null): Promise<GlobResponse> {
		return this.post<GlobResponse>("/v1/glob", { pattern, base_dir: baseDir, limit });
	}

	grep(request: GrepRequestBody): Promise<GrepResponse> {
		return this.post<GrepResponse>("/v1/grep", request);
	}
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

function notFoundError(op: string, absolutePath: string): Error {
	return new Error(`ENOENT: no such file or directory, ${op} '${absolutePath}'`);
}

/** Extension-based MIME sniffing. The bridge has no dedicated endpoint for
 * this (see CONTRACT.md "Implementation notes"); ReadOperations.detectImageMimeType
 * is optional and only used to decide whether to treat a file as an image. */
function detectImageMimeTypeByExtension(absolutePath: string): string | null {
	switch (path.extname(absolutePath).toLowerCase()) {
		case ".png":
			return "image/png";
		case ".jpg":
		case ".jpeg":
			return "image/jpeg";
		case ".gif":
			return "image/gif";
		case ".webp":
			return "image/webp";
		case ".bmp":
			return "image/bmp";
		case ".svg":
			return "image/svg+xml";
		default:
			return null;
	}
}

/** Minimal glob-pattern -> RegExp for FindOperations' `ignore` list (e.g.
 * ["**\/node_modules/**", ".git", "*.log"]). Deliberately small: supports
 * `**`, `*`, `?` only, no brace/character-class expansion. See CONTRACT.md
 * "Implementation notes" for why this is done client-side instead of on the
 * bridge (operation-protocol's GlobRequest has no ignore field). */
function globToRegExp(pattern: string): RegExp {
	let source = "";
	for (let i = 0; i < pattern.length; i++) {
		const char = pattern[i];
		if (char === "*" && pattern[i + 1] === "*") {
			source += ".*";
			i++;
		} else if (char === "*") {
			source += "[^/]*";
		} else if (char === "?") {
			source += "[^/]";
		} else if (".+^${}()|[]\\".includes(char as string)) {
			source += `\\${char}`;
		} else {
			source += char;
		}
	}
	return new RegExp(`(^|/)${source}(/|$)`);
}

function matchesAnyIgnorePattern(candidatePath: string, ignorePatterns: string[]): boolean {
	const normalized = candidatePath.split(path.sep).join("/");
	return ignorePatterns.some((pattern) => globToRegExp(pattern).test(normalized));
}

function posixBasename(candidatePath: string): string {
	const normalized = candidatePath.split(path.sep).join("/");
	const segments = normalized.split("/").filter((segment) => segment.length > 0);
	return segments.length > 0 ? (segments[segments.length - 1] as string) : normalized;
}

function sanitizeEnv(env: NodeJS.ProcessEnv | undefined): Record<string, string> | null {
	if (!env) return null;
	const result: Record<string, string> = {};
	for (const [key, value] of Object.entries(env)) {
		if (typeof value === "string") result[key] = value;
	}
	return Object.keys(result).length > 0 ? result : null;
}

/**
 * Rewrites a pi-supplied path from the daemon host's coordinate system into
 * the sandbox's. Pi's session cwd is the spawned `pi` process's
 * `process.cwd()` — i.e. the daemon's own working directory — and that host
 * path leaks into the model's tool arguments via pi's system prompt (see
 * pi_demo.md §11 "cwd 锚定" for the observed failure). Paths under the host
 * cwd have no meaning inside the sandbox, so they are remapped onto the
 * session's sandbox workspace root; paths already inside the workspace root
 * (or anywhere else in the sandbox) pass through untouched.
 *
 * Applied at the Operations boundary of every file/exec tool, so the
 * rewrite happens regardless of which tool (or the model) produced the path.
 */
function rewriteHostCwdPath(workspaceRoot: string, inputPath: string): string {
	const normalized = path.normalize(inputPath);
	if (!path.isAbsolute(normalized)) {
		// Pi resolves relative paths against its session cwd before calling
		// into Operations, so a relative path here is an unexpected fallback
		// case: anchor it to the sandbox workspace root.
		return path.resolve(workspaceRoot, normalized);
	}
	if (normalized === workspaceRoot || normalized.startsWith(workspaceRoot + path.sep)) {
		// Already sandbox-native (e.g. pi's own tool-root cwd): pass through.
		return normalized;
	}
	const hostCwd = process.cwd();
	if (normalized === hostCwd) {
		return workspaceRoot;
	}
	if (normalized.startsWith(hostCwd + path.sep)) {
		return path.join(workspaceRoot, normalized.slice(hostCwd.length + 1));
	}
	return normalized;
}

/** Cap on results fetched to synthesize a directory listing via /v1/glob("*",
 * dir). See CONTRACT.md "Implementation notes" on LsOperations.readdir. */
const LS_READDIR_LIMIT = 10_000;

// ---------------------------------------------------------------------------
// Pluggable *Operations implementations, backed by BridgeClient.
// ---------------------------------------------------------------------------

function createBridgeReadOperations(client: BridgeClient, workspaceRoot: string): ReadOperations {
	return {
		readFile: async (absolutePath) => {
			const response = await client.read(rewriteHostCwdPath(workspaceRoot, absolutePath));
			return Buffer.from(response.content_base64, "base64");
		},
		access: async (absolutePath) => {
			const stat = await client.stat(rewriteHostCwdPath(workspaceRoot, absolutePath));
			if (!stat.exists) throw notFoundError("access", absolutePath);
		},
		detectImageMimeType: async (absolutePath) =>
			detectImageMimeTypeByExtension(rewriteHostCwdPath(workspaceRoot, absolutePath)),
	};
}

function createBridgeWriteOperations(client: BridgeClient, workspaceRoot: string): WriteOperations {
	return {
		writeFile: async (absolutePath, content) => {
			// Pi's WriteOperations interface does not distinguish "create" from
			// "overwrite" (see CONTRACT.md "Implementation notes"); always use
			// atomic_overwrite, a safe superset for both cases.
			await client.write(
				rewriteHostCwdPath(workspaceRoot, absolutePath),
				Buffer.from(content, "utf8").toString("base64"),
				"atomic_overwrite",
			);
		},
		mkdir: async (dir) => {
			await client.mkdir(rewriteHostCwdPath(workspaceRoot, dir));
		},
	};
}

function createBridgeEditOperations(client: BridgeClient, workspaceRoot: string): EditOperations {
	const readOps = createBridgeReadOperations(client, workspaceRoot);
	const writeOps = createBridgeWriteOperations(client, workspaceRoot);
	return {
		readFile: readOps.readFile,
		writeFile: writeOps.writeFile,
		access: readOps.access,
	};
}

function createBridgeLsOperations(client: BridgeClient, workspaceRoot: string): LsOperations {
	return {
		exists: async (absolutePath) =>
			(await client.stat(rewriteHostCwdPath(workspaceRoot, absolutePath))).exists,
		stat: async (absolutePath) => {
			const sandboxPath = rewriteHostCwdPath(workspaceRoot, absolutePath);
			const stat = await client.stat(sandboxPath);
			if (!stat.exists) throw notFoundError("stat", absolutePath);
			return { isDirectory: () => stat.kind === "directory" };
		},
		readdir: async (absolutePath) => {
			const sandboxPath = rewriteHostCwdPath(workspaceRoot, absolutePath);
			// No dedicated list-directory endpoint exists on the bridge (nor, at
			// the operation-protocol level, an OperationFileSystem::list_dir
			// capability at all — see CONTRACT.md). Synthesize a listing via
			// /v1/glob("*", sandboxPath), relying on "*" not crossing "/" the
			// way shell globs conventionally don't, and de-duplicating basenames.
			const { paths } = await client.glob("*", sandboxPath, LS_READDIR_LIMIT);
			const names = new Set<string>();
			for (const entryPath of paths) names.add(posixBasename(entryPath));
			return Array.from(names);
		},
	};
}

function createBridgeFindOperations(client: BridgeClient, workspaceRoot: string): FindOperations {
	return {
		exists: async (absolutePath) =>
			(await client.stat(rewriteHostCwdPath(workspaceRoot, absolutePath))).exists,
		glob: async (pattern, cwd, options) => {
			const { paths } = await client.glob(pattern, rewriteHostCwdPath(workspaceRoot, cwd), options.limit ?? null);
			const filtered = options.ignore && options.ignore.length > 0
				? paths.filter((candidatePath) => !matchesAnyIgnorePattern(candidatePath, options.ignore))
				: paths;
			return filtered.slice(0, options.limit);
		},
	};
}

function createBridgeBashOperations(client: BridgeClient, workspaceRoot: string): BashOperations {
	return {
		exec: async (command, cwd, { onData, signal, timeout, env }) => {
			if (signal?.aborted) throw new Error("aborted");

			// See CONTRACT.md "Implementation notes": `command` carries the full
			// shell command line pi's bash tool produces, `args` is always
			// empty, and `shell: "bash"` tells the backend to run it through a
			// shell -- matching the exact usage already established in
			// crates/backend/src/local/exec.rs's own tests.
			const response = await client.exec(
				{
					command,
					args: [],
					cwd: cwd ? rewriteHostCwdPath(workspaceRoot, cwd) : null,
					env: sanitizeEnv(env),
					timeout_ms: timeout && timeout > 0 ? Math.round(timeout * 1000) : null,
					shell: "bash",
				},
				signal,
			);

			const stdout = Buffer.from(response.stdout_base64, "base64");
			const stderr = Buffer.from(response.stderr_base64, "base64");
			// The bridge's /v1/exec is a single blocking round trip, not a
			// stream (see CONTRACT.md "Implementation notes"): onData is
			// invoked at most twice, with the full stdout and then the full
			// stderr, once the command has already finished -- not
			// incrementally as output is produced.
			if (stdout.length > 0) onData(stdout);
			if (stderr.length > 0) onData(stderr);

			if (response.timed_out) throw new Error(`timeout:${timeout}`);

			return { exitCode: response.exit_code };
		},
	};
}

// ---------------------------------------------------------------------------
// Hand-rolled grep tool.
//
// Pi's SDK does expose a `GrepOperations` interface (isDirectory + readFile),
// but that seam is for delegating *file I/O* to a remote filesystem while
// pi's own createGrepTool() still does the pattern matching and directory
// walking locally. Our bridge's /v1/grep instead does full server-side
// matching (query/base_dir/include/mode/head_limit -> entries), so there is
// nothing for GrepOperations' narrow seam to usefully delegate -- exactly
// the situation the official Gondolin example is in, and exactly why it
// (and this extension) implement grep as a fully custom tool via
// pi.registerTool() instead of createGrepTool({ operations }).
// ---------------------------------------------------------------------------

const grepSchema = Type.Object({
	pattern: Type.String(),
	path: Type.Optional(Type.String()),
	glob: Type.Optional(Type.String()),
	mode: Type.Optional(
		Type.Union([Type.Literal("files_with_matches"), Type.Literal("content"), Type.Literal("count")]),
	),
	limit: Type.Optional(Type.Number()),
});

const DEFAULT_GREP_LIMIT = 100;

function resolveAgainstWorkspace(workspaceRoot: string, inputPath: string | undefined): string {
	const trimmed = (inputPath ?? ".").trim();
	if (trimmed.length === 0 || trimmed === ".") return workspaceRoot;
	return path.isAbsolute(trimmed)
		? rewriteHostCwdPath(workspaceRoot, trimmed)
		: path.resolve(workspaceRoot, trimmed);
}

function registerGrepTool(pi: ExtensionAPI, client: BridgeClient, workspaceRoot: string): void {
	pi.registerTool({
		name: "grep",
		label: "Grep",
		description:
			"Search file contents for a pattern via the xGovernor sandbox backend. Matching happens entirely server-side.",
		promptSnippet: "Search file contents for patterns",
		parameters: grepSchema,
		async execute(_id, params) {
			const baseDir = resolveAgainstWorkspace(workspaceRoot, params.path);
			const mode: GrepMode = params.mode ?? "content";
			const limit = params.limit && params.limit > 0 ? params.limit : DEFAULT_GREP_LIMIT;

			const result = await client.grep({
				query: params.pattern,
				base_dir: baseDir,
				include: params.glob ?? null,
				mode,
				head_limit: limit,
			});

			if (result.entries.length === 0) {
				return { content: [{ type: "text", text: "No matches found" }], details: undefined };
			}

			let text = result.entries.join("\n");
			if (result.entries.length >= limit) {
				text += `\n\n[${limit} matches limit reached]`;
			}
			return { content: [{ type: "text", text }], details: undefined };
		},
	});
}

// ---------------------------------------------------------------------------
// Extension entry point
// ---------------------------------------------------------------------------

export default function (pi: ExtensionAPI): void {
	const env = readBridgeEnv();
	const client = new BridgeClient(env.bridgeUrl, env.bridgeToken);
	const workspaceRoot = env.workspaceRoot;

	// Each *Operations object is built once at extension-load time -- unlike
	// the Gondolin example, there is no lazy async backend-startup step here
	// (apps/runtime-pi::Bridge is already listening, and this session's token
	// already registered, before PiRuntime spawns this `pi` process and sets
	// these env vars -- see apps/runtime-pi/src/bridge.rs's module doc).
	const readTool = createReadTool(workspaceRoot, { operations: createBridgeReadOperations(client, workspaceRoot) });
	const writeTool = createWriteTool(workspaceRoot, { operations: createBridgeWriteOperations(client, workspaceRoot) });
	const editTool = createEditTool(workspaceRoot, { operations: createBridgeEditOperations(client, workspaceRoot) });
	const bashTool = createBashTool(workspaceRoot, { operations: createBridgeBashOperations(client, workspaceRoot) });
	const lsTool = createLsTool(workspaceRoot, { operations: createBridgeLsOperations(client, workspaceRoot) });
	const findTool = createFindTool(workspaceRoot, { operations: createBridgeFindOperations(client, workspaceRoot) });

	// AgentTool.execute() has no `ctx` parameter, but ToolDefinition.execute()
	// (what pi.registerTool() expects) requires one -- this thin wrap adapts
	// between the two, same as the official Gondolin example does for every
	// tool it overrides.
	pi.registerTool({
		...readTool,
		async execute(id, params, signal, onUpdate) {
			return readTool.execute(id, params, signal, onUpdate);
		},
	});
	pi.registerTool({
		...writeTool,
		async execute(id, params, signal, onUpdate) {
			return writeTool.execute(id, params, signal, onUpdate);
		},
	});
	pi.registerTool({
		...editTool,
		async execute(id, params, signal, onUpdate) {
			return editTool.execute(id, params, signal, onUpdate);
		},
	});
	pi.registerTool({
		...bashTool,
		async execute(id, params, signal, onUpdate) {
			return bashTool.execute(id, params, signal, onUpdate);
		},
	});
	pi.registerTool({
		...lsTool,
		async execute(id, params, signal, onUpdate) {
			return lsTool.execute(id, params, signal, onUpdate);
		},
	});
	pi.registerTool({
		...findTool,
		async execute(id, params, signal, onUpdate) {
			return findTool.execute(id, params, signal, onUpdate);
		},
	});

	registerGrepTool(pi, client, workspaceRoot);
}
