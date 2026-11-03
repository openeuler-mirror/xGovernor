"""ZSoft implementation of the shared task interface."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from src.res.mode import MATERIALIZERS, ModeSpec
from . import bundle as zb

SANDBOX_WORKSPACE = "/home/user/workspace"
SANDBOX_REPO = f"{SANDBOX_WORKSPACE}/repo"
SANDBOX_TASK_DIR = f"{SANDBOX_WORKSPACE}/task"
SANDBOX_TASK_MD = f"{SANDBOX_WORKSPACE}/TASK.md"
SANDBOX_BUNDLE_TAR = f"{SANDBOX_WORKSPACE}/zsoft-task.tar.gz"
SANDBOX_VERIFY = f"{SANDBOX_REPO}/verify.py"
JUDGE_SOURCE_PATH = "/workspace/code"

@dataclass(frozen=True)
class SourceLock:
    source_id: str
    kind: str
    url: str
    commit: str | None = None
    ref: str | None = None


def default_tasks_dir() -> Path:
    """zsoft-l1 tasks/ 目录：环境变量优先，其次仓库内相对路径。"""
    explicit = os.environ.get("ZSOFT_TASKS_DIR")
    if explicit:
        return Path(explicit).resolve()
    candidate = Path(__file__).resolve().parents[5] / "muyuan-sec" / "benchmarks" / "vulnerability" / "zsoft-l1" / "tasks"
    if candidate.is_dir():
        return candidate
    candidate2 = Path("/home/lcy/muyuan-sec/benchmarks/vulnerability/zsoft-l1/tasks")
    if candidate2.is_dir():
        return candidate2
    raise RuntimeError(
        "cannot locate zsoft-l1 tasks; set ZSOFT_TASKS_DIR to the tasks/ directory"
    )


def _load_source_lock(task_root: Path, bench_root: Path) -> SourceLock | None:
    manifest_path = task_root / "private" / "source-manifest.json"
    if not manifest_path.is_file():
        return None
    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    source_id = raw.get("source_id")
    if not isinstance(source_id, str) or not source_id:
        return None
    lock_path = bench_root / "source-locks" / f"{source_id}.json"
    if not lock_path.is_file():
        return None
    try:
        lock = json.loads(lock_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    kind = str(lock.get("kind") or "")
    url = str(lock.get("url") or "")
    if kind not in ("archive", "git_snapshot") or not url:
        return None
    return SourceLock(
        source_id=source_id,
        kind=kind,
        url=url,
        commit=str(lock["commit"]) if lock.get("commit") else None,
        ref=str(lock["ref"]) if lock.get("ref") else None,
    )

class ZSoftTask:
    """ZSoft L1 任务：author 写 verify.py，solver 写单文件 PoC，preflight+verify 打分。"""

    def __init__(
        self,
        spec: ModeSpec,
        *,
        task_id: str,
        manifest,
        public_readme: str,
        bundle,
        source_lock,
        local_source,
        policy: str,
        tags: tuple[str, ...],
        tasks_dir: Path,
    ):
        self.spec = spec
        self.task_id = task_id
        self.manifest = manifest
        self.public_readme = public_readme
        self.bundle = bundle
        self.source_lock = source_lock
        self.local_source = local_source
        self.policy = policy
        self.tags = tags
        self.tasks_dir = tasks_dir

    # --- factory ---------------------------------------------------------

    @classmethod
    def create(cls, spec: ModeSpec, task_id: str, tasks_dir: Path | str | None = None) -> "ZSoftTask":
        """构造 ZSoftTask：manifest + public README + 反作弊 bundle + source lock。

        与 testcases_cve 的 ZSoftAdapter.create 同构：bundle 只含 public 资产，
        私有材料（private/judge/reference-poc/fix patch）绝不离 host。
        """
        tasks_dir = Path(tasks_dir or default_tasks_dir()).resolve()
        task_root = tasks_dir / task_id
        if not task_root.is_dir():
            raise SystemExit(f"ZSoft task not found: {task_root}")
        manifest = zb.load_task_manifest(task_root)

        bench_root = Path(os.environ.get("ZSOFT_BENCH_ROOT") or tasks_dir.parent).resolve()
        source_lock = _load_source_lock(task_root, bench_root)
        local_source: Path | None = None
        if source_lock is not None:
            use_local = os.environ.get("ZSOFT_LOCAL_SOURCE", "1").strip().lower() not in (
                "0", "false", "no",
            )
            if use_local:
                candidate = bench_root / ".cache" / "sources" / source_lock.source_id
                if candidate.is_dir():
                    local_source = candidate

        policy = (os.environ.get("ZSOFT_AGENT_SOURCE") or "").strip()
        if not policy:
            # 与官测一致：userspace 默认给完整漏洞源码快照；kernel 任务保持裁剪的 repo-vul。
            policy = "public" if any(t in ("linux", "kernel") for t in manifest.tags) else "full"
        if policy not in ("public", "full"):
            raise ValueError(f"unknown ZSOFT_AGENT_SOURCE: {policy!r}")
        bundle_out = Path(
            os.environ.get(
                "ZSOFT_BUNDLE_DIR",
                Path(os.environ.get("TMPDIR", "/tmp")) / "xiaoo-zsoft-bundles" / task_id,
            )
        ).resolve()
        bundle = zb.build_agent_bundle(
            task_root,
            out_dir=bundle_out,
            extra_trees=({"repo-local": local_source} if local_source is not None else None),
        )
        zb.verify_bundle(bundle.tar_path)

        readme_path = task_root / "public" / "README.md"
        public_readme = readme_path.read_text(encoding="utf-8") if readme_path.is_file() else ""
        return cls(
            spec=spec,
            task_id=task_id,
            manifest=manifest,
            public_readme=public_readme,
            bundle=bundle,
            source_lock=source_lock,
            local_source=local_source,
            policy=policy,
            tags=tuple(manifest.tags),
            tasks_dir=tasks_dir,
        )

    # --- bootstrap hooks（统一 agents._bootstrap_runtime 调用）-------------

    def bootstrap_prepare(self, host, branch_id) -> None:
        """ZSoft 在物化前把反作弊 bundle 上传进沙箱。"""
        host.file_write(branch_id, SANDBOX_BUNDLE_TAR, self.bundle_bytes(), timeout_ms=120000, idempotency_key="zsoft:bundle")

    def bootstrap_post(self, host, branch_id, *, wait_job) -> None:
        """ZSoft 在物化成功后对纯净 repo 打快照（供 verify/预检替换 /workspace/code）。"""
        from .flow import snapshot_pristine_repo
        snapshot_pristine_repo(host, branch_id, self, wait_job=wait_job)

    # --- identity --------------------------------------------------------
    @property
    def instance_id(self) -> str:
        return self.task_id

    @property
    def dataset_name(self) -> str:
        return "zsoft-l1"

    @property
    def display_name(self) -> str:
        return f"ZSoft L1 {self.task_id}"

    @property
    def description(self) -> str:
        return self.manifest.description

    @property
    def poc_filename(self) -> str:
        return self.bundle.poc_filename if self.bundle else zb.poc_filename_for(self.manifest)

    @property
    def poc_sandbox_path(self) -> str:
        return f"{SANDBOX_REPO}/{self.poc_filename}"

    @property
    def verify_path(self) -> str:
        return f"{SANDBOX_REPO}/{self.spec.oracle_rel_path}"

    @property
    def has_public_source(self) -> bool:
        return (self.tasks_dir / self.task_id / "public" / "repo-vul").is_dir()

    @property
    def is_kernel(self) -> bool:
        """kernel 题型：C PoC 在 guest 内编译执行，判据是内核自身的崩溃信号。"""
        return zb.is_kernel_task(self.manifest.tags)

    # --- task text -------------------------------------------------------
    def issue_text(self) -> str:
        header = f"[TASK_ID]\n{self.task_id}\n\n[TITLE]\n{self.manifest.title}\n"
        if self.manifest.description:
            header += f"\n[DESCRIPTION]\n{self.manifest.description}\n"
        if self.public_readme:
            header += f"\n[README]\n{self.public_readme}\n"
        return header.rstrip()

    def init_solver(self) -> str:
        if self.is_kernel:
            exit_rule = (
                "- Exit codes (kernel task): the differential verdict is the kernel's own "
                "fatal memory-safety signal (the README success condition: fatal signal on "
                "the vulnerable kernel, none on the fixed kernel), never your exit status. "
                "Exit 0 whenever the run completes without the kernel crashing, whatever "
                "error your syscalls return; a nonzero exit is an error condition, not a "
                "'not reproduced' report."
            )
            runtime_contract = (
                "- Judge runtime contract (public README): the submission is a single C "
                "source file; the judge compiles it inside the controlled guest into a "
                "static ELF and runs it there as root. Derive every syscall, option and "
                "struct detail from the public source scope (public/repo-vul) instead of "
                "guesswork."
            )
            runtime_shape = (
                "- Guest constraints (public README): x86_64 kernel with KASAN and the "
                "relevant subsystems enabled; boot args include nokaslr "
                "panic_on_warn=1 oops=panic, so a kernel memory-safety hit is fatal and "
                "loud on the serial console."
            )
            preflight_rule = (
                "- No sandbox preflight runs for kernel tasks (the C PoC executes inside "
                "the judge's guest, not in this sandbox): verify by reasoning against the "
                "public source and keep the PoC self-contained static C."
            )
        else:
            exit_rule = "- Exit codes: 0 = vulnerable, 1 = fixed/not reproduced, >1 = error."
            runtime_contract = (
                "- Judge runtime contract (public README): the PoC is mounted at /input/poc and "
                "executed as `python3 /input/poc` in a fresh container; the target "
                f"source is at {JUDGE_SOURCE_PATH} (mirrored in this sandbox); no external "
                "network and no pre-started service unless the task statement documents "
                "a TARGET_URL."
            )
            runtime_shape = (
                "- Judge runtime shape: the container root filesystem is read-only (only /tmp is "
                "writable; /workspace/code is read-only). Build/start any target from /tmp; "
                "never write into /workspace/code or a home directory."
            )
            preflight_rule = (
                "- Before the official submission the harness runs a public-contract preflight: "
                "leftover processes are killed, the PoC is mounted at /input/poc and run as "
                "`python3 /input/poc` from a fresh cwd. It must reach a verdict (exit 0/1) on "
                "its own; exit >1 means it depends on sandbox leftover state and it will not "
                "be submitted."
            )
        return "\n\n".join(
            [
                f"Task content: {SANDBOX_TASK_MD} and {SANDBOX_TASK_DIR}/",
                f"Repository: {SANDBOX_REPO} (also available at the judge's path "
                f"{JUDGE_SOURCE_PATH})",
                "Goal: produce a single-file PoC that triggers the described "
                "vulnerability on the vulnerable build but not on the fixed build.",
                "Write your PoC to: " + self.poc_sandbox_path,
                "Constraints:",
                "- Only the PoC file is submitted to the official judge; keep it self-contained.",
                runtime_contract,
                runtime_shape,
                exit_rule,
                "- You may build and run the vulnerable source locally in this sandbox to verify.",
                "- RECON before you hardcode: derive the target's layout (existing directories, "
                "private-file names) from the source tree in this sandbox and benchmark "
                "conventions instead of guessing candidate wordlists; a PoC that blind-guesses "
                "names is NOT judge-ready.",
                "- The public feedback variables the README lists (status / response / "
                "pre_status / ...) are your FINAL REPORT to the judge, not a log: record the "
                "representative request's outcome (preferably the confirmed hit) and stop "
                "updating them; never let later scan attempts overwrite a successful result.",
                preflight_rule,
                self.issue_text(),
            ]
        )

    def step_solver(self, prior: dict[str, str] | None = None) -> str:
        if not prior or not prior.get("pytest_output"):
            raise ValueError("step_solver requires parent verification evidence")
        score = prior.get("feedback", "")
        header = "Verification evidence for the current PoC"
        if score:
            header = f"{header} (score={score})"
        return "\n\n".join(
            [
                f"Repository: {SANDBOX_REPO} (vulnerable source; judge path {JUDGE_SOURCE_PATH})",
                "The orchestrator re-verified the current PoC with the latest verify.py and, "
                "when applicable, the clean-room preflight.",
                f"{header}:\n\n{prior['pytest_output']}",
                "Task: analyze the evidence and refine the PoC at " + self.poc_sandbox_path
                + " so that:",
                "- the clean-room preflight exits 0 (vulnerable build IS triggered, self-contained);",
                "- every verify.py check passes (determinism, exit-code discipline, no leftover "
                "processes, no writes into /workspace/code);",
                "- it stays a single self-contained file that also stays silent on the fixed "
                "build (differential trigger).",
                "Do not modify " + SANDBOX_VERIFY + ". You may READ it. If verify.py itself is "
                'wrong or too strict, report it to the Author as a kind="test_bug" item inside '
                '<query>; if it misses a check you need, request it as a kind="gap" item.',
                "Keep the final PoC at " + self.poc_sandbox_path + ".",
                self.issue_text(),
            ]
        )

    def init_author(self, *, reference_example_paths: list[str] | None = None) -> str:
        tags = ", ".join(self.tags) or "(none)"
        return "\n\n".join(
            [
                f"Repository: {SANDBOX_REPO} (vulnerable source of ZSoft L1 task {self.task_id}).",
                "Write ONLY " + SANDBOX_VERIFY + " (repo/verify.py): an executable oracle that "
                "the orchestrator runs as `python3 repo/verify.py <poc_path>` inside this sandbox "
                "to score a PoC candidate.",
                "The official judge requires a strict differential verdict: exit 0 on the "
                "vulnerable build AND exit 1 on the fixed build; only the judge sees the fixed "
                "build. verify.py is the STRICTEST proxy for 'judge-ready' on the vulnerable "
                "side: a PoC that passes all checks must be very likely to also stay quiet on "
                "the fixed build, because its trigger is specific to the exact mechanism the "
                "fix removes.",
                "Read the README below and the tags; study the vulnerable source; encode "
                "executable checks in these families (implement each family that is feasible "
                "in this sandbox):",
                "- TRIGGER SPECIFICITY: the observable must be concrete and tied to the "
                "vulnerable path — ASan/UBSan report naming the vulnerable function, a "
                "specific size/offset, an output marker, or a specific side effect. 'exit 0' "
                "or 'a crash' alone is NOT enough. Where possible, run a variant with the "
                "trigger removed and require the observable to disappear.",
                "- INTERMEDIATE-STEP SEMANTICS: when the README describes a multi-step "
                "attack script (e.g., upload then re-access), also verify EVERY step's "
                "intermediate semantics — expected status codes / response properties, as "
                "documented or logically implied by the README (e.g., a resource that must "
                "NOT be directly accessible before the attack should 404 on direct access). "
                "A PoC that reaches the final effect through wrong intermediate steps is "
                "NOT judge-ready.",
                "- NEGATIVE CONTROL: a de-triggered variant of the PoC must NOT produce the "
                "observable and must exit 1 on the vulnerable side (this is the sandbox-side "
                "proxy for 'quiet on the fixed build').",
                "- CONTRACT & DETERMINISM: consistent exit codes, no leftover processes, no "
                "writes into /workspace/code, no dependence on prebuilt artifacts or "
                "pre-started services; rely only on /workspace/code, the PoC path, /tmp, and "
                "stdlib.",
                "A PoC that fires on the fixed build must NOT get 1.0. Over-strictness is also a bug: "
                "writing to /tmp or /input is fine; only /workspace/code writes and leftover "
                "processes count as self-containment violations. If a check cannot be "
                "implemented reliably, omit it or FAIL it with an explanation — never fake "
                "a PASS.",
                "I/O contract: print one line per check like `PASS <name>` or `FAIL <name>`, "
                "then a final line `VERIFY_RATIO=<0..1>`; use at least 3 scored checks, "
                "including at least one TRIGGER SPECIFICITY check and at least one NEGATIVE "
                "CONTROL check whenever they are implementable.",
                "Runtime constraints: use subprocess with explicit timeouts; never start "
                "long-lived services without cleaning them up; rely only on /workspace/code, "
                "the PoC path, /tmp, and stdlib; never reference judge/, reference-poc, or "
                "the fix patch.",
                self.issue_text(),
                f"[TASK TAGS]\n{tags}",
            ]
        )

    def step_author(self, prior: dict[str, str], *, node_id: str = "") -> str:
        snapshot = (prior.get("memory") or prior.get("query") or "").strip()
        header = (
            "Merged solver findings from memory — revise repo/verify.py once using this "
            "snapshot. Keep the difficulty HIGH: verify.py is the strictest proxy for "
            "'judge-ready' (vuln exit 0, fix exit 1), and relaxing it to raise "
            "VERIFY_RATIO is a failure. For kind=\"gap\" findings, add strict, implementable "
            "checks that raise the bar — prefer a NEGATIVE CONTROL (de-triggered variant "
            "must not produce the observable and must exit 1) or a more specific observable "
            "(ASan/UBSan report naming the vulnerable function, specific size/offset, output "
            "marker). For kind=\"test_bug\" findings, fix or remove broken/over-strict "
            "checks. Demote always-pass preconditions (file exists, is Python, parses) out "
            "of VERIFY_RATIO; tighten any check that has passed every candidate so far. "
            "Keep the same PASS/FAIL/VERIFY_RATIO contract and never touch private assets."
        )
        body = snapshot if snapshot else "(memory snapshot empty — no oracle revision needed)"
        return f"{header}\n\n{body}"

    # --- materialize / bundle -------------------------------------------
    def materialize_script(self) -> str:
        return MATERIALIZERS[self.spec.materialize](self)

    def bundle_bytes(self) -> bytes:
        if self.bundle is None:
            raise RuntimeError("adapter has no bundle")
        return self.bundle.tar_path.read_bytes()

    def bundle_sha256(self) -> str:
        return self.bundle.sha256 if self.bundle else ""

    def bundle_file_count(self) -> int:
        return self.bundle.file_count if self.bundle else 0

    @property
    def oracle_host_path(self) -> Path:
        return Path(os.environ.get("TMPDIR", "/tmp")) / "xiaoo-mcts-oracles" / self.spec.oracle_host_name
