# SWE-bench Verified 批量运行

`scripts/run_swebench.sh` 统一执行环境准备、xGovernor Docker/pi 解题、trace 导出、SWE-bench 验证和资源清理。支持多个 instance ID，默认逐题运行；某题失败不会阻止下一题。每次调用使用新的输出目录，不覆盖历史实验。

## 在 xxyserver 使用

```bash
ssh xxyserver
cd /mnt/cz/xGovernor

./scripts/run_swebench.sh --instance-ids \
  astropy__astropy-12907 \
  astropy__astropy-14995
```

如果没有配置 API key，脚本会通过隐藏输入提示读取。默认使用 DeepSeek 官方 `https://api.deepseek.com/v1` 和 `deepseek-flash`。key 不写入脚本、命令行参数、结果文件或工具容器；只传给独立管控进程的环境变量。脚本不会删除用户自己提供的 key 文件。

也可以先在 shell 中安全输入，再用于批量任务：

```bash
read -r -s -p 'DeepSeek API key: ' DEEPSEEK_API_KEY; printf '\n'
export DEEPSEEK_API_KEY
./scripts/run_swebench.sh --instance-ids astropy__astropy-12907 astropy__astropy-14995
unset DEEPSEEK_API_KEY
```

以上 `read` 示例适用于 bash。自动化环境可以设置 `DEEPSEEK_API_KEY`，或通过 `--api-key-env MY_KEY` 使用其他环境变量；也可以使用 `--api-key-file /path/to/private/key`。非交互环境缺少凭证会直接报错，不会等待输入。不要将 key 放进命令行。

自定义兼容 OpenAI Chat Completions 的服务：

```bash
./scripts/run_swebench.sh \
  --instance-ids astropy__astropy-12907 \
  --api-base https://example.com/v1 \
  --model your-model-id \
  --api-key-env MODEL_API_KEY
```

启动前检查 `/models` 中是否存在指定模型，不会自动替换模型。不支持该接口的服务可显式使用 `--skip-model-check`。

## 从文件输入题目

`instances.txt` 可以逐行填写 ID，也可以使用逗号分隔，并支持 `#` 注释：

```text
# Astropy
astropy__astropy-12907
astropy__astropy-14995
```

```bash
./scripts/run_swebench.sh --instances-file instances.txt
```

可与 `--instance-ids` 混用；重复 ID 只运行一次。所有 ID 会在请求模型前检查是否存在于数据集中。

## 只验证补丁，不调用模型

验证已有单题补丁：

```bash
./scripts/run_swebench.sh --mode evaluate \
  --instance-ids astropy__astropy-12907 \
  --patch /path/to/model.patch
```

批量验证使用 SWE-bench 格式的 JSON/JSONL，每条记录包含 `instance_id`、`model_name_or_path`、`model_patch`：

```bash
./scripts/run_swebench.sh --mode evaluate \
  --instances-file instances.txt --predictions predictions.jsonl
```

检查未修改代码的基线：

```bash
./scripts/run_swebench.sh --mode baseline \
  --instance-ids astropy__astropy-12907 astropy__astropy-14995
```

基线模式期望 FAIL_TO_PASS 测试失败、PASS_TO_PASS 测试通过，结果标为 `baseline_verified`。验证模式和基线模式不需要 API key、pi 或运行中的 xGovernor。

## 输出和清理

`pi-trace` **只保留 pi 的原始上下文 `trace.jsonl`**：包括 pi 会话元数据、用户消息、模型返回内容、工具调用和工具结果。不再生成 HTML、事件流副本、独立补丁、预测文件、报告或汇总文件。

默认路径是仓库同级的 `pi-trace/<UTC时间-随机ID>/<instance-id>/trace.jsonl`，在 xxyserver 即 `/mnt/cz/pi-trace/`。可以通过 `--output-dir` 指定根目录、`--run-id` 指定新的子目录名；已有运行目录不会覆盖。

```text
<run-id>/
  astropy__astropy-12907/
    trace.jsonl
  astropy__astropy-14995/
    trace.jsonl
```

构建文件、daemon 状态、补丁和判分数据只放在系统临时目录中，处理完每题后删除。运行结果、汇总和错误诊断只在终端输出。`--mode evaluate`、`--mode baseline` 不产生 pi 上下文，因此不留下输出文件；`--predictions` 或 `--patch` 接收用户自行提供的文件，脚本不再额外导出这些文件。

每题创建独立 daemon、端口、session 和容器。正常结束、错误、Ctrl-C、SIGTERM 均尝试保留已有上下文，并清理本次临时文件、session、进程和容器。尚未产生 pi 上下文的失败不保留空目录。镜像和依赖缓存不属于 trace，仍保留供后续运行复用。SIGKILL、机器掉电等无法执行清理逻辑的情况不在此保证内。

退出码：`0` 表示所有题目通过（或基线符合预期）；`1` 表示至少一题未解决/执行失败；`2` 表示参数或启动前检查失败；`130` 表示中断。

## 依赖与配置

- Linux、本地 Docker socket、cgroup v2。模型请求由宿主机发出；agent 工具容器和验收容器均使用 `network=none`。
- Python **3.11+**。入口自动准备独立 venv 并安装 `scripts/swebench-requirements.txt`，固定使用 `swebench==4.1.0` 的测试规范和判分 API。
- 数据集默认 `SWE-bench/SWE-bench_Verified`，优先复用仓库同级 `.cache/huggingface/datasets` 中对应的 Arrow 缓存，否则下载。可用 `--dataset-file` 指定完整的 JSON/JSONL/Arrow 数据集，或 `--dataset-cache` 指定缓存。
- 解题和验收都直接使用 `swebench/sweb.eval...` 官方实例镜像，不构建派生镜像。xGovernor 在解题容器启动后写入 helper、准备 `/testbed` 和 `/home/agent` 的 UID 10001 权限；运行脚本在容器内恢复到 `base_commit`。准备过程不把参考补丁、官方测试补丁放进 agent 容器。
- 验收使用单独的官方实例容器，应用生成补丁，然后运行官方 `TestSpec.eval_script` 和 `get_eval_report`。脚本自行管理 Docker 容器，未调用 harness 的整个 CLI。离线环境缺少构建依赖时会在终端显示失败信息，需要补全镜像环境；不会用其他测试命令悄悄替代官方脚本。

构建管控进程和安装 pi 扩展依赖：

```bash
cargo build --locked -p xgovernor-server -p xgovernor-runtime-pi \
  --bin xgovernor-server --bin pi-worker
(cd apps/runtime-pi/extension && npm ci)
```

自动查找二进制顺序：`target/release`、`target/debug`、仓库同级 `.cache/xgovernor-pi-target/debug`。可用 `--server-bin`、`--worker-bin`、`--pi`、`--extension-dir` 显式覆盖。pi 优先使用扩展目录 `node_modules/.bin/pi`，避免全局版本漂移。

运行脚本要求 server/worker 支持 `provider_options.workspace_root` 和 `bootstrap`，明确使用 `/testbed` 与 `network_mode=none`，不再回退到旧版固定目录构建。提交 prompt 前检查容器工作目录、网络、UID 和挂载。

正式部署应使用同一版本源码编译的 server 和 worker 二进制。

常用限制：`--max-turns 80`、`--agent-timeout 1800`、`--test-timeout 1800`、`--prepare-timeout 1800`、`--cpus 2`、`--memory-mb 4096`。当前按顺序执行多个 ID；不提供批量并发或中断续跑。事件流断开时保留已有记录并将该题标记失败，不重新提交 prompt。

`XGOVERNOR_SWE_VENV` 可指定 venv 目录；`XGOVERNOR_SWE_PYTHON` 可指定已经准备好的 Python。运行单元测试：

```bash
python3 -m unittest discover -s scripts -p 'test_run_swebench.py' -v
```


## Docker 自定义工作目录

pi 和 xiaoO 均支持在会话打开请求中指定**容器内**的工作目录：

```json
{
  "workspace": {"kind": "daemon_default"},
  "deployment": {
    "options": {
      "provider_options": {
        "workspace_root": "/testbed",
        "network_mode": "none",
        "bootstrap": true
      }
    }
  }
}
```

不传 `workspace_root` 时保持 `/workspace`。路径必须是规范的绝对路径（允许空格和中文，不接受 `..`、`.`、重复分隔符、末尾斜杠或控制字符）；这不是宿主机目录挂载。命令未指定 cwd 时、相对路径解析和默认 glob 均使用该目录。重连、复用容器及恢复快照时要求请求目录与原目录一致；旧快照默认仍是 `/workspace`。

设置 `bootstrap: true` 后，xGovernor 将编译时内嵌的 helper 写入容器、准备工作目录及 `/home/agent` 的 UID/GID `10001:10001` 所有权，并设置 helper 的 Python 路径。所有改动仅发生在容器可写层，不执行 `docker build` 或 `docker commit`。SWE-bench 运行脚本默认启用此选项，并直接使用官方镜像的内容 ID。

初始化要求镜像已有 `/bin/sh`、`/bin/bash`、`/usr/bin/sleep` 和 Python 3.11+（自动检查 `/usr/local/bin/python3`、`/usr/bin/python3`、`/opt/miniconda3/bin/python`）。不自动下载依赖。初始化目录不能为 `/` 或符号链接；递归修改所有权不会跟随仓库中的符号链接。测试依赖仍由镜像提供，SWE-bench 的 Python 测试使用 `conda run --no-capture-output -n testbed ...`。

初始化由管控进程执行 root 命令，容器仅增加 `CHOWN` capability 到 bounding set 用于改所有权；agent 默认仍是 UID 10001，有效与 permitted capabilities 均为 0，保留 `no-new-privileges`、无挂载和所选网络策略。失败时新容器被移除，不向 agent 开放工具。默认 `bootstrap: false` 保持已有预配置镜像的行为及全部 capabilities 被移除的策略。
