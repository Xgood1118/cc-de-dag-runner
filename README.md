# dag-runner

一个只用 Python 标准库实现的 DAG 任务编排器：按 `depends_on` 做拓扑执行，
失败任务的下游自动 `skipped`，无关分支继续跑完；支持退避重试、单任务超时、
SQLite 状态持久化和 `--resume` 续跑。

## 用法

```powershell
python -m dag.cli tasks.json                 # 默认串行，1s/2s/4s 退避
python -m dag.cli tasks.json -j 4            # 同层最多 4 个任务并行
python -m dag.cli tasks.json --resume        # 从最近一次运行续跑
python -m dag.cli tasks.json --resume <RUN_ID>
python -m dag.cli --history                  # 列出历史运行
python -m dag.cli --history <RUN_ID>         # 按 run_id 查询某次运行
```

退出码：`0` 全部成功；`1` 有失败或跳过；`2` 存在依赖环，或任务定义/参数错误。

## 设计要点

- **拓扑执行**：按 `depends_on` 建图，Kahn 拓扑排序；只有依赖全部 `ok` 的任务才进入就绪队列。
- **环检测**：用强连通分量（Tarjan）找出全部环节点，所以交叉环里的节点也不会漏报。
  环上节点及其下游记为 `skipped`，其余无关分支照常执行完，最终退出码为 `2`。
  不会死循环，也不会静默跳过。
- **并行**：`-j/--concurrency` 控制并发度，默认 1。结果统一由主线程按任务 id 归集后输出，
  不会出现两个任务的输出交叉；同一任务的多次尝试输出也聚合在一起。
- **失败语义**：失败任务的所有传递下游标记 `skipped`，绝不执行；与失败任务无依赖关系的分支继续跑。
- **重试**：`retries: N` 表示首次执行之外再重试 N 次，退避 1s、2s、4s（`--retry-delays` 可覆盖）。
  重试次数用尽才算失败。超时同样计入一次尝试，失败原因记为 `timeout`。
- **状态**：SQLite（默认 `.pipeline/state.db`）保存 `run_id`、`task_id`、状态
  （`pending/running/ok/failed/skipped`）、尝试次数、开始/结束时间、耗时、输出摘要。
  每次运行生成唯一 `run_id`，可用 `--history <RUN_ID>` 查询。
- **续跑**：`--resume` 会继承上次已 `ok` 的任务（不重跑），只执行未成功且依赖已满足的任务；
  续跑本身是新的 `run_id`，并记录 `resumed_from`。

## 实跑输出

`python -m dag.cli tasks.json`（`tasks.json` 未改动）：

```text
[run] 20261116-015435-a0a795e2
[cycle] E -> F -> E
[A] OK (1 attempt, 0.235s)
    A ok
[B] OK (1 attempt, 0.235s)
    B ok
[C] OK (1 attempt, 0.235s)
    C ok
[D] OK (1 attempt, 0.281s)
    D ok
[G] FAILED (3 attempts, 3.750s) error=exit code 3
    G failed
[H] SKIPPED (0.000s) reason=dependency G failed
{
  "run_id": "20261116-015435-a0a795e2",
  "resumed_from": null,
  "started_at": "2026-11-16T01:54:35+08:00",
  "finished_at": "2026-11-16T01:54:40+08:00",
  "status": "cycle",
  "ok": false,
  "exit_code": 2,
  "cycles": [
    [
      "E",
      "F",
      "E"
    ]
  ],
  "cycle_nodes": [
    "E",
    "F"
  ],
  "cycle_components": [
    [
      "E",
      "F"
    ]
  ],
  "counts": {
    "pending": 0,
    "running": 0,
    "ok": 4,
    "failed": 1,
    "skipped": 3
  },
  "tasks": [
    {
      "id": "A",
      "status": "ok",
      "ok": true,
      "attempts": 1,
      "duration": 0.235,
      "started_at": "2026-11-16T01:54:35+08:00",
      "finished_at": "2026-11-16T01:54:36+08:00",
      "output": "A ok",
      "error": null,
      "reason": null,
      "resumed": false
    },
    {
      "id": "B",
      "status": "ok",
      "ok": true,
      "attempts": 1,
      "duration": 0.235,
      "started_at": "2026-11-16T01:54:36+08:00",
      "finished_at": "2026-11-16T01:54:36+08:00",
      "output": "B ok",
      "error": null,
      "reason": null,
      "resumed": false
    },
    {
      "id": "C",
      "status": "ok",
      "ok": true,
      "attempts": 1,
      "duration": 0.235,
      "started_at": "2026-11-16T01:54:36+08:00",
      "finished_at": "2026-11-16T01:54:36+08:00",
      "output": "C ok",
      "error": null,
      "reason": null,
      "resumed": false
    },
    {
      "id": "D",
      "status": "ok",
      "ok": true,
      "attempts": 1,
      "duration": 0.281,
      "started_at": "2026-11-16T01:54:36+08:00",
      "finished_at": "2026-11-16T01:54:36+08:00",
      "output": "D ok",
      "error": null,
      "reason": null,
      "resumed": false
    },
    {
      "id": "G",
      "status": "failed",
      "ok": false,
      "attempts": 3,
      "duration": 3.75,
      "started_at": "2026-11-16T01:54:36+08:00",
      "finished_at": "2026-11-16T01:54:40+08:00",
      "output": "G failed",
      "error": "exit code 3",
      "reason": null,
      "resumed": false
    },
    {
      "id": "H",
      "status": "skipped",
      "ok": false,
      "attempts": 0,
      "duration": 0.0,
      "started_at": "2026-11-16T01:54:40+08:00",
      "finished_at": "2026-11-16T01:54:40+08:00",
      "output": "",
      "error": null,
      "reason": "dependency G failed",
      "resumed": false
    },
    {
      "id": "E",
      "status": "skipped",
      "ok": false,
      "attempts": 0,
      "duration": 0.0,
      "started_at": "2026-11-16T01:54:35+08:00",
      "finished_at": "2026-11-16T01:54:35+08:00",
      "output": "",
      "error": null,
      "reason": "dependency cycle: E -> F -> E",
      "resumed": false
    },
    {
      "id": "F",
      "status": "skipped",
      "ok": false,
      "attempts": 0,
      "duration": 0.0,
      "started_at": "2026-11-16T01:54:35+08:00",
      "finished_at": "2026-11-16T01:54:35+08:00",
      "output": "",
      "error": null,
      "reason": "dependency cycle: E -> F -> E",
      "resumed": false
    }
  ]
}
```

对应退出码 `2`：环 `E -> F -> E` 被检出；`A → B/C → D` 全部成功；`G` 重试 3 次
（1s + 2s 退避）后失败；`H` 因依赖 `G` 被 `skipped`；环上的 `E/F` 被 `skipped`
且没有被执行。对同一个 `run_id` 再执行 `--resume` 时，已成功的任务会被继承而
不重跑，失败的 `G` 仍会重新执行。

## 测试

```powershell
python -m pytest tests -q
```

覆盖：拓扑顺序、环检测（含交叉环与自环）、下游 `skipped`、无关分支继续、
重试后成功、重试耗尽、超时、并行输出归集、SQLite 状态、resume 不重跑。
