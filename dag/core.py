# -*- coding: utf-8 -*-
"""任务编排核心。

基于 ``depends_on`` 构建 DAG，支持：

- 拓扑执行；同层任务可并行（并发度可配置，默认 1），输出按任务 id 归集
- 环检测：报出环上的全部节点；环上节点及其下游标记 skipped，其余分支继续，
  最终以退出码 2 结束（不会死循环，也不会静默跳过）
- 失败后传递下游 skipped，无关分支继续执行
- ``retries`` 字段：按 1s / 2s / 4s 退避重试，次数用尽才算失败
- 单任务超时，超时视为失败并记 ``timeout``
- SQLite 状态持久化、run_id 历史查询、``resume`` 续跑

只使用 Python 标准库。
"""
import heapq
import json
import os
import signal
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

from .state import StateStore, new_run_id, now_iso

PENDING = "pending"
RUNNING = "running"
OK = "ok"
FAILED = "failed"
SKIPPED = "skipped"

DEFAULT_TIMEOUT = 60
DEFAULT_RETRY_DELAYS = (1.0, 2.0, 4.0)
OUTPUT_LIMIT = 2000


class DagError(Exception):
    """任务定义或运行参数错误（CLI 以退出码 2 结束）。"""


class CycleError(DagError):
    """依赖图中存在环。``nodes`` 是环上的全部节点。"""

    def __init__(self, cycles, nodes=None):
        self.cycles = [list(cycle) for cycle in cycles]
        if nodes is None:
            nodes = {node for cycle in self.cycles for node in cycle}
        self.nodes = sorted(nodes)
        detail = "; ".join(" -> ".join(cycle) for cycle in self.cycles)
        super(CycleError, self).__init__(
            "检测到依赖环，涉及节点: %s（%s）"
            % (", ".join(self.nodes), detail or "未知环")
        )


# ---------------------------------------------------------------------------
# 任务加载与图构建
# ---------------------------------------------------------------------------

def load_tasks(path):
    """读取任务文件，返回 tasks 列表。"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError) as exc:
        raise DagError("无法读取任务文件 %s: %s" % (path, exc))
    if not isinstance(data, dict) or not isinstance(data.get("tasks"), list):
        raise DagError("任务文件格式错误：缺少 tasks 数组")
    return data["tasks"]


def _build_graph(tasks):
    """校验任务定义并构建邻接关系。

    返回 ``(order, by_id, deps, dependents)``，order 保持文件中的声明顺序。
    """
    if not isinstance(tasks, (list, tuple)):
        raise DagError("tasks 必须是列表")

    order = []
    by_id = {}
    deps = {}

    for index, task in enumerate(tasks):
        if not isinstance(task, dict):
            raise DagError("第 %d 个任务不是对象" % (index + 1))
        tid = task.get("id")
        if not isinstance(tid, str) or not tid.strip():
            raise DagError("第 %d 个任务缺少合法的 id" % (index + 1))
        if tid in by_id:
            raise DagError("重复的任务 id: %s" % tid)
        cmd = task.get("cmd")
        if not isinstance(cmd, str) or not cmd.strip():
            raise DagError("任务 %s 缺少 cmd" % tid)

        raw_deps = task.get("depends_on", [])
        if raw_deps is None:
            raw_deps = []
        if isinstance(raw_deps, str) or not isinstance(raw_deps, (list, tuple)):
            raise DagError("任务 %s 的 depends_on 必须是数组" % tid)
        dep_list = []
        for dep in raw_deps:
            if not isinstance(dep, str) or not dep.strip():
                raise DagError("任务 %s 的依赖必须是任务 id" % tid)
            if dep not in dep_list:
                dep_list.append(dep)

        retries = task.get("retries", 0)
        if isinstance(retries, bool) or not isinstance(retries, int) or retries < 0:
            raise DagError("任务 %s 的 retries 必须是非负整数" % tid)

        timeout = task.get("timeout", DEFAULT_TIMEOUT)
        if timeout is not None:
            if (isinstance(timeout, bool)
                    or not isinstance(timeout, (int, float))
                    or timeout <= 0):
                raise DagError("任务 %s 的 timeout 必须是正数" % tid)

        by_id[tid] = task
        order.append(tid)
        deps[tid] = dep_list

    dependents = {tid: [] for tid in order}
    for tid in order:
        for dep in deps[tid]:
            if dep not in by_id:
                raise DagError("任务 %s 依赖不存在的任务 %s" % (tid, dep))
            dependents[dep].append(tid)
    return order, by_id, deps, dependents


def _strongly_connected(order, deps):
    """迭代式 Tarjan：返回所有强连通分量（节点数 >= 1）。"""
    index = {}
    low = {}
    on_stack = set()
    stack = []
    components = []
    counter = [0]

    for root in order:
        if root in index:
            continue
        index[root] = low[root] = counter[0]
        counter[0] += 1
        stack.append(root)
        on_stack.add(root)
        work = [(root, iter(deps[root]))]

        while work:
            node, iterator = work[-1]
            try:
                neighbor = next(iterator)
            except StopIteration:
                work.pop()
                if work:
                    parent = work[-1][0]
                    low[parent] = min(low[parent], low[node])
                if low[node] == index[node]:
                    component = []
                    while True:
                        member = stack.pop()
                        on_stack.discard(member)
                        component.append(member)
                        if member == node:
                            break
                    components.append(component)
                continue

            if neighbor not in index:
                index[neighbor] = low[neighbor] = counter[0]
                counter[0] += 1
                stack.append(neighbor)
                on_stack.add(neighbor)
                work.append((neighbor, iter(deps[neighbor])))
            elif neighbor in on_stack:
                low[node] = min(low[node], index[neighbor])

    return components


def _cycle_in_component(component, deps):
    """在强连通分量里找一个具体环（该分量内必然存在）。"""
    members = set(component)
    if len(component) == 1:
        node = component[0]
        if node in deps[node]:
            return [node, node]

    # DFS 找到第一条回边；同一分量内必然存在。
    start = min(component)
    path = [start]
    position = {start: 0}
    visited = {start}
    stack = [(start, iter([d for d in deps[start] if d in members]))]

    while stack:
        node, iterator = stack[-1]
        try:
            neighbor = next(iterator)
        except StopIteration:
            stack.pop()
            position.pop(node, None)
            path.pop()
            continue
        if neighbor in position:
            return _canonical_cycle(path[position[neighbor]:] + [neighbor])
        if neighbor in visited:
            continue
        visited.add(neighbor)
        position[neighbor] = len(path)
        path.append(neighbor)
        stack.append((neighbor, iter([d for d in deps[neighbor] if d in members])))

    return []


def _find_cycles(order, deps):
    """返回 ``(cycles, cycle_nodes, cyclic_components)``。

    一个强连通分量就是“互相可达”的最大节点集合；任何落在环上的节点都必然
    属于某个规模 > 1 的分量（或存在自环），因此这样不会漏掉环上的节点。
    每个分量返回一个代表环用于展示，``cycle_nodes`` 是全部环节点的并集。
    """
    cycles = []
    cycle_nodes = set()
    cyclic_components = []
    for component in _strongly_connected(order, deps):
        if len(component) == 1 and component[0] not in deps[component[0]]:
            continue
        cycle = _cycle_in_component(component, deps)
        if cycle:
            cycles.append(cycle)
            cycle_nodes.update(component)
            cyclic_components.append(sorted(component))
    cycles.sort(key=lambda cycle: (len(cycle), cycle))
    cyclic_components.sort()
    return cycles, sorted(cycle_nodes), cyclic_components


def _canonical_cycle(cycle):
    """把环旋转到最小节点开头，便于稳定输出。"""
    nodes = cycle[:-1]
    if not nodes:
        return cycle
    smallest = min(nodes)
    index = nodes.index(smallest)
    rotated = nodes[index:] + nodes[:index]
    return rotated + [rotated[0]]


def find_cycles(tasks):
    """公开接口：返回所有环（每个环是首尾相接的 id 列表）。"""
    order, _by_id, deps, _dependents = _build_graph(tasks)
    cycles, _nodes, _components = _find_cycles(order, deps)
    return cycles


def find_cycle_nodes(tasks):
    """公开接口：返回所有位于环上的节点（交叉环里的节点也不会漏）。"""
    order, _by_id, deps, _dependents = _build_graph(tasks)
    _cycles, nodes, _components = _find_cycles(order, deps)
    return nodes


def _topological_order(order, deps, dependents, exclude=None):
    """Kahn 拓扑排序；exclude 中的节点及其依赖边被移除。"""
    exclude = set(exclude or ())
    index = {tid: i for i, tid in enumerate(order)}
    indegree = {}
    ready = []
    for tid in order:
        if tid in exclude:
            continue
        indegree[tid] = sum(1 for dep in deps[tid] if dep not in exclude)
        if indegree[tid] == 0:
            heapq.heappush(ready, (index[tid], tid))

    result = []
    while ready:
        _i, tid = heapq.heappop(ready)
        result.append(tid)
        for child in dependents[tid]:
            if child in exclude:
                continue
            indegree[child] -= 1
            if indegree[child] == 0:
                heapq.heappush(ready, (index[child], child))

    if len(result) != len(order) - len(exclude):
        cycles, nodes, _components = _find_cycles(order, deps)
        raise CycleError(cycles, nodes)
    return result


def topological_sort(tasks, exclude=None):
    """公开接口：返回拓扑序；有环时抛 ``CycleError``。"""
    order, _by_id, deps, dependents = _build_graph(tasks)
    cycles, nodes, _components = _find_cycles(order, deps)
    if cycles:
        raise CycleError(cycles, nodes)
    return _topological_order(order, deps, dependents, exclude=exclude)


topological_order = topological_sort


# ---------------------------------------------------------------------------
# 单任务执行
# ---------------------------------------------------------------------------

def _terminate_process(process):
    """超时后杀掉整棵进程树（Windows 下 shell 会派生子进程）。"""
    if os.name == "nt":
        try:
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(process.pid)],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10,
            )
        except Exception:
            pass
        try:
            process.kill()
        except Exception:
            pass
    else:
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
        except Exception:
            try:
                process.kill()
            except Exception:
                pass


def _run_once(task, cwd=None):
    """执行一次命令，返回 {ok, output, error, timed_out, duration}。"""
    timeout = task.get("timeout", DEFAULT_TIMEOUT)
    kwargs = dict(
        shell=True, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, errors="replace",
    )
    if os.name == "nt" and hasattr(subprocess, "CREATE_NEW_PROCESS_GROUP"):
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True

    started = time.monotonic()
    try:
        process = subprocess.Popen(task["cmd"], **kwargs)
    except OSError as exc:
        return {
            "ok": False,
            "output": "",
            "error": "spawn error: %s" % exc,
            "timed_out": False,
            "duration": time.monotonic() - started,
        }

    try:
        output, _ = process.communicate(timeout=timeout)
        duration = time.monotonic() - started
        output = (output or "").strip()
        if process.returncode == 0:
            return {"ok": True, "output": output, "error": None,
                    "timed_out": False, "duration": duration}
        return {
            "ok": False,
            "output": output,
            "error": "exit code %d" % process.returncode,
            "timed_out": False,
            "duration": duration,
        }
    except subprocess.TimeoutExpired as exc:
        _terminate_process(process)
        try:
            remainder, _ = process.communicate(timeout=5)
        except Exception:
            remainder = ""
        duration = time.monotonic() - started
        partial = (exc.output or "") + (remainder or "")
        partial = partial.strip()
        text = (partial + "\n[timeout]").strip() if partial else "timeout"
        return {"ok": False, "output": text, "error": "timeout",
                "timed_out": True, "duration": duration}


def run_task(task, cwd=None):
    """执行单个任务的 cmd，返回 (ok, output)。保留原有函数签名。"""
    result = _run_once(task, cwd)
    return result["ok"], result["output"]


def _execute_task(task, cwd, retry_delays):
    """带退避重试地执行一个任务。在线程池中运行，不写共享状态。"""
    retries = task.get("retries", 0)
    attempts = 0
    started = time.monotonic()
    result = None
    while True:
        attempts += 1
        result = _run_once(task, cwd)
        if result["ok"]:
            break
        if attempts > retries:
            break
        if retry_delays:
            delay = retry_delays[min(attempts - 1, len(retry_delays) - 1)]
            if delay > 0:
                time.sleep(delay)
    result = dict(result)
    result["attempts"] = attempts
    result["retries_used"] = attempts - 1
    result["duration"] = time.monotonic() - started
    return result


# ---------------------------------------------------------------------------
# 编排
# ---------------------------------------------------------------------------

def _summarize(text, limit=OUTPUT_LIMIT):
    """生成输出摘要：去掉首尾空白并截断，避免状态库无限膨胀。"""
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "...[truncated]"


def _emit(echo, event):
    if echo is not None:
        echo(event)


def _record_event(record):
    return {
        "kind": "task",
        "id": record["id"],
        "status": record["status"],
        "attempts": record["attempts"],
        "duration": round(record["duration"], 3),
        "output": record["output"],
        "error": record["error"],
        "reason": record["reason"],
        "resumed": record["resumed"],
    }


def run_dag(tasks, cwd=None, concurrency=1, store=None, run_id=None,
            resume_from=None, task_file=None, echo=None,
            retry_delays=DEFAULT_RETRY_DELAYS):
    """执行整个 DAG，返回结构化摘要。

    有环时不抛出异常：环上节点及其下游记为 skipped，其余分支照常执行，
    摘要里的 ``exit_code`` 为 2。任务定义错误则抛 ``DagError``。
    """
    order, by_id, deps, dependents = _build_graph(tasks)
    cycles, cycle_node_list, cyclic_components = _find_cycles(order, deps)
    cycle_nodes = set(cycle_node_list)
    acyclic_order = _topological_order(order, deps, dependents, exclude=cycle_nodes)

    try:
        concurrency = int(concurrency)
    except (TypeError, ValueError):
        raise DagError("并发度必须是整数")
    if concurrency < 1:
        raise DagError("并发度必须 >= 1")
    delays = tuple(float(delay) for delay in (retry_delays or ()))

    run_id = run_id or new_run_id()
    started_at = now_iso()

    previous = {}
    if resume_from:
        if store is None:
            raise DagError("resume 需要状态库")
        previous = store.load_tasks(resume_from)
        if not previous:
            raise DagError("找不到 run_id=%s 的历史状态" % resume_from)

    if store is not None:
        if store.get_run(run_id) is not None:
            raise DagError("run_id 已存在: %s" % run_id)
        store.create_run(run_id, started_at, task_file=task_file,
                         resumed_from=resume_from)

    records = {}
    for tid in order:
        records[tid] = {
            "id": tid, "status": PENDING, "attempts": 0, "duration": 0.0,
            "output": "", "error": None, "reason": None,
            "started_at": None, "finished_at": None, "resumed": False,
        }

    # 1) 续跑：已成功的任务直接继承，不再执行。
    resumed_ids = set()
    if previous:
        for tid in acyclic_order:
            old = previous.get(tid)
            if not old or old.get("status") != OK:
                continue
            record = records[tid]
            record.update({
                "status": OK,
                "attempts": int(old.get("attempts") or 0),
                "duration": float(old.get("duration") or 0.0),
                "output": old.get("output_summary") or "",
                "started_at": old.get("started_at"),
                "finished_at": old.get("finished_at"),
                "reason": "resumed from %s" % resume_from,
                "resumed": True,
            })
            resumed_ids.add(tid)

    # 2) 环上的节点不执行，直接 skipped。
    component_text = {}
    for component in cyclic_components:
        members = set(component)
        for cycle in cycles:
            if set(cycle[:-1]) & members:
                text = " -> ".join(cycle)
                for node in component:
                    component_text.setdefault(node, text)
    for tid in cycle_nodes:
        record = records[tid]
        record.update({
            "status": SKIPPED,
            "reason": "dependency cycle: %s" % component_text.get(tid, ""),
            "started_at": started_at,
            "finished_at": started_at,
        })

    _emit(echo, {"kind": "run_start", "run_id": run_id,
                 "resumed_from": resume_from, "started_at": started_at})
    for cycle in cycles:
        _emit(echo, {"kind": "cycle", "cycle": list(cycle),
                     "nodes": sorted(set(cycle) - {cycle[-1]})})

    if store is not None:
        for tid in acyclic_order:
            record = records[tid]
            if tid in resumed_ids:
                store.set_task(
                    run_id, tid, OK, attempts=record["attempts"],
                    started_at=record["started_at"],
                    finished_at=record["finished_at"],
                    duration=record["duration"],
                    output_summary=_summarize(record["output"]),
                    reason=record["reason"],
                )
            else:
                store.set_task(run_id, tid, PENDING)
        for tid in cycle_nodes:
            record = records[tid]
            store.set_task(run_id, tid, SKIPPED,
                           started_at=record["started_at"],
                           finished_at=record["finished_at"],
                           output_summary="", reason=record["reason"])

    pending = {tid for tid in acyclic_order if records[tid]["status"] == PENDING}
    running = {}
    order_index = {tid: i for i, tid in enumerate(acyclic_order)}
    pool = ThreadPoolExecutor(max_workers=concurrency)
    try:
        while pending or running:
            # 失败/跳过的传递下游：立即 skipped，但绝不执行。
            changed = True
            while changed:
                changed = False
                for tid in acyclic_order:
                    if tid not in pending:
                        continue
                    bad = [dep for dep in deps[tid]
                           if records[dep]["status"] in (FAILED, SKIPPED)]
                    if not bad:
                        continue
                    record = records[tid]
                    record["status"] = SKIPPED
                    record["reason"] = "dependency %s %s" % (
                        ", ".join(bad), records[bad[0]]["status"])
                    record["started_at"] = now_iso()
                    record["finished_at"] = record["started_at"]
                    pending.discard(tid)
                    if store is not None:
                        store.set_task(run_id, tid, SKIPPED,
                                       started_at=record["started_at"],
                                       finished_at=record["finished_at"],
                                       reason=record["reason"])
                    _emit(echo, _record_event(record))
                    changed = True

            slots = concurrency - len(running)
            if slots > 0:
                ready = [
                    tid for tid in acyclic_order
                    if tid in pending
                    and all(records[dep]["status"] == OK for dep in deps[tid])
                ]
                for tid in ready[:slots]:
                    pending.discard(tid)
                    record = records[tid]
                    record["status"] = RUNNING
                    record["started_at"] = now_iso()
                    if store is not None:
                        store.set_task(run_id, tid, RUNNING,
                                       started_at=record["started_at"])
                    future = pool.submit(_execute_task, by_id[tid], cwd, delays)
                    running[future] = tid

            if running:
                done, _ = wait(list(running), return_when=FIRST_COMPLETED)
                finished = sorted(done, key=lambda item: order_index[running[item]])
                for future in finished:
                    tid = running.pop(future)
                    record = records[tid]
                    try:
                        result = future.result()
                    except Exception as exc:  # 线程内未预料的异常
                        result = {
                            "ok": False, "output": "", "attempts": 1,
                            "error": "internal error: %r" % exc,
                            "duration": 0.0,
                        }
                    record["attempts"] = int(result.get("attempts") or 0)
                    record["duration"] = float(result.get("duration") or 0.0)
                    record["output"] = _summarize(result.get("output"))
                    record["error"] = result.get("error")
                    record["status"] = OK if result.get("ok") else FAILED
                    record["finished_at"] = now_iso()
                    if store is not None:
                        store.set_task(
                            run_id, tid, record["status"],
                            attempts=record["attempts"],
                            started_at=record["started_at"],
                            finished_at=record["finished_at"],
                            duration=record["duration"],
                            output_summary=record["output"],
                            error=record["error"],
                        )
                    _emit(echo, _record_event(record))
            elif pending:
                # 防御：理论上不会走到这里；避免静默丢任务或死循环。
                for tid in list(pending):
                    record = records[tid]
                    record["status"] = SKIPPED
                    record["reason"] = "unresolved dependency"
                    record["started_at"] = now_iso()
                    record["finished_at"] = record["started_at"]
                    pending.discard(tid)
                    if store is not None:
                        store.set_task(run_id, tid, SKIPPED,
                                       started_at=record["started_at"],
                                       finished_at=record["finished_at"],
                                       reason=record["reason"])
                    _emit(echo, _record_event(record))
    finally:
        pool.shutdown(wait=True)

    # 理论上运行结束不会再有 pending/running；兜底标记，保证摘要完整。
    for tid in acyclic_order:
        record = records[tid]
        if record["status"] == RUNNING:
            record["status"] = FAILED
            record["error"] = "interrupted"
            record["finished_at"] = now_iso()
        elif record["status"] == PENDING:
            record["status"] = SKIPPED
            record["reason"] = "unresolved dependency"
            record["finished_at"] = now_iso()

    finished_at = now_iso()
    counts = Counter(record["status"] for record in records.values())
    all_ok = all(record["status"] == OK for record in records.values())
    if cycles:
        run_status = "cycle"
        exit_code = 2
    elif all_ok:
        run_status = OK
        exit_code = 0
    else:
        run_status = FAILED
        exit_code = 1

    result_order = list(acyclic_order) + [tid for tid in order if tid in cycle_nodes]
    summary = {
        "run_id": run_id,
        "resumed_from": resume_from,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": run_status,
        "ok": all_ok and not cycles,
        "exit_code": exit_code,
        "cycles": cycles,
        "cycle_nodes": sorted(cycle_nodes),
        "cycle_components": cyclic_components,
        "counts": {status: counts.get(status, 0)
                   for status in (PENDING, RUNNING, OK, FAILED, SKIPPED)},
        "tasks": [],
    }
    for tid in result_order:
        record = records[tid]
        summary["tasks"].append({
            "id": tid,
            "status": record["status"],
            "ok": record["status"] == OK,
            "attempts": record["attempts"],
            "duration": round(record["duration"], 3),
            "started_at": record["started_at"],
            "finished_at": record["finished_at"],
            "output": record["output"],
            "error": record["error"],
            "reason": record["reason"],
            "resumed": record["resumed"],
        })

    if store is not None:
        store.finish_run(run_id, run_status, finished_at, summary)
    _emit(echo, {"kind": "run_done", "run_id": run_id,
                 "status": run_status, "exit_code": exit_code})
    return summary


def run_all(tasks, cwd=None, concurrency=1, **kwargs):
    """保留原有函数名：执行 DAG 并返回每个任务的结果列表。"""
    summary = run_dag(tasks, cwd=cwd, concurrency=concurrency, **kwargs)
    return summary["tasks"]
