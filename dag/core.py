# -*- coding: utf-8 -*-
"""任务编排。

骨架：按列表顺序执行，每步执行一条 shell 命令，失败就中断。
不识别依赖、不检测环、不支持并行与重跑。
"""
import subprocess
import sys


def load_tasks(path):
    import json
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)["tasks"]


def run_task(task, cwd=None):
    """执行单个任务的 cmd，返回 (ok, output)。"""
    try:
        p = subprocess.run(task["cmd"], shell=True, cwd=cwd,
                           capture_output=True, text=True, timeout=task.get("timeout", 60))
    except subprocess.TimeoutExpired:
        return False, "timeout"
    out = (p.stdout or "") + (p.stderr or "")
    return p.returncode == 0, out.strip()


def run_all(tasks, cwd=None):
    results = []
    for t in tasks:
        ok, out = run_task(t, cwd)
        results.append({"id": t["id"], "ok": ok, "output": out})
        if not ok:
            break
    return results
