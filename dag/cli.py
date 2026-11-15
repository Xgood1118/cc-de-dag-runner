# -*- coding: utf-8 -*-
"""命令行入口：python -m dag.cli tasks.json"""
import json
import sys

from .core import load_tasks, run_all


def main(argv=None):
    argv = argv or sys.argv[1:]
    if not argv:
        sys.stderr.write("需要任务文件\n")
        return 2
    tasks = load_tasks(argv[0])
    results = run_all(tasks)
    ok = sum(1 for r in results if r["ok"])
    print("执行 %d 个，成功 %d 个" % (len(results), ok))
    for r in results:
        print("  %-10s %s %s" % (r["id"], "OK" if r["ok"] else "FAIL", r["output"][:60]))
    return 0 if ok == len(tasks) else 1


if __name__ == "__main__":
    sys.exit(main())
