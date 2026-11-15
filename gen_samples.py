# -*- coding: utf-8 -*-
"""生成任务定义。

tasks.json 里埋了：依赖关系（D depends_on C）、一个必然失败的任务、
一个环（E -> F -> E）、以及一个只依赖部分上游的汇合任务。
任务是可移植的 python -c 命令。
"""
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
PY = "python"


def t(tid, code, deps=None, retries=0):
    d = {"id": tid, "cmd": '%s -c "%s"' % (PY, code)}
    if deps:
        d["depends_on"] = deps
    if retries:
        d["retries"] = retries
    return d


TASKS = {
    "tasks": [
        t("A", "print('A ok')"),
        t("B", "print('B ok')", ["A"]),
        t("C", "print('C ok')", ["A"]),
        t("D", "print('D ok')", ["C"]),
        t("E", "print('E ok')", ["F"]),
        t("F", "print('F ok')", ["E"]),
        t("G", "import sys; print('G failed'); sys.exit(3)", ["B", "D"], retries=2),
        t("H", "print('H ok')", ["G"]),
    ]
}


def main():
    with open(os.path.join(HERE, "tasks.json"), "w", encoding="utf-8") as f:
        json.dump(TASKS, f, ensure_ascii=False, indent=2)
    print("任务 %d 个" % len(TASKS["tasks"]))


if __name__ == "__main__":
    main()
