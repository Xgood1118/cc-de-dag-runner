# -*- coding: utf-8 -*-
"""命令行入口：python -m dag.cli tasks.json

退出码：0 全部成功；1 有失败/跳过；2 依赖环或参数、定义错误。
"""
import argparse
import json
import os
import sys

from .core import (DagError, run_dag)
from .state import StateStore, new_run_id

DEFAULT_DB = os.path.join(".pipeline", "state.db")


def _format_output(event):
    """把一个任务的结果渲染成连续的一段，保证并行时输出不会交叉。"""
    lines = []
    status = event["status"].upper()
    head = "[%s] %s" % (event["id"], status)
    details = []
    if event["resumed"]:
        details.append("resumed")
    if event["attempts"]:
        details.append("%d attempt%s" % (
            event["attempts"], "" if event["attempts"] == 1 else "s"))
    details.append("%.3fs" % event["duration"])
    head += " (%s)" % ", ".join(details)
    if event["error"]:
        head += " error=%s" % event["error"]
    if event["reason"]:
        head += " reason=%s" % event["reason"]
    lines.append(head)
    if event["output"]:
        for line in str(event["output"]).splitlines():
            lines.append("    " + line)
    return "\n".join(lines)


def _make_echo(stream=None):
    stream = stream or sys.stdout
    def echo(event):
        kind = event.get("kind")
        if kind == "task":
            stream.write(_format_output(event) + "\n")
            stream.flush()
        elif kind == "cycle":
            stream.write("[cycle] %s\n" % " -> ".join(event["cycle"]))
            stream.flush()
        elif kind == "run_start":
            if event.get("resumed_from"):
                stream.write("[run] %s (resume from %s)\n"
                             % (event["run_id"], event["resumed_from"]))
            else:
                stream.write("[run] %s\n" % event["run_id"])
            stream.flush()
    return echo


def _write_err(message):
    sys.stderr.write(message.rstrip() + "\n")
    sys.stderr.flush()


def _print_summary(summary, stream=None):
    stream = stream or sys.stdout
    stream.write(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    stream.flush()


def _build_parser():
    parser = argparse.ArgumentParser(
        prog="python -m dag.cli",
        description="按依赖拓扑执行任务，支持失败恢复与 SQLite 状态持久化。",
    )
    parser.add_argument("tasks_file", nargs="?", help="任务定义 JSON（如 tasks.json）")
    parser.add_argument("-j", "--concurrency", type=int, default=1,
                        help="并行度，默认 1")
    parser.add_argument("--resume", nargs="?", const="", metavar="RUN_ID",
                        help="续跑：只执行未成功且依赖已满足的任务；"
                             "省略 RUN_ID 时使用最近一次运行")
    parser.add_argument("--run-id", dest="run_id",
                        help="指定本次运行的 run_id（默认自动生成）")
    parser.add_argument("--db", default=DEFAULT_DB,
                        help="SQLite 状态库路径（默认 %s）" % DEFAULT_DB)
    parser.add_argument("--history", nargs="?", const="", metavar="RUN_ID",
                        help="查询某次运行的状态；省略 RUN_ID 时列出全部运行")
    parser.add_argument("--cwd", help="任务命令的工作目录（默认当前目录）")
    parser.add_argument("--retry-delays", default="1,2,4",
                        help="重试退避秒数，逗号分隔（默认 1,2,4）")
    return parser


def _parse_delays(text):
    try:
        return [float(part) for part in str(text).split(",") if part.strip()]
    except ValueError:
        raise DagError("--retry-delays 必须是逗号分隔的数字")


def _open_store(path):
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)
    return StateStore(path)


def _cmd_history(store, run_id, stream=None):
    stream = stream or sys.stdout
    if not run_id:
        runs = store.list_runs()
        if not runs:
            _write_err("还没有历史运行记录")
            return 1
        print(json.dumps(runs, ensure_ascii=False, indent=2), file=stream)
        return 0
    history = store.history(run_id)
    if history is None:
        _write_err("找不到 run_id=%s" % run_id)
        return 1
    print(json.dumps(history, ensure_ascii=False, indent=2), file=stream)
    return 0


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = _build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    store = None
    try:
        store = _open_store(args.db)
        if args.history is not None:
            return _cmd_history(store, args.history)

        if not args.tasks_file:
            _write_err("需要任务文件，例如 python -m dag.cli tasks.json")
            return 2

        from .core import load_tasks

        tasks = load_tasks(args.tasks_file)
        resume_from = None
        if args.resume is not None:
            resume_from = args.resume or store.latest_run_id()
            if not resume_from:
                _write_err("没有可续跑的历史运行")
                return 2

        summary = run_dag(
            tasks,
            cwd=args.cwd,
            concurrency=args.concurrency,
            store=store,
            run_id=args.run_id or new_run_id(),
            resume_from=resume_from,
            task_file=args.tasks_file,
            echo=_make_echo(),
            retry_delays=_parse_delays(args.retry_delays),
        )
        _print_summary(summary)
        return summary["exit_code"]
    except DagError as exc:
        _write_err("错误: %s" % exc)
        nodes = getattr(exc, "nodes", None)
        if nodes:
            _write_err("环上节点: %s" % ", ".join(nodes))
        return 2
    except KeyboardInterrupt:
        _write_err("已中断")
        return 130
    finally:
        if store is not None:
            store.close()


if __name__ == "__main__":
    sys.exit(main())
