# -*- coding: utf-8 -*-
"""DAG 编排器测试：拓扑、环、失败语义、重试、超时、并行、SQLite 与 resume。"""
import io
import json
import os
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from dag import cli
from dag.core import (CycleError, DagError, find_cycles, run_all, run_dag,
                      run_task, topological_sort)
from dag.state import StateStore

PY = sys.executable


def t(tid, code, deps=None, **extra):
    task = {"id": tid, "cmd": '%s -c "%s"' % (PY, code)}
    if deps:
        task["depends_on"] = deps
    task.update(extra)
    return task


def ok(tid, deps=None, **extra):
    return t(tid, "print('%s ok')" % tid, deps, **extra)


def fail(tid, deps=None, code="1", **extra):
    return t(tid, "import sys; sys.exit(%s)" % code, deps, **extra)


def by_id(results):
    return {item["id"]: item for item in results}


class TempDirTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = self._tmp.name
        self.db = os.path.join(self.tmp, "state.db")

    def tearDown(self):
        self._tmp.cleanup()

    def store(self):
        return StateStore(self.db)

    def run_dag(self, tasks, **kwargs):
        kwargs.setdefault("retry_delays", ())
        store = kwargs.pop("store", None)
        return run_dag(tasks, store=store, **kwargs)


class TopologyTest(TempDirTestCase):
    def test_topological_sort_respects_dependencies(self):
        tasks = [ok("a"), ok("b", ["a"]), ok("c", ["a"]), ok("d", ["b", "c"])]
        order = topological_sort(tasks)
        position = {tid: i for i, tid in enumerate(order)}
        for task in tasks:
            for dep in task.get("depends_on", []):
                self.assertLess(position[dep], position[task["id"]])
        self.assertEqual(order[0], "a")
        self.assertEqual(order[-1], "d")

    def test_execution_order_is_topological(self):
        marker = os.path.join(self.tmp, "order.txt")
        code = "open(r'%s','a').write('%%s\\n')" % marker
        tasks = [
            {"id": "a", "cmd": '%s -c "%s"' % (PY, code % "a")},
            {"id": "b", "cmd": '%s -c "%s"' % (PY, code % "b"), "depends_on": ["a"]},
            {"id": "c", "cmd": '%s -c "%s"' % (PY, code % "c"), "depends_on": ["a"]},
            {"id": "d", "cmd": '%s -c "%s"' % (PY, code % "d"),
             "depends_on": ["b", "c"]},
        ]
        summary = run_dag(tasks, retry_delays=())
        self.assertEqual(summary["exit_code"], 0)
        with open(marker, "r", encoding="utf-8") as handle:
            seen = handle.read().split()
        self.assertEqual(seen[0], "a")
        self.assertEqual(seen[-1], "d")
        self.assertEqual(sorted(seen), ["a", "b", "c", "d"])

    def test_duplicate_and_unknown_dependency(self):
        with self.assertRaises(DagError):
            run_dag([ok("a"), ok("a")], retry_delays=())
        with self.assertRaises(DagError):
            run_dag([ok("a", ["missing"])], retry_delays=())


class CycleTest(TempDirTestCase):
    def test_find_all_nodes_in_cycle(self):
        tasks = [ok("a"), ok("b", ["d"]), ok("c", ["b"]), ok("d", ["c"])]
        cycles = find_cycles(tasks)
        self.assertEqual(len(cycles), 1)
        self.assertEqual(set(cycles[0]), {"b", "c", "d"})

    def test_topological_sort_raises_cycle_error(self):
        tasks = [ok("a"), ok("b", ["c"]), ok("c", ["b"])]
        with self.assertRaises(CycleError) as ctx:
            topological_sort(tasks)
        self.assertEqual(ctx.exception.nodes, ["b", "c"])

    def test_cycle_is_reported_and_others_still_run(self):
        tasks = [ok("a"), ok("b", ["c"]), ok("c", ["b"]), ok("d", ["a"])]
        summary = self.run_dag(tasks)
        self.assertEqual(summary["exit_code"], 2)
        self.assertEqual(summary["status"], "cycle")
        self.assertEqual(set(summary["cycle_nodes"]), {"b", "c"})
        states = {item["id"]: item["status"] for item in summary["tasks"]}
        self.assertEqual(states["a"], "ok")
        self.assertEqual(states["d"], "ok")
        self.assertEqual(states["b"], "skipped")
        self.assertEqual(states["c"], "skipped")

    def test_self_cycle(self):
        summary = self.run_dag([ok("a", ["a"])])
        self.assertEqual(summary["exit_code"], 2)
        self.assertEqual(summary["cycle_nodes"], ["a"])

    def test_intersecting_cycles_report_every_node(self):
        # 依赖边方向为 任务 -> 依赖：
        #   a->c->b->a 是环；b->d->c->b 是另一个环，d 也在环上。
        tasks = [ok("a", ["c"]), ok("b", ["a", "d"]), ok("c", ["b"]),
                 ok("d", ["c"]), ok("e", ["d"]), ok("x")]
        summary = self.run_dag(tasks)
        self.assertEqual(summary["exit_code"], 2)
        self.assertEqual(set(summary["cycle_nodes"]), {"a", "b", "c", "d"})
        states = {item["id"]: item["status"] for item in summary["tasks"]}
        self.assertEqual(states["e"], "skipped")
        self.assertEqual(states["x"], "ok")

    def test_cli_exit_code_two_for_cycle(self):
        path = os.path.join(self.tmp, "tasks.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump({"tasks": [ok("a", ["a"])]}, handle)
        out = io.StringIO()
        old = sys.stdout
        sys.stdout = out
        try:
            code = cli.main([path, "--db", self.db])
        finally:
            sys.stdout = old
        self.assertEqual(code, 2)
        self.assertIn("[cycle]", out.getvalue())


class FailureTest(TempDirTestCase):
    def test_downstream_skipped(self):
        tasks = [ok("a"), fail("b", ["a"]), ok("c", ["b"]), ok("d", ["c"])]
        summary = self.run_dag(tasks)
        states = {item["id"]: item["status"] for item in summary["tasks"]}
        self.assertEqual(states, {"a": "ok", "b": "failed",
                                  "c": "skipped", "d": "skipped"})
        self.assertEqual(summary["exit_code"], 1)

    def test_unrelated_branch_continues(self):
        tasks = [fail("a"), ok("b", ["a"]), ok("c"), ok("d", ["c"])]
        summary = self.run_dag(tasks)
        states = {item["id"]: item["status"] for item in summary["tasks"]}
        self.assertEqual(states["a"], "failed")
        self.assertEqual(states["b"], "skipped")
        self.assertEqual(states["c"], "ok")
        self.assertEqual(states["d"], "ok")

    def test_skipped_task_never_executes(self):
        marker = os.path.join(self.tmp, "ran.txt")
        tasks = [fail("a"), t("b", "print('ran'); open(r'%s','w').write('x')" % marker,
                             ["a"])]
        summary = self.run_dag(tasks)
        states = {item["id"]: item["status"] for item in summary["tasks"]}
        self.assertEqual(states["b"], "skipped")
        self.assertFalse(os.path.exists(marker))

    def test_legacy_run_all_keeps_function(self):
        tasks = [ok("a"), ok("b", ["a"])]
        results = run_all(tasks, retry_delays=())
        self.assertEqual(by_id(results)["b"]["status"], "ok")


class RetryTest(TempDirTestCase):
    def test_retry_then_success(self):
        counter = os.path.join(self.tmp, "count.txt")
        code = (
            "import os,sys\n"
            "p=r'%s'\n"
            "n=int(open(p).read()) if os.path.exists(p) else 0\n"
            "open(p,'w').write(str(n+1))\n"
            "sys.exit(0 if n >= 2 else 1)" % counter
        )
        tasks = [{"id": "a", "cmd": '%s -c "%s"' % (PY, code.replace("\n", ";")),
                  "retries": 2}]
        summary = run_dag(tasks, retry_delays=(0, 0, 0))
        task = summary["tasks"][0]
        self.assertEqual(task["status"], "ok")
        self.assertEqual(task["attempts"], 3)

    def test_retries_exhausted_marks_failed(self):
        tasks = [fail("a", retries=2)]
        summary = run_dag(tasks, retry_delays=(0, 0, 0))
        task = summary["tasks"][0]
        self.assertEqual(task["status"], "failed")
        self.assertEqual(task["attempts"], 3)

    def test_default_backoff_sequence(self):
        # 只验证退避序列本身，实际运行用 0 延迟避免拖慢测试。
        from dag.core import DEFAULT_RETRY_DELAYS
        self.assertEqual(DEFAULT_RETRY_DELAYS, (1.0, 2.0, 4.0))


class TimeoutTest(TempDirTestCase):
    def test_timeout_marks_failed_and_timeout(self):
        tasks = [t("slow", "import time; time.sleep(5)", timeout=0.4),
                 ok("other")]
        summary = self.run_dag(tasks)
        states = by_id(summary["tasks"])
        self.assertEqual(states["slow"]["status"], "failed")
        self.assertEqual(states["slow"]["error"], "timeout")
        self.assertEqual(states["other"]["status"], "ok")
        self.assertEqual(summary["exit_code"], 1)

    def test_timeout_counts_as_attempt_and_skips_downstream(self):
        tasks = [t("slow", "import time; time.sleep(5)", timeout=0.3),
                 ok("after", ["slow"])]
        summary = self.run_dag(tasks)
        result = by_id(summary["tasks"])
        self.assertEqual(result["slow"]["attempts"], 1)
        self.assertEqual(result["after"]["status"], "skipped")


class ParallelTest(TempDirTestCase):
    def test_parallel_output_grouped_by_task(self):
        events = []
        tasks = [ok("a"), ok("b"), ok("c")]
        summary = run_dag(tasks, concurrency=3, echo=events.append,
                          retry_delays=())
        self.assertEqual(summary["exit_code"], 0)
        task_events = [e for e in events if e["kind"] == "task"]
        self.assertEqual(len(task_events), 3)
        ids = [e["id"] for e in task_events]
        self.assertEqual(sorted(ids), ["a", "b", "c"])
        for event in task_events:
            self.assertTrue(event["output"].startswith(event["id"]))

    def test_cli_parallel_render_does_not_interleave(self):
        path = os.path.join(self.tmp, "tasks.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump({"tasks": [ok("a"), ok("b"), ok("c")]}, handle)
        out = io.StringIO()
        old = sys.stdout
        sys.stdout = out
        try:
            code = cli.main([path, "--db", self.db, "-j", "3"])
        finally:
            sys.stdout = old
        self.assertEqual(code, 0)
        text = out.getvalue()
        for tid in ("a", "b", "c"):
            self.assertIn("[%s] OK" % tid, text)

    def test_parallel_respects_dependencies(self):
        tasks = [ok("a"), ok("b", ["a"]), ok("c", ["a"]), ok("d", ["b", "c"])]
        summary = run_dag(tasks, concurrency=4, retry_delays=())
        self.assertEqual(summary["counts"]["ok"], 4)


class StateAndResumeTest(TempDirTestCase):
    def test_state_persisted_with_run_id(self):
        store = self.store()
        try:
            summary = run_dag([ok("a"), fail("b", ["a"]), ok("c", ["b"])],
                              store=store, retry_delays=())
            run_id = summary["run_id"]
            self.assertTrue(run_id)
            history = store.history(run_id)
        finally:
            store.close()
        self.assertEqual(history["run"]["status"], "failed")
        states = {task["task_id"]: task for task in history["tasks"]}
        self.assertEqual(states["a"]["status"], "ok")
        self.assertEqual(states["b"]["status"], "failed")
        self.assertEqual(states["c"]["status"], "skipped")
        for task in history["tasks"]:
            self.assertIsNotNone(task["started_at"])
            self.assertIsNotNone(task["finished_at"])
            self.assertIsNotNone(task["duration"])

    def test_run_ids_are_unique(self):
        store = self.store()
        try:
            first = run_dag([ok("a")], store=store, retry_delays=())
            second = run_dag([ok("a")], store=store, retry_delays=())
            runs = store.list_runs()
        finally:
            store.close()
        self.assertNotEqual(first["run_id"], second["run_id"])
        self.assertEqual(len(runs), 2)

    def test_resume_does_not_rerun_successful_tasks(self):
        marker = os.path.join(self.tmp, "a-runs.txt")
        code = "open(r'%s','a').write('x')" % marker
        tasks = [
            {"id": "a", "cmd": '%s -c "%s"' % (PY, code)},
            fail("b", ["a"]),
            ok("c", ["b"]),
        ]
        store = self.store()
        try:
            first = run_dag(tasks, store=store, retry_delays=())
            first_count = len(open(marker).read())
            self.assertEqual(first_count, 1)

            tasks[1] = ok("b", ["a"])  # 修复失败任务后续跑
            second = run_dag(tasks, store=store, resume_from=first["run_id"],
                             retry_delays=())
            second_count = len(open(marker).read())
            history_a = store.history(second["run_id"])["tasks"]
        finally:
            store.close()

        self.assertEqual(second_count, 1, "已成功的 a 不应重跑")
        states = {item["id"]: item["status"] for item in second["tasks"]}
        self.assertEqual(states, {"a": "ok", "b": "ok", "c": "ok"})
        resumed = {task["task_id"]: task for task in history_a}
        self.assertEqual(resumed["a"]["reason"], "resumed from %s" % first["run_id"])
        self.assertEqual(second["exit_code"], 0)

    def test_resume_skips_tasks_whose_dependencies_failed(self):
        tasks = [fail("a"), ok("b", ["a"]), ok("c")]
        store = self.store()
        try:
            first = run_dag(tasks, store=store, retry_delays=())
            second = run_dag(tasks, store=store, resume_from=first["run_id"],
                             retry_delays=())
        finally:
            store.close()
        states = {item["id"]: item["status"] for item in second["tasks"]}
        self.assertEqual(states["a"], "failed")
        self.assertEqual(states["b"], "skipped")
        self.assertEqual(states["c"], "ok")

    def test_resume_missing_run_id_errors(self):
        store = self.store()
        try:
            with self.assertRaises(DagError):
                run_dag([ok("a")], store=store, resume_from="nope",
                        retry_delays=())
        finally:
            store.close()


class CliTest(TempDirTestCase):
    def _write(self, tasks):
        path = os.path.join(self.tmp, "tasks.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump({"tasks": tasks}, handle)
        return path

    def _run(self, argv):
        out, err = io.StringIO(), io.StringIO()
        old_out, old_err = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = out, err
        try:
            code = cli.main(argv)
        finally:
            sys.stdout, sys.stderr = old_out, old_err
        return code, out.getvalue(), err.getvalue()

    def test_json_summary_and_exit_codes(self):
        path = self._write([ok("a"), ok("b", ["a"])])
        code, out, _err = self._run([path, "--db", self.db])
        self.assertEqual(code, 0)
        payload = out[out.index("{"):]
        summary = json.loads(payload)
        self.assertEqual(summary["exit_code"], 0)
        self.assertEqual([item["id"] for item in summary["tasks"]], ["a", "b"])
        for item in summary["tasks"]:
            self.assertIn("status", item)
            self.assertIn("duration", item)
            self.assertIn("attempts", item)

    def test_failure_exit_code(self):
        path = self._write([fail("a")])
        code, _out, _err = self._run([path, "--db", self.db])
        self.assertEqual(code, 1)

    def test_history_command(self):
        path = self._write([ok("a")])
        code, out, _err = self._run([path, "--db", self.db])
        run_id = json.loads(out[out.index("{"):])["run_id"]
        code, out, _err = self._run(["--db", self.db, "--history", run_id])
        self.assertEqual(code, 0)
        history = json.loads(out)
        self.assertEqual(history["run"]["run_id"], run_id)
        self.assertEqual(history["tasks"][0]["task_id"], "a")

    def test_resume_flag(self):
        path = self._write([fail("a")])
        code, out, _err = self._run([path, "--db", self.db])
        self.assertEqual(code, 1)
        run_id = json.loads(out[out.index("{"):])["run_id"]
        self._write([ok("a")])
        code, out, _err = self._run([path, "--db", self.db, "--resume", run_id])
        self.assertEqual(code, 0)
        summary = json.loads(out[out.index("{"):])
        self.assertEqual(summary["resumed_from"], run_id)
        # 上次失败的 a 应重新执行，而不是被当作已完成继承。
        self.assertEqual(summary["tasks"][0]["status"], "ok")
        self.assertFalse(summary["tasks"][0]["resumed"])

    def test_missing_file_exit_code_two(self):
        code, _out, err = self._run([os.path.join(self.tmp, "missing.json"),
                                     "--db", self.db])
        self.assertEqual(code, 2)
        self.assertIn("错误", err)


if __name__ == "__main__":
    unittest.main()
