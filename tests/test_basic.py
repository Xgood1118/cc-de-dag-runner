# -*- coding: utf-8 -*-
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from dag.core import load_tasks, run_all, run_task


class TaskTest(unittest.TestCase):
    def test_load(self):
        self.assertEqual(len(load_tasks(os.path.join(ROOT, "tasks.json"))), 8)

    def test_success(self):
        ok, out = run_task({"id": "x", "cmd": 'python -c "print(1)"'})
        self.assertTrue(ok)
        self.assertEqual(out, "1")

    def test_failure(self):
        ok, out = run_task({"id": "x", "cmd": 'python -c "import sys; sys.exit(2)"'})
        self.assertFalse(ok)

    def test_failure_does_not_stop_unrelated_tasks(self):
        tasks = [{"id": "a", "cmd": 'python -c "print(1)"'},
                 {"id": "b", "cmd": 'python -c "import sys; sys.exit(1)"'},
                 {"id": "c", "cmd": 'python -c "print(3)"'}]
        results = run_all(tasks)
        states = {r["id"]: r["status"] for r in results}
        self.assertEqual(states, {"a": "ok", "b": "failed", "c": "ok"})


if __name__ == "__main__":
    unittest.main()
