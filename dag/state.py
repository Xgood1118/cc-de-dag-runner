# -*- coding: utf-8 -*-
"""SQLite 状态存储：run_id、任务状态、尝试次数、耗时、输出摘要与历史查询。"""
import json
import sqlite3
import uuid
from datetime import datetime


SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id       TEXT PRIMARY KEY,
    started_at   TEXT NOT NULL,
    finished_at  TEXT,
    status       TEXT NOT NULL,
    task_file    TEXT,
    resumed_from TEXT,
    summary      TEXT
);

CREATE TABLE IF NOT EXISTS task_runs (
    run_id         TEXT NOT NULL,
    task_id        TEXT NOT NULL,
    status         TEXT NOT NULL,
    attempts       INTEGER NOT NULL DEFAULT 0,
    started_at     TEXT,
    finished_at    TEXT,
    duration       REAL NOT NULL DEFAULT 0,
    output_summary TEXT NOT NULL DEFAULT '',
    error          TEXT,
    reason         TEXT,
    PRIMARY KEY (run_id, task_id)
);
"""


def now_iso():
    """本地时区的 ISO-8601 时间戳。"""
    return datetime.now().astimezone().isoformat(timespec="seconds")


def new_run_id():
    """生成全局唯一的 run_id。"""
    return "%s-%s" % (datetime.now().strftime("%Y%m%d-%H%M%S"),
                      uuid.uuid4().hex[:8])


class StateStore(object):
    """封装 SQLite 读写；本项目里所有写操作都由主线程完成。"""

    def __init__(self, path):
        self.path = path
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self):
        self._conn.close()

    def create_run(self, run_id, started_at, task_file=None, resumed_from=None):
        self._conn.execute(
            "INSERT INTO runs (run_id, started_at, status, task_file, resumed_from) "
            "VALUES (?, ?, 'running', ?, ?)",
            (run_id, started_at, task_file, resumed_from),
        )
        self._conn.commit()

    def finish_run(self, run_id, status, finished_at, summary=None):
        if summary is not None and not isinstance(summary, str):
            summary = json.dumps(summary, ensure_ascii=False)
        self._conn.execute(
            "UPDATE runs SET status = ?, finished_at = ?, summary = ? WHERE run_id = ?",
            (status, finished_at, summary, run_id),
        )
        self._conn.commit()

    def set_task(self, run_id, task_id, status, attempts=0, started_at=None,
                 finished_at=None, duration=0.0, output_summary="", error=None,
                 reason=None):
        self._conn.execute(
            "INSERT INTO task_runs (run_id, task_id, status, attempts, started_at, "
            "finished_at, duration, output_summary, error, reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(run_id, task_id) DO UPDATE SET "
            "status = excluded.status, attempts = excluded.attempts, "
            "started_at = excluded.started_at, finished_at = excluded.finished_at, "
            "duration = excluded.duration, output_summary = excluded.output_summary, "
            "error = excluded.error, reason = excluded.reason",
            (run_id, task_id, status, int(attempts), started_at, finished_at,
             float(duration), output_summary or "", error, reason),
        )
        self._conn.commit()

    def load_tasks(self, run_id):
        """返回 {task_id: row-dict}，供 resume 使用。"""
        rows = self._conn.execute(
            "SELECT * FROM task_runs WHERE run_id = ? ORDER BY rowid", (run_id,)
        ).fetchall()
        return {row["task_id"]: dict(row) for row in rows}

    def get_run(self, run_id):
        row = self._conn.execute(
            "SELECT * FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        return dict(row) if row else None

    def latest_run_id(self):
        row = self._conn.execute(
            "SELECT run_id FROM runs ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        return row["run_id"] if row else None

    def list_runs(self):
        rows = self._conn.execute(
            "SELECT run_id, started_at, finished_at, status, task_file, resumed_from "
            "FROM runs ORDER BY rowid DESC"
        ).fetchall()
        return [dict(row) for row in rows]

    def history(self, run_id):
        """按 run_id 查询一次运行及其任务状态。"""
        run = self.get_run(run_id)
        if run is None:
            return None
        summary = run.get("summary")
        if summary:
            try:
                summary = json.loads(summary)
            except ValueError:
                pass
        run["summary"] = summary
        rows = self._conn.execute(
            "SELECT task_id, status, attempts, started_at, finished_at, duration, "
            "output_summary, error, reason FROM task_runs "
            "WHERE run_id = ? ORDER BY rowid",
            (run_id,),
        ).fetchall()
        return {"run": run, "tasks": [dict(row) for row in rows]}
