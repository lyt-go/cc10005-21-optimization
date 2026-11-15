"""流水线 SQLite 持久层：建表、连接、种子装载与基础查询。

只依赖标准库 sqlite3，结果落盘后在页面刷新与服务重启之间保持一致。
历史节点与规则版本全部保留，回填只追加新版本，不覆盖旧版本。
"""
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

from app.config import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_raw_records (
    record_id     INTEGER PRIMARY KEY,
    source        TEXT NOT NULL,
    payload_json  TEXT NOT NULL,
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_rule_versions (
    rule_id       TEXT NOT NULL,
    version       INTEGER NOT NULL,
    module        TEXT NOT NULL,
    field_name    TEXT NOT NULL,
    config_json   TEXT NOT NULL,
    references_json TEXT NOT NULL DEFAULT '[]',
    status        TEXT NOT NULL,
    confirmed_seq INTEGER,
    batch_id      INTEGER,
    created_at    TEXT NOT NULL,
    confirmed_at  TEXT,
    PRIMARY KEY (rule_id, version)
);
CREATE INDEX IF NOT EXISTS idx_rule_versions_field
    ON pipeline_rule_versions(field_name, status);

CREATE TABLE IF NOT EXISTS pipeline_stage_nodes (
    node_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    record_id     INTEGER NOT NULL,
    field_name    TEXT NOT NULL,
    stage         TEXT NOT NULL,
    rule_id       TEXT NOT NULL,
    rule_version  INTEGER NOT NULL,
    config_hash   TEXT NOT NULL,
    input_hash    TEXT NOT NULL,
    output_value  TEXT,
    reused        INTEGER NOT NULL DEFAULT 0,
    source_node_id INTEGER,
    created_batch INTEGER,
    created_at    TEXT NOT NULL
);
-- 同一规则版本可能因上游字段变化而被再次执行（追加新节点行，旧行留作历史），
-- 当前生效节点取“已确认规则版本下 node_id 最大”的那一行。
CREATE INDEX IF NOT EXISTS idx_stage_nodes_lookup
    ON pipeline_stage_nodes(field_name, stage, config_hash, input_hash);
CREATE INDEX IF NOT EXISTS idx_stage_nodes_active
    ON pipeline_stage_nodes(record_id, field_name, stage, node_id);

CREATE TABLE IF NOT EXISTS pipeline_backfill_batches (
    batch_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    status        TEXT NOT NULL,
    trigger_note  TEXT NOT NULL,
    affected_fields_json TEXT NOT NULL,
    reused_fields_json   TEXT NOT NULL,
    node_summary_json    TEXT NOT NULL,
    error_message TEXT,
    created_at    TEXT NOT NULL,
    finished_at   TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_batch_versions (
    batch_id   INTEGER NOT NULL,
    rule_id    TEXT NOT NULL,
    version    INTEGER NOT NULL,
    PRIMARY KEY (batch_id, rule_id, version)
);
"""


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def loads(value: str | None) -> Any:
    return json.loads(value) if value is not None else None


def _connect(db_path: str) -> sqlite3.Connection:
    directory = os.path.dirname(db_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


class PipelineDB:
    """流水线数据访问对象；所有写操作由 service/engine 在事务里编排。"""

    def __init__(self, db_path: str | None = None) -> None:
        self.db_path = db_path or settings.pipeline_db_path

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        conn = _connect(self.db_path)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def connect(self) -> sqlite3.Connection:
        return _connect(self.db_path)

    def initialize(self, conn: sqlite3.Connection | None = None) -> None:
        """建表并在首次启动时装载种子（已存在数据则保持不动，保证重启一致）。"""
        if conn is None:
            with self.transaction() as owned:
                self._initialize_locked(owned)
        else:
            self._initialize_locked(conn)

    def _initialize_locked(self, conn: sqlite3.Connection) -> None:
        conn.executescript(SCHEMA)
        count = conn.execute(
            "SELECT COUNT(*) AS c FROM pipeline_raw_records"
        ).fetchone()["c"]
        if count:
            return
        # 原始采集记录：含 HTML、混合精度数值、敏感词，便于观察解析与清洗结果。
        raw_records = [
            (
                1,
                "种子采集批次",
                dumps({
                    "标题": "会员专享 智能耳机",
                    "价格": "￥1,299.5678",
                    "内容": "<p>限时 9 折，加微信 internal-wx 领券</p>",
                }),
                now_text(),
            ),
            (
                2,
                "种子采集批次",
                dumps({
                    "标题": "降噪耳机 Pro",
                    "价格": "￥899.20",
                    "内容": "<div>晒单返券，联系电话 13800001234</div>",
                }),
                now_text(),
            ),
            (
                3,
                "种子采集批次",
                dumps({
                    "标题": "运动耳机 青春版",
                    "价格": "￥399",
                    "内容": "<b>学生价</b>，客服 QQ 80012345 咨询",
                }),
                now_text(),
            ),
        ]
        conn.executemany(
            "INSERT INTO pipeline_raw_records(record_id, source, payload_json, created_at)"
            " VALUES (?, ?, ?, ?)",
            raw_records,
        )
        conn.execute(
            "INSERT INTO pipeline_backfill_batches(batch_id, status, trigger_note,"
            " affected_fields_json, reused_fields_json, node_summary_json, created_at, finished_at)"
            " VALUES (1, 'succeeded', '初始规则装载', '[]', '[]', '{}', ?, ?)",
            (now_text(), now_text()),
        )

    # ---------- 原始记录 ----------
    def list_raw_records(self, conn: sqlite3.Connection) -> list[dict[str, Any]]:
        rows = conn.execute(
            "SELECT * FROM pipeline_raw_records ORDER BY record_id"
        ).fetchall()
        return [
            {
                "record_id": row["record_id"],
                "source": row["source"],
                "payload": loads(row["payload_json"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    # ---------- 规则版本 ----------
    def insert_rule_version(
        self,
        conn: sqlite3.Connection,
        *,
        rule_id: str,
        version: int,
        module: str,
        field_name: str,
        config: dict[str, Any],
        references: list[str],
        status: str,
        confirmed_seq: int | None,
        batch_id: int,
        confirmed_at: str | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO pipeline_rule_versions(rule_id, version, module, field_name,"
            " config_json, references_json, status, confirmed_seq, batch_id, created_at, confirmed_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                rule_id, version, module, field_name, dumps(config), dumps(references),
                status, confirmed_seq, batch_id, now_text(), confirmed_at,
            ),
        )

    def latest_version_number(self, conn: sqlite3.Connection, rule_id: str) -> int:
        row = conn.execute(
            "SELECT MAX(version) AS v FROM pipeline_rule_versions WHERE rule_id = ?",
            (rule_id,),
        ).fetchone()
        return int(row["v"] or 0)

    def next_confirmed_seq(self, conn: sqlite3.Connection) -> int:
        row = conn.execute(
            "SELECT MAX(confirmed_seq) AS s FROM pipeline_rule_versions"
        ).fetchone()
        return int(row["s"] or 0) + 1

    def get_rule_version(
        self, conn: sqlite3.Connection, rule_id: str, version: int
    ) -> dict[str, Any] | None:
        row = conn.execute(
            "SELECT * FROM pipeline_rule_versions WHERE rule_id = ? AND version = ?",
            (rule_id, version),
        ).fetchone()
        return self._version_row(row) if row else None

    def active_versions_by_field(
        self, conn: sqlite3.Connection, fields: list[str] | None = None
    ) -> dict[str, dict[str, Any]]:
        """每个字段当前生效的规则版本（同模块覆盖同字段时，confirmed_seq 最大者胜）。"""
        sql = (
            "SELECT v.* FROM pipeline_rule_versions v"
            " JOIN (SELECT field_name, module, MAX(confirmed_seq) AS max_seq"
            "       FROM pipeline_rule_versions WHERE status = 'confirmed' GROUP BY field_name, module) m"
            " ON v.field_name = m.field_name AND v.module = m.module"
            " AND v.confirmed_seq = m.max_seq WHERE v.status = 'confirmed'"
        )
        params: tuple[Any, ...] = ()
        if fields:
            placeholders = ",".join("?" for _ in fields)
            sql += f" AND v.field_name IN ({placeholders})"
            params = tuple(fields)
        result: dict[str, dict[str, Any]] = {}
        for row in conn.execute(sql, params).fetchall():
            parsed = self._version_row(row)
            result.setdefault(parsed["field_name"], {})[parsed["module"]] = parsed
        return result

    def list_rule_versions(
        self, conn: sqlite3.Connection, field_name: str | None = None
    ) -> list[dict[str, Any]]:
        if field_name:
            rows = conn.execute(
                "SELECT * FROM pipeline_rule_versions WHERE field_name = ?"
                " ORDER BY rule_id, version",
                (field_name,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM pipeline_rule_versions ORDER BY rule_id, version"
            ).fetchall()
        return [self._version_row(row) for row in rows]

    def confirm_version(
        self, conn: sqlite3.Connection, rule_id: str, version: int, seq: int
    ) -> None:
        conn.execute(
            "UPDATE pipeline_rule_versions SET status = 'confirmed', confirmed_seq = ?,"
            " confirmed_at = ? WHERE rule_id = ? AND version = ?",
            (seq, now_text(), rule_id, version),
        )

    def link_batch_version(
        self, conn: sqlite3.Connection, batch_id: int, rule_id: str, version: int
    ) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO pipeline_batch_versions(batch_id, rule_id, version)"
            " VALUES (?, ?, ?)",
            (batch_id, rule_id, version),
        )

    def _version_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "rule_id": row["rule_id"],
            "version": row["version"],
            "module": row["module"],
            "field_name": row["field_name"],
            "config": loads(row["config_json"]),
            "references": loads(row["references_json"]),
            "status": row["status"],
            "confirmed_seq": row["confirmed_seq"],
            "batch_id": row["batch_id"],
            "created_at": row["created_at"],
            "confirmed_at": row["confirmed_at"],
        }

    # ---------- 步骤节点 ----------
    def find_reusable_node(
        self,
        conn: sqlite3.Connection,
        *,
        record_id: int,
        field_name: str,
        stage: str,
        config_hash: str,
        input_hash: str,
        input_sensitive: bool,
    ) -> dict[str, Any] | None:
        """找同一采集记录下、已确认版本中配置哈希（输入敏感阶段还要输入哈希）一致的节点复用。"""
        sql = (
            "SELECT n.* FROM pipeline_stage_nodes n"
            " JOIN pipeline_rule_versions v ON n.rule_id = v.rule_id"
            " AND n.rule_version = v.version AND v.status = 'confirmed'"
            " WHERE n.record_id = ? AND n.field_name = ? AND n.stage = ? AND n.config_hash = ?"
        )
        params: list[Any] = [record_id, field_name, stage, config_hash]
        if input_sensitive:
            sql += " AND n.input_hash = ?"
            params.append(input_hash)
        sql += " ORDER BY n.node_id DESC LIMIT 1"
        row = conn.execute(sql, params).fetchone()
        return dict(row) if row else None

    def insert_node(
        self,
        conn: sqlite3.Connection,
        *,
        record_id: int,
        field_name: str,
        stage: str,
        rule_id: str,
        rule_version: int,
        config_hash: str,
        input_hash: str,
        output_value: str | None,
        reused: bool,
        source_node_id: int | None,
        batch_id: int,
    ) -> int:
        cur = conn.execute(
            "INSERT INTO pipeline_stage_nodes(record_id, field_name, stage, rule_id,"
            " rule_version, config_hash, input_hash, output_value, reused, source_node_id,"
            " created_batch, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                record_id, field_name, stage, rule_id, rule_version, config_hash,
                input_hash, output_value, 1 if reused else 0, source_node_id,
                batch_id, now_text(),
            ),
        )
        return int(cur.lastrowid)

    def find_node(
        self,
        conn: sqlite3.Connection,
        *,
        record_id: int,
        field_name: str,
        stage: str,
        rule_id: str,
        rule_version: int,
    ) -> dict[str, Any] | None:
        row = conn.execute(
            "SELECT * FROM pipeline_stage_nodes WHERE record_id = ? AND field_name = ?"
            " AND stage = ? AND rule_id = ? AND rule_version = ?"
            " ORDER BY node_id DESC LIMIT 1",
            (record_id, field_name, stage, rule_id, rule_version),
        ).fetchone()
        return dict(row) if row else None

    def list_nodes_for_active(
        self, conn: sqlite3.Connection, record_id: int, field_name: str
    ) -> list[dict[str, Any]]:
        # 每个阶段只取已确认规则版本下 node_id 最大的节点（重算后旧节点保留为历史）。
        rows = conn.execute(
            "SELECT n.* FROM pipeline_stage_nodes n"
            " JOIN pipeline_rule_versions v ON n.rule_id = v.rule_id"
            " AND n.rule_version = v.version AND v.status = 'confirmed'"
            " JOIN (SELECT stage, MAX(node_id) AS max_id FROM pipeline_stage_nodes"
            "       WHERE record_id = ? AND field_name = ? GROUP BY stage) latest"
            " ON n.node_id = latest.max_id"
            " WHERE n.record_id = ? AND n.field_name = ?"
            " ORDER BY n.node_id",
            (record_id, field_name, record_id, field_name),
        ).fetchall()
        return [dict(row) for row in rows]

    def list_nodes_for_version(
        self,
        conn: sqlite3.Connection,
        rule_id: str,
        version: int,
    ) -> list[dict[str, Any]]:
        rows = conn.execute(
            "SELECT * FROM pipeline_stage_nodes WHERE rule_id = ? AND rule_version = ?"
            " ORDER BY record_id, field_name, node_id",
            (rule_id, version),
        ).fetchall()
        return [dict(row) for row in rows]

    # ---------- 回填批次 ----------
    def create_batch(self, conn: sqlite3.Connection, trigger_note: str) -> int:
        cur = conn.execute(
            "INSERT INTO pipeline_backfill_batches(status, trigger_note, affected_fields_json,"
            " reused_fields_json, node_summary_json, created_at)"
            " VALUES ('building', ?, '[]', '[]', '{}', ?)",
            (trigger_note, now_text()),
        )
        return int(cur.lastrowid)

    def finish_batch(
        self,
        conn: sqlite3.Connection,
        batch_id: int,
        *,
        status: str,
        affected_fields: list[str],
        reused_fields: list[str],
        node_summary: dict[str, Any],
        error_message: str | None = None,
    ) -> None:
        conn.execute(
            "UPDATE pipeline_backfill_batches SET status = ?, affected_fields_json = ?,"
            " reused_fields_json = ?, node_summary_json = ?, error_message = ?,"
            " finished_at = ? WHERE batch_id = ?",
            (
                status, dumps(affected_fields), dumps(reused_fields), dumps(node_summary),
                error_message, now_text(), batch_id,
            ),
        )

    def list_batches(self, conn: sqlite3.Connection) -> list[dict[str, Any]]:
        rows = conn.execute(
            "SELECT * FROM pipeline_backfill_batches ORDER BY batch_id DESC"
        ).fetchall()
        return [
            {
                "batch_id": row["batch_id"],
                "status": row["status"],
                "trigger_note": row["trigger_note"],
                "affected_fields": loads(row["affected_fields_json"]),
                "reused_fields": loads(row["reused_fields_json"]),
                "node_summary": loads(row["node_summary_json"]),
                "error_message": row["error_message"],
                "created_at": row["created_at"],
                "finished_at": row["finished_at"],
            }
            for row in rows
        ]

    def get_batch(self, conn: sqlite3.Connection, batch_id: int) -> dict[str, Any] | None:
        rows = [b for b in self.list_batches(conn) if b["batch_id"] == batch_id]
        return rows[0] if rows else None


pipeline_db = PipelineDB()
