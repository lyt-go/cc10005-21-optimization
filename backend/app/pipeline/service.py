"""流水线应用服务：对外提供依赖图、解析/清洗结果、版本历史与回填编排。

列表与详情走同一套“当前生效节点”查询，保证两个视图同步；历史版本始终可回溯。
"""
from __future__ import annotations

from typing import Any

from app.pipeline.db import PipelineDB, pipeline_db
from app.pipeline.engine import (
    ALL_STAGES,
    PARSER_STAGES,
    PipelineConfigError,
    PipelineEngine,
)

PARSER_OUTPUT_STAGE = "convert"
CLEAN_OUTPUT_STAGE = "mask"


class PipelineService:
    def __init__(self, db: PipelineDB | None = None) -> None:
        self.db = db or pipeline_db
        self.engine = PipelineEngine(self.db)

    def initialize(self) -> None:
        with self.db.transaction() as conn:
            self.db.initialize(conn)
            self.engine.seed_initial_pipeline(conn)

    # ---------- 依赖图 ----------
    def dependency_graph(self) -> dict[str, Any]:
        with self.db.transaction() as conn:
            graph = self.engine.build_graph(conn)
            active = self.db.active_versions_by_field(conn)
            nodes = []
            for field_name in sorted(active):
                versions = active[field_name]
                nodes.append({
                    "field_name": field_name,
                    "references": versions.get("parser", {}).get("references", []),
                    "parser_rule": versions["parser"]["rule_id"],
                    "parser_version": versions["parser"]["version"],
                    "clean_rule": versions["dataclean"]["rule_id"],
                    "clean_version": versions["dataclean"]["version"],
                })
            return {"nodes": nodes, "edges": graph.to_edges()}

    # ---------- 当前结果（列表与详情共用） ----------
    def list_results(self, field_name: str | None = None) -> list[dict[str, Any]]:
        with self.db.transaction() as conn:
            active = self.db.active_versions_by_field(conn)
            fields = [field_name] if field_name else sorted(active)
            unknown = [field for field in fields if field not in active]
            if unknown:
                raise PipelineConfigError(f"字段 {unknown[0]} 没有已生效的解析规则")
            raw_records = self.db.list_raw_records(conn)
            results = []
            for record in raw_records:
                item: dict[str, Any] = {
                    "record_id": record["record_id"],
                    "source": record["source"],
                    "raw_payload": record["payload"],
                    "fields": {},
                }
                for field in fields:
                    item["fields"][field] = self._field_result(
                        conn, record["record_id"], field, active
                    )
                results.append(item)
            return results

    def get_record_result(self, record_id: int) -> dict[str, Any] | None:
        with self.db.transaction() as conn:
            records = {row["record_id"]: row for row in self.db.list_raw_records(conn)}
            if record_id not in records:
                return None
            active = self.db.active_versions_by_field(conn)
            record = records[record_id]
            return {
                "record_id": record_id,
                "source": record["source"],
                "raw_payload": record["payload"],
                "created_at": record["created_at"],
                "fields": {
                    field: self._field_result(conn, record_id, field, active)
                    for field in sorted(active)
                },
            }

    def _field_result(
        self, conn: Any, record_id: int, field_name: str, active: dict[str, Any]
    ) -> dict[str, Any]:
        nodes = self.db.list_nodes_for_active(conn, record_id, field_name)
        by_stage = {node["stage"]: node for node in nodes}
        versions = active[field_name]
        stages = []
        for stage in ALL_STAGES:
            node = by_stage.get(stage)
            if node is None:
                continue
            stages.append({
                "stage": stage,
                "module": "parser" if stage in PARSER_STAGES else "dataclean",
                "rule_id": node["rule_id"],
                "rule_version": node["rule_version"],
                "output_value": node["output_value"],
                "reused": bool(node["reused"]),
                "source_node_id": node["source_node_id"],
                "node_id": node["node_id"],
            })
        parse_value = by_stage[PARSER_OUTPUT_STAGE]["output_value"] if PARSER_OUTPUT_STAGE in by_stage else None
        clean_value = by_stage[CLEAN_OUTPUT_STAGE]["output_value"] if CLEAN_OUTPUT_STAGE in by_stage else None
        return {
            "field_name": field_name,
            "parser_rule": versions["parser"]["rule_id"],
            "parser_version": versions["parser"]["version"],
            "clean_rule": versions["dataclean"]["rule_id"],
            "clean_version": versions["dataclean"]["version"],
            "parsed_value": parse_value,
            "cleaned_value": clean_value,
            "stages": stages,
        }

    # ---------- 规则版本与历史 ----------
    def list_rule_versions(self, field_name: str | None = None) -> list[dict[str, Any]]:
        with self.db.transaction() as conn:
            return self.db.list_rule_versions(conn, field_name)

    def get_rule_version(self, rule_id: str, version: int) -> dict[str, Any] | None:
        with self.db.transaction() as conn:
            return self.db.get_rule_version(conn, rule_id, version)

    def list_version_nodes(self, rule_id: str, version: int) -> list[dict[str, Any]]:
        with self.db.transaction() as conn:
            rule_version = self.db.get_rule_version(conn, rule_id, version)
            if rule_version is None:
                raise PipelineConfigError(f"规则版本 {rule_id} v{version} 不存在")
            nodes = self.db.list_nodes_for_version(conn, rule_id, version)
            # 同阶段保留最新一次执行结果，旧节点仍属于该历史版本的执行痕迹。
            latest: dict[tuple[int, str], dict[str, Any]] = {}
            for node in nodes:
                key = (node["record_id"], node["stage"])
                latest[key] = node
            return [
                {
                    "node_id": node["node_id"],
                    "record_id": node["record_id"],
                    "field_name": node["field_name"],
                    "stage": node["stage"],
                    "output_value": node["output_value"],
                    "reused": bool(node["reused"]),
                    "source_node_id": node["source_node_id"],
                    "created_batch": node["created_batch"],
                    "created_at": node["created_at"],
                }
                for node in sorted(latest.values(), key=lambda item: (item["record_id"], item["node_id"]))
            ]

    # ---------- 回填批次 ----------
    def list_batches(self) -> list[dict[str, Any]]:
        with self.db.transaction() as conn:
            return self.db.list_batches(conn)

    def get_batch(self, batch_id: int) -> dict[str, Any] | None:
        with self.db.transaction() as conn:
            return self.db.get_batch(conn, batch_id)

    def submit_rules(
        self, rules: list[dict[str, Any]], trigger_note: str
    ) -> dict[str, Any]:
        if not rules:
            raise PipelineConfigError("本批没有任何规则变更，无法发起回填")
        # 整批在一个事务里完成：任何阶段失败都会回滚，confirmed 当前结果不受影响。
        with self.db.transaction() as conn:
            batch = self.engine.submit_batch(conn, rules, trigger_note or "规则批次回填")
        # 事务提交成功后再同步内存里的模板/规则台账，保证字段解析器、数据去噪
        # 两个原有页面的列表与详情同步更新；失败回滚时不会触碰它们。
        self._sync_module_rows(rules)
        return batch

    def _sync_module_rows(self, rules: list[dict[str, Any]]) -> None:
        from app.store import store

        for rule in rules:
            module = "parser" if rule["module"] == "parser" else "dataclean"
            code_key = "模板编号" if module == "parser" else "规则编号"
            rows = store.rows(module)
            target = next(
                (row for row in rows if row.get(code_key) == rule["rule_id"]), None
            )
            if target is None:
                continue
            config = rule.get("config") or {}
            if module == "parser":
                target["类型转换规则"] = self._render_parser_config(config)
            else:
                target["HTML标签清理"] = "开启" if config.get("enabled_html") else "关闭"
                target["特殊字符过滤"] = "开启" if config.get("enabled_special") else "关闭"
                target["空白规整模式"] = str(config.get("whitespace_mode") or "collapse")
                target["长度截断阈值"] = str(config.get("max_length") or 0)
                target["敏感词脱敏"] = "、".join(config.get("sensitive_words") or []) or "无"

    @staticmethod
    def _render_parser_config(config: dict[str, Any]) -> str:
        convert_type = config.get("convert_type", "text")
        precision = config.get("precision")
        if convert_type == "number" and precision not in (None, ""):
            return f"number(precision={precision})"
        return str(convert_type)


pipeline_service = PipelineService()
