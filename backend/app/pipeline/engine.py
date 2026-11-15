"""流水线引擎：字段依赖图、步骤节点哈希、增量重算与原子版本切换。

关键口径：
1. 依赖图节点是“字段”，边来自解析规则的 references；改动字段只影响它和传递依赖它的字段。
2. 每个处理阶段（extract/convert/六类清洗/脱敏）是独立节点，配置哈希 + 输入哈希决定能否复用：
   - 输入敏感阶段（extract、convert、html_clean 等）必须配置与输入都不变才复用；
   - 敏感词脱敏属于规则型阶段，配置不变即复用，上游引用字段变化不错误推进它。
3. 新规则版本先以 pending 落库，所有节点构建成功后在同一事务里转 confirmed；
   任何一步失败整批回滚，confirmed 结果与列表读取在构建期间始终可用。
"""
from __future__ import annotations

import hashlib
import html
import re
from typing import Any

from app.pipeline.db import PipelineDB, dumps, now_text

PARSER_STAGES = ["extract", "convert"]
CLEAN_STAGES = [
    "html_clean",
    "special_char",
    "whitespace",
    "truncate",
    "encoding_fix",
    "mask",
]
ALL_STAGES = PARSER_STAGES + CLEAN_STAGES

# 输入敏感阶段：输入哈希变化就必须重算；mask 只看脱敏配置，配置不变继续复用。
INPUT_SENSITIVE = {stage: stage != "mask" for stage in ALL_STAGES}

TAG_RE = re.compile(r"<[^>]+>")
SPECIAL_RE = re.compile(r"[^\w一-鿿\s\.\-]")
SPACE_RE = re.compile(r"\s+")
DIGIT_RE = re.compile(r"-?\d+(?:\.\d+)?")


class PipelineConfigError(ValueError):
    """规则配置不完整或不合法：在版本确认前抛出，触发整批回滚。"""


class DependencyGraph:
    """字段 -> 依赖它的字段集合（反向边），支持传递闭包求影响面。"""

    def __init__(self, references: dict[str, list[str]]) -> None:
        self.dependents: dict[str, set[str]] = {}
        for field_name, deps in references.items():
            for dep in deps:
                self.dependents.setdefault(dep, set()).add(field_name)

    def affected_fields(self, changed: list[str]) -> list[str]:
        seen: set[str] = set()
        stack = list(changed)
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            stack.extend(self.dependents.get(current, set()))
        return sorted(seen)

    def to_edges(self) -> list[dict[str, str]]:
        edges: list[dict[str, str]] = []
        for dep, owners in sorted(self.dependents.items()):
            for owner in sorted(owners):
                edges.append({"from": dep, "to": owner})
        return edges


def stable_hash(value: Any) -> str:
    return hashlib.sha256(dumps(value).encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------- 单阶段转换函数

def _extract(raw_value: Any, config: dict[str, Any]) -> str:
    text = "" if raw_value is None else str(raw_value)
    regex = str(config.get("regex") or "").strip()
    if regex:
        match = re.search(regex, text)
        text = match.group(0) if match else ""
    if not text:
        text = str(config.get("default") if config.get("default") is not None else "")
    return text


def _convert(value: str, config: dict[str, Any]) -> str:
    convert_type = str(config.get("convert_type") or "text")
    default = config.get("default")
    if convert_type == "number":
        match = DIGIT_RE.search(str(value).replace(",", ""))
        if not match:
            return str(default if default is not None else "")
        number = float(match.group(0))
        precision = config.get("precision")
        if precision is not None and str(precision) != "":
            number = round(number, int(precision))
            return f"{number:.{int(precision)}f}"
        return str(number)
    if convert_type == "integer":
        match = DIGIT_RE.search(str(value).replace(",", ""))
        if not match:
            return str(default if default is not None else "")
        return str(int(float(match.group(0))))
    return str(value)


def _html_clean(value: str, config: dict[str, Any]) -> str:
    if not config.get("enabled"):
        return value
    return html.unescape(TAG_RE.sub("", value))


def _special_char(value: str, config: dict[str, Any]) -> str:
    if not config.get("enabled"):
        return value
    return SPECIAL_RE.sub("", value)


def _whitespace(value: str, config: dict[str, Any]) -> str:
    mode = str(config.get("mode") or "collapse")
    if mode == "collapse":
        return SPACE_RE.sub(" ", value).strip()
    if mode == "strip":
        return value.strip()
    if mode == "keep":
        return value
    return value.strip()


def _truncate(value: str, config: dict[str, Any]) -> str:
    max_length = config.get("max_length")
    if max_length in (None, "", 0) or int(max_length) <= 0:
        return value
    return value[: int(max_length)]


def _encoding_fix(value: str, config: dict[str, Any]) -> str:
    if not config.get("enabled"):
        return value
    # 演示口径：把常见 mojibake 片段纠正回正常字符，真实项目会接 charset 探测。
    return value.replace("Ã¥", "å").replace("Ã©", "é").replace("â€™", "’")


def _mask(value: str, config: dict[str, Any]) -> str:
    result = value
    for word in config.get("sensitive_words") or []:
        if word:
            result = result.replace(str(word), "**")
    return result


STAGE_FUNCS = {
    "extract": _extract,
    "convert": _convert,
    "html_clean": _html_clean,
    "special_char": _special_char,
    "whitespace": _whitespace,
    "truncate": _truncate,
    "encoding_fix": _encoding_fix,
    "mask": _mask,
}


def run_stage(stage: str, value: Any, config: dict[str, Any]) -> str:
    return STAGE_FUNCS[stage](value, config)


# ---------------------------------------------------------------- 配置校验

def validate_parser_config(config: dict[str, Any]) -> None:
    if not str(config.get("xpath") or "").strip():
        raise PipelineConfigError("解析规则缺少 xpath，无法定位目标字段")
    convert_type = str(config.get("convert_type") or "text")
    if convert_type not in {"text", "number", "integer"}:
        raise PipelineConfigError(f"类型转换规则不支持 {convert_type}")
    precision = config.get("precision")
    if precision not in (None, "") and int(precision) < 0:
        raise PipelineConfigError("数值精度不能为负数")


def validate_clean_config(config: dict[str, Any]) -> None:
    max_length = config.get("max_length")
    if max_length not in (None, "") and int(max_length) < 0:
        raise PipelineConfigError("长度截断阈值不能为负数")
    if not isinstance(config.get("sensitive_words") or [], list):
        raise PipelineConfigError("敏感词脱敏必须是词表数组")


# ---------------------------------------------------------------- 引擎

class PipelineEngine:
    def __init__(self, db: PipelineDB | None = None) -> None:
        self.db = db or PipelineDB()

    # ---------- 依赖图 ----------
    def build_graph(self, conn: Any) -> DependencyGraph:
        active = self.db.active_versions_by_field(conn)
        references: dict[str, list[str]] = {}
        # 字段间引用关系统一以已确认的 parser 版本为准。
        for field_name, versions in active.items():
            parser_version = versions.get("parser")
            references[field_name] = list(parser_version["references"]) if parser_version else []
        return DependencyGraph(references)

    # ---------- 初始种子版本 + 节点 ----------
    def seed_initial_pipeline(self, conn: Any) -> None:
        existing = self.db.list_rule_versions(conn)
        if existing:
            return
        seeds = self._seed_versions()
        seq = 0
        for item in seeds:
            seq += 1
            self.db.insert_rule_version(
                conn,
                rule_id=item["rule_id"], version=1, module=item["module"],
                field_name=item["field_name"], config=item["config"],
                references=item["references"], status="confirmed",
                confirmed_seq=seq, batch_id=1, confirmed_at=now_text(),
            )
            self.db.link_batch_version(conn, 1, item["rule_id"], 1)
        self._materialize_active(conn, batch_id=1, mark_reused=False)

    def _seed_versions(self) -> list[dict[str, Any]]:
        return [
            {
                "rule_id": "PARS-0001", "module": "parser", "field_name": "标题",
                "references": [],
                "config": {
                    "xpath": "//item/title/text()", "regex": "", "default": "未命名",
                    "convert_type": "text", "precision": None,
                },
            },
            {
                "rule_id": "PARS-0002", "module": "parser", "field_name": "价格",
                "references": [],
                "config": {
                    "xpath": "//item/price/text()", "regex": r"\d[\d,\.]*", "default": "0",
                    "convert_type": "number", "precision": 2,
                },
            },
            {
                "rule_id": "PARS-0003", "module": "parser", "field_name": "内容",
                "references": ["标题"],
                "config": {
                    "xpath": "//item/content/text()", "regex": "", "default": "",
                    "convert_type": "text", "precision": None,
                },
            },
            {
                "rule_id": "DATA-0001", "module": "dataclean", "field_name": "标题",
                "references": [],
                "config": {
                    "enabled_html": False, "enabled_special": True,
                    "whitespace_mode": "collapse", "max_length": 32,
                    "enabled_encoding": True,
                    "sensitive_words": ["微信"],
                },
            },
            {
                "rule_id": "DATA-0002", "module": "dataclean", "field_name": "价格",
                "references": [],
                "config": {
                    "enabled_html": False, "enabled_special": True,
                    "whitespace_mode": "collapse", "max_length": 0,
                    "enabled_encoding": False,
                    "sensitive_words": [],
                },
            },
            {
                "rule_id": "DATA-0003", "module": "dataclean", "field_name": "内容",
                "references": ["标题"],
                "config": {
                    "enabled_html": True, "enabled_special": True,
                    "whitespace_mode": "collapse", "max_length": 64,
                    "enabled_encoding": True,
                    "sensitive_words": ["微信", "QQ", "电话", "internal-wx",
                                        "13800001234", "80012345"],
                },
            },
        ]

    # ---------- 规则配置 -> 阶段配置 ----------
    def _parser_stage_config(self, stage: str, config: dict[str, Any]) -> dict[str, Any]:
        if stage == "extract":
            return {
                "xpath": config.get("xpath"),
                "regex": config.get("regex", ""),
                "default": config.get("default", ""),
            }
        return {
            "convert_type": config.get("convert_type", "text"),
            "precision": config.get("precision"),
            "default": config.get("default", ""),
        }

    def _clean_stage_config(self, stage: str, config: dict[str, Any]) -> dict[str, Any]:
        if stage == "html_clean":
            return {"enabled": bool(config.get("enabled_html"))}
        if stage == "special_char":
            return {"enabled": bool(config.get("enabled_special"))}
        if stage == "whitespace":
            return {"mode": config.get("whitespace_mode", "collapse")}
        if stage == "truncate":
            return {"max_length": config.get("max_length", 0)}
        if stage == "encoding_fix":
            return {"enabled": bool(config.get("enabled_encoding"))}
        return {"sensitive_words": list(config.get("sensitive_words") or [])}

    def _stage_config(self, module: str, stage: str, config: dict[str, Any]) -> dict[str, Any]:
        if module == "parser":
            return self._parser_stage_config(stage, config)
        return self._clean_stage_config(stage, config)

    # ---------- 节点物化 ----------
    def _materialize_node(
        self,
        conn: Any,
        *,
        record_id: int,
        field_name: str,
        module: str,
        stage: str,
        rule_version: dict[str, Any],
        stage_input: Any,
        batch_id: int,
        allow_reuse: bool,
    ) -> dict[str, Any]:
        stage_config = self._stage_config(module, stage, rule_version["config"])
        config_hash = stable_hash(stage_config)
        input_hash = stable_hash(stage_input)
        input_sensitive = INPUT_SENSITIVE[stage]
        reused_from = None
        if allow_reuse:
            reused_from = self.db.find_reusable_node(
                conn, record_id=record_id, field_name=field_name, stage=stage,
                config_hash=config_hash, input_hash=input_hash,
                input_sensitive=input_sensitive,
            )
        if reused_from is not None:
            node_id = self.db.insert_node(
                conn, record_id=record_id, field_name=field_name, stage=stage,
                rule_id=rule_version["rule_id"], rule_version=rule_version["version"],
                config_hash=config_hash, input_hash=input_hash,
                output_value=reused_from["output_value"], reused=True,
                source_node_id=reused_from["node_id"], batch_id=batch_id,
            )
        else:
            output = run_stage(stage, stage_input, stage_config)
            node_id = self.db.insert_node(
                conn, record_id=record_id, field_name=field_name, stage=stage,
                rule_id=rule_version["rule_id"], rule_version=rule_version["version"],
                config_hash=config_hash, input_hash=input_hash,
                output_value=output, reused=False, source_node_id=None,
                batch_id=batch_id,
            )
        node = self.db.find_node(
            conn, record_id=record_id, field_name=field_name, stage=stage,
            rule_id=rule_version["rule_id"], rule_version=rule_version["version"],
        )
        assert node is not None
        return node

    def _materialize_active(self, conn: Any, *, batch_id: int, mark_reused: bool) -> None:
        """按当前 confirmed 版本物化全部字段（种子装载用，节点全部新建）。"""
        active = self.db.active_versions_by_field(conn)
        raw_records = self.db.list_raw_records(conn)
        for record in raw_records:
            payload = record["payload"]
            for field_name in sorted(active):
                versions = active[field_name]
                parser_version = versions["parser"]
                extract_input = payload.get(field_name)
                self._materialize_node(
                    conn, record_id=record["record_id"], field_name=field_name,
                    module="parser", stage="extract", rule_version=parser_version,
                    stage_input=extract_input, batch_id=batch_id, allow_reuse=False,
                )
                self._materialize_node(
                    conn, record_id=record["record_id"], field_name=field_name,
                    module="parser", stage="convert", rule_version=parser_version,
                    stage_input=self._node_output(conn, record["record_id"], field_name,
                                                  "extract", parser_version),
                    batch_id=batch_id, allow_reuse=False,
                )
                clean_version = versions["dataclean"]
                prev_output = self._node_output(conn, record["record_id"], field_name,
                                                "convert", parser_version)
                for stage in CLEAN_STAGES:
                    node = self._materialize_node(
                        conn, record_id=record["record_id"], field_name=field_name,
                        module="dataclean", stage=stage, rule_version=clean_version,
                        stage_input=prev_output, batch_id=batch_id, allow_reuse=False,
                    )
                    prev_output = node["output_value"]

    def _node_output(
        self, conn: Any, record_id: int, field_name: str, stage: str,
        rule_version: dict[str, Any],
    ) -> str:
        node = self.db.find_node(
            conn, record_id=record_id, field_name=field_name, stage=stage,
            rule_id=rule_version["rule_id"], rule_version=rule_version["version"],
        )
        return node["output_value"] if node else ""

    # ---------- 回填：新版本构建 + 原子切换 ----------
    def submit_batch(
        self, conn: Any, rules: list[dict[str, Any]], trigger_note: str
    ) -> dict[str, Any]:
        # 1. 校验入参并确定每条规则的新版本号（先不落 confirmed）。
        normalized: list[dict[str, Any]] = []
        changed_fields: list[str] = []
        for rule in rules:
            module = str(rule.get("module") or "").strip()
            rule_id = str(rule.get("rule_id") or "").strip()
            field_name = str(rule.get("field_name") or "").strip()
            config = rule.get("config") or {}
            references = list(rule.get("references") or [])
            if module not in {"parser", "dataclean"}:
                raise PipelineConfigError(f"规则模块 {module} 不属于解析/去噪范围")
            if not rule_id or not field_name:
                raise PipelineConfigError("规则编号与目标字段均不能为空")
            if module == "parser":
                validate_parser_config(config)
            else:
                validate_clean_config(config)
            next_version = self.db.latest_version_number(conn, rule_id) + 1
            normalized.append({
                "rule_id": rule_id, "version": next_version, "module": module,
                "field_name": field_name, "config": config, "references": references,
            })
            changed_fields.append(field_name)

        # 2. 依赖图（基于当前 confirmed 版本）求传递影响面。
        graph = self.build_graph(conn)
        affected = graph.affected_fields(sorted(set(changed_fields)))

        batch_id = self.db.create_batch(conn, trigger_note)
        seq = self.db.next_confirmed_seq(conn)
        new_versions: list[dict[str, Any]] = []
        for item in normalized:
            self.db.insert_rule_version(
                conn, rule_id=item["rule_id"], version=item["version"],
                module=item["module"], field_name=item["field_name"],
                config=item["config"], references=item["references"],
                status="pending", confirmed_seq=None, batch_id=batch_id,
            )
            self.db.link_batch_version(conn, batch_id, item["rule_id"], item["version"])
            new_versions.append(item)

        # 3. 仅对受影响字段按新版本重建节点；未受影响字段完全不触碰。
        summary = self._rebuild_affected(
            conn, batch_id=batch_id, affected=affected, new_versions=new_versions,
        )

        # 4. 测试注入：节点已构建但确认前失败，验证整批回滚与旧版本仍可读。
        if any(item.get("config", {}).get("_fail_after_build") for item in rules):
            raise PipelineConfigError("模拟回填确认失败：本批节点将整体回滚")

        # 5. 全部成功：同一事务内把 pending 转 confirmed（可切换的新版本正式替换当前结果）。
        for item in new_versions:
            self.db.confirm_version(conn, item["rule_id"], item["version"], seq)
            seq += 1
        reused_fields = sorted(
            field for field, stat in summary["fields"].items()
            if stat["reused"] > 0 and stat["created"] == 0
        )
        self.db.finish_batch(
            conn, batch_id, status="succeeded", affected_fields=affected,
            reused_fields=reused_fields, node_summary=summary,
        )
        result = self.db.get_batch(conn, batch_id)
        assert result is not None
        result["dependency_edges"] = graph.to_edges()
        return result

    def _rebuild_affected(
        self,
        conn: Any,
        *,
        batch_id: int,
        affected: list[str],
        new_versions: list[dict[str, Any]],
    ) -> dict[str, Any]:
        active_before = self.db.active_versions_by_field(conn, affected)
        missing = [field for field in affected if field not in active_before]
        if missing:
            # 新字段尚未建立首版规则，当前流水线只支持对已生效字段做版本化回填。
            raise PipelineConfigError(
                f"字段 {missing[0]} 还没有已生效的解析/去噪规则，无法增量回填"
            )
        raw_records = self.db.list_raw_records(conn)
        field_stats: dict[str, dict[str, int]] = {
            field: {"created": 0, "reused": 0} for field in affected
        }
        stage_stats: dict[str, dict[str, int]] = {
            stage: {"created": 0, "reused": 0} for stage in ALL_STAGES
        }
        for record in raw_records:
            payload = record["payload"]
            for field_name in affected:
                current = active_before[field_name]
                parser_update = next(
                    (v for v in new_versions
                     if v["field_name"] == field_name and v["module"] == "parser"),
                    None,
                )
                clean_update = next(
                    (v for v in new_versions
                     if v["field_name"] == field_name and v["module"] == "dataclean"),
                    None,
                )
                parser_version = parser_update or current["parser"]
                clean_version = clean_update or current["dataclean"]

                # 解析两段：extract 输入是原始值；convert 输入是同版 extract 输出。
                extract_node = self._materialize_node(
                    conn, record_id=record["record_id"], field_name=field_name,
                    module="parser", stage="extract", rule_version=parser_version,
                    stage_input=payload.get(field_name),
                    batch_id=batch_id, allow_reuse=True,
                )
                self._count(field_stats, stage_stats, field_name, "extract", extract_node)
                convert_node = self._materialize_node(
                    conn, record_id=record["record_id"], field_name=field_name,
                    module="parser", stage="convert", rule_version=parser_version,
                    stage_input=extract_node["output_value"],
                    batch_id=batch_id, allow_reuse=True,
                )
                self._count(field_stats, stage_stats, field_name, "convert", convert_node)

                prev_output = convert_node["output_value"]
                for stage in CLEAN_STAGES:
                    node = self._materialize_node(
                        conn, record_id=record["record_id"], field_name=field_name,
                        module="dataclean", stage=stage, rule_version=clean_version,
                        stage_input=prev_output,
                        batch_id=batch_id, allow_reuse=True,
                    )
                    self._count(field_stats, stage_stats, field_name, stage, node)
                    prev_output = node["output_value"]
        return {"fields": field_stats, "stages": stage_stats}

    def _count(
        self,
        field_stats: dict[str, dict[str, int]],
        stage_stats: dict[str, dict[str, int]],
        field_name: str,
        stage: str,
        node: dict[str, Any],
    ) -> None:
        bucket = "reused" if node["reused"] else "created"
        field_stats[field_name][bucket] += 1
        stage_stats[stage][bucket] += 1
