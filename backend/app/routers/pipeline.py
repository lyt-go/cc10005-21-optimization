"""解析清洗流水线接口：依赖图、版本化结果、历史版本与增量回填。

回填采用“先构建 pending 新版本、全部成功再原子切换 confirmed”的口径，
构建期间列表/详情读取的始终是上一版已确认结果。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.pipeline.engine import PipelineConfigError
from app.pipeline.service import pipeline_service

router = APIRouter(prefix="/api/pipeline", tags=["解析清洗流水线"])


class RuleChange(BaseModel):
    module: str = Field(description="parser 或 dataclean")
    rule_id: str = Field(description="规则编号，如 PARS-0002 / DATA-0002")
    field_name: str = Field(description="目标字段")
    config: dict[str, Any] = Field(default_factory=dict)
    references: list[str] = Field(default_factory=list)


class BackfillPayload(BaseModel):
    rules: list[RuleChange]
    trigger_note: str | None = None


@router.get("/graph")
def get_graph() -> dict[str, Any]:
    """字段依赖图：节点为字段，边表示解析规则对其他字段的引用。"""
    return pipeline_service.dependency_graph()


@router.get("/results")
def list_results(
    field_name: str | None = Query(default=None, description="只看某个目标字段"),
) -> dict[str, Any]:
    """当前生效的解析字段结果与清洗结果；列表与详情共用同一份数据口径。"""
    try:
        items = pipeline_service.list_results(field_name)
    except PipelineConfigError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"items": items, "total": len(items)}


@router.get("/records/{record_id}")
def get_record(record_id: int) -> dict[str, Any]:
    """单条采集记录详情：原始值、各阶段节点、解析结果与清洗结果。"""
    result = pipeline_service.get_record_result(record_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"采集记录 {record_id} 不存在")
    return result


@router.get("/rules/versions")
def list_versions(
    field_name: str | None = Query(default=None, description="按目标字段过滤版本"),
) -> dict[str, Any]:
    """规则版本清单（含 pending/confirmed 与历史版本，全部保留）。"""
    items = pipeline_service.list_rule_versions(field_name)
    return {"items": items, "total": len(items)}


@router.get("/rules/{rule_id}/versions/{version}")
def get_version(rule_id: str, version: int) -> dict[str, Any]:
    item = pipeline_service.get_rule_version(rule_id, version)
    if item is None:
        raise HTTPException(status_code=404, detail=f"规则版本 {rule_id} v{version} 不存在")
    item["nodes"] = pipeline_service.list_version_nodes(rule_id, version)
    return item


@router.get("/batches")
def list_batches() -> dict[str, Any]:
    items = pipeline_service.list_batches()
    return {"items": items, "total": len(items)}


@router.get("/batches/{batch_id}")
def get_batch(batch_id: int) -> dict[str, Any]:
    item = pipeline_service.get_batch(batch_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"回填批次 {batch_id} 不存在")
    return item


@router.post("/backfill")
def run_backfill(payload: BackfillPayload) -> dict[str, Any]:
    """提交规则变更批次：依赖图缩小重算范围，成功后原子切换新版本。

    配置非法或构建确认失败时整批回滚，已确认结果保持不变，接口返回 409。
    """
    rules = [change.model_dump() for change in payload.rules]
    try:
        batch = pipeline_service.submit_rules(rules, payload.trigger_note or "")
    except PipelineConfigError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"ok": True, "batch": batch}
