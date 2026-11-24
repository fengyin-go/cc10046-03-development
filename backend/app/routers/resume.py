"""续采计划专用接口：计划生成、调度派发、回执登记、缺口核对与窗口校验。

路由必须在通用资源路由（/api/{module}）之前注册，固定路径才不会被动态模块路径抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.resume_plan import ResumePlanService

router = APIRouter(prefix="/api/resume", tags=["续采计划"])

_service = ResumePlanService()


class PlanPayload(BaseModel):
    source: str
    start: str
    end: str
    granularity_minutes: int = 60


class DispatchPayload(BaseModel):
    plan_id: str


class ReceiptPayload(BaseModel):
    source: str
    segment_start: str
    segment_end: str
    status: str  # 全部成功 / 部分成功 / 采集失败
    batch_id: str
    rows: list[Any] = Field(default_factory=list)
    fail_reason: str | None = None


class VerifyPayload(BaseModel):
    source: str
    start: str
    end: str
    granularity_minutes: int = 60


def _guard(call):
    try:
        return call()
    except ValueError as exc:
        return {"ok": False, "message": str(exc)}


@router.post("/plans")
def create_plan(payload: PlanPayload) -> dict[str, Any]:
    """生成（或幂等复用）一份续采计划。"""
    return _guard(lambda: _unwrap(
        _service.domain_create_plan(
            payload.source, payload.start, payload.end, payload.granularity_minutes)))


def _unwrap(result: tuple[dict[str, Any], bool]) -> dict[str, Any]:
    plan, reused = result
    return {"ok": True, "message": "续采计划已存在，沿用既有分片与流转记录" if reused else "续采计划已生成",
            "reused": reused, "plan": plan}


@router.post("/dispatch")
def dispatch(payload: DispatchPayload) -> dict[str, Any]:
    """采集调度按续采计划真正发起下一轮任务，只补还缺数据的区间。"""
    data = _guard(lambda: _service.domain_dispatch(payload.plan_id))
    if isinstance(data, dict) and data.get("ok") is False:
        return data
    return {"ok": True, "message": f"已派发 {len(data['dispatched'])} 个时段任务", **data}


@router.post("/receipts")
def register_receipt(payload: ReceiptPayload) -> dict[str, Any]:
    """登记来源采集完成回执；重复回执幂等，不会重复落库。"""
    data = _guard(lambda: _service.domain_register_receipt(
        payload.source,
        payload.segment_start,
        payload.segment_end,
        status=payload.status,
        batch_id=payload.batch_id,
        rows=payload.rows,
        fail_reason=payload.fail_reason,
    ))
    if isinstance(data, dict) and data.get("ok") is False:
        return data
    if data["duplicate"]:
        message = "该时段完成回执已登记过，重复回执已忽略，未重复落库"
    elif not data["matched_plan"]:
        message = "回执已入落库/失败台账，暂无续采计划引用该时段"
    else:
        message = f"回执已登记，新增落库 {data['inserted_count']} 行"
    return {"ok": True, "message": message, **data}


@router.post("/rearm")
def rearm(payload: DispatchPayload) -> dict[str, Any]:
    """值班员把计划切换回待采状态并触发下一轮同步前的核对。"""
    data = _guard(lambda: _service.domain_rearm(payload.plan_id))
    if isinstance(data, dict) and data.get("ok") is False:
        return data
    return {"ok": True, "message": "已切换到待采状态，可触发下一轮同步", **data}


@router.get("/plans/{plan_id}/gaps")
def gap_report(plan_id: str) -> dict[str, Any]:
    """查看本次会采哪些时段、哪些时段仍待补。"""
    data = _guard(lambda: _service.domain_gap_report(plan_id))
    if isinstance(data, dict) and data.get("ok") is False:
        return data
    return {"ok": True, **data}


@router.post("/verify")
def verify_window(payload: VerifyPayload) -> dict[str, Any]:
    """补采完成后核对原数据窗口：无漏行、无重复落库。"""
    data = _guard(lambda: _service.domain_verify_window(
        payload.source, payload.start, payload.end, payload.granularity_minutes))
    if isinstance(data, dict) and data.get("ok") is False:
        return data
    return {"ok": True, **data}
