"""续采计划路由层测试：直接调用路由函数，覆盖固定路径优先与错误兜底。"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.routers import resume as resume_router
from app.routers.resume import (
    DispatchPayload,
    PlanPayload,
    ReceiptPayload,
    VerifyPayload,
)
from app.store import store

SRC = "来源C-政务站"
W10, W11, W12 = "2026-11-24 10:00", "2026-11-24 11:00", "2026-11-24 12:00"


@pytest.fixture()
def clean_ledger():
    store._tables["resume_plan"] = []
    store._tables["resume_landed"] = []
    store._tables["resume_failure"] = []
    yield


def test_full_http_flow(clean_ledger):
    created = resume_router.create_plan(PlanPayload(
        source=SRC, start=W10, end=W12, granularity_minutes=60))
    assert created["ok"] is True
    plan_id = created["plan"]["计划标识"]

    # 固定路径 /api/resume/... 与通用模块路径 /api/resume_plan 共存
    assert resume_router.router.prefix == "/api/resume"

    dispatched = resume_router.dispatch(DispatchPayload(plan_id=plan_id))
    assert dispatched["ok"] is True
    assert len(dispatched["dispatched"]) == 2

    # 采集中再派发一次：不重复建任务
    again = resume_router.dispatch(DispatchPayload(plan_id=plan_id))
    assert len(again["dispatched"]) == 0

    # 前段失败、后段成功：缺口不能被后段完成掩盖
    resume_router.register_receipt(ReceiptPayload(
        source=SRC, segment_start=W10, segment_end=W11,
        status="部分成功", batch_id="C-FAIL", rows=[{"biz_id": "C-1"}],
        fail_reason="连接重置"))
    resume_router.register_receipt(ReceiptPayload(
        source=SRC, segment_start=W11, segment_end=W12,
        status="全部成功", batch_id="C-OK", rows=[{"biz_id": "C-2"}, {"biz_id": "C-3"}]))

    gaps = resume_router.gap_report(plan_id)
    assert [s["start"] for s in gaps["failed_segments"]] == [W10]
    assert [s["start"] for s in gaps["landed_segments"]] == [W11]

    # 切换待采后只补缺口段，已落库段跳过
    armed = resume_router.rearm(DispatchPayload(plan_id=plan_id))
    assert armed["ok"] is True
    second_round = resume_router.dispatch(DispatchPayload(plan_id=plan_id))
    assert [d["start"] for d in second_round["dispatched"]] == [W10]
    assert len(second_round["skipped_landed"]) == 1

    # 补采成功，重复业务键不再落库
    backfill = resume_router.register_receipt(ReceiptPayload(
        source=SRC, segment_start=W10, segment_end=W11,
        status="全部成功", batch_id="C-BACKFILL",
        rows=[{"biz_id": "C-1"}, {"biz_id": "C-4"}]))
    assert backfill["inserted_count"] == 1
    assert backfill["duplicate_rows"] == ["C-1"]

    # 重复完成回执：幂等
    dup = resume_router.register_receipt(ReceiptPayload(
        source=SRC, segment_start=W11, segment_end=W12,
        status="全部成功", batch_id="C-OK",
        rows=[{"biz_id": "C-2"}, {"biz_id": "C-3"}]))
    assert dup["duplicate"] is True
    assert dup["inserted_count"] == 0

    verified = resume_router.verify_window(VerifyPayload(
        source=SRC, start=W10, end=W12, granularity_minutes=60))
    assert verified["complete"] is True
    assert verified["total_rows"] == 4
    assert verified["duplicate_biz_ids"] == []
    assert verified["missing_segments"] == []


def test_invalid_window_returns_guard_message(clean_ledger):
    result = resume_router.create_plan(PlanPayload(
        source=SRC, start=W12, end=W10, granularity_minutes=60))
    assert result == {"ok": False, "message": "区间终点必须晚于区间起点"}


def test_unknown_plan_returns_guard_message(clean_ledger):
    result = resume_router.dispatch(DispatchPayload(plan_id="RP-4040"))
    assert result["ok"] is False and "不存在" in result["message"]
    gaps = resume_router.gap_report("RP-4040")
    assert gaps["ok"] is False


def test_internal_ledgers_hidden_from_overview(clean_ledger):
    """落库/失败台账是内部表，不对外暴露为业务模块。"""
    from app.store import HIDDEN_TABLES, store as data_store

    assert HIDDEN_TABLES == {"resume_landed", "resume_failure"}
    # 台账有数据后仍不出现在模块列表与运营概览
    data_store.rows("resume_landed").append({"id": 1, "biz_id": "x"})
    data_store.rows("resume_failure").append({"id": 1, "reason": "y"})
    names = data_store.module_names()
    assert "resume_landed" not in names and "resume_failure" not in names
    overview = data_store.overview()
    overview_names = {item["name"] for item in overview["modules"]}
    assert "resume_landed" not in overview_names
    assert "resume_failure" not in overview_names
    # 续采计划本身仍然对外可见
    assert "resume_plan" in names
