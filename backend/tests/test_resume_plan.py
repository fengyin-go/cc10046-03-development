"""续采计划端到端场景测试。

场景（题目验收点）：
- 来源 A：11:00-12:00 已保存四条，10:00-11:00 采集中途失败；
- 12:00 之后的新网页记录（12:00-13:00）到达后，值班员切换待采并触发同步，
  要能看到本次会采哪些时段、哪些仍待补；
- 补回进度指向实际结果批次；同一来源完成回执重复收到不能再落一遍；
- 已读到后段也不能把前面的缺口登记为采集完成；
- 补采后查 10:00-12:00 原数据，无漏行、无重复落库；
- 来源 B 同期也有失败，补完来源 A 后，来源 B 的缺口仍可按原范围续采。
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.crawl_schedule import CrawlScheduleService
from app.services.resume_plan import (
    SEG_FAILED,
    SEG_LANDED,
    SEG_PENDING,
    SEG_RUNNING,
    PLAN_DONE,
    PLAN_GAP,
    PLAN_IDLE,
    PLAN_RUNNING,
    ResumePlanService,
)
from app.store import store

SRC_A = "来源A-新闻站"
SRC_B = "来源B-论坛站"
W10, W11, W12, W13 = (
    "2026-11-24 10:00",
    "2026-11-24 11:00",
    "2026-11-24 12:00",
    "2026-11-24 13:00",
)


@pytest.fixture()
def svc() -> ResumePlanService:
    # 每个用例用干净的续采台账，但保留 seed 里的其它模块数据
    store._tables["resume_plan"] = []
    store._tables["resume_landed"] = []
    store._tables["resume_failure"] = []
    return ResumePlanService()


def _seg(plan: dict, index: int) -> dict:
    return plan["segments"][index]


class TestScenarioSourceA:
    """来源 A 全链路：后段先落库、前段失败、切换待采、补采恢复。"""

    def test_pre_arrived_receipts_rebuild_plan_state(self, svc: ResumePlanService):
        """11-12 已保存四条、10-11 中途失败时再生成计划，状态从台账恢复。"""
        # 后段先到且成功（先于计划存在的回执也要能入账）
        rows_1112 = [{"biz_id": f"A-{i}"} for i in range(101, 105)]
        r1 = svc.domain_register_receipt(
            SRC_A, W11, W12, status="全部成功", batch_id="B-A-1112", rows=rows_1112)
        assert r1["inserted_count"] == 4
        assert r1["matched_plan"] is False  # 暂无计划引用，台账先兜住

        # 前段中途失败
        r2 = svc.domain_register_receipt(
            SRC_A, W10, W11, status="部分成功", batch_id="B-A-1011-fail",
            rows=[{"biz_id": "A-PARTIAL-1"}], fail_reason="代理连接中断")
        assert r2["matched_plan"] is False
        # 失败时段里已采到的行仍允许入账，缺口由失败台账保留
        assert r2["inserted_count"] == 1

        plan, reused = svc.domain_create_plan(SRC_A, W10, W13, 60)
        assert reused is False
        assert [s["start"] for s in plan["segments"]] == [W10, W11, W12]
        # 已读到后段，前段缺口仍然是采集中断，不能被登记为采集完成
        assert _seg(plan, 0)["status"] == SEG_FAILED
        assert _seg(plan, 1)["status"] == SEG_LANDED
        assert _seg(plan, 1)["result_batch_id"] == "B-A-1112"
        assert _seg(plan, 2)["status"] == SEG_PENDING
        assert plan["计划状态"] == PLAN_GAP
        assert plan["待补段数"] == 2

    def test_dispatch_only_covers_missing_segments(self, svc: ResumePlanService):
        """调度真正发起下一轮任务时，只补还缺数据的区间，已落库段跳过。"""
        svc.domain_register_receipt(
            SRC_A, W11, W12, status="全部成功", batch_id="B-A-1112",
            rows=[{"biz_id": f"A-{i}"} for i in range(101, 105)])
        svc.domain_register_receipt(
            SRC_A, W10, W11, status="部分成功", batch_id="B-A-1011-fail",
            rows=[{"biz_id": "A-PARTIAL-1"}], fail_reason="代理连接中断")
        plan, _ = svc.domain_create_plan(SRC_A, W10, W13, 60)

        before = len(store.rows("crawl_schedule"))
        result = svc.domain_dispatch(plan["计划标识"])
        dispatched_ids = {item["segment_id"] for item in result["dispatched"]}
        assert dispatched_ids == {_seg(plan, 0)["segment_id"], _seg(plan, 2)["segment_id"]}
        # 已落库的 11-12 不再派发
        assert result["skipped_landed"] == [{
            "segment_id": _seg(plan, 1)["segment_id"],
            "start": W11, "end": W12, "result_batch_id": "B-A-1112",
        }]
        # 调度任务在 crawl_schedule 里真实落库，且绑定续采计划与分片
        tasks = store.rows("crawl_schedule")[before:]
        assert len(tasks) == 2
        assert all(t["status"] == "运行中" for t in tasks)
        assert {t["起始水位"] for t in tasks} == {W10, W12}
        assert {t["截止水位"] for t in tasks} == {W11, W13}
        assert all(t["plan_code"] == plan["计划标识"] for t in tasks)
        assert _seg(plan, 0)["status"] == SEG_RUNNING
        assert _seg(plan, 2)["status"] == SEG_RUNNING
        assert plan["计划状态"] == PLAN_RUNNING

    def test_gap_report_shows_next_round_windows(self, svc: ResumePlanService):
        """值班员切换待采后，缺口报告要说明本次会采哪些、哪些仍待补。"""
        svc.domain_register_receipt(
            SRC_A, W11, W12, status="全部成功", batch_id="B-A-1112",
            rows=[{"biz_id": f"A-{i}"} for i in range(101, 105)])
        svc.domain_register_receipt(
            SRC_A, W10, W11, status="部分成功", batch_id="B-A-1011-fail",
            rows=[{"biz_id": "A-PARTIAL-1"}], fail_reason="代理连接中断")
        plan, _ = svc.domain_create_plan(SRC_A, W10, W13, 60)

        # 先派发再等 12-13 失败，形成采集中断 + 待采并存
        svc.domain_dispatch(plan["计划标识"])
        svc.domain_register_receipt(
            SRC_A, W12, W13, status="采集失败", batch_id="B-A-1213-fail",
            rows=[], fail_reason="目标站点限流")
        # 10-11 的任务也以失败回执收尾，全部缺口段进入终态后才允许切换待采
        svc.domain_register_receipt(
            SRC_A, W10, W11, status="部分成功", batch_id="B-A-1011-fail-2",
            rows=[{"biz_id": "A-PARTIAL-1"}], fail_reason="代理连接中断")

        rearm = svc.domain_rearm(plan["计划标识"])
        assert rearm["plan"]["status"] in {PLAN_GAP, PLAN_IDLE}
        report = svc.domain_gap_report(plan["计划标识"])
        assert {s["start"] for s in report["failed_segments"]} == {W10, W12}
        assert {s["start"] for s in report["landed_segments"]} == {W11}
        # 下一轮同步会采的时段 = 中断段 + 待采段，已落库段不在其中
        next_round = {s["start"] for s in report["failed_segments"]} | {
            s["start"] for s in report["pending_segments"]}
        assert W11 not in next_round
        assert W10 in next_round and W12 in next_round

    def test_duplicate_completion_receipt_is_idempotent(self, svc: ResumePlanService):
        """同一来源同一时段的完成回执重复收到，不能再落一遍。"""
        rows = [{"biz_id": f"A-DUP-{i}"} for i in range(3)]
        plan, _ = svc.domain_create_plan(SRC_A, W11, W12, 60)
        svc.domain_dispatch(plan["计划标识"])
        first = svc.domain_register_receipt(
            SRC_A, W11, W12, status="全部成功", batch_id="B-A-DUP", rows=rows)
        assert first["inserted_count"] == 3
        assert _seg(plan, 0)["status"] == SEG_LANDED
        assert _seg(plan, 0)["receipts"] == 1

        second = svc.domain_register_receipt(
            SRC_A, W11, W12, status="全部成功", batch_id="B-A-DUP", rows=rows)
        assert second["duplicate"] is True
        assert second["inserted_count"] == 0
        assert second["duplicate_rows"] == [r["biz_id"] for r in rows]
        # 结果存储批次没有重复登记
        store_batches = [r for r in store.rows("result_store") if r["批次标识"] == "B-A-DUP"]
        assert len(store_batches) == 1
        landed = svc._landed_in_segment(SRC_A, W11, W12)
        assert len(landed) == 3
        assert _seg(plan, 0)["status"] == SEG_LANDED
        # 回执次数留痕，状态不回退，数据不重复
        assert _seg(plan, 0)["receipts"] == 2

    def test_backfill_points_to_real_batch_and_window_verifies(self, svc: ResumePlanService):
        """补采进度指向实际结果批次，完成后 10-12 窗口无漏行、无重复落库。"""
        svc.domain_register_receipt(
            SRC_A, W11, W12, status="全部成功", batch_id="B-A-1112",
            rows=[{"biz_id": f"A-{i}"} for i in range(101, 105)])
        svc.domain_register_receipt(
            SRC_A, W10, W11, status="部分成功", batch_id="B-A-1011-fail",
            rows=[{"biz_id": "A-PARTIAL-1"}], fail_reason="代理连接中断")
        plan, _ = svc.domain_create_plan(SRC_A, W10, W13, 60)
        svc.domain_dispatch(plan["计划标识"])

        # 补采 10-11：把中途失败后缺的行补齐，已入账的 PARTIAL-1 不重复落
        backfill_rows = [
            {"biz_id": "A-PARTIAL-1"},  # 已落库，幂等忽略
            *({"biz_id": f"A-BACK-{i}"} for i in range(6)),
        ]
        receipt = svc.domain_register_receipt(
            SRC_A, W10, W11, status="全部成功", batch_id="B-A-1011-BACKFILL",
            rows=backfill_rows)
        assert receipt["inserted_count"] == 6
        assert receipt["duplicate_rows"] == ["A-PARTIAL-1"]

        seg10 = _seg(plan, 0)
        assert seg10["status"] == SEG_LANDED
        assert seg10["result_batch_id"] == "B-A-1011-BACKFILL"  # 指向实际结果批次
        assert seg10["row_count"] == 7

        # 调度任务随成功回执收尾
        task = next(t for t in store.rows("crawl_schedule") if t["任务标识"] == seg10["task_id"])
        assert task["status"] == "已完成"

        # 10-12 原数据窗口核对：无漏行、无重复
        verify = svc.domain_verify_window(SRC_A, W10, W12, 60)
        assert verify["complete"] is True
        assert verify["missing_segments"] == []
        assert verify["duplicate_biz_ids"] == []
        assert verify["total_rows"] == 11  # 10-11 七条 + 11-12 四条
        assert all(s["status"] == SEG_LANDED for s in verify["segments"])

    def test_full_window_completes_plan(self, svc: ResumePlanService):
        """三段全部补齐后计划才允许完成，后段先成功不会提前宣告完成。"""
        svc.domain_register_receipt(
            SRC_A, W11, W12, status="全部成功", batch_id="B-A-1112",
            rows=[{"biz_id": "A-101"}])
        plan, _ = svc.domain_create_plan(SRC_A, W10, W13, 60)
        assert plan["计划状态"] != PLAN_DONE

        svc.domain_dispatch(plan["计划标识"])
        svc.domain_register_receipt(
            SRC_A, W10, W11, status="全部成功", batch_id="B-A-1011",
            rows=[{"biz_id": "A-201"}])
        assert plan["计划状态"] == PLAN_RUNNING  # 12-13 仍在采集中，计划不能提前完成
        assert plan["待补段数"] == 1  # 仅剩 12-13 一段未落库
        svc.domain_register_receipt(
            SRC_A, W12, W13, status="全部成功", batch_id="B-A-1213",
            rows=[{"biz_id": "A-301"}])
        assert plan["计划状态"] == PLAN_DONE
        assert plan["pending"] is False
        verify = svc.domain_verify_window(SRC_A, W10, W13, 60)
        assert verify["complete"] is True

    def test_undispatched_gap_keeps_plan_in_gap_state(self, svc: ResumePlanService):
        """没有派发过的缺口段必须保持待补采，不会被后段落库掩盖。"""
        svc.domain_register_receipt(
            SRC_A, W12, W13, status="全部成功", batch_id="B-A-1213",
            rows=[{"biz_id": "A-301"}])
        plan, _ = svc.domain_create_plan(SRC_A, W10, W13, 60)
        assert plan["计划状态"] == PLAN_GAP  # 12-13 已落库，前面两段待补
        assert plan["待补段数"] == 2
        # 计划生成后只对 10-11 制造失败回执（不经派发），11-12 仍待采
        svc.domain_register_receipt(
            SRC_A, W10, W11, status="采集失败", batch_id="B-FAIL",
            rows=[], fail_reason="超时")
        assert plan["计划状态"] == PLAN_GAP
        assert plan["待补段数"] == 2
        assert _seg(plan, 0)["status"] == SEG_FAILED
        assert _seg(plan, 1)["status"] == SEG_PENDING


class TestScenarioSourceB:
    """来源 B 同期失败，且与来源 A 的续采互不干扰。"""

    def test_source_b_keeps_own_gap_after_source_a_backfill(self, svc: ResumePlanService):
        svc.domain_register_receipt(
            SRC_A, W11, W12, status="全部成功", batch_id="B-A-1112",
            rows=[{"biz_id": "A-1"}])
        svc.domain_register_receipt(
            SRC_A, W10, W11, status="部分成功", batch_id="B-A-1011-fail",
            rows=[{"biz_id": "A-2"}], fail_reason="代理连接中断")
        plan_a, _ = svc.domain_create_plan(SRC_A, W10, W12, 60)

        # 来源 B 同期也失败
        svc.domain_register_receipt(
            SRC_B, W10, W11, status="部分成功", batch_id="B-B-1011-fail",
            rows=[{"biz_id": "B-1"}], fail_reason="鉴权过期")
        svc.domain_register_receipt(
            SRC_B, W11, W12, status="全部成功", batch_id="B-B-1112",
            rows=[{"biz_id": "B-2"}, {"biz_id": "B-3"}])
        plan_b, _ = svc.domain_create_plan(SRC_B, W10, W12, 60)
        assert _seg(plan_b, 0)["status"] == SEG_FAILED
        assert _seg(plan_b, 1)["status"] == SEG_LANDED

        # 补完来源 A
        svc.domain_dispatch(plan_a["计划标识"])
        svc.domain_register_receipt(
            SRC_A, W10, W11, status="全部成功", batch_id="B-A-1011-BACKFILL",
            rows=[{"biz_id": "A-2"}, {"biz_id": "A-3"}])
        assert plan_a["计划状态"] == PLAN_DONE

        # 来源 B 的缺口仍按原范围保留并可续采
        report_b = svc.domain_gap_report(plan_b["计划标识"])
        assert [s["start"] for s in report_b["failed_segments"]] == [W10]
        dispatch_b = svc.domain_dispatch(plan_b["计划标识"])
        assert len(dispatch_b["dispatched"]) == 1
        assert dispatch_b["dispatched"][0]["start"] == W10
        assert len(dispatch_b["skipped_landed"]) == 1

        svc.domain_register_receipt(
            SRC_B, W10, W11, status="全部成功", batch_id="B-B-1011-BACKFILL",
            rows=[{"biz_id": "B-1"}, {"biz_id": "B-4"}])
        verify_b = svc.domain_verify_window(SRC_B, W10, W12, 60)
        assert verify_b["complete"] is True
        assert verify_b["total_rows"] == 4  # 10-11 两条 + 11-12 两条，B-1 未重复

        # 两来源台账严格隔离
        assert all(row["来源配置"] != SRC_A for row in svc._landed_in_segment(SRC_B, W10, W11))
        verify_a = svc.domain_verify_window(SRC_A, W10, W12, 60)
        assert verify_a["complete"] is True


class TestPlanLifecycle:
    def test_create_plan_is_idempotent(self, svc: ResumePlanService):
        plan1, reused1 = svc.domain_create_plan(SRC_A, W10, W12, 60)
        plan2, reused2 = svc.domain_create_plan(SRC_A, W10, W12, 60)
        assert reused1 is False and reused2 is True
        assert plan1["计划标识"] == plan2["计划标识"]
        # 同来源不同区间是另一份计划
        plan3, reused3 = svc.domain_create_plan(SRC_A, W10, W13, 60)
        assert reused3 is False
        assert plan3["计划标识"] != plan1["计划标识"]

    def test_failed_receipt_keeps_segment_recoverable(self, svc: ResumePlanService):
        plan, _ = svc.domain_create_plan(SRC_A, W10, W12, 60)
        svc.domain_dispatch(plan["计划标识"])
        svc.domain_register_receipt(
            SRC_A, W10, W11, status="采集失败", batch_id="B-FAIL",
            rows=[], fail_reason="超时")
        seg = _seg(plan, 0)
        assert seg["status"] == SEG_FAILED
        assert seg["attempts"] >= 1
        task = next(t for t in store.rows("crawl_schedule") if t["任务标识"] == seg["task_id"])
        assert task["status"] == "调度失败"
        # 失败段仍能被再次派发
        again = svc.domain_dispatch(plan["计划标识"])
        assert [d["start"] for d in again["dispatched"]] == [W10]
        assert seg["status"] == SEG_RUNNING

    def test_workbench_action_wiring(self, svc: ResumePlanService):
        plan, _ = svc.domain_create_plan(SRC_A, W10, W12, 60)
        entry, message = svc.run_action(plan["id"], "触发同步")
        assert entry is not None and "补采任务" in message
        # 采集中不能切换待采
        bad, err = svc.run_action(plan["id"], "切换待采")
        assert bad is None and "采集中" in err
        # 失败回执让分片回到终态后，可切换待采并核对缺口
        svc.domain_register_receipt(
            SRC_A, W10, W11, status="采集失败", batch_id="B-WB-FAIL",
            rows=[], fail_reason="超时")
        svc.domain_register_receipt(
            SRC_A, W11, W12, status="采集失败", batch_id="B-WB-FAIL-2",
            rows=[], fail_reason="超时")
        entry, message = svc.run_action(plan["id"], "切换待采")
        assert entry is not None and "待采" in message
        entry, message = svc.run_action(plan["id"], "核对缺口")
        assert entry is not None and "待补" in message
        # 未知动作
        bad, err = svc.run_action(plan["id"], "不存在的动作")
        assert bad is None and "不属于" in err

    def test_create_entry_validation(self, svc: ResumePlanService):
        entry, missing = svc.create_entry({"来源配置": "", "区间起点": W10, "区间终点": W12})
        assert entry is None and "来源配置" in missing
        entry, missing = svc.create_entry(
            {"来源配置": SRC_A, "区间起点": W10, "区间终点": W12, "分片粒度": 60})
        assert entry is not None and missing == []
        items, total = svc.list_entries(keyword=SRC_A)
        assert total == 1 and items[0]["计划标识"] == entry["计划标识"]
