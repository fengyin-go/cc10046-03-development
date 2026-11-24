"""续采计划领域服务。

增量同步在来源增量数据区间部分写入后，需要一份可恢复的续采计划：
- 按来源与时间区间切出时段分片，记录每段「待采/采集中/已落库/采集中断」状态及流转；
- 已落库台账（resume_landed）按「来源 + 业务键」唯一，重复完成回执不再落第二遍；
- 失败台账（resume_failure）记录中途失败的区间，已读到后段也不会把前段缺口登记成完成；
- 采集调度只对还缺数据的区间真正发起任务，补回进度指向实际结果存储批次。
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from app.services.base import BaseModuleService
from app.services.batch_tracker import BatchTrackerService
from app.services.crawl_schedule import CrawlScheduleService
from app.services.result_store import ResultStoreService
from app.store import store

TIME_FMT = "%Y-%m-%d %H:%M"

# 分片状态
SEG_PENDING = "待采"
SEG_RUNNING = "采集中"
SEG_LANDED = "已落库"
SEG_FAILED = "采集中断"
SEG_GAP_STATUSES = (SEG_PENDING, SEG_FAILED)

# 计划状态
PLAN_IDLE = "待采"
PLAN_RUNNING = "采集中"
PLAN_GAP = "待补采"
PLAN_DONE = "已完成"

RECEIPT_SUCCESS = "全部成功"


def _parse(ts: str) -> datetime:
    return datetime.strptime(ts.strip(), TIME_FMT)


def _fmt(dt: datetime) -> str:
    return dt.strftime(TIME_FMT)


def _now() -> str:
    return _fmt(datetime.now().replace(second=0, microsecond=0))


def build_segments(start: str, end: str, granularity_minutes: int) -> list[dict[str, str]]:
    """把 [start, end) 半开区间按粒度切成时段分片。"""
    granularity = int(granularity_minutes)
    if granularity <= 0:
        raise ValueError("分片粒度必须是正整数分钟")
    cur, stop = _parse(start), _parse(end)
    if stop <= cur:
        raise ValueError("区间终点必须晚于区间起点")
    pieces: list[dict[str, str]] = []
    while cur < stop:
        nxt = min(cur + timedelta(minutes=granularity), stop)
        pieces.append({"start": _fmt(cur), "end": _fmt(nxt)})
        cur = nxt
    return pieces


class ResumePlanService(BaseModuleService):
    MODULE = "resume_plan"
    ENTITY = "续采计划"
    OPERATION_TERM = "续采"
    LIST_FIELDS = [
        "计划标识", "来源配置", "区间起点", "区间终点",
        "分片粒度", "计划状态", "待补段数", "结果批次",
    ]
    REQUIRED_FIELDS = ["来源配置", "区间起点", "区间终点"]
    STATUS_ORDER = [PLAN_IDLE, PLAN_RUNNING, PLAN_GAP, PLAN_DONE]
    ACTION_RULES = {"触发同步": PLAN_RUNNING, "切换待采": PLAN_IDLE, "核对缺口": PLAN_GAP}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------ 台账
    def _landed_table(self) -> list[dict[str, Any]]:
        return store.rows("resume_landed")

    def _failure_table(self) -> list[dict[str, Any]]:
        return store.rows("resume_failure")

    def _find_failure(self, source: str, start: str, end: str) -> dict[str, Any] | None:
        for row in self._failure_table():
            if (row["来源配置"] == source
                    and row["segment_start"] == start
                    and row["segment_end"] == end
                    and row.get("resolved_at") is None):
                return row
        return None

    def _resolve_failure(self, source: str, start: str, end: str) -> None:
        """补采成功后把历史失败标记为已解决，保留尝试次数但不再算缺口。"""
        for row in self._failure_table():
            if (row["来源配置"] == source
                    and row["segment_start"] == start
                    and row["segment_end"] == end
                    and row.get("resolved_at") is None):
                row["resolved_at"] = _now()

    def _landed_in_segment(self, source: str, start: str, end: str) -> list[dict[str, Any]]:
        return [
            row for row in self._landed_table()
            if row["来源配置"] == source
            and row["segment_start"] == start
            and row["segment_end"] == end
        ]

    def _upsert_failure(self, source: str, start: str, end: str, reason: str | None) -> dict[str, Any]:
        table = self._failure_table()
        failure = self._find_failure(source, start, end)
        if failure is None:
            failure = {
                "id": max((int(r.get("id", 0)) for r in table), default=0) + 1,
                "来源配置": source,
                "segment_start": start,
                "segment_end": end,
                "reason": reason or "采集中途失败",
                "attempts": 0,
                "last_at": _now(),
            }
            table.append(failure)
        failure["attempts"] = int(failure["attempts"]) + 1
        if reason:
            failure["reason"] = reason
        failure["last_at"] = _now()
        return failure

    # ------------------------------------------------------------------ 计划
    def _find_plan(self, plan_code: str) -> dict[str, Any] | None:
        for plan in store.rows(self.MODULE):
            if plan.get("计划标识") == plan_code or str(plan.get("id")) == str(plan_code):
                return plan
        return None

    def _refresh_plan(self, plan: dict[str, Any]) -> dict[str, Any]:
        """按各分片状态重算计划级状态与待补段数。"""
        segments = plan["segments"]
        if segments and all(seg["status"] == SEG_LANDED for seg in segments):
            status = PLAN_DONE
        elif any(seg["status"] == SEG_FAILED for seg in segments):
            # 已读到后段也不能把前面的采集中断登记为完成：有中断就是待补采
            status = PLAN_GAP
        elif any(seg["status"] == SEG_RUNNING for seg in segments):
            # 缺口段都已在本轮采集中
            status = PLAN_RUNNING
        elif any(seg["status"] == SEG_LANDED for seg in segments):
            # 已有部分落库、剩余段尚未派发：明确待补采
            status = PLAN_GAP
        else:
            status = PLAN_IDLE
        batches = sorted({
            seg["result_batch_id"]
            for seg in segments
            if seg.get("result_batch_id")
        })
        plan["计划状态"] = status
        plan["status"] = status
        # 待补段数：所有尚未落库的时段，含待采、采集中、采集中断
        plan["待补段数"] = sum(1 for seg in segments if seg["status"] != SEG_LANDED)
        plan["结果批次"] = "、".join(batches)
        plan["pending"] = status != PLAN_DONE
        plan["abnormal"] = any(seg["status"] == SEG_FAILED for seg in segments)
        return plan

    def domain_create_plan(
        self,
        source: str,
        start: str,
        end: str,
        granularity_minutes: int = 60,
    ) -> tuple[dict[str, Any], bool]:
        """生成续采计划；同来源同区间同粒度重复调用幂等，返回 (计划, 是否复用旧计划)。"""
        source = (source or "").strip()
        start = (start or "").strip()
        end = (end or "").strip()
        if not source or not start or not end:
            raise ValueError("来源配置、区间起点、区间终点均不能为空")
        granularity = int(granularity_minutes)
        pieces = build_segments(start, end, granularity)

        rows = store.rows(self.MODULE)
        for plan in rows:
            if (plan["来源配置"] == source
                    and plan["区间起点"] == start
                    and plan["区间终点"] == end
                    and int(plan["分片粒度"]) == granularity):
                return plan, True

        plan_id_num = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        plan_code = f"RP-{plan_id_num:04d}"
        segments: list[dict[str, Any]] = []
        for idx, piece in enumerate(pieces, start=1):
            landed = self._landed_in_segment(source, piece["start"], piece["end"])
            failure = self._find_failure(source, piece["start"], piece["end"])
            if failure is not None:
                status = SEG_FAILED
            elif landed:
                status = SEG_LANDED
            else:
                status = SEG_PENDING
            segments.append({
                "segment_id": f"{plan_code}-S{idx:02d}",
                "start": piece["start"],
                "end": piece["end"],
                "status": status,
                "row_count": len(landed),
                "result_batch_id": landed[0]["batch_id"] if landed else None,
                "task_id": None,
                "attempts": int(failure["attempts"]) if failure else 0,
                "last_reason": failure["reason"] if failure else None,
                "receipts": 0,
            })
        plan = {
            "id": plan_id_num,
            "计划标识": plan_code,
            "来源配置": source,
            "区间起点": start,
            "区间终点": end,
            "分片粒度": granularity,
            "segments": segments,
            "created_at": _now(),
        }
        self._refresh_plan(plan)
        rows.append(plan)
        return plan, False

    # ------------------------------------------------------------------ 派发
    def domain_dispatch(self, plan_code: str) -> dict[str, Any]:
        """采集调度按续采计划发起下一轮任务：只补还缺数据的区间。"""
        plan = self._find_plan(plan_code)
        if plan is None:
            raise ValueError(f"续采计划 {plan_code} 不存在或已归档")
        schedule_svc = CrawlScheduleService()
        dispatched: list[dict[str, Any]] = []
        skipped_landed: list[dict[str, Any]] = []
        for seg in plan["segments"]:
            if seg["status"] == SEG_LANDED:
                skipped_landed.append({
                    "segment_id": seg["segment_id"],
                    "start": seg["start"],
                    "end": seg["end"],
                    "result_batch_id": seg["result_batch_id"],
                })
                continue
            if seg["status"] == SEG_RUNNING:
                # 已在采集，不重复派发
                continue
            task = schedule_svc.domain_create_segment_task(
                source=plan["来源配置"],
                segment_start=seg["start"],
                segment_end=seg["end"],
                plan_code=plan["计划标识"],
                segment_id=seg["segment_id"],
            )
            seg["task_id"] = task["任务标识"]
            seg["status"] = SEG_RUNNING
            dispatched.append({
                "segment_id": seg["segment_id"],
                "task_id": task["任务标识"],
                "start": seg["start"],
                "end": seg["end"],
            })
        self._refresh_plan(plan)
        return {
            "plan": plan,
            "dispatched": dispatched,
            "skipped_landed": skipped_landed,
        }

    # ------------------------------------------------------------------ 回执
    def domain_register_receipt(
        self,
        source: str,
        segment_start: str,
        segment_end: str,
        *,
        status: str,
        batch_id: str,
        rows: list[dict[str, Any] | str] | None = None,
        fail_reason: str | None = None,
    ) -> dict[str, Any]:
        """登记来源采集完成回执，成功则落库，重复回执幂等忽略。"""
        source = (source or "").strip()
        if not source or not segment_start or not segment_end:
            raise ValueError("来源配置与时段区间均不能为空")
        rows = rows or []
        success = status == RECEIPT_SUCCESS

        # 1. 落库台账：来源 + 业务键唯一，天然挡住重复落库
        landed_table = self._landed_table()
        inserted: list[str] = []
        duplicate_rows: list[str] = []
        for raw in rows:
            biz_id = str(raw["biz_id"] if isinstance(raw, dict) else raw).strip()
            if not biz_id:
                continue
            if any(r["来源配置"] == source and r["biz_id"] == biz_id for r in landed_table):
                duplicate_rows.append(biz_id)
                continue
            landed_table.append({
                "id": max((int(r.get("id", 0)) for r in landed_table), default=0) + 1,
                "来源配置": source,
                "biz_id": biz_id,
                "segment_start": segment_start,
                "segment_end": segment_end,
                "batch_id": batch_id,
                "landed_at": _now(),
            })
            inserted.append(biz_id)

        # 2. 结果存储批次只登记一次，重复回执不产生第二批
        store_entry = None
        if batch_id and not any(
            r.get("批次标识") == batch_id for r in store.rows("result_store")
        ):
            store_entry = ResultStoreService().domain_store_result(
                batch_id, source, len(rows), len(inserted))

        # 3. 失败台账：部分失败/中断的区间保留缺口；成功则结清历史失败
        if not success:
            self._upsert_failure(source, segment_start, segment_end, fail_reason)
        else:
            self._resolve_failure(source, segment_start, segment_end)

        # 4. 回写所有命中的计划分片及其调度任务
        matched_segments: list[dict[str, Any]] = []
        duplicate_receipt = False
        schedule_svc = CrawlScheduleService()
        tracker_svc = BatchTrackerService()
        for plan in store.rows(self.MODULE):
            if plan["来源配置"] != source:
                continue
            for seg in plan["segments"]:
                if seg["start"] != segment_start or seg["end"] != segment_end:
                    continue
                matched_segments.append(seg)
                if success and seg["status"] == SEG_LANDED:
                    # 同一来源同一区间的完成回执再来一遍：只计数，不再落库、不建批次
                    duplicate_receipt = True
                    seg["receipts"] += 1
                    continue
                landed = self._landed_in_segment(source, segment_start, segment_end)
                if success:
                    seg["status"] = SEG_LANDED
                    seg["row_count"] = len(landed)
                    seg["result_batch_id"] = batch_id
                    seg["last_reason"] = None
                    seg["receipts"] += 1
                    if seg.get("task_id"):
                        schedule_svc.domain_finish_task(seg["task_id"], ok=True)
                    tracker = tracker_svc.domain_track_batch(
                        batch_id, seg.get("task_id") or plan["计划标识"])
                    tracker["status"] = "全部成功"
                    tracker["批次状态"] = "全部成功"
                    tracker["采集总数"] = str(len(landed))
                    tracker["失败行数"] = "0"
                else:
                    failure = self._find_failure(source, segment_start, segment_end)
                    seg["status"] = SEG_FAILED
                    seg["attempts"] = int(failure["attempts"]) if failure else 1
                    seg["last_reason"] = fail_reason or (failure["reason"] if failure else "采集中途失败")
                    seg["row_count"] = len(landed)
                    if seg.get("task_id"):
                        schedule_svc.domain_finish_task(seg["task_id"], ok=False)
                    tracker = tracker_svc.domain_track_batch(
                        batch_id or f"FAIL-{seg['segment_id']}",
                        seg.get("task_id") or plan["计划标识"])
                    tracker["status"] = "部分成功"
                    tracker["批次状态"] = "部分成功"
                    tracker["采集总数"] = str(len(rows))
                    tracker["失败行数"] = str(max(len(rows) - len(inserted), 0))
        for plan in store.rows(self.MODULE):
            if plan["来源配置"] == source and any(
                seg["start"] == segment_start and seg["end"] == segment_end
                for seg in plan["segments"]
            ):
                self._refresh_plan(plan)

        return {
            "duplicate": duplicate_receipt,
            "matched_plan": bool(matched_segments),
            "inserted": inserted,
            "inserted_count": len(inserted),
            "duplicate_rows": duplicate_rows,
            "result_batch": store_entry,
            "batch_id": batch_id,
        }

    # ------------------------------------------------------------------ 核对
    def domain_rearm(self, plan_code: str) -> dict[str, Any]:
        """值班员把计划重新切换到待采，准备触发下一轮同步。"""
        plan = self._find_plan(plan_code)
        if plan is None:
            raise ValueError(f"续采计划 {plan_code} 不存在或已归档")
        if any(seg["status"] == SEG_RUNNING for seg in plan["segments"]):
            raise ValueError("存在采集中的时段，需等回执落库后再切换待采")
        self._refresh_plan(plan)
        return {"plan": plan, "gaps": self.domain_gap_report(plan_code)}

    def domain_gap_report(self, plan_code: str) -> dict[str, Any]:
        """核对已完成批次与还缺位置：本次会采哪些时段、哪些仍待补。"""
        plan = self._find_plan(plan_code)
        if plan is None:
            raise ValueError(f"续采计划 {plan_code} 不存在或已归档")
        return {
            "plan_id": plan["计划标识"],
            "source": plan["来源配置"],
            "window": {"start": plan["区间起点"], "end": plan["区间终点"]},
            "plan_status": plan["计划状态"],
            "pending_segments": [
                {"segment_id": s["segment_id"], "start": s["start"], "end": s["end"]}
                for s in plan["segments"] if s["status"] == SEG_PENDING
            ],
            "failed_segments": [
                {
                    "segment_id": s["segment_id"], "start": s["start"], "end": s["end"],
                    "attempts": s["attempts"], "last_reason": s["last_reason"],
                }
                for s in plan["segments"] if s["status"] == SEG_FAILED
            ],
            "running_segments": [
                {"segment_id": s["segment_id"], "start": s["start"], "end": s["end"],
                 "task_id": s["task_id"]}
                for s in plan["segments"] if s["status"] == SEG_RUNNING
            ],
            "landed_segments": [
                {
                    "segment_id": s["segment_id"], "start": s["start"], "end": s["end"],
                    "row_count": s["row_count"], "result_batch_id": s["result_batch_id"],
                }
                for s in plan["segments"] if s["status"] == SEG_LANDED
            ],
        }

    def domain_verify_window(
        self,
        source: str,
        start: str,
        end: str,
        granularity_minutes: int = 60,
    ) -> dict[str, Any]:
        """补采完成后核对原数据窗口：无漏行、无重复落库。"""
        pieces = build_segments(start, end, granularity_minutes)
        segment_reports: list[dict[str, Any]] = []
        missing_segments: list[dict[str, str]] = []
        total_rows = 0
        seen: dict[str, int] = {}
        for piece in pieces:
            landed = self._landed_in_segment(source, piece["start"], piece["end"])
            failure = self._find_failure(source, piece["start"], piece["end"])
            biz_ids = [row["biz_id"] for row in landed]
            for biz_id in biz_ids:
                seen[biz_id] = seen.get(biz_id, 0) + 1
            total_rows += len(landed)
            complete = failure is None and len(landed) > 0
            report = {
                "start": piece["start"],
                "end": piece["end"],
                "status": SEG_LANDED if complete else (SEG_FAILED if failure else SEG_PENDING),
                "row_count": len(landed),
                "result_batch_id": landed[0]["batch_id"] if landed else None,
                "failure_reason": failure["reason"] if failure else None,
            }
            segment_reports.append(report)
            if not complete:
                missing_segments.append({"start": piece["start"], "end": piece["end"]})
        duplicate_biz_ids = sorted(biz_id for biz_id, count in seen.items() if count > 1)
        return {
            "source": source,
            "start": start,
            "end": end,
            "segments": segment_reports,
            "missing_segments": missing_segments,
            "duplicate_biz_ids": duplicate_biz_ids,
            "total_rows": total_rows,
            "complete": not missing_segments and not duplicate_biz_ids,
        }

    # ------------------------------------------------------------ 工作台适配
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        try:
            plan, _reused = self.domain_create_plan(
                str(values.get("来源配置")).strip(),
                str(values.get("区间起点")).strip(),
                str(values.get("区间终点")).strip(),
                values.get("分片粒度") or 60,
            )
        except ValueError as exc:
            return None, [str(exc)]
        return plan, []

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(self.MODULE))
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword:
            rows = [
                row for row in rows
                if keyword.casefold() in str(row.get("计划标识", "")).casefold()
                or keyword.casefold() in str(row.get("来源配置", "")).casefold()
            ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start_idx = (page - 1) * size
        return rows[start_idx:start_idx + size], total

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        plan = store.find(self.MODULE, entry_id)
        if plan is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        try:
            if action == "触发同步":
                data = self.domain_dispatch(plan["计划标识"])
                return plan, (
                    f"已发起 {len(data['dispatched'])} 个补采任务，"
                    f"跳过 {len(data['skipped_landed'])} 个已落库时段"
                )
            if action == "切换待采":
                rearmed = self.domain_rearm(plan["计划标识"])
                if rearmed["plan"]["status"] == PLAN_DONE:
                    return plan, "全部时段已落库，计划已完成，无需切换待采"
                return plan, "已切换到待采状态，可触发下一轮同步"
            if action == "核对缺口":
                report = self.domain_gap_report(plan["计划标识"])
                gap_count = len(report["pending_segments"]) + len(report["failed_segments"])
                return plan, (
                    f"待补 {gap_count} 段、采集中 {len(report['running_segments'])} 段、"
                    f"已落库 {len(report['landed_segments'])} 段"
                )
        except ValueError as exc:
            return None, str(exc)
        return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
