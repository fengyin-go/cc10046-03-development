"""采集调度领域服务：除共享骨架外，承载调度计划生成与续采分片任务。"""
from __future__ import annotations

import time

from app.services.base import BaseModuleService
from app.store import store


class CrawlScheduleService(BaseModuleService):
    MODULE = "crawl_schedule"
    ENTITY = "调度任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["任务标识", "关联配置", "调度周期", "起始水位", "截止水位", "分片策略", "上次执行", "下次执行"]
    REQUIRED_FIELDS = ["任务标识", "关联配置", "调度周期"]
    STATUS_ORDER = ["待启动", "运行中", "已完成", "调度失败"]
    ACTION_RULES = {"启动调度": "待启动", "取消调度": "调度失败"}
    NEGATIVE_ACTIONS = []

    def domain_create_schedule(self, config_name: str, period: str = "daily") -> dict:
        """生成采集调度计划。"""
        rows = store.rows(self.MODULE)
        task_id = f"SCHED-{max((int(r.get('id', 0)) for r in rows), default=0) + 1}"
        plan = {
            "task_id": task_id,
            "config_name": config_name,
            "period": period,
            "shard_count": 4,
            "start_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "watermark_start": "2026-01-01",
            "watermark_end": time.strftime("%Y-%m-%d"),
        }
        return plan

    def domain_create_segment_task(
        self,
        *,
        source: str,
        segment_start: str,
        segment_end: str,
        plan_code: str,
        segment_id: str,
    ) -> dict:
        """采集调度拿续采计划真正发起一轮分片任务，返回已落库的调度任务行。"""
        rows = store.rows(self.MODULE)
        seq = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        task_code = f"SCHED-RP-{seq:04d}"
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        entry = {
            "id": seq,
            "任务标识": task_code,
            "关联配置": source,
            "调度周期": "续采补采",
            "起始水位": segment_start,
            "截止水位": segment_end,
            "分片策略": f"{plan_code}/{segment_id}",
            "上次执行": now,
            "下次执行": "—",
            "status": "运行中",
            "pending": True,
            "abnormal": False,
            "plan_code": plan_code,
            "segment_id": segment_id,
        }
        rows.append(entry)
        return entry

    def domain_finish_task(self, task_code: str, *, ok: bool) -> dict | None:
        """回执到达后给调度任务收尾。"""
        for row in store.rows(self.MODULE):
            if row.get("任务标识") == task_code:
                row["status"] = "已完成" if ok else "调度失败"
                row["pending"] = not ok
                row["abnormal"] = not ok
                row["下次执行"] = time.strftime("%Y-%m-%d %H:%M:%S")
                return row
        return None
