"""采集编排业务规则：Cron校验、依赖解析、执行计划生成与状态轮转。"""
from __future__ import annotations

import re
from typing import Any

from app.store import store

MODULE = "crawl_schedule"
LIST_FIELDS = ["计划编号", "关联种子", "Cron表达式", "并行批次", "优先级", "依赖条件", "失败动作", "计划版本"]
REQUIRED_FIELDS = ["计划编号", "关联种子", "Cron表达式"]
STATUS_ORDER = ["草稿", "待校验", "已激活", "已暂停"]
ACTION_RULES = {"提交校验": "待校验", "激活计划": "已激活", "暂停计划": "已暂停"}
NEGATIVE_ACTIONS = []


def _validate_cron(cron: str) -> tuple[bool, str]:
    """基础Cron表达式校验：5段或6段格式。"""
    if not cron or not isinstance(cron, str):
        return False, "Cron为空"
    parts = cron.strip().split()
    if len(parts) not in (5, 6):
        return False, f"Cron必须是5或6段，实际{len(parts)}段"
    # 简单检查每段是否只含数字、*、,、-、/
    for i, p in enumerate(parts):
        if not re.match(r"^[\d\*\,\-/]+$", p):
            return False, f"第{i+1}段格式异常: {p}"
    return True, "Cron格式合法"


def _resolve_dependencies(dep_text: str) -> tuple[bool, list[str]]:
    """解析依赖条件：逗号分隔的计划编号列表。"""
    if not dep_text:
        return True, []
    deps = [d.strip() for d in str(dep_text).split(",") if d.strip()]
    # 检查是否都存在
    all_schedules = store.rows(MODULE)
    existing = {str(r.get("计划编号")) for r in all_schedules}
    missing = [d for d in deps if d not in existing]
    return len(missing) == 0, missing


class CrawlScheduleService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("计划编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        enriched = []
        logs = store.pipeline_state("schedule_execution_log") or []
        for row in rows[start:start + size]:
            d = dict(row)
            d["_exec_count"] = sum(1 for log in logs if log.get("schedule_id") == d["id"])
            enriched.append(d)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        logs = store.pipeline_state("schedule_execution_log") or []
        d["_exec_count"] = sum(1 for log in logs if log.get("schedule_id") == entry_id)
        return d

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        # Cron校验
        ok, msg = _validate_cron(entry.get("Cron表达式") or "")
        entry["_cron_check"] = {"ok": ok, "msg": msg}
        rows.append(entry)
        # 绑定种子
        seed_id = values.get("关联种子ID")
        if seed_id:
            try:
                sid = int(seed_id)
                store.add_relation("schedule_to_session", MODULE, entry["id"], "seed_config", sid)
            except (ValueError, TypeError):
                pass
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"采集计划 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集编排可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        if action == "激活计划":
            ok, missing = _resolve_dependencies(entry.get("依赖条件") or "")
            if not ok:
                return None, f"依赖计划未就绪: {', '.join(missing)}"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"采集计划已{action}"

    def generate_execution_plan(self, entry_id: int) -> dict[str, Any]:
        """为计划生成执行批次与依赖拓扑。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": "计划不存在"}
        try:
            batches = int(str(entry.get("并行批次") or "1"))
        except (ValueError, TypeError):
            batches = 1
        deps = [d.strip() for d in str(entry.get("依赖条件") or "").split(",") if d.strip()]
        plan = {
            "schedule_id": entry_id,
            "batches": batches,
            "dependencies": deps,
            "priority": entry.get("优先级") or "normal",
            "fail_action": entry.get("失败动作") or "abort",
            "tasks": [{"batch": i + 1, "status": "pending"} for i in range(batches)],
        }
        logs = store.pipeline_state("schedule_execution_log") or []
        logs.append({"schedule_id": entry_id, "plan": plan, "at": "2026-11-16T00:00:00Z"})
        if len(logs) > 200:
            store.set_pipeline_state("schedule_execution_log", logs[-200:])
        else:
            store.set_pipeline_state("schedule_execution_log", logs)
        return {"ok": True, "plan": plan}
