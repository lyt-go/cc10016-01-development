"""重试策略业务规则：退避计算、异常分类、重试计数追踪。"""
from __future__ import annotations

import time
from typing import Any

from app.store import store

MODULE = "retry_policy"
LIST_FIELDS = ["策略编号", "目标源", "最大重试次数", "退避算法", "初始延迟", "可重试异常", "不可重试异常", "策略版本"]
REQUIRED_FIELDS = ["策略编号", "目标源", "最大重试次数"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用策略": "已停用"}
NEGATIVE_ACTIONS = ["停用策略"]


def _parse_delay(value: Any) -> float:
    try:
        v = str(value).replace("ms", "").replace("s", "").replace("秒", "")
        return float(v)
    except (ValueError, TypeError):
        return 1.0


def _compute_backoff(entry: dict[str, Any], attempt: int) -> float:
    """根据退避算法计算第attempt次重试的延迟。"""
    initial = _parse_delay(entry.get("初始延迟"))
    algo = str(entry.get("退避算法") or "fixed").lower()
    if algo == "fixed":
        return initial
    if algo == "linear":
        return initial * attempt
    if algo == "exponential":
        return initial * (2 ** (attempt - 1))
    if algo == "jitter":
        import random
        return initial * (2 ** (attempt - 1)) * (0.5 + random.random())
    return initial


def _classify_exception(exc_name: str, entry: dict[str, Any]) -> str:
    """将异常分类为可重试/不可重试/未知。"""
    retryable = str(entry.get("可重试异常") or "").split(",")
    non_retryable = str(entry.get("不可重试异常") or "").split(",")
    if any(exc_name.strip().lower() in r.strip().lower() for r in retryable if r):
        return "retryable"
    if any(exc_name.strip().lower() in r.strip().lower() for r in non_retryable if r):
        return "non_retryable"
    return "unknown"


class RetryPolicyService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("策略编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        enriched = []
        records = store.pipeline_state("retry_backoff_records") or {}
        for row in rows[start:start + size]:
            d = dict(row)
            d["_backoff_history"] = records.get(str(d["id"]), [])
            enriched.append(d)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        records = store.pipeline_state("retry_backoff_records") or {}
        d["_backoff_history"] = records.get(str(entry_id), [])
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
        rows.append(entry)
        source_id = values.get("关联采集源ID")
        if source_id:
            try:
                sid = int(source_id)
                store.add_relation("source_to_retry", "crawl_source", sid, MODULE, entry["id"])
            except (ValueError, TypeError):
                pass
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"重试策略 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于重试策略可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"重试策略已{action}"

    def should_retry(self, entry_id: int, exc_name: str, attempt: int) -> dict[str, Any]:
        """判断是否应该重试并计算延迟。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"should_retry": False, "reason": "策略不存在"}
        max_attempts = int(str(entry.get("最大重试次数") or "3"))
        classification = _classify_exception(exc_name, entry)
        if classification == "non_retryable":
            return {"should_retry": False, "reason": "不可重试异常", "classification": classification}
        if attempt >= max_attempts:
            return {"should_retry": False, "reason": "已达最大重试次数", "max": max_attempts, "classification": classification}
        delay = _compute_backoff(entry, attempt + 1)
        records = store.pipeline_state("retry_backoff_records") or {}
        hist = records.setdefault(str(entry_id), [])
        hist.append({"attempt": attempt + 1, "delay": round(delay, 3), "exc": exc_name})
        if len(hist) > 50:
            records[str(entry_id)] = hist[-50:]
        store.set_pipeline_state("retry_backoff_records", records)
        return {"should_retry": True, "delay": round(delay, 3), "attempt": attempt + 1, "classification": classification}
