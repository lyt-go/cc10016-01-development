"""频次控制业务规则：令牌桶限流、并发计数、日配额追踪。"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "rate_limit"
LIST_FIELDS = ["策略编号", "目标源", "请求间隔", "并发上限", "日请求总量", "冷却时长", "队列上限", "优先级"]
REQUIRED_FIELDS = ["策略编号", "目标源"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用策略": "已停用"}
NEGATIVE_ACTIONS = ["停用策略"]


def _parse_number(value: Any, default: int) -> int:
    try:
        return int(str(value).replace("ms", "").replace("s", "").replace("次", ""))
    except (ValueError, TypeError):
        return default


class RateLimitService:
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
        states = store.pipeline_state("rate_limit_states") or {}
        for row in rows[start:start + size]:
            d = dict(row)
            sid = str(d["id"])
            st = states.get(sid, {"tokens": _parse_number(d.get("并发上限"), 10), "daily_used": 0})
            d["_tokens"] = st.get("tokens")
            d["_daily_used"] = st.get("daily_used", 0)
            d["_capacity"] = _parse_number(d.get("日请求总量"), 1000)
            enriched.append(d)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        states = store.pipeline_state("rate_limit_states") or {}
        st = states.get(str(entry_id), {"tokens": _parse_number(d.get("并发上限"), 10), "daily_used": 0})
        d["_tokens"] = st.get("tokens")
        d["_daily_used"] = st.get("daily_used", 0)
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
        # 绑定采集源
        source_id = values.get("关联采集源ID")
        if source_id:
            try:
                sid = int(source_id)
                store.add_relation("source_to_rate_limit", "crawl_source", sid, MODULE, entry["id"])
            except (ValueError, TypeError):
                pass
        # 初始化令牌桶
        states = store.pipeline_state("rate_limit_states") or {}
        states[str(entry["id"])] = {"tokens": _parse_number(entry.get("并发上限"), 10), "daily_used": 0, "last_refill": "2026-11-16T00:00:00Z"}
        store.set_pipeline_state("rate_limit_states", states)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"限流策略 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于频次控制可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"限流策略已{action}"

    def acquire_token(self, entry_id: int) -> dict[str, Any]:
        """尝试获取一个请求令牌，返回是否允许及剩余配额。"""
        entry = store.find(MODULE, entry_id)
        if entry is None or entry.get("status") != "已生效":
            return {"allowed": False, "reason": "策略不存在或未生效"}
        states = store.pipeline_state("rate_limit_states") or {}
        sid = str(entry_id)
        st = states.get(sid, {"tokens": _parse_number(entry.get("并发上限"), 10), "daily_used": 0})
        cap = _parse_number(entry.get("日请求总量"), 1000)
        if st["daily_used"] >= cap:
            return {"allowed": False, "reason": "日配额已耗尽", "daily_used": st["daily_used"], "capacity": cap}
        if st["tokens"] <= 0:
            return {"allowed": False, "reason": "并发令牌耗尽", "tokens": 0}
        st["tokens"] -= 1
        st["daily_used"] += 1
        states[sid] = st
        store.set_pipeline_state("rate_limit_states", states)
        return {"allowed": True, "tokens": st["tokens"], "daily_used": st["daily_used"], "capacity": cap}

    def release_token(self, entry_id: int) -> dict[str, Any]:
        """释放令牌回桶。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False}
        states = store.pipeline_state("rate_limit_states") or {}
        sid = str(entry_id)
        st = states.get(sid, {"tokens": _parse_number(entry.get("并发上限"), 10), "daily_used": 0})
        max_tokens = _parse_number(entry.get("并发上限"), 10)
        st["tokens"] = min(max_tokens, st["tokens"] + 1)
        states[sid] = st
        store.set_pipeline_state("rate_limit_states", states)
        return {"ok": True, "tokens": st["tokens"]}
