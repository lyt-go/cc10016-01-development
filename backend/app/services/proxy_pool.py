"""代理池调度业务规则：健康检查、评分衰减、轮替调度。"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "proxy_pool"
LIST_FIELDS = ["节点编号", "代理地址", "端口号", "协议类型", "地域标识", "响应延迟", "成功率", "失效次数"]
REQUIRED_FIELDS = ["节点编号", "代理地址", "端口号"]
STATUS_ORDER = ["草稿", "待校验", "可用", "已失效"]
ACTION_RULES = {"提交校验": "待校验", "标记可用": "可用", "标记失效": "已失效"}
NEGATIVE_ACTIONS = ["标记失效"]


def _compute_health_score(entry: dict[str, Any]) -> float:
    """基于成功率、延迟、失效次数计算健康评分（0-1）。"""
    try:
        success_rate = float(str(entry.get("成功率") or "0.95").replace("%", ""))
        if success_rate > 1:
            success_rate = success_rate / 100.0
    except (ValueError, TypeError):
        success_rate = 0.95
    try:
        latency = float(str(entry.get("响应延迟") or "200").replace("ms", ""))
    except (ValueError, TypeError):
        latency = 200.0
    try:
        failures = int(str(entry.get("失效次数") or "0"))
    except (ValueError, TypeError):
        failures = 0
    latency_score = max(0.0, 1.0 - latency / 1000.0)
    failure_penalty = min(0.5, failures * 0.1)
    score = success_rate * 0.5 + latency_score * 0.3 - failure_penalty
    return round(max(0.0, min(1.0, score)), 3)


class ProxyPoolService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("节点编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        enriched = []
        scores = store.pipeline_state("proxy_health_scores") or {}
        for row in rows[start:start + size]:
            d = dict(row)
            d["_health_score"] = _compute_health_score(d)
            d["_history"] = scores.get(str(d["id"]), [])
            enriched.append(d)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        d["_health_score"] = _compute_health_score(d)
        scores = store.pipeline_state("proxy_health_scores") or {}
        d["_history"] = scores.get(str(entry_id), [])
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
        entry["_health_score"] = _compute_health_score(entry)
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"代理节点 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于代理池调度可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        if action == "标记失效":
            failures = int(str(entry.get("失效次数") or "0")) + 1
            entry["失效次数"] = str(failures)
            scores = store.pipeline_state("proxy_health_scores") or {}
            hist = scores.setdefault(str(entry_id), [])
            hist.append({"score": _compute_health_score(entry), "action": "失效"})
            store.set_pipeline_state("proxy_health_scores", scores)
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"代理节点已{action}"

    def select_proxy(self, region: str | None = None) -> dict[str, Any] | None:
        """按健康评分选择最优可用代理。"""
        rows = store.rows(MODULE)
        candidates = [r for r in rows if r.get("status") == "可用"]
        if region:
            candidates = [r for r in candidates if region in str(r.get("地域标识") or "")]
        if not candidates:
            return None
        scored = [(r, _compute_health_score(r)) for r in candidates]
        scored.sort(key=lambda x: x[1], reverse=True)
        best = dict(scored[0][0])
        best["_selected_score"] = scored[0][1]
        return best

    def record_request(self, entry_id: int, success: bool, latency_ms: float) -> dict[str, Any]:
        """记录代理请求结果，更新历史评分。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False}
        scores = store.pipeline_state("proxy_health_scores") or {}
        hist = scores.setdefault(str(entry_id), [])
        hist.append({"success": success, "latency": latency_ms, "score": _compute_health_score(entry)})
        if len(hist) > 50:
            scores[str(entry_id)] = hist[-50:]
        store.set_pipeline_state("proxy_health_scores", scores)
        return {"ok": True, "current_score": _compute_health_score(entry)}
