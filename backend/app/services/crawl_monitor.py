"""采集监控业务规则：实时指标聚合、成功率/延迟/采集量计算、异常告警。"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "crawl_monitor"
LIST_FIELDS = ["指标编号", "关联源", "成功率", "平均延迟", "失败分类", "采集量", "数据量", "最近告警"]
REQUIRED_FIELDS = ["指标编号", "关联源", "成功率"]
STATUS_ORDER = ["草稿", "待校验", "监控中", "已静默"]
ACTION_RULES = {"提交校验": "待校验", "启动监控": "监控中", "静默告警": "已静默"}
NEGATIVE_ACTIONS = []


def _compute_source_metrics(source_id: int) -> dict[str, Any]:
    """基于采集源关联模块计算真实指标。"""
    # 读取会话请求日志
    logs = store.pipeline_state("session_request_logs") or {}
    total_requests = 0
    total_latency = 0.0
    success_count = 0
    for sess_id, sess_logs in logs.items():
        for log in sess_logs:
            total_requests += 1
            total_latency += log.get("latency", 0)
            if log.get("success", True):
                success_count += 1
    # 读取反爬触发日志
    anti_logs = store.pipeline_state("anti_spider_trigger_log") or []
    anti_count = len(anti_logs)
    # 读取校验报告
    reports = store.pipeline_state("data_validate_reports") or {}
    validate_failures = sum(r.get("failed", 0) for r in reports.values())
    avg_latency = round(total_latency / max(total_requests, 1), 3)
    success_rate = round(success_count / max(total_requests, 1), 3)
    return {
        "total_requests": total_requests,
        "success_rate": success_rate,
        "avg_latency_ms": avg_latency,
        "anti_trigger_count": anti_count,
        "validate_failures": validate_failures,
        "source_id": source_id,
    }


class CrawlMonitorService:
    def list_results(self) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        results: list[dict] = []
        cache = store.pipeline_state("monitor_metrics_cache") or {}
        for row in rows:
            d = dict(row)
            source_id = None
            rels = store.get_relations("schedule_to_monitor", target_module=MODULE, target_id=d["id"])
            if rels:
                source_id = rels[0].get("source_id")
            if source_id is None:
                # 尝试从关联源字段解析
                try:
                    source_id = int(str(d.get("关联源") or "0"))
                except (ValueError, TypeError):
                    source_id = 0
            metrics = cache.get(str(source_id)) or _compute_source_metrics(source_id)
            cache[str(source_id)] = metrics
            d["_computed"] = metrics
            d["指标值"] = metrics["success_rate"]
            d["对比基准"] = round(metrics["success_rate"] * 0.95, 3)
            d["差异"] = round(d["指标值"] - d["对比基准"], 3)
            results.append(d)
        store.set_pipeline_state("monitor_metrics_cache", cache)
        return results

    def run_analysis(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        source_id = int(str(params.get("关联源") or "0"))
        metrics = _compute_source_metrics(source_id)
        results = [
            {"id": 1, "分组": "成功率", "指标值": metrics["success_rate"], "对比基准": 0.95, "差异": round(metrics["success_rate"] - 0.95, 3)},
            {"id": 2, "分组": "平均延迟", "指标值": metrics["avg_latency_ms"], "对比基准": 200.0, "差异": round(metrics["avg_latency_ms"] - 200.0, 3)},
            {"id": 3, "分组": "反爬触发", "指标值": metrics["anti_trigger_count"], "对比基准": 0, "差异": metrics["anti_trigger_count"]},
            {"id": 4, "分组": "校验失败", "指标值": metrics["validate_failures"], "对比基准": 0, "差异": metrics["validate_failures"]},
        ]
        return results

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        cache = store.pipeline_state("monitor_metrics_cache") or {}
        source_id = 0
        try:
            source_id = int(str(d.get("关联源") or "0"))
        except (ValueError, TypeError):
            pass
        d["_computed"] = cache.get(str(source_id)) or _compute_source_metrics(source_id)
        return d

    def refresh_metrics(self, source_id: int) -> dict[str, Any]:
        """刷新指定采集源的监控指标。"""
        metrics = _compute_source_metrics(source_id)
        cache = store.pipeline_state("monitor_metrics_cache") or {}
        cache[str(source_id)] = metrics
        store.set_pipeline_state("monitor_metrics_cache", cache)
        return metrics

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
                store.add_relation("schedule_to_monitor", "crawl_source", sid, MODULE, entry["id"])
            except (ValueError, TypeError):
                pass
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"监控指标 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集监控可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"监控指标已{action}"
