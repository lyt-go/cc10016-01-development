"""采集源管理业务规则：状态流转、连接校验、跨模块关系绑定。"""
from __future__ import annotations

import re
import urllib.parse
from typing import Any

from app.store import store

MODULE = "crawl_source"
LIST_FIELDS = ["源标识", "源名称", "协议类型", "基础URL", "认证方式", "地区归属", "标签分类", "最近采集时间"]
REQUIRED_FIELDS = ["源标识", "源名称", "协议类型"]
STATUS_ORDER = ["草稿", "待校验", "已接入", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "确认接入": "已接入", "停用源": "已停用"}
NEGATIVE_ACTIONS = ["停用源"]


def _validate_url(url: str) -> tuple[bool, str, float]:
    """模拟连接校验：解析URL并返回可达性、协议类型、模拟延迟。"""
    if not url or not isinstance(url, str):
        return False, "URL为空", 0.0
    try:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False, f"不支持的协议: {parsed.scheme}", 0.0
        if not parsed.netloc:
            return False, "URL缺少主机名", 0.0
        # 模拟延迟计算：基于主机名长度+固定开销
        latency = 0.05 + (len(parsed.netloc) % 7) * 0.03
        return True, "连接正常", round(latency, 3)
    except Exception as exc:
        return False, f"URL解析异常: {exc}", 0.0


def _check_source_health(entry: dict[str, Any]) -> dict[str, Any]:
    """校验采集源健康状态并写回缓存。"""
    url = str(entry.get("基础URL") or "")
    reachable, message, latency = _validate_url(url)
    cache = store.pipeline_state("source_validation_cache") or {}
    cache[str(entry["id"])] = {
        "reachable": reachable,
        "message": message,
        "latency": latency,
        "checked_at": "2026-11-16T00:00:00Z",
    }
    store.set_pipeline_state("source_validation_cache", cache)
    entry["_health"] = cache[str(entry["id"])]
    return entry


class CrawlSourceService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("源标识", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        # 补充健康缓存
        enriched = []
        cache = store.pipeline_state("source_validation_cache") or {}
        for row in rows[start:start + size]:
            d = dict(row)
            d["_health"] = cache.get(str(d["id"]), {"reachable": None, "message": "未校验", "latency": 0.0})
            # 计算关联种子数
            d["_seed_count"] = len(store.get_relations("source_to_seed", source_module=MODULE, source_id=d["id"]))
            enriched.append(d)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        d = _check_source_health(d)
        d["_seed_count"] = len(store.get_relations("source_to_seed", source_module=MODULE, source_id=entry_id))
        d["_rate_limit_id"] = None
        rels = store.get_relations("source_to_rate_limit", source_module=MODULE, source_id=entry_id)
        if rels:
            d["_rate_limit_id"] = rels[-1]["target_id"]
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
        # 自动连接校验
        _check_source_health(entry)
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"采集源 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集源管理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        if action == "提交校验":
            _check_source_health(entry)
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"采集源已{action}"

    def validate_connection(self, entry_id: int) -> dict[str, Any]:
        """显式触发连接校验并返回结果。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": "采集源不存在"}
        _check_source_health(entry)
        cache = store.pipeline_state("source_validation_cache") or {}
        return cache.get(str(entry_id), {"ok": False, "error": "无缓存"})

    def get_linked_seeds(self, entry_id: int) -> list[dict[str, Any]]:
        """获取该采集源关联的所有种子。"""
        rels = store.get_relations("source_to_seed", source_module=MODULE, source_id=entry_id)
        seeds = []
        for rel in rels:
            seed = store.find("seed_config", rel["target_id"])
            if seed:
                seeds.append(dict(seed))
        return seeds
