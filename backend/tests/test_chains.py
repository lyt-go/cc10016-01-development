"""跨模块数据链路关键测试（>=15个）。"""
from __future__ import annotations

import pytest

from app.engine import (
    chain_source_seed_parse_field,
    chain_schedule_session_proxy_rate,
    chain_anti_retry_cursor,
    chain_convert_dedup_validate_store,
    chain_extract_convert,
    chain_render_source,
    chain_monitor_schedule,
    chain_seed_cursor_advance,
    chain_full_pipeline,
)
from app.store import store


# ------------------------------------------------------------------
# 链路1: 采集源 -> 种子 -> 解析规则 -> 字段抽取
# ------------------------------------------------------------------

def test_chain_source_seed_parse_field_ok() -> None:
    result = chain_source_seed_parse_field(1)
    assert result["ok"] is True
    assert result["source_id"] == 1
    assert result.get("seed_id") is not None
    assert len(result.get("parse_rules", [])) >= 1


def test_chain_source_seed_parse_field_missing_source() -> None:
    result = chain_source_seed_parse_field(9999)
    assert result["ok"] is False
    assert "不存在" in result["message"]


# ------------------------------------------------------------------
# 链路2: 采集编排 -> 会话 -> 代理池 -> 频次控制
# ------------------------------------------------------------------

def test_chain_schedule_session_proxy_rate_ok() -> None:
    result = chain_schedule_session_proxy_rate(1)
    assert result["ok"] is True
    assert result["schedule_id"] == 1
    assert result.get("session_id") is not None
    assert result.get("proxy_id") is not None
    assert result["token"].get("allowed") is True


def test_chain_schedule_session_proxy_rate_missing_schedule() -> None:
    result = chain_schedule_session_proxy_rate(9999)
    assert result["ok"] is False
    assert "不存在" in result["message"]


# ------------------------------------------------------------------
# 链路3: 反爬对抗 -> 重试策略 -> 增量游标
# ------------------------------------------------------------------

def test_chain_anti_retry_cursor_triggered() -> None:
    result = chain_anti_retry_cursor(1, "too many requests", 429, "TimeoutError", 1)
    assert result["ok"] is True
    assert result["anti_spider"]["triggered"] is True
    assert "频率限制" in result["anti_spider"]["reason"] or "rate limit" in result["anti_spider"]["reason"].lower()


def test_chain_anti_retry_cursor_not_triggered_retry_ok() -> None:
    result = chain_anti_retry_cursor(1, "normal page content", 200, "TimeoutError", 1)
    assert result["ok"] is True
    assert result["anti_spider"]["triggered"] is False
    assert result["retry"]["should_retry"] is True


def test_chain_anti_retry_cursor_non_retryable() -> None:
    result = chain_anti_retry_cursor(1, "error", 500, "AuthError", 1)
    assert result["ok"] is True
    assert result["retry"]["should_retry"] is False
    assert result["retry"]["classification"] == "non_retryable"


# ------------------------------------------------------------------
# 链路4: 格式转换 -> 去重 -> 校验 -> 结果落地
# ------------------------------------------------------------------

def test_chain_convert_dedup_validate_store_ok() -> None:
    records = [
        {"item_id": "A001", "title": "商品A", "price": "199"},
        {"item_id": "A002", "title": "商品B", "price": "299"},
        {"item_id": "A001", "title": "商品A重复", "price": "199"},
    ]
    result = chain_convert_dedup_validate_store(1, 2, 1, 1, records)
    assert result["ok"] is True
    assert result["dedup"]["duplicates"] >= 1
    assert result["validate"]["passed"] >= 1
    assert result["store"]["ok"] is True


def test_chain_convert_dedup_validate_store_similarity() -> None:
    records = [
        {"news_id": "N01", "title": "今日热点", "content": "abc"},
        {"news_id": "N02", "title": "今日热点事件", "content": "def"},
    ]
    result = chain_convert_dedup_validate_store(2, 3, 2, 2, records)
    assert result["ok"] is True
    assert result["convert"]["output"] == len(records)


# ------------------------------------------------------------------
# 链路5: 内容抽取 -> 格式转换
# ------------------------------------------------------------------

def test_chain_extract_convert_ok() -> None:
    html = "<html><body><h1>标题</h1><p>正文内容</p></body></html>"
    result = chain_extract_convert(1, 1, html)
    assert result["ok"] is True
    assert "summary" in result["extract"] or "raw_length" in result["extract"]
    assert result["convert"]["ok"] is True


def test_chain_extract_convert_fail() -> None:
    html = ""
    result = chain_extract_convert(9999, 1, html)
    assert result["ok"] is False


# ------------------------------------------------------------------
# 链路6: 渲染编排 -> 采集源资源过滤
# ------------------------------------------------------------------

def test_chain_render_source_allowed() -> None:
    result = chain_render_source(1, "https://mall-a.example.com/api/static/data.json")
    assert result["ok"] is True
    assert result["allowed"] is True


def test_chain_render_source_blocked() -> None:
    result = chain_render_source(1, "https://mall-a.example.com/static/tracker.js")
    assert result["ok"] is True
    assert result["allowed"] is False
    assert "拦截" in result["reason"] or "block" in result["reason"].lower()


# ------------------------------------------------------------------
# 链路7: 监控 -> 采集编排执行状态
# ------------------------------------------------------------------

def test_chain_monitor_schedule_ok() -> None:
    result = chain_monitor_schedule(1)
    assert result["ok"] is True
    assert result["schedule_id"] == 1
    assert "metrics" in result
    assert "plan" in result


def test_chain_monitor_schedule_missing() -> None:
    result = chain_monitor_schedule(9999)
    assert result["ok"] is False


# ------------------------------------------------------------------
# 链路8: 种子 -> 增量游标推进
# ------------------------------------------------------------------

def test_chain_seed_cursor_advance_ok() -> None:
    result = chain_seed_cursor_advance(1, "2026-11-16T10:00:00Z")
    assert result["ok"] is True
    assert result["seed_id"] == 1
    assert result["advance"]["new"] == "2026-11-16T10:00:00Z"


def test_chain_seed_cursor_advance_numeric() -> None:
    result = chain_seed_cursor_advance(2, "2500")
    assert result["ok"] is True
    assert result["advance"]["new"] == "2500"


# ------------------------------------------------------------------
# 链路9: 全链路贯通
# ------------------------------------------------------------------

def test_chain_full_pipeline_ok() -> None:
    html = "<html><body><h1>全链路测试</h1><p>数据内容</p></body></html>"
    records = [
        {"item_id": "F001", "title": "测试商品", "price": "99"},
        {"item_id": "F002", "title": "测试商品2", "price": "199"},
    ]
    result = chain_full_pipeline(1, html, records)
    assert result["ok"] is True
    assert result["source_id"] == 1
    assert result["store"]["ok"] is True
    assert result["dedup"]["unique"] >= 1


# ------------------------------------------------------------------
# store 关系索引完整性测试
# ------------------------------------------------------------------

def test_store_relations_populated() -> None:
    assert len(store.get_relations("source_to_seed")) >= 1
    assert len(store.get_relations("seed_to_parse_rule")) >= 1
    assert len(store.get_relations("parse_to_field")) >= 1
    assert len(store.get_relations("source_to_rate_limit")) >= 1
    assert len(store.get_relations("source_to_anti_spider")) >= 1
    assert len(store.get_relations("source_to_retry")) >= 1
    assert len(store.get_relations("seed_to_cursor")) >= 1


# ------------------------------------------------------------------
# 服务层真实逻辑测试（补充关键链路）
# ------------------------------------------------------------------

def test_rate_limit_token_bucket() -> None:
    from app.services.rate_limit import RateLimitService
    svc = RateLimitService()
    entry = store.find("rate_limit", 1)
    assert entry is not None
    result = svc.acquire_token(1)
    assert result["allowed"] is True
    assert result["tokens"] >= 0


def test_proxy_health_score() -> None:
    from app.services.proxy_pool import ProxyPoolService
    svc = ProxyPoolService()
    proxy = svc.select_proxy()
    assert proxy is not None
    assert proxy.get("_selected_score", 0) > 0


def test_dedup_exact_and_similarity() -> None:
    from app.services.dedup_filter import DedupFilterService
    svc = DedupFilterService()
    records = [{"item_id": "X"}, {"item_id": "X"}, {"item_id": "Y"}]
    r = svc.deduplicate(1, records)
    assert r["ok"] is True
    assert len(r["unique"]) == 2
    assert len(r["duplicates"]) == 1
