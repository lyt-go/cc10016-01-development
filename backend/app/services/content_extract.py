"""内容抽取业务规则：内容分块、关键词过滤、质量评分。"""
from __future__ import annotations

import re
from typing import Any

from app.store import store

MODULE = "content_extract"
LIST_FIELDS = ["任务编号", "来源页面", "内容类型", "抽取算法", "关键词过滤", "摘要长度", "质量阈值", "任务批次"]
REQUIRED_FIELDS = ["任务编号", "来源页面"]
STATUS_ORDER = ["草稿", "提取中", "已完成", "已取消"]
ACTION_RULES = {"发起提取": "提取中", "确认完成": "已完成", "取消提取": "已取消"}
NEGATIVE_ACTIONS = ["取消提取"]


def _extract_by_algorithm(html: str, algo: str) -> str:
    """基于算法名称模拟内容抽取。"""
    a = (algo or "").lower()
    if a == "xpath":
        # 模拟提取<body>内容
        m = re.search(r"<body[^>]*>(.*?)</body>", html, re.DOTALL | re.IGNORECASE)
        return m.group(1).strip()[:500] if m else html[:500]
    if a == "regex":
        # 提取所有段落文本
        texts = re.findall(r"<p[^>]*>(.*?)</p>", html, re.DOTALL | re.IGNORECASE)
        return " ".join(t.strip() for t in texts)[:500]
    if a == "density":
        # 按文本密度提取：找最长连续文本块
        texts = re.findall(r">([^<]{20,})<", html)
        longest = max(texts, key=len, default="")
        return longest[:500]
    return html[:500]


def _keyword_filter(text: str, keywords: str) -> tuple[str, bool]:
    """关键词过滤：只保留包含所有关键词的文本。"""
    if not keywords:
        return text, True
    kws = [k.strip() for k in keywords.split(",") if k.strip()]
    missing = [k for k in kws if k.lower() not in text.lower()]
    return text, len(missing) == 0


def _quality_score(text: str, threshold: float) -> tuple[float, bool]:
    """计算文本质量分：基于长度、标点比例、中文占比。"""
    if not text:
        return 0.0, False
    length_score = min(1.0, len(text) / 200.0)
    punct_count = sum(1 for c in text if c in "。，！？；：")
    punct_score = min(1.0, punct_count / max(len(text) / 20, 1))
    chinese_count = sum(1 for c in text if "\u4e00" <= c <= "\u9fff")
    chinese_score = min(1.0, chinese_count / max(len(text) / 2, 1))
    score = round((length_score * 0.4 + punct_score * 0.3 + chinese_score * 0.3), 3)
    return score, score >= threshold


class ContentExtractService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("任务编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        enriched = []
        results = store.pipeline_state("content_extract_results") or {}
        for row in rows[start:start + size]:
            d = dict(row)
            d["_result"] = results.get(str(d["id"]))
            enriched.append(d)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        results = store.pipeline_state("content_extract_results") or {}
        d["_result"] = results.get(str(entry_id))
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
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"提取任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于内容抽取可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        if action == "发起提取":
            self._execute_extraction(entry_id)
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"提取任务已{action}"

    def _execute_extraction(self, entry_id: int) -> dict[str, Any]:
        """执行内容抽取pipeline。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False}
        html = str(entry.get("来源页面") or "")
        algo = str(entry.get("抽取算法") or "density")
        raw = _extract_by_algorithm(html, algo)
        keywords = str(entry.get("关键词过滤") or "")
        filtered, kw_pass = _keyword_filter(raw, keywords)
        try:
            threshold = float(str(entry.get("质量阈值") or "0.5"))
        except (ValueError, TypeError):
            threshold = 0.5
        score, quality_pass = _quality_score(filtered, threshold)
        try:
            max_len = int(str(entry.get("摘要长度") or "200"))
        except (ValueError, TypeError):
            max_len = 200
        summary = filtered[:max_len]
        result = {
            "ok": kw_pass and quality_pass,
            "raw_length": len(raw),
            "filtered_length": len(filtered),
            "keyword_pass": kw_pass,
            "quality_score": score,
            "quality_pass": quality_pass,
            "summary": summary,
        }
        results = store.pipeline_state("content_extract_results") or {}
        results[str(entry_id)] = result
        store.set_pipeline_state("content_extract_results", results)
        return result

    def extract(self, entry_id: int, html: str) -> dict[str, Any]:
        """对外暴露的抽取接口。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": "任务不存在"}
        algo = str(entry.get("抽取算法") or "density")
        raw = _extract_by_algorithm(html, algo)
        keywords = str(entry.get("关键词过滤") or "")
        filtered, kw_pass = _keyword_filter(raw, keywords)
        try:
            threshold = float(str(entry.get("质量阈值") or "0.5"))
        except (ValueError, TypeError):
            threshold = 0.5
        score, quality_pass = _quality_score(filtered, threshold)
        try:
            max_len = int(str(entry.get("摘要长度") or "200"))
        except (ValueError, TypeError):
            max_len = 200
        return {
            "ok": kw_pass and quality_pass,
            "summary": filtered[:max_len],
            "score": score,
        }
