"""字段抽取业务规则：模板匹配、字段精炼、输出类型转换。"""
from __future__ import annotations

import re
from typing import Any

from app.store import store

MODULE = "field_extract"
LIST_FIELDS = ["模板编号", "关联种子", "字段名", "选择器", "精炼规则", "输出类型", "可为空", "抽取优先级"]
REQUIRED_FIELDS = ["模板编号", "关联种子", "字段名"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已废弃"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "标记废弃": "已废弃"}
NEGATIVE_ACTIONS = []


def _refine_value(value: str, rule: str) -> tuple[str, bool]:
    """使用精炼规则处理原始值，返回(结果,是否成功)。"""
    if not rule or not value:
        return value, True
    try:
        m = re.search(rule, str(value))
        if m:
            return m.group(1) if m.lastindex else m.group(0), True
        return value, False
    except re.error:
        return value, False


def _cast_output(value: str, output_type: str) -> tuple[Any, bool]:
    """将字符串值转换为目标输出类型。"""
    t = (output_type or "").lower()
    if t == "integer":
        try:
            return int(value), True
        except (ValueError, TypeError):
            return None, False
    if t == "float":
        try:
            return float(value), True
        except (ValueError, TypeError):
            return None, False
    if t == "boolean":
        return value.lower() in ("true", "1", "yes", "是"), True
    if t == "date":
        if re.match(r"\d{4}-\d{2}-\d{2}", str(value)):
            return str(value), True
        return None, False
    return str(value), True


class FieldExtractService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("模板编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

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
        # 预计算输出示例
        sample_value = "示例文本 123.45"
        refined, refine_ok = _refine_value(sample_value, entry.get("精炼规则") or "")
        casted, cast_ok = _cast_output(refined, entry.get("输出类型"))
        entry["_preview"] = {"sample": sample_value, "refined": refined, "casted": casted, "ok": refine_ok and cast_ok}
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"抽取模板 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于字段抽取可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        if action == "提交校验":
            sample_value = "示例文本 123.45"
            refined, refine_ok = _refine_value(sample_value, entry.get("精炼规则") or "")
            casted, cast_ok = _cast_output(refined, entry.get("输出类型"))
            entry["_preview"] = {"sample": sample_value, "refined": refined, "casted": casted, "ok": refine_ok and cast_ok}
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"抽取模板已{action}"

    def extract_fields(self, entry_id: int, raw_html: str) -> dict[str, Any]:
        """对给定原始HTML执行字段抽取，返回抽取结果与元数据。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": "模板不存在"}
        selector = str(entry.get("选择器") or "")
        refine_rule = str(entry.get("精炼规则") or "")
        output_type = str(entry.get("输出类型") or "text")
        nullable = str(entry.get("可为空") or "").lower() in ("是", "true", "yes")
        # 模拟选择器匹配：在HTML中查找包含选择器关键文本的片段
        matched = ""
        if selector in raw_html:
            start = raw_html.find(selector)
            end = raw_html.find("</", start)
            matched = raw_html[start:end] if end > start else raw_html[start:start + 100]
        if not matched:
            matched = raw_html[:200]
        refined, refine_ok = _refine_value(matched, refine_rule)
        casted, cast_ok = _cast_output(refined, output_type)
        result = {
            "ok": cast_ok or nullable,
            "field": entry.get("字段名"),
            "matched_length": len(matched),
            "refined": refined,
            "casted": casted,
            "type": output_type,
            "nullable": nullable,
        }
        # 保存抽取结果到store供下游消费
        results = store.pipeline_state("content_extract_results") or {}
        results[f"field_{entry_id}"] = result
        store.set_pipeline_state("content_extract_results", results)
        return result
