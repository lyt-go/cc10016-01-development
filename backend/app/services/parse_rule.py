"""解析规则业务规则：选择器语法校验、版本管理、字段抽取链生成。"""
from __future__ import annotations

import re
from typing import Any

from app.store import store

MODULE = "parse_rule"
LIST_FIELDS = ["规则编号", "目标字段名", "选择器类型", "选择器表达式", "提取方式", "默认值", "正则过滤", "规则版本"]
REQUIRED_FIELDS = ["规则编号", "目标字段名", "选择器类型"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已废弃"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "标记废弃": "已废弃"}
NEGATIVE_ACTIONS = []


def _validate_selector(selector_type: str, expression: str) -> tuple[bool, str]:
    """校验选择器表达式与类型是否匹配。"""
    if not expression or not isinstance(expression, str):
        return False, "选择器表达式为空"
    t = (selector_type or "").lower()
    if t == "xpath":
        if not expression.startswith(("/", "//")):
            return False, "XPath必须以/开头"
        if expression.count("(") != expression.count(")"):
            return False, "括号不匹配"
        return True, "XPath合法"
    if t == "css":
        if not re.search(r"[.#\[\]]", expression) and not expression.isalnum():
            return False, "CSS选择器格式异常"
        return True, "CSS选择器合法"
    if t == "jsonpath":
        if not expression.startswith("$"):
            return False, "JSONPath必须以$开头"
        return True, "JSONPath合法"
    if t == "regex":
        try:
            re.compile(expression)
            return True, "正则合法"
        except re.error as exc:
            return False, f"正则错误: {exc}"
    return True, "未知类型跳过校验"


class ParseRuleService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("规则编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        enriched = []
        for row in rows[start:start + size]:
            d = dict(row)
            d["_field_count"] = len(store.get_relations("parse_to_field", source_module=MODULE, source_id=d["id"]))
            enriched.append(d)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        d["_field_count"] = len(store.get_relations("parse_to_field", source_module=MODULE, source_id=entry_id))
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
        # 选择器校验
        ok, msg = _validate_selector(entry.get("选择器类型"), entry.get("选择器表达式") or "")
        entry["_selector_check"] = {"ok": ok, "msg": msg}
        rows.append(entry)
        # 绑定种子
        seed_id = values.get("关联种子ID")
        if seed_id:
            try:
                sid = int(seed_id)
                store.add_relation("seed_to_parse_rule", "seed_config", sid, MODULE, entry["id"])
            except (ValueError, TypeError):
                pass
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"解析规则 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于解析规则可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        if action == "提交校验":
            ok, msg = _validate_selector(entry.get("选择器类型"), entry.get("选择器表达式") or "")
            entry["_selector_check"] = {"ok": ok, "msg": msg}
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"解析规则已{action}"

    def generate_field_extracts(self, entry_id: int) -> list[dict[str, Any]]:
        """基于解析规则生成字段抽取模板，写入 field_extract 模块。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return []
        from app.services.field_extract import FieldExtractService
        svc = FieldExtractService()
        templates = []
        field_name = str(entry.get("目标字段名") or "")
        selector = str(entry.get("选择器表达式") or "")
        extract_type = str(entry.get("提取方式") or "text")
        payload = {
            "模板编号": f"GEN-{entry.get('规则编号')}-FE",
            "关联种子": entry.get("规则编号"),
            "字段名": field_name,
            "选择器": selector,
            "精炼规则": entry.get("正则过滤") or "",
            "输出类型": extract_type,
            "可为空": "否",
            "抽取优先级": "10",
        }
        new_entry, _ = svc.create_entry(payload)
        if new_entry:
            store.add_relation("parse_to_field", MODULE, entry_id, "field_extract", new_entry["id"])
            templates.append(new_entry)
        return templates
