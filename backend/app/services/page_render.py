"""渲染编排业务规则：渲染配置校验、拦截规则解析、资源白名单过滤。"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "page_render"
LIST_FIELDS = ["配置编号", "目标源", "渲染引擎", "超时时间", "拦截规则", "屏幕尺寸", "滚动策略", "资源白名单"]
REQUIRED_FIELDS = ["配置编号", "目标源", "渲染引擎"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用配置": "已停用"}
NEGATIVE_ACTIONS = ["停用配置"]


def _parse_block_rules(rule_text: str) -> list[dict[str, Any]]:
    """解析拦截规则文本为结构化规则列表。"""
    rules = []
    if not rule_text:
        return rules
    for line in rule_text.split(";"):
        line = line.strip()
        if not line:
            continue
        parts = line.split("=>")
        if len(parts) == 2:
            rules.append({"pattern": parts[0].strip(), "action": parts[1].strip()})
        else:
            rules.append({"pattern": line, "action": "block"})
    return rules


class PageRenderService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("配置编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        enriched = []
        for row in rows[start:start + size]:
            d = dict(row)
            d["_block_rules"] = _parse_block_rules(str(d.get("拦截规则") or ""))
            enriched.append(d)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        d["_block_rules"] = _parse_block_rules(str(d.get("拦截规则") or ""))
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
                store.add_relation("source_to_render", "crawl_source", sid, MODULE, entry["id"])
            except (ValueError, TypeError):
                pass
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"渲染配置 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于渲染编排可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"渲染配置已{action}"

    def match_resource(self, entry_id: int, resource_url: str) -> dict[str, Any]:
        """判断资源URL是否被白名单允许或拦截规则命中。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"allowed": True, "reason": "配置不存在默认放行"}
        whitelist = str(entry.get("资源白名单") or "").split(",")
        whitelist = [w.strip() for w in whitelist if w.strip()]
        if whitelist and not any(w in resource_url for w in whitelist):
            return {"allowed": False, "reason": "不在白名单", "matched_rule": None}
        rules = _parse_block_rules(str(entry.get("拦截规则") or ""))
        for rule in rules:
            if rule["pattern"] in resource_url:
                return {"allowed": False, "reason": f"拦截规则命中: {rule['action']}", "matched_rule": rule}
        return {"allowed": True, "reason": "通过"}
