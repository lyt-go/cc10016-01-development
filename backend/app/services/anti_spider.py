"""反爬对抗业务规则：触发检测、自动应对、冷静期管理。"""
from __future__ import annotations

import re
from typing import Any

from app.store import store

MODULE = "anti_spider"
LIST_FIELDS = ["策略编号", "目标源", "反爬类型", "触发条件", "应对方式", "冷静时长", "切换代理", "标识伪造"]
REQUIRED_FIELDS = ["策略编号", "目标源", "反爬类型"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用策略": "已停用"}
NEGATIVE_ACTIONS = ["停用策略"]


def _check_trigger(entry: dict[str, Any], response_text: str, status_code: int) -> tuple[bool, str]:
    """检查是否触发反爬条件。"""
    trigger = str(entry.get("触发条件") or "")
    anti_type = str(entry.get("反爬类型") or "").lower()
    if anti_type == "状态码":
        try:
            target_codes = [int(x.strip()) for x in trigger.split(",") if x.strip()]
            if status_code in target_codes:
                return True, f"状态码命中: {status_code}"
        except ValueError:
            pass
    if anti_type == "关键词":
        keywords = [k.strip() for k in trigger.split(",") if k.strip()]
        for kw in keywords:
            if kw.lower() in response_text.lower():
                return True, f"关键词命中: {kw}"
    if anti_type == "验证码":
        captcha_signals = ["captcha", "验证码", "人机验证", "请验证"]
        for sig in captcha_signals:
            if sig.lower() in response_text.lower():
                return True, f"验证码信号: {sig}"
    if anti_type == "频率限制":
        rate_signals = ["rate limit", "too many requests", "429", "访问过快"]
        for sig in rate_signals:
            if sig.lower() in response_text.lower() or str(status_code) == "429":
                return True, f"频率限制信号: {sig}"
    return False, "未触发"


def _apply_countermeasure(entry: dict[str, Any]) -> dict[str, Any]:
    """执行应对方式并返回动作详情。"""
    counter = str(entry.get("应对方式") or "").lower()
    result = {"switch_proxy": False, "spoof_identity": False, "cool_down": 0, "actions": []}
    if "代理" in counter or "proxy" in counter:
        result["switch_proxy"] = True
        result["actions"].append("切换代理")
    if "标识" in counter or "ua" in counter or "cookie" in counter:
        result["spoof_identity"] = True
        result["actions"].append("伪造标识")
    if "冷静" in counter or "等待" in counter or "sleep" in counter:
        try:
            result["cool_down"] = int(str(entry.get("冷静时长") or "60").replace("s", "").replace("秒", ""))
        except (ValueError, TypeError):
            result["cool_down"] = 60
        result["actions"].append(f"冷静{result['cool_down']}秒")
    return result


class AntiSpiderService:
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
        logs = store.pipeline_state("anti_spider_trigger_log") or []
        for row in rows[start:start + size]:
            d = dict(row)
            d["_trigger_count"] = sum(1 for log in logs if log.get("policy_id") == d["id"])
            enriched.append(d)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        logs = store.pipeline_state("anti_spider_trigger_log") or []
        d["_trigger_count"] = sum(1 for log in logs if log.get("policy_id") == entry_id)
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
                store.add_relation("source_to_anti_spider", "crawl_source", sid, MODULE, entry["id"])
            except (ValueError, TypeError):
                pass
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"对抗策略 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于反爬对抗可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"对抗策略已{action}"

    def detect_and_respond(self, entry_id: int, response_text: str, status_code: int) -> dict[str, Any]:
        """检测反爬触发并自动执行应对。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"triggered": False, "reason": "策略不存在"}
        triggered, reason = _check_trigger(entry, response_text, status_code)
        result = {
            "triggered": triggered,
            "reason": reason,
            "policy_id": entry_id,
            "source": entry.get("目标源"),
        }
        if triggered:
            counter = _apply_countermeasure(entry)
            result["countermeasure"] = counter
            logs = store.pipeline_state("anti_spider_trigger_log") or []
            logs.append({
                "policy_id": entry_id,
                "source": entry.get("目标源"),
                "status_code": status_code,
                "reason": reason,
                "actions": counter["actions"],
            })
            if len(logs) > 200:
                store.set_pipeline_state("anti_spider_trigger_log", logs[-200:])
            else:
                store.set_pipeline_state("anti_spider_trigger_log", logs)
        return result
