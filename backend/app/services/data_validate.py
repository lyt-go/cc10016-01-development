"""数据校验业务规则：规则引擎、采样校验、阻断策略执行。"""
from __future__ import annotations

import re
from typing import Any

from app.store import store

MODULE = "data_validate"
LIST_FIELDS = ["规则编号", "目标字段", "校验函数", "合法范围", "异常动作", "采样比例", "校验阶段", "阻断策略"]
REQUIRED_FIELDS = ["规则编号", "目标字段", "校验函数"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
NEGATIVE_ACTIONS = ["停用规则"]


def _run_validator(value: Any, func_name: str, valid_range: str) -> tuple[bool, str]:
    """执行单个校验函数。"""
    fn = (func_name or "").lower()
    s = str(value) if value is not None else ""
    if fn == "not_empty":
        return bool(s.strip()), "字段不能为空"
    if fn == "is_number":
        return bool(re.match(r"^-?\d+(\.\d+)?$", s)), "必须是数字"
    if fn == "is_url":
        return bool(re.match(r"^https?://", s)), "必须是URL"
    if fn == "regex":
        if valid_range:
            try:
                return bool(re.match(valid_range, s)), f"不匹配正则: {valid_range}"
            except re.error as exc:
                return False, f"正则错误: {exc}"
        return True, "无正则规则"
    if fn == "range":
        try:
            parts = [p.strip() for p in (valid_range or "").split(",")]
            num = float(s)
            if len(parts) == 2:
                low, high = float(parts[0]), float(parts[1])
                return low <= num <= high, f"必须在[{low},{high}]之间"
        except (ValueError, TypeError):
            return False, "范围校验需要数字"
    if fn == "length":
        try:
            max_len = int(valid_range or "255")
            return len(s) <= max_len, f"长度不能超过{max_len}"
        except (ValueError, TypeError):
            return False, "长度规则异常"
    if fn == "enum":
        allowed = [p.strip() for p in (valid_range or "").split(",")]
        return s in allowed, f"必须是其中之一: {allowed}"
    return True, "未知校验函数跳过"


class DataValidateService:
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
        rows.append(entry)
        source_id = values.get("关联采集源ID")
        if source_id:
            try:
                sid = int(source_id)
                store.add_relation("source_to_validate", "crawl_source", sid, MODULE, entry["id"])
            except (ValueError, TypeError):
                pass
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"校验规则 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据校验可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"校验规则已{action}"

    def validate_records(self, entry_id: int, records: list[dict[str, Any]]) -> dict[str, Any]:
        """对记录列表执行校验规则，返回通过/失败/阻断统计。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": "规则不存在"}
        target_field = str(entry.get("目标字段") or "")
        func = str(entry.get("校验函数") or "")
        valid_range = str(entry.get("合法范围") or "")
        action_on_error = str(entry.get("异常动作") or "log").lower()
        block_policy = str(entry.get("阻断策略") or "per_record").lower()
        sample = float(str(entry.get("采样比例") or "1.0").replace("%", ""))
        if sample > 1:
            sample = sample / 100.0
        passed, failed, blocked = [], [], []
        should_block = False
        for i, record in enumerate(records):
            if sample < 1.0 and (i % max(1, int(1 / sample))) != 0:
                passed.append(record)
                continue
            value = record.get(target_field)
            ok, msg = _run_validator(value, func, valid_range)
            if ok:
                passed.append(record)
            else:
                record["_error"] = msg
                failed.append(record)
                if block_policy == "per_record":
                    blocked.append(record)
                elif block_policy == "first_fail":
                    should_block = True
                    blocked = failed
                    break
                elif block_policy == "threshold":
                    if len(failed) > len(records) * 0.1:
                        should_block = True
                        blocked = failed
                        break
        # 保存报告
        reports = store.pipeline_state("data_validate_reports") or {}
        reports[str(entry_id)] = {"passed": len(passed), "failed": len(failed), "blocked": len(blocked)}
        store.set_pipeline_state("data_validate_reports", reports)
        return {
            "ok": len(failed) == 0 or action_on_error != "block",
            "passed": passed,
            "failed": failed,
            "blocked": blocked,
            "blocked_policy": block_policy,
        }
