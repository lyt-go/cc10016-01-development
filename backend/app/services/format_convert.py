"""格式转换业务规则：Schema映射、嵌套展开、数组拍平、编码转换pipeline。"""
from __future__ import annotations

import json
import re
from typing import Any

from app.store import store

MODULE = "format_convert"
LIST_FIELDS = ["配置编号", "源格式", "目标格式", "映射规则", "字符编码", "嵌套展开", "数组拍平", "缺失处理"]
REQUIRED_FIELDS = ["配置编号", "源格式", "目标格式"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用配置": "已停用"}
NEGATIVE_ACTIONS = ["停用配置"]


def _parse_mapping(rule_text: str) -> dict[str, str]:
    """解析映射规则文本为字段映射字典。"""
    mapping: dict[str, str] = {}
    if not rule_text:
        return mapping
    for line in rule_text.split(";"):
        line = line.strip()
        if "->" in line:
            src, dst = line.split("->", 1)
            mapping[src.strip()] = dst.strip()
        elif ":" in line:
            src, dst = line.split(":", 1)
            mapping[src.strip()] = dst.strip()
    return mapping


def _flatten_nested(obj: Any, prefix: str = "") -> dict[str, Any]:
    """递归拍平嵌套对象。"""
    result: dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, (dict, list)):
                result.update(_flatten_nested(v, new_key))
            else:
                result[new_key] = v
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            new_key = f"{prefix}[{i}]"
            if isinstance(v, (dict, list)):
                result.update(_flatten_nested(v, new_key))
            else:
                result[new_key] = v
    else:
        result[prefix] = obj
    return result


def _flatten_arrays(records: list[dict[str, Any]], array_field: str) -> list[dict[str, Any]]:
    """将指定数组字段拍平为多行。"""
    result = []
    for record in records:
        arr = record.get(array_field)
        if isinstance(arr, list) and arr:
            for item in arr:
                new_row = dict(record)
                new_row[array_field] = item
                result.append(new_row)
        else:
            result.append(record)
    return result


class FormatConvertService:
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
        pipelines = store.pipeline_state("format_pipelines") or {}
        for row in rows[start:start + size]:
            d = dict(row)
            d["_pipeline_runs"] = len(pipelines.get(str(d["id"]), []))
            enriched.append(d)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        pipelines = store.pipeline_state("format_pipelines") or {}
        d["_pipeline_runs"] = len(pipelines.get(str(entry_id), []))
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
            return None, f"转换配置 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于格式转换可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"转换配置已{action}"

    def run_pipeline(self, entry_id: int, input_records: list[dict[str, Any]]) -> dict[str, Any]:
        """执行格式转换pipeline：映射->展开->拍平->编码。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": "配置不存在"}
        mapping = _parse_mapping(str(entry.get("映射规则") or ""))
        expand = str(entry.get("嵌套展开") or "").lower() in ("是", "true", "yes", "1")
        flatten = str(entry.get("数组拍平") or "").lower() in ("是", "true", "yes", "1")
        missing_policy = str(entry.get("缺失处理") or "keep")
        # 1. 字段映射
        mapped = []
        for record in input_records:
            new_row: dict[str, Any] = {}
            for src, dst in mapping.items():
                if src in record:
                    new_row[dst] = record[src]
                elif missing_policy == "fill_empty":
                    new_row[dst] = ""
                elif missing_policy == "drop":
                    continue
                else:
                    new_row[dst] = record.get(src)
            # 保留未映射字段
            for k, v in record.items():
                if k not in mapping:
                    new_row[k] = v
            mapped.append(new_row)
        # 2. 嵌套展开
        if expand:
            expanded = []
            for row in mapped:
                expanded.append(_flatten_nested(row))
            mapped = expanded
        # 3. 数组拍平（检测第一个数组字段）
        if flatten and mapped:
            first = mapped[0]
            array_field = None
            for k, v in first.items():
                if isinstance(v, list):
                    array_field = k
                    break
            if array_field:
                mapped = _flatten_arrays(mapped, array_field)
        # 4. 编码处理（模拟UTF-8清洗）
        cleaned = []
        for row in mapped:
            clean_row = {}
            for k, v in row.items():
                if isinstance(v, str):
                    clean_row[k] = v.encode("utf-8", "ignore").decode("utf-8")
                else:
                    clean_row[k] = v
            cleaned.append(clean_row)
        # 记录pipeline运行
        pipelines = store.pipeline_state("format_pipelines") or {}
        runs = pipelines.setdefault(str(entry_id), [])
        runs.append({"input_count": len(input_records), "output_count": len(cleaned)})
        if len(runs) > 50:
            pipelines[str(entry_id)] = runs[-50:]
        store.set_pipeline_state("format_pipelines", pipelines)
        return {"ok": True, "output": cleaned, "input_count": len(input_records), "output_count": len(cleaned)}
