"""结果落地业务规则：分区计算、批次写入、生命周期管理。"""
from __future__ import annotations

import hashlib
from typing import Any

from app.store import store

MODULE = "result_store"
LIST_FIELDS = ["配置编号", "输出格式", "存储路径", "分区规则", "压缩算法", "保留天数", "写入批次", "回滚水位"]
REQUIRED_FIELDS = ["配置编号", "输出格式", "存储路径"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用配置": "已停用"}
NEGATIVE_ACTIONS = ["停用配置"]


def _compute_partition(record: dict[str, Any], rule: str) -> str:
    """根据分区规则计算记录应落到的分区路径。"""
    if not rule:
        return "default"
    # 支持按字段分区: {field_name}
    import re
    fields = re.findall(r"\{([^}]+)\}", rule)
    if fields:
        parts = []
        for f in fields:
            parts.append(str(record.get(f) or "unknown"))
        return "/".join(parts)
    # 支持按哈希分区: hash_{field}_{mod}
    m = re.match(r"hash_(\w+)_(\d+)", rule)
    if m:
        field, mod = m.group(1), int(m.group(2))
        val = str(record.get(field) or "")
        h = int(hashlib.md5(val.encode()).hexdigest(), 16)
        return f"hash_{h % mod}"
    return rule


class ResultStoreService:
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
        partitions = store.pipeline_state("result_store_partitions") or {}
        for row in rows[start:start + size]:
            d = dict(row)
            d["_partition_meta"] = partitions.get(str(d["id"]), {})
            enriched.append(d)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        partitions = store.pipeline_state("result_store_partitions") or {}
        d["_partition_meta"] = partitions.get(str(entry_id), {})
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
                store.add_relation("source_to_result", "crawl_source", sid, MODULE, entry["id"])
            except (ValueError, TypeError):
                pass
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"落地配置 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于结果落地可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"落地配置已{action}"

    def write_batch(self, entry_id: int, records: list[dict[str, Any]]) -> dict[str, Any]:
        """批量写入记录，按分区规则组织。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": "配置不存在"}
        partition_rule = str(entry.get("分区规则") or "")
        batch_size = int(str(entry.get("写入批次") or "100").replace("条", ""))
        partitions = store.pipeline_state("result_store_partitions") or {}
        meta = partitions.setdefault(str(entry_id), {"total_written": 0, "partitions": {}})
        grouped: dict[str, list] = {}
        for record in records:
            part = _compute_partition(record, partition_rule)
            grouped.setdefault(part, []).append(record)
        total = 0
        for part, items in grouped.items():
            written = len(items)
            meta["partitions"][part] = meta["partitions"].get(part, 0) + written
            total += written
        meta["total_written"] += total
        partitions[str(entry_id)] = meta
        store.set_pipeline_state("result_store_partitions", partitions)
        return {"ok": True, "written": total, "batch_size": batch_size, "partitions": list(grouped.keys())}
