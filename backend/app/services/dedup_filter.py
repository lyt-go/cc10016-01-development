"""去重过滤业务规则：布隆过滤器、相似度比对、冲突解决。"""
from __future__ import annotations

import hashlib
import re
from typing import Any

from app.store import store

MODULE = "dedup_filter"
LIST_FIELDS = ["规则编号", "目标源", "去重键列", "比较方式", "相似度阈值", "时间窗口", "冲突策略", "规则版本"]
REQUIRED_FIELDS = ["规则编号", "目标源", "去重键列"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
NEGATIVE_ACTIONS = ["停用规则"]


def _compute_simhash(text: str) -> int:
    """计算简化版simhash（仅用于演示去重引擎）。"""
    tokens = re.findall(r"\w+", str(text).lower())
    vec = [0] * 32
    for tok in tokens:
        h = int(hashlib.md5(tok.encode()).hexdigest(), 16) & 0xFFFFFFFF
        for i in range(32):
            bit = (h >> i) & 1
            vec[i] += 1 if bit else -1
    result = 0
    for i in range(32):
        if vec[i] > 0:
            result |= (1 << i)
    return result


def _hamming_distance(a: int, b: int) -> int:
    x = a ^ b
    dist = 0
    while x:
        dist += 1
        x &= x - 1
    return dist


def _normalize_key(record: dict[str, Any], key_columns: str) -> str:
    cols = [c.strip() for c in str(key_columns).split(",")]
    parts = []
    for c in cols:
        parts.append(str(record.get(c) or ""))
    return "|".join(parts)


class DedupFilterService:
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
        blooms = store.pipeline_state("dedup_bloom_filters") or {}
        for row in rows[start:start + size]:
            d = dict(row)
            d["_bloom_size"] = len(blooms.get(str(d["id"]), set()))
            enriched.append(d)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        blooms = store.pipeline_state("dedup_bloom_filters") or {}
        d["_bloom_size"] = len(blooms.get(str(entry_id), set()))
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
                store.add_relation("source_to_dedup", "crawl_source", sid, MODULE, entry["id"])
            except (ValueError, TypeError):
                pass
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"去重规则 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于去重过滤可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"去重规则已{action}"

    def deduplicate(self, entry_id: int, records: list[dict[str, Any]]) -> dict[str, Any]:
        """对记录列表执行去重，返回唯一/重复/冲突分组。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": "规则不存在"}
        key_cols = str(entry.get("去重键列") or "")
        compare = str(entry.get("比较方式") or "exact").lower()
        threshold = float(str(entry.get("相似度阈值") or "0.9").replace("%", ""))
        if threshold > 1:
            threshold = threshold / 100.0
        blooms = store.pipeline_state("dedup_bloom_filters") or {}
        seen = blooms.setdefault(str(entry_id), set())
        unique, duplicates, conflicts = [], [], []
        for record in records:
            key = _normalize_key(record, key_cols)
            if compare == "exact":
                if key in seen:
                    duplicates.append(record)
                else:
                    seen.add(key)
                    unique.append(record)
            elif compare == "similarity":
                sh = _compute_simhash(key)
                dup = False
                for old_sh in list(seen):
                    sim = 1.0 - _hamming_distance(sh, old_sh) / 32.0
                    if sim >= threshold:
                        dup = True
                        record["_similarity"] = round(sim, 3)
                        conflicts.append(record)
                        break
                if not dup:
                    seen.add(sh)
                    unique.append(record)
            else:
                if key in seen:
                    duplicates.append(record)
                else:
                    seen.add(key)
                    unique.append(record)
        store.set_pipeline_state("dedup_bloom_filters", blooms)
        return {"ok": True, "unique": unique, "duplicates": duplicates, "conflicts": conflicts, "seen_count": len(seen)}
