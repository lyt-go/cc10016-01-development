"""增量游标业务规则：断点快照、位置推进、覆写策略管理。"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "incre_cursor"
LIST_FIELDS = ["标记编号", "关联种子", "增量字段", "上次位置", "当前位置", "增量类型", "覆写策略", "更新间隔"]
REQUIRED_FIELDS = ["标记编号", "关联种子", "增量字段"]
STATUS_ORDER = ["草稿", "待校验", "采集中", "已复位"]
ACTION_RULES = {"提交校验": "待校验", "标记采集中": "采集中", "复位游标": "已复位"}
NEGATIVE_ACTIONS = []


def _parse_position(pos: Any) -> tuple[int, str]:
    """解析位置值：支持整数ID或日期字符串。"""
    if pos is None:
        return 0, ""
    s = str(pos).strip()
    if not s:
        return 0, ""
    try:
        return int(s), "numeric"
    except (ValueError, TypeError):
        return 0, "string"


class IncreCursorService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("标记编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        enriched = []
        snapshots = store.pipeline_state("cursor_snapshots") or {}
        for row in rows[start:start + size]:
            d = dict(row)
            d["_snapshot"] = snapshots.get(str(d["id"]), {})
            last = str(d.get("上次位置") or "")
            curr = str(d.get("当前位置") or "")
            try:
                progress = int(curr) - int(last) if last and curr else 0
            except (ValueError, TypeError):
                progress = 0
            d["_progress"] = progress
            enriched.append(d)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        snapshots = store.pipeline_state("cursor_snapshots") or {}
        d["_snapshot"] = snapshots.get(str(entry_id), {})
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
        entry["上次位置"] = values.get("上次位置") or "0"
        entry["当前位置"] = values.get("当前位置") or entry["上次位置"]
        rows.append(entry)
        # 初始化快照
        snapshots = store.pipeline_state("cursor_snapshots") or {}
        snapshots[str(entry["id"])] = {"created_at": "2026-11-16T00:00:00Z", "checkpoints": []}
        store.set_pipeline_state("cursor_snapshots", snapshots)
        # 绑定种子
        seed_id = values.get("关联种子ID")
        if seed_id:
            try:
                sid = int(seed_id)
                store.add_relation("seed_to_cursor", "seed_config", sid, MODULE, entry["id"])
            except (ValueError, TypeError):
                pass
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"增量标记 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于增量游标可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        if action == "复位游标":
            entry["当前位置"] = entry.get("上次位置") or "0"
            snapshots = store.pipeline_state("cursor_snapshots") or {}
            sp = snapshots.get(str(entry_id), {})
            sp.setdefault("checkpoints", []).append({
                "action": "reset", "to": entry["当前位置"], "at": "2026-11-16T00:00:00Z"
            })
            snapshots[str(entry_id)] = sp
            store.set_pipeline_state("cursor_snapshots", snapshots)
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"增量标记已{action}"

    def advance_cursor(self, entry_id: int, new_position: str) -> dict[str, Any]:
        """推进游标到新位置，保存断点快照。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": "游标不存在"}
        old = str(entry.get("当前位置") or "0")
        entry["上次位置"] = old
        entry["当前位置"] = str(new_position)
        snapshots = store.pipeline_state("cursor_snapshots") or {}
        sp = snapshots.get(str(entry_id), {})
        sp.setdefault("checkpoints", []).append({
            "from": old, "to": str(new_position), "at": "2026-11-16T00:00:00Z"
        })
        if len(sp["checkpoints"]) > 100:
            sp["checkpoints"] = sp["checkpoints"][-100:]
        snapshots[str(entry_id)] = sp
        store.set_pipeline_state("cursor_snapshots", snapshots)
        return {"ok": True, "old": old, "new": str(new_position)}

    def rollback_to_checkpoint(self, entry_id: int, checkpoint_index: int) -> dict[str, Any]:
        """回滚到指定检查点。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": "游标不存在"}
        snapshots = store.pipeline_state("cursor_snapshots") or {}
        sp = snapshots.get(str(entry_id), {})
        checkpoints = sp.get("checkpoints", [])
        if checkpoint_index < 0 or checkpoint_index >= len(checkpoints):
            return {"ok": False, "error": "检查点索引越界"}
        target = checkpoints[checkpoint_index]
        entry["当前位置"] = target.get("from", "0")
        return {"ok": True, "restored_to": entry["当前位置"]}
