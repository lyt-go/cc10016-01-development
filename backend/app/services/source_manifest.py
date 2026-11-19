"""采集源接入清单业务规则：读取接入配置、校验接入边界、加工与发布交接批次。

职责边界（与种子管理的约定）：
- 本批采集源接入清单由采集源管理（本服务）完成加工；
- 每个采集源条目的连通状态与关联种子数量必须同时写入清单，只保存一端的批次不予发布；
- 清单发布后，种子管理按来源标识读取清单，把可继续使用的种子写入本批接入记录；
- 同一采集源再次导入产生版本冲突时，由本服务按仲裁策略（沿用绑定 / 重建批次）定序处理。

一致性约定：
- 加工、导入、发布全程先拍事务快照，任何中断或落库/缓存不一致都整体回滚，
  保证接入清单只能发布一个完整批次，不会留下半套种子引用。
"""
from __future__ import annotations

import copy
import csv
import io
import json
import urllib.parse
import zipfile
from datetime import datetime, timezone
from typing import Any

from app.store import store

MODULE = "source_manifest"
SEED_MODULE = "seed_config"
SOURCE_MODULE = "crawl_source"

# 清单条目字段：连通状态与种子数量成对出现，是导出后复核接入批次的最小集合
ITEM_FIELDS = [
    "来源标识", "源名称", "协议类型", "基础URL",
    "连通状态", "连通消息", "种子数量", "种子编号序列",
    "种子集合版本", "连通补写项",
]
REQUIRED_IMPORT_FIELDS = ["来源标识"]
BATCH_STATUS = ["加工中", "已发布", "已归档"]
CONFLICT_STRATEGIES = ["keep_binding", "rebuild"]
IMPORT_FORMATS = ["json", "csv"]

# 接入边界：协议白名单、单批次源数量上限、单源种子数量上限
ALLOWED_SCHEMES = ("http", "https")
MAX_SOURCES_PER_BATCH = 100
MAX_SEEDS_PER_SOURCE = 1000

# 连通状态枚举（与种子数量同时落条目，不允许只写一端）
CONN_OK = "可达"
CONN_FAIL = "不可达"
SEED_VERSION_KEEP = "沿用绑定"
SEED_VERSION_REBUILD = "重建批次"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _batches_state() -> dict[str, Any]:
    return store.pipeline_state("access_manifest_batches") or {}


def _save_batches_state(value: dict[str, Any]) -> None:
    store.set_pipeline_state("access_manifest_batches", value)


def _next_batch_no() -> str:
    return f"BATCH-{len(_batches_state()) + 1:04d}"


def _validate_boundary(source: dict[str, Any]) -> list[str]:
    """校验接入边界：来源标识、协议白名单、主机名、种子规模。"""
    errors: list[str] = []
    code = str(source.get("源标识") or "").strip()
    if not code:
        errors.append("采集源缺少来源标识（源标识）")
    url = str(source.get("基础URL") or "").strip()
    if not url:
        errors.append(f"采集源 {code or '未知'} 缺少基础URL")
    else:
        try:
            parsed = urllib.parse.urlparse(url)
            if parsed.scheme not in ALLOWED_SCHEMES:
                errors.append(f"采集源 {code} 协议 {parsed.scheme or '空'} 不在接入边界 {ALLOWED_SCHEMES} 内")
            if not parsed.netloc:
                errors.append(f"采集源 {code} 的URL缺少主机名")
        except Exception as exc:  # pragma: no cover - 解析异常兜底
            errors.append(f"采集源 {code} 的URL解析异常: {exc}")
    return errors


def _probe_connection(source: dict[str, Any]) -> dict[str, Any]:
    """连通校验：复用采集源管理的URL校验，并回写连接校验缓存保持两侧一致。"""
    from app.services.crawl_source import _validate_url

    reachable, message, latency = _validate_url(str(source.get("基础URL") or ""))
    result = {
        "连通状态": CONN_OK if reachable else CONN_FAIL,
        "连通消息": message,
        "连通延迟": latency,
        "checked_at": _now(),
    }
    cache = store.pipeline_state("source_validation_cache") or {}
    cache[str(source["id"])] = {
        "reachable": reachable,
        "message": message,
        "latency": latency,
        "checked_at": result["checked_at"],
    }
    store.set_pipeline_state("source_validation_cache", cache)
    return result


def _linked_seeds_in_order(source_id: int) -> list[dict[str, Any]]:
    """按绑定关系顺序返回采集源关联种子（关系索引的写入顺序即原种子顺序）。"""
    rels = store.get_relations("source_to_seed", source_module=SOURCE_MODULE, source_id=source_id)
    seeds: list[dict[str, Any]] = []
    for rel in rels:
        seed = store.find(SEED_MODULE, rel["target_id"])
        if seed:
            seeds.append(dict(seed))
    return seeds


def _find_published_batch() -> dict[str, Any] | None:
    for batch in _batches_state().values():
        if batch.get("status") == "已发布":
            return batch
    return None


def _find_processing_batch() -> dict[str, Any] | None:
    for batch in _batches_state().values():
        if batch.get("status") == "加工中":
            return batch
    return None


class SourceManifestService:
    """采集源接入清单加工、仲裁、发布与交接导出。"""

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def list_batches(self, *, status: str | None = None, page: int = 1, size: int = 20) -> tuple[list[dict[str, Any]], int]:
        batches = sorted(_batches_state().values(), key=lambda b: str(b.get("批次编号")), reverse=True)
        if status:
            batches = [b for b in batches if b.get("status") == status]
        total = len(batches)
        start = (page - 1) * size
        return [self._summary(b) for b in batches[start:start + size]], total

    def get_batch(self, batch_id: int) -> dict[str, Any] | None:
        for batch in _batches_state().values():
            if int(batch.get("id", 0)) == batch_id:
                return copy.deepcopy(batch)
        return None

    def get_published_batch(self) -> dict[str, Any] | None:
        batch = _find_published_batch()
        return copy.deepcopy(batch) if batch else None

    def list_archives(self) -> list[dict[str, Any]]:
        archived = [b for b in _batches_state().values() if b.get("status") == "已归档"]
        return [self._summary(b) for b in sorted(archived, key=lambda b: str(b.get("批次编号")))]

    def _summary(self, batch: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": batch.get("id"),
            "批次编号": batch.get("批次编号"),
            "status": batch.get("status"),
            "条目数": len(batch.get("items", [])),
            "冲突策略": batch.get("冲突策略"),
            "导入格式": batch.get("导入格式"),
            "remark": batch.get("remark"),
            "created_at": batch.get("created_at"),
            "published_at": batch.get("published_at"),
        }

    # ------------------------------------------------------------------
    # 加工：读取采集源接入配置 + 边界校验，连通状态与种子数量同时写入
    # ------------------------------------------------------------------
    def build_batch(self, source_ids: list[int] | None = None, remark: str | None = None) -> tuple[dict[str, Any] | None, list[str]]:
        if _find_processing_batch() is not None:
            return None, ["存在加工中的接入批次，请先发布或废弃后再加工，接入清单同一时间只允许一个完整批次"]
        snap = store.snapshot()
        try:
            sources = self._resolve_sources(source_ids)
            if not sources:
                store.restore(snap)
                return None, ["没有可加工的采集源接入配置"]
            if len(sources) > MAX_SOURCES_PER_BATCH:
                store.restore(snap)
                return None, [f"单批次采集源数量超过接入边界上限 {MAX_SOURCES_PER_BATCH}"]
            errors: list[str] = []
            items: list[dict[str, Any]] = []
            for source in sources:
                errors.extend(_validate_boundary(source))
                items.append(self._build_item(source))
            if errors:
                # 边界不通过：整体回滚，不允许留下半套清单或种子引用
                store.restore(snap)
                return None, errors
            batch = self._persist_processing_batch(items, conflict_strategy=None, import_format="build", remark=remark)
            return batch, []
        except Exception as exc:  # pragma: no cover - 加工中断兜底
            store.restore(snap)
            return None, [f"清单加工中断，已回滚: {exc}"]

    def _resolve_sources(self, source_ids: list[int] | None) -> list[dict[str, Any]]:
        rows = store.rows(SOURCE_MODULE)
        if not source_ids:
            return [dict(r) for r in rows]
        resolved: list[dict[str, Any]] = []
        for sid in source_ids:
            row = store.find(SOURCE_MODULE, int(sid))
            if row is not None:
                resolved.append(dict(row))
        return resolved

    def _build_item(self, source: dict[str, Any], *, seed_codes: list[str] | None = None,
                    version: str = SEED_VERSION_KEEP, connectivity_patch: dict[str, Any] | None = None) -> dict[str, Any]:
        """加工单源条目：连通状态与关联种子数量在同一步写入，保证成对出现。"""
        conn = _probe_connection(source)
        if seed_codes is None:
            seeds = _linked_seeds_in_order(int(source["id"]))
            seed_codes = [str(s.get("种子编号") or "") for s in seeds]
        if len(seed_codes) > MAX_SEEDS_PER_SOURCE:
            raise ValueError(f"采集源 {source.get('源标识')} 关联种子超过接入边界上限 {MAX_SEEDS_PER_SOURCE}")
        return {
            "source_id": int(source["id"]),
            "来源标识": str(source.get("源标识") or ""),
            "源名称": str(source.get("源名称") or ""),
            "协议类型": str(source.get("协议类型") or ""),
            "基础URL": str(source.get("基础URL") or ""),
            # 连通状态与种子数量同时写入，缺一不可
            "连通状态": conn["连通状态"],
            "连通消息": conn["连通消息"],
            "连通延迟": conn["连通延迟"],
            "连通校验时间": conn["checked_at"],
            "种子数量": len(seed_codes),
            "种子编号序列": list(seed_codes),
            "种子集合版本": version,
            "连通补写项": connectivity_patch,
        }

    # ------------------------------------------------------------------
    # 导入：解析外部清单 + 冲突仲裁（沿用绑定 / 重建批次）
    # ------------------------------------------------------------------
    def import_batch(self, payload: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        if _find_processing_batch() is not None:
            return None, ["存在加工中的接入批次，请先发布或废弃后再导入"]
        fmt = str(payload.get("format") or "json").strip().lower()
        if fmt not in IMPORT_FORMATS:
            return None, [f"不支持的导入格式 {fmt}，仅支持 {IMPORT_FORMATS}"]
        strategy = payload.get("conflict_strategy")
        if strategy is not None and strategy not in CONFLICT_STRATEGIES:
            return None, [f"冲突仲裁策略 {strategy} 非法，可选 {CONFLICT_STRATEGIES}"]

        snap = store.snapshot()
        try:
            codes, parse_errors = self._parse_import_content(fmt, str(payload.get("content") or ""))
            if parse_errors:
                store.restore(snap)
                return None, parse_errors
            if not codes:
                store.restore(snap)
                return None, ["导入内容未解析出任何来源标识"]
            if len(codes) > MAX_SOURCES_PER_BATCH:
                store.restore(snap)
                return None, [f"单批次采集源数量超过接入边界上限 {MAX_SOURCES_PER_BATCH}"]

            published = _find_published_batch()
            published_codes = {it["来源标识"] for it in published.get("items", [])} if published else set()
            conflicts = [code for code in codes if code in published_codes]
            if conflicts and not strategy:
                # 冲突必须仲裁处理顺序：不指定策略直接拒绝并回滚
                store.restore(snap)
                return None, [
                    f"来源标识 {conflicts} 已存在于已发布批次 {published.get('批次编号')}，"
                    f"数据版本冲突，请选择仲裁策略：{CONFLICT_STRATEGIES}"
                ]

            items: list[dict[str, Any]] = []
            errors: list[str] = []
            for code in codes:
                source = store.find_by_field(SOURCE_MODULE, "源标识", code)
                if source is None:
                    errors.append(f"来源标识 {code} 无法匹配已登记采集源，超出接入边界")
                    continue
                errors.extend(_validate_boundary(source))
                if code in published_codes:
                    items.append(self._arbitrate_conflict(source, published, strategy))  # type: ignore[arg-type]
                else:
                    items.append(self._build_item(dict(source)))
            if errors:
                store.restore(snap)
                return None, errors
            batch = self._persist_processing_batch(
                items, conflict_strategy=strategy, import_format=fmt, remark=payload.get("remark"))
            return batch, []
        except Exception as exc:
            # 导入中断：恢复快照，绝不留下半套种子引用
            store.restore(snap)
            return None, [f"导入中断，已回滚: {exc}"]

    def _parse_import_content(self, fmt: str, content: str) -> tuple[list[str], list[str]]:
        raw = content.strip()
        if not raw:
            return [], ["导入内容为空"]
        if fmt == "json":
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as exc:
                return [], [f"JSON解析失败: {exc}"]
            rows = data.get("sources", []) if isinstance(data, dict) else data
            if not isinstance(rows, list):
                return [], ["JSON结构应为来源标识数组或 {'sources': [...]}"]
            codes: list[str] = []
            for row in rows:
                if isinstance(row, str):
                    codes.append(row.strip())
                elif isinstance(row, dict):
                    code = str(row.get("来源标识") or "").strip()
                    if not code:
                        return [], ["存在缺少来源标识的导入条目"]
                    codes.append(code)
                else:
                    return [], [f"无法识别的导入条目类型: {type(row).__name__}"]
            return [c for c in codes if c], []
        # csv：首行表头必须包含来源标识列
        reader = csv.DictReader(io.StringIO(raw))
        if not reader.fieldnames or "来源标识" not in reader.fieldnames:
            return [], ["CSV表头必须包含「来源标识」列"]
        csv_codes: list[str] = []
        for row in reader:
            code = str(row.get("来源标识") or "").strip()
            if code:
                csv_codes.append(code)
        return csv_codes, []

    def _arbitrate_conflict(self, source: dict[str, Any], published: dict[str, Any],
                            strategy: str) -> dict[str, Any]:
        """同一采集源再次导入：最新连通结果与既有种子绑定同时出现时的版本冲突仲裁。"""
        old_item = next(it for it in published["items"] if it["来源标识"] == source.get("源标识"))
        old_seed_codes = list(old_item.get("种子编号序列", []))
        conn = _probe_connection(source)

        if strategy == "keep_binding":
            # 沿用绑定：保留原种子顺序，只为本次导入补充连通差异
            patch = {
                "上次连通状态": old_item.get("连通状态"),
                "本次连通状态": conn["连通状态"],
                "本次连通消息": conn["连通消息"],
                "差异": "无变化" if old_item.get("连通状态") == conn["连通状态"]
                else f"状态变化: {old_item.get('连通状态')} -> {conn['连通状态']}",
                "补写时间": conn["checked_at"],
            }
            return self._build_item(
                source, seed_codes=old_seed_codes, version=SEED_VERSION_KEEP, connectivity_patch=patch)

        # rebuild：本次种子集合成为唯一可用入口，旧种子转历史记录
        new_seeds = _linked_seeds_in_order(int(source["id"]))
        new_codes = [str(s.get("种子编号") or "") for s in new_seeds]
        retired = [code for code in old_seed_codes if code not in new_codes]
        history = store.pipeline_state("seed_migration_history") or []
        batch_no = _next_batch_no()
        for code in retired:
            history.append({
                "来源标识": str(source.get("源标识") or ""),
                "旧种子编号": code,
                "迁移到批次": batch_no,
                "迁移原因": "重建批次：本次种子集合成为唯一可用入口，旧种子转历史记录",
                "迁移时间": conn["checked_at"],
            })
        store.set_pipeline_state("seed_migration_history", history)
        return self._build_item(source, seed_codes=new_codes, version=SEED_VERSION_REBUILD)

    # ------------------------------------------------------------------
    # 批次持久化（加工中）与发布（完整性 + 缓存一致性，失败回滚）
    # ------------------------------------------------------------------
    def _persist_processing_batch(self, items: list[dict[str, Any]], *, conflict_strategy: str | None,
                                  import_format: str, remark: Any) -> dict[str, Any]:
        table = store.rows(MODULE)
        batch_id = max((int(row.get("id", 0)) for row in table), default=0) + 1
        batch = {
            "id": batch_id,
            "status": "加工中",
            "批次编号": _next_batch_no(),
            "冲突策略": conflict_strategy,
            "导入格式": import_format,
            "remark": remark or "",
            "created_at": _now(),
            "published_at": None,
            "items": items,
            "迁移记录": [],
        }
        table.append(batch)
        # 跨模块关系索引：清单条目 -> 采集源 / 种子，供种子管理按来源标识回查
        for item in items:
            store.add_relation("manifest_to_source", MODULE, batch_id, SOURCE_MODULE, item["source_id"])
            for seed in _linked_seeds_in_order(item["source_id"]):
                if str(seed.get("种子编号") or "") in item["种子编号序列"]:
                    store.add_relation("manifest_to_seed", MODULE, batch_id, SEED_MODULE, int(seed["id"]))
        batches = _batches_state()
        batches[batch["批次编号"]] = copy.deepcopy(batch)
        _save_batches_state(batches)
        return copy.deepcopy(batch)

    def publish_batch(self, batch_id: int) -> tuple[dict[str, Any] | None, str]:
        batch = next((b for b in _batches_state().values() if int(b.get("id", 0)) == batch_id), None)
        if batch is None:
            return None, f"接入批次 {batch_id} 不存在"
        if batch["status"] != "加工中":
            return None, f"接入批次 {batch['批次编号']} 状态为 {batch['status']}，仅加工中批次可发布"

        snap = store.snapshot()
        try:
            # 1) 完整性：连通状态与种子数量必须同时存在，种子引用必须全部可解析
            integrity_errors = self._integrity_check(batch)
            if integrity_errors:
                store.restore(snap)
                return None, "完整性校验失败，已回滚：" + "；".join(integrity_errors)

            # 2) 同一时间只发布一个完整批次：旧批次归档
            previous = _find_published_batch()
            table_rows = store.rows(MODULE)
            for row in table_rows:
                if row.get("status") == "已发布":
                    row["status"] = "已归档"
                    row["published_at"] = row.get("published_at") or _now()
            target_row = store.find(MODULE, batch_id)
            target_row["status"] = "已发布"
            target_row["published_at"] = _now()
            batch["status"] = "已发布"
            batch["published_at"] = target_row["published_at"]

            # 3) 重建批次的迁移记录落到批次内
            if batch.get("冲突策略") == "rebuild":
                history = store.pipeline_state("seed_migration_history") or []
                batch["迁移记录"] = [h for h in history if h.get("迁移到批次") == batch["批次编号"]]

            # 4) 写清单缓存（缓存侧），随后与落库条目逐项比对
            cache = store.pipeline_state("access_manifest_cache") or {}
            cache["current_batch"] = batch["批次编号"]
            cache["items"] = copy.deepcopy(batch["items"])
            cache["published_at"] = batch["published_at"]
            store.set_pipeline_state("access_manifest_cache", cache)
            if not self._cache_consistent(batch):
                # 落库与缓存不一致：整体回滚，批次退回加工中
                store.restore(snap)
                return None, "落库与采集源接入清单缓存不一致，已回滚，未产生半套批次"

            # 5) 跨模块衔接：种子管理按来源标识同步本批接入记录
            self._sync_seed_access_records(batch)

            # 6) 归档状态同步到批次存档
            batches = _batches_state()
            if previous is not None:
                prev = batches.get(previous["批次编号"])
                if prev is not None:
                    prev["status"] = "已归档"
            batches[batch["批次编号"]] = copy.deepcopy(batch)
            _save_batches_state(batches)
            return copy.deepcopy(batch), f"接入批次 {batch['批次编号']} 已完整发布，种子管理接入记录已同步"
        except Exception as exc:  # pragma: no cover - 发布中断兜底
            store.restore(snap)
            return None, f"发布中断，已回滚: {exc}"

    def _integrity_check(self, batch: dict[str, Any]) -> list[str]:
        errors: list[str] = []
        if not batch.get("items"):
            errors.append("批次没有任何清单条目")
        valid_seed_codes = {str(s.get("种子编号") or "") for s in store.rows(SEED_MODULE)}
        for item in batch.get("items", []):
            # 连通状态与关联种子数量必须同时写入
            if not item.get("连通状态"):
                errors.append(f"来源 {item.get('来源标识')} 缺少连通状态")
            if item.get("种子数量") is None:
                errors.append(f"来源 {item.get('来源标识')} 缺少关联种子数量")
            if item.get("连通状态") and item.get("种子数量") is not None and len(item.get("种子编号序列", [])) != item.get("种子数量"):
                errors.append(f"来源 {item.get('来源标识')} 种子数量与种子编号序列长度不一致")
            for code in item.get("种子编号序列", []):
                if code not in valid_seed_codes:
                    errors.append(f"来源 {item.get('来源标识')} 存在无法解析的种子引用 {code}（半套种子引用）")
        return errors

    def _cache_consistent(self, batch: dict[str, Any]) -> bool:
        cache = store.pipeline_state("access_manifest_cache") or {}
        if cache.get("current_batch") != batch["批次编号"]:
            return False
        cached_items = cache.get("items", [])
        if len(cached_items) != len(batch["items"]):
            return False
        for cached, persisted in zip(cached_items, batch["items"]):
            for key in ("来源标识", "连通状态", "种子数量", "种子编号序列"):
                if cached.get(key) != persisted.get(key):
                    return False
        return True

    def _sync_seed_access_records(self, batch: dict[str, Any]) -> None:
        """种子管理读取清单来源标识，把可继续使用的种子写入本批接入记录。"""
        records: dict[str, Any] = {}
        for item in batch["items"]:
            usable: list[str] = []
            for code in item.get("种子编号序列", []):
                seed = store.find_by_field(SEED_MODULE, "种子编号", code)
                # 已过期种子不属于可继续使用范围
                if seed is not None and seed.get("status") != "已过期":
                    usable.append(code)
            records[item["来源标识"]] = {
                "来源标识": item["来源标识"],
                "批次编号": batch["批次编号"],
                "种子集合版本": item.get("种子集合版本"),
                "可用种子": usable,
                "可用种子数量": len(usable),
            }
        store.set_pipeline_state("seed_access_records", {
            "批次编号": batch["批次编号"],
            "published_at": batch["published_at"],
            "records": records,
        })

    def discard_batch(self, batch_id: int) -> tuple[bool, str]:
        """废弃加工中批次：清除半成品，保证不残留半套种子引用。"""
        batches = _batches_state()
        batch = next((b for b in batches.values() if int(b.get("id", 0)) == batch_id), None)
        if batch is None:
            return False, f"接入批次 {batch_id} 不存在"
        if batch["status"] != "加工中":
            return False, f"接入批次 {batch['批次编号']} 已发布，不能废弃"
        store.delete_row(MODULE, batch_id)
        # 清理该批次建立的关系索引
        for rel_type in ("manifest_to_source", "manifest_to_seed"):
            store._relations[rel_type] = [
                r for r in store.get_relations(rel_type) if r["source_id"] != batch_id
            ]
        batches.pop(batch["批次编号"], None)
        _save_batches_state(batches)
        return True, f"加工中批次 {batch['批次编号']} 已废弃，半成品引用已清除"

    # ------------------------------------------------------------------
    # 导出 / 打包：交接记录可回查
    # ------------------------------------------------------------------
    def export_csv(self, batch_id: int) -> tuple[str, str] | None:
        batch = self.get_batch(batch_id)
        if batch is None:
            return None
        output = io.StringIO()
        output.write("\ufeff")
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(["批次编号", *ITEM_FIELDS, "连通校验时间"])
        for item in batch.get("items", []):
            writer.writerow([
                batch["批次编号"],
                item.get("来源标识", ""),
                item.get("源名称", ""),
                item.get("协议类型", ""),
                item.get("基础URL", ""),
                item.get("连通状态", ""),
                item.get("连通消息", ""),
                item.get("种子数量", ""),
                "|".join(item.get("种子编号序列", [])),
                item.get("种子集合版本", ""),
                json.dumps(item.get("连通补写项"), ensure_ascii=False) if item.get("连通补写项") else "",
                item.get("连通校验时间", ""),
            ])
        # 重建批次：导出清单追加旧种子迁移记录段
        migrations = batch.get("迁移记录", [])
        if migrations:
            writer.writerow([])
            writer.writerow(["旧种子迁移记录段"])
            writer.writerow(["来源标识", "旧种子编号", "迁移到批次", "迁移原因", "迁移时间"])
            for m in migrations:
                writer.writerow([m.get("来源标识", ""), m.get("旧种子编号", ""),
                                 m.get("迁移到批次", ""), m.get("迁移原因", ""), m.get("迁移时间", "")])
        return output.getvalue(), f"source_manifest_{batch['批次编号']}.csv"

    def download_package(self, batch_id: int) -> tuple[bytes, str] | None:
        batch = self.get_batch(batch_id)
        if batch is None:
            return None
        csv_pair = self.export_csv(batch_id)
        assert csv_pair is not None
        csv_text, csv_name = csv_pair
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(csv_name, csv_text)
            zf.writestr(f"{batch['批次编号']}.json", json.dumps(batch, ensure_ascii=False, indent=2))
            if batch.get("迁移记录"):
                zf.writestr("migration_history.json",
                            json.dumps(batch["迁移记录"], ensure_ascii=False, indent=2))
            zf.writestr("README.txt", self._handover_readme(batch))
        return buffer.getvalue(), f"source_manifest_{batch['批次编号']}.zip"

    def _handover_readme(self, batch: dict[str, Any]) -> str:
        lines = [
            "采集源接入清单交接记录",
            f"批次编号: {batch['批次编号']}",
            f"批次状态: {batch['status']}",
            f"条目数量: {len(batch.get('items', []))}",
            f"冲突仲裁策略: {batch.get('冲突策略') or '无'}",
            f"发布时间: {batch.get('published_at') or '未发布'}",
            "复核要点: 每条来源标识均同时包含连通状态与关联种子数量，可逐项核对。",
        ]
        if batch.get("迁移记录"):
            lines.append("本批为重建批次，旧种子迁移记录见 migration_history.json。")
        return "\n".join(lines) + "\n"
