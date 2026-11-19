"""采集源接入清单：加工、边界校验、冲突仲裁、发布回滚、跨模块同步与交接导出测试。"""
from __future__ import annotations

import csv
import io
import json
import zipfile

import pytest

from app.services.crawl_source import CrawlSourceService
from app.services.seed_config import SeedConfigService
from app.services.source_manifest import (
    CONN_FAIL,
    CONN_OK,
    MAX_SOURCES_PER_BATCH,
    SEED_VERSION_KEEP,
    SEED_VERSION_REBUILD,
    SourceManifestService,
)
from app.store import store


def setup_function() -> None:
    store._tables.clear()
    store._pipeline_states.clear()
    store._relations.clear()
    # 重新装载种子数据与关系索引，保证每个用例从同一基线出发
    from app.seed import SEED_ROWS
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    store._build_relations()


def _publish(svc: SourceManifestService, source_ids: list[int]):
    batch, errors = svc.build_batch(source_ids=source_ids)
    assert not errors, errors
    published, msg = svc.publish_batch(batch["id"])
    assert published is not None, msg
    return published


# ------------------------------------------------------------------
# 1. 加工：读取接入配置，连通状态与关联种子数量同时写入清单
# ------------------------------------------------------------------

def test_build_batch_writes_connectivity_and_seed_count_together() -> None:
    svc = SourceManifestService()
    batch, errors = svc.build_batch(source_ids=[1])
    assert not errors
    item = batch["items"][0]
    # 连通状态与种子数量成对出现
    assert item["连通状态"] in (CONN_OK, CONN_FAIL)
    assert "连通消息" in item and item["连通校验时间"]
    assert item["种子数量"] == 1
    assert item["种子编号序列"] == ["SEED-E001"]
    assert len(item["种子编号序列"]) == item["种子数量"]


def test_full_build_boundary_rejects_wss_source() -> None:
    """源6使用 wss 协议，超出 http/https 接入边界，整批拒绝并回滚。"""
    svc = SourceManifestService()
    batch, errors = svc.build_batch()
    assert batch is None
    assert any("接入边界" in e for e in errors)
    # 回滚后没有留下加工中批次
    assert svc.list_batches()[0] == []


def test_unknown_source_id_skipped_gracefully() -> None:
    svc = SourceManifestService()
    batch, errors = svc.build_batch(source_ids=[9999])
    assert batch is None
    assert any("没有可加工" in e for e in errors)


# ------------------------------------------------------------------
# 2. 单一完整批次：加工中互斥
# ------------------------------------------------------------------

def test_only_one_processing_batch_at_a_time() -> None:
    svc = SourceManifestService()
    first, errors = svc.build_batch(source_ids=[1])
    assert not errors
    second, errors2 = svc.build_batch(source_ids=[2])
    assert second is None
    assert any("一个完整批次" in e for e in errors2)
    # 废弃后可重新加工
    ok, msg = svc.discard_batch(first["id"])
    assert ok, msg
    third, errors3 = svc.build_batch(source_ids=[2])
    assert not errors3 and third is not None


# ------------------------------------------------------------------
# 3. 发布：完整性校验、单发布批次、旧批次归档
# ------------------------------------------------------------------

def test_publish_only_one_complete_batch_and_archives_previous() -> None:
    svc = SourceManifestService()
    first = _publish(svc, [1])
    second = _publish(svc, [2])
    items, total = svc.list_batches()
    assert total == 2
    statuses = {b["批次编号"]: b["status"] for b in items}
    assert statuses[first["批次编号"]] == "已归档"
    assert statuses[second["批次编号"]] == "已发布"
    assert svc.get_published_batch()["批次编号"] == second["批次编号"]


def test_cannot_publish_nonexistent_or_published_batch() -> None:
    svc = SourceManifestService()
    batch, _ = svc.build_batch(source_ids=[1])
    svc.publish_batch(batch["id"])
    again, msg = svc.publish_batch(batch["id"])
    assert again is None and "仅加工中批次可发布" in msg
    missing, msg2 = svc.publish_batch(9999)
    assert missing is None and "不存在" in msg2


def test_integrity_check_blocks_half_seed_reference() -> None:
    """篡改条目制造无法解析的种子引用，完整性校验拦截并回滚。"""
    svc = SourceManifestService()
    batch, _ = svc.build_batch(source_ids=[1])
    batch["items"][0]["种子编号序列"] = ["SEED-E001", "SEED-GHOST-999"]
    batch["items"][0]["种子数量"] = 2
    # 同时篡改落库表与批次存档，模拟条目已损坏的半套引用
    store.find("source_manifest", batch["id"])["items"] = batch["items"]
    batches = store.pipeline_state("access_manifest_batches")
    batches[batch["批次编号"]]["items"] = batch["items"]
    store.set_pipeline_state("access_manifest_batches", batches)
    published, msg = svc.publish_batch(batch["id"])
    assert published is None
    assert "完整性校验失败" in msg and "SEED-GHOST-999" in msg
    # 回滚后仍为加工中，且没有已发布批次
    assert svc.get_published_batch() is None
    assert store.find("source_manifest", batch["id"])["status"] == "加工中"


# ------------------------------------------------------------------
# 4. 落库与缓存不一致时回滚
# ------------------------------------------------------------------

def test_cache_inconsistency_rolls_back_publish() -> None:
    svc = SourceManifestService()
    batch, _ = svc.build_batch(source_ids=[1])
    # 破坏缓存写入逻辑：让缓存条目与落库条目出现种子数量分叉
    original = svc._cache_consistent
    svc._cache_consistent = lambda b: False  # type: ignore[method-assign]
    try:
        published, msg = svc.publish_batch(batch["id"])
        assert published is None
        assert "缓存不一致" in msg
    finally:
        svc._cache_consistent = original  # type: ignore[method-assign]
    # 回滚后无发布、无缓存指向、种子接入记录未写入
    assert svc.get_published_batch() is None
    assert (store.pipeline_state("access_manifest_cache") or {}).get("current_batch") is None
    assert not (store.pipeline_state("seed_access_records") or {})


# ------------------------------------------------------------------
# 5. 跨模块衔接：种子管理读取来源标识，同步本批接入记录
# ------------------------------------------------------------------

def test_seed_management_syncs_access_records_after_publish() -> None:
    svc = SourceManifestService()
    _publish(svc, [1, 2])
    seed_svc = SeedConfigService()
    state = seed_svc.get_access_records()
    assert set(state["records"]) == {"SRC-ELEC-001", "SRC-NEWS-002"}
    result = seed_svc.get_usable_seeds_by_source("SRC-ELEC-001")
    assert result["ok"] is True
    assert result["可用种子数量"] == 1
    assert result["items"][0]["种子编号"] == "SEED-E001"
    # 未在清单中的来源标识查不到可用种子
    missing = seed_svc.get_usable_seeds_by_source("SRC-UNKNOWN")
    assert missing["ok"] is False


def test_expired_seed_excluded_from_usable_records() -> None:
    """已过期种子不属于可继续使用范围（源6种子为已过期）。"""
    svc = SourceManifestService()
    # 源6协议 wss 不通过边界校验，改为临时修正URL后再加工
    source6 = store.find("crawl_source", 6)
    source6["基础URL"] = "https://fin-f.example.com/quote"
    _publish(svc, [6])
    seed_svc = SeedConfigService()
    result = seed_svc.get_usable_seeds_by_source("SRC-FIN-006")
    assert result["ok"] is True
    # 清单保留种子数量（含已过期），但接入记录里可用种子被剔除
    batch_item = svc.get_published_batch()["items"][0]
    assert batch_item["种子数量"] == 1
    assert result["可用种子数量"] == 0


# ------------------------------------------------------------------
# 6. 导入 + 冲突仲裁
# ------------------------------------------------------------------

def test_import_json_creates_processing_batch() -> None:
    svc = SourceManifestService()
    payload = {"format": "json", "content": json.dumps({"sources": [{"来源标识": "SRC-ELEC-001"}]})}
    batch, errors = svc.import_batch(payload)
    assert not errors
    assert batch["导入格式"] == "json"
    assert batch["items"][0]["来源标识"] == "SRC-ELEC-001"


def test_import_csv_requires_source_code_header() -> None:
    svc = SourceManifestService()
    bad, errors = svc.import_batch({"format": "csv", "content": "名称,值\nx,1"})
    assert bad is None and any("来源标识" in e for e in errors)
    ok_payload = {"format": "csv", "content": "来源标识,备注\nSRC-NEWS-002,导入\n"}
    batch, errors2 = svc.import_batch(ok_payload)
    assert not errors2
    assert batch["items"][0]["来源标识"] == "SRC-NEWS-002"


def test_import_conflict_without_strategy_is_rejected() -> None:
    svc = SourceManifestService()
    _publish(svc, [1])
    batch, errors = svc.import_batch({"format": "json", "content": json.dumps(["SRC-ELEC-001"])})
    assert batch is None
    assert any("数据版本冲突" in e and "keep_binding" in e for e in errors)


def test_conflict_keep_binding_preserves_seed_order_and_patches_connectivity() -> None:
    svc = SourceManifestService()
    published = _publish(svc, [1])
    old_item = published["items"][0]
    # 给源1再绑定一个新种子，模拟"最新连通结果与既有种子绑定同时出现"
    new_seed, missing = SeedConfigService().create_entry({
        "种子编号": "SEED-E001B",
        "目标URL模式": "https://mall-a.example.com/new?page={page}",
        "列表页XPath": "//div[@class='new']",
    })
    assert not missing
    SeedConfigService().bind_source(new_seed["id"], 1)

    batch, errors = svc.import_batch({
        "format": "json",
        "conflict_strategy": "keep_binding",
        "content": json.dumps(["SRC-ELEC-001"]),
    })
    assert not errors
    item = batch["items"][0]
    # 沿用绑定：保留原种子顺序，新种子不进入，只为本次导入补充连通差异
    assert item["种子集合版本"] == SEED_VERSION_KEEP
    assert item["种子编号序列"] == old_item["种子编号序列"] == ["SEED-E001"]
    assert item["连通补写项"] is not None
    assert item["连通补写项"]["上次连通状态"] == old_item["连通状态"]
    assert item["连通补写项"]["本次连通状态"] == item["连通状态"]


def test_conflict_rebuild_replaces_seed_set_and_migrates_old_seed() -> None:
    svc = SourceManifestService()
    _publish(svc, [1])  # 原种子 SEED-E001
    # 重建前解绑旧种子、绑定新种子集合
    store._relations["source_to_seed"] = [
        r for r in store.get_relations("source_to_seed")
        if not (r["source_module"] == "crawl_source" and r["source_id"] == 1)
    ]
    new_seed, _ = SeedConfigService().create_entry({
        "种子编号": "SEED-E009",
        "目标URL模式": "https://mall-a.example.com/rebuild?page={page}",
        "列表页XPath": "//div[@id='rebuild']",
    })
    SeedConfigService().bind_source(new_seed["id"], 1)

    batch, errors = svc.import_batch({
        "format": "json",
        "conflict_strategy": "rebuild",
        "content": json.dumps(["SRC-ELEC-001"]),
    })
    assert not errors
    item = batch["items"][0]
    # 本次种子集合成为唯一可用入口
    assert item["种子集合版本"] == SEED_VERSION_REBUILD
    assert item["种子编号序列"] == ["SEED-E009"]

    published, msg = svc.publish_batch(batch["id"])
    assert published is not None, msg
    # 旧种子转历史记录，且批次内含迁移记录
    migrations = published["迁移记录"]
    assert len(migrations) == 1
    assert migrations[0]["旧种子编号"] == "SEED-E001"
    assert migrations[0]["来源标识"] == "SRC-ELEC-001"
    history = SeedConfigService().get_migration_history()
    assert any(h["旧种子编号"] == "SEED-E001" for h in history)
    # 种子管理接入记录只暴露新集合
    usable = SeedConfigService().get_usable_seeds_by_source("SRC-ELEC-001")
    assert [s["种子编号"] for s in usable["items"]] == ["SEED-E009"]


def test_import_interruption_leaves_no_half_batch() -> None:
    """导入内容引用不存在的来源标识时整体回滚，无加工中批次残留。"""
    svc = SourceManifestService()
    before = svc.list_batches()[1]
    batch, errors = svc.import_batch({"format": "json", "content": json.dumps(["SRC-GHOST-X"])})
    assert batch is None and errors
    assert svc.list_batches()[1] == before
    assert "source_manifest" not in store._tables or not store.rows("source_manifest")


# ------------------------------------------------------------------
# 7. 导出 / 打包交接记录
# ------------------------------------------------------------------

def test_export_csv_contains_all_three_review_columns() -> None:
    svc = SourceManifestService()
    published = _publish(svc, [1, 2])
    pair = svc.export_csv(published["id"])
    assert pair is not None
    text, filename = pair
    assert filename.endswith(".csv")
    rows = list(csv.reader(io.StringIO(text.lstrip("\ufeff"))))
    header = rows[0]
    for column in ("来源标识", "连通状态", "种子数量"):
        assert column in header
    data_rows = [r for r in rows[1:] if r]
    codes = {r[header.index("来源标识")] for r in data_rows}
    assert codes == {"SRC-ELEC-001", "SRC-NEWS-002"}
    for r in data_rows:
        assert r[header.index("连通状态")] in (CONN_OK, CONN_FAIL)
        assert int(r[header.index("种子数量")]) >= 1


def test_rebuild_export_contains_migration_section() -> None:
    svc = SourceManifestService()
    _publish(svc, [1])
    store._relations["source_to_seed"] = [
        r for r in store.get_relations("source_to_seed")
        if not (r["source_module"] == "crawl_source" and r["source_id"] == 1)
    ]
    new_seed, _ = SeedConfigService().create_entry({
        "种子编号": "SEED-E010",
        "目标URL模式": "https://mall-a.example.com/r2?page={page}",
        "列表页XPath": "//div[@id='r2']",
    })
    SeedConfigService().bind_source(new_seed["id"], 1)
    batch, _ = svc.import_batch({
        "format": "json", "conflict_strategy": "rebuild",
        "content": json.dumps(["SRC-ELEC-001"]),
    })
    published, _ = svc.publish_batch(batch["id"])
    text, _ = svc.export_csv(published["id"])
    assert "旧种子迁移记录段" in text
    assert "SEED-E001" in text


def test_download_package_is_zip_with_handover_files() -> None:
    svc = SourceManifestService()
    published = _publish(svc, [1])
    pair = svc.download_package(published["id"])
    assert pair is not None
    content, filename = pair
    assert filename.endswith(".zip")
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        names = zf.namelist()
        assert any(n.endswith(".csv") for n in names)
        assert any(n.endswith(".json") for n in names)
        readme = zf.read("README.txt").decode("utf-8")
        assert published["批次编号"] in readme and "连通状态" in readme


def test_archives_are_reviewable() -> None:
    svc = SourceManifestService()
    first = _publish(svc, [1])
    _publish(svc, [2])
    archives = svc.list_archives()
    assert any(a["批次编号"] == first["批次编号"] and a["status"] == "已归档" for a in archives)
    detail = svc.get_batch(first["id"])
    assert detail["status"] == "已归档" and detail["items"]


# ------------------------------------------------------------------
# 8. 连续两个已有采集源导出，逐项核对连通状态、种子数量、来源标识
# ------------------------------------------------------------------

def test_two_existing_sources_export_itemwise_correspondence() -> None:
    svc = SourceManifestService()
    published = _publish(svc, [1, 2])
    text, _ = svc.export_csv(published["id"])
    rows = list(csv.DictReader(io.StringIO(text.lstrip("\ufeff").split("\n\n")[0])))
    by_code = {r["来源标识"]: r for r in rows}
    expected = {
        "SRC-ELEC-001": ("可达", 1),
        "SRC-NEWS-002": ("可达", 1),
    }
    for code, (conn, seed_count) in expected.items():
        row = by_code[code]
        assert row["连通状态"] == conn
        assert int(row["种子数量"]) == seed_count
        assert row["来源标识"] == code
    # 与采集源管理侧的连接校验缓存、关系索引保持一致
    for source_id, code in ((1, "SRC-ELEC-001"), (2, "SRC-NEWS-002")):
        cache = store.pipeline_state("source_validation_cache")[str(source_id)]
        assert cache["reachable"] is True
        rels = store.get_relations("source_to_seed", source_module="crawl_source", source_id=source_id)
        assert len(rels) == seed_count if code == "SRC-ELEC-001" else len(rels) == 1
