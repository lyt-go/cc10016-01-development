"""采集源接入清单接口：导入、加工、仲裁发布、导出与打包下载、交接回查。"""
from __future__ import annotations

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from app.schemas import ActionResult, PageResult
from app.services.source_manifest import CONFLICT_STRATEGIES, IMPORT_FORMATS, SourceManifestService

router = APIRouter(prefix="/api/source_manifest", tags=["采集源接入清单"])

service = SourceManifestService()


@router.get("/batches", response_model=PageResult[dict])
def list_batches(
    status: str | None = Query(default=None, description="加工中、已发布、已归档"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """分页查看接入批次（交接记录）。"""
    items, total = service.list_batches(status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/published", response_model=dict)
def get_published_batch() -> dict:
    """读取当前已发布的完整接入批次；未发布时返回空批次说明。"""
    batch = service.get_published_batch()
    if batch is None:
        return {"ok": False, "message": "尚无已发布的采集源接入清单批次"}
    return {"ok": True, "entry": batch}


@router.get("/archives")
def list_archives() -> dict:
    """回查历史归档批次。"""
    return {"items": service.list_archives()}


@router.get("/meta/options")
def conflict_options() -> dict:
    """前端取导入格式与冲突仲裁策略枚举。"""
    return {"import_formats": IMPORT_FORMATS, "conflict_strategies": CONFLICT_STRATEGIES}


@router.get("/{batch_id}", response_model=dict)
def get_batch(batch_id: int) -> dict:
    """读取单批次明细（含每条来源的连通状态、种子数量、迁移记录）。"""
    batch = service.get_batch(batch_id)
    if batch is None:
        return {"ok": False, "message": f"接入批次 {batch_id} 不存在"}
    return {"ok": True, "entry": batch}


@router.post("/build", response_model=ActionResult)
def build_batch(payload: dict[str, object]) -> ActionResult:
    """读取采集源接入配置并校验接入边界，加工一个接入清单批次。"""
    raw_ids = payload.get("source_ids") or []
    try:
        source_ids = [int(v) for v in raw_ids] if isinstance(raw_ids, list) else None
    except (ValueError, TypeError):
        return ActionResult(ok=False, message="source_ids 必须是整数数组")
    batch, errors = service.build_batch(source_ids=source_ids, remark=payload.get("remark"))
    if errors:
        return ActionResult(ok=False, message="；".join(errors))
    return ActionResult(ok=True, message="接入清单加工完成（加工中），连通状态与种子数量已同时写入", entry=batch)


@router.post("/import", response_model=ActionResult)
def import_batch(payload: dict[str, object]) -> ActionResult:
    """导入外部接入清单；同源再次导入按冲突策略仲裁（沿用绑定 / 重建批次）。"""
    batch, errors = service.import_batch(payload)
    if errors:
        return ActionResult(ok=False, message="；".join(errors))
    return ActionResult(ok=True, message="接入清单导入完成（加工中），冲突已按仲裁策略定序", entry=batch)


@router.post("/{batch_id}/publish", response_model=ActionResult)
def publish_batch(batch_id: int) -> ActionResult:
    """发布完整批次；完整性或缓存一致性不通过时整体回滚。"""
    batch, message = service.publish_batch(batch_id)
    if batch is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=batch)


@router.post("/{batch_id}/discard", response_model=ActionResult)
def discard_batch(batch_id: int) -> ActionResult:
    """废弃加工中批次，清除半成品引用。"""
    ok, message = service.discard_batch(batch_id)
    return ActionResult(ok=ok, message=message)


@router.get("/{batch_id}/export")
def export_batch(batch_id: int) -> StreamingResponse:
    """导出批次 CSV（含连通状态、种子数量、来源标识及迁移记录段）。"""
    pair = service.export_csv(batch_id)
    if pair is None:
        return StreamingResponse(iter(["接入批次不存在"]), media_type="text/plain; charset=utf-8",
                                 status_code=404)
    csv_text, filename = pair
    return StreamingResponse(
        iter([csv_text]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@router.get("/{batch_id}/download")
def download_package(batch_id: int) -> StreamingResponse:
    """下载打包后的交接记录（CSV + 批次JSON + 迁移记录 + 复核说明），可回查。"""
    pair = service.download_package(batch_id)
    if pair is None:
        return StreamingResponse(iter(["接入批次不存在"]), media_type="text/plain; charset=utf-8",
                                 status_code=404)
    content, filename = pair
    return StreamingResponse(
        iter([content]), media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'})
