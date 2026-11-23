"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse

from app.schemas import (
    ActionPayload, ActionResult, ArchiveExportPayload, BatchRecheckPayload,
    EntryPayload, PageResult,
)
from app.services import archive_export_service, service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


# ---------------------------------------------------------------------------
# 归档导出：固定路径必须注册在 /{module} 动态路由之前
# ---------------------------------------------------------------------------

def _export_error(exc: Exception) -> HTTPException:
    return HTTPException(
        status_code=getattr(exc, "status_code", 400),
        detail=getattr(exc, "message", str(exc)),
    )


@router.post("/data_archive/exports")
def publish_archive_export(payload: ArchiveExportPayload) -> dict[str, Any]:
    """按归档清单打包已校验通过的批次，生成可校验的归档导出包。"""
    service = archive_export_service()
    try:
        return service.publish(
            requester=payload.requester.strip(),
            batch_ids=payload.batches,
            note=payload.note,
        )
    except Exception as exc:  # 领域错误统一转成 HTTP 状态
        raise _export_error(exc)


@router.get("/data_archive/exports")
def list_archive_exports(
    requester: str | None = Query(default=None),
) -> dict[str, Any]:
    """归档导出记录；带 requester 时只看该发起批次的导出。"""
    items = archive_export_service().list_exports(requester=(requester or "").strip())
    return {"items": items, "total": len(items)}


@router.get("/data_archive/exports/request")
def get_archive_export_request(
    requester: str = Query(...),
) -> dict[str, Any]:
    """按发起批次查询已发布、已校验和尚未完成的范围。"""
    try:
        return archive_export_service().request_state(requester.strip())
    except Exception as exc:
        raise _export_error(exc)


@router.get("/data_archive/exports/ledger")
def get_archive_export_ledger(
    requester: str | None = Query(default=None),
) -> dict[str, Any]:
    """可回查的归档导出台账（发布 / 下载 / 校验 / 同步）。"""
    items = archive_export_service().ledger(requester=(requester or "").strip())
    return {"items": items, "total": len(items)}


@router.get("/data_archive/exports/{export_id}")
def get_archive_export(export_id: str) -> dict[str, Any]:
    service = archive_export_service()
    detail = service._find_export(export_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"归档包 {export_id} 不存在")
    return service.public_view(detail, include_manifest=True)


@router.get("/data_archive/exports/{export_id}/download")
def download_archive_export(
    export_id: str,
    request: Request,
    requester: str | None = Query(default=None),
) -> Response:
    """下载已确认的归档包；重复下载返回同一份内容，支持断点续传。"""
    try:
        detail, archive = archive_export_service().download(
            export_id, requester=(requester or "").strip())
    except Exception as exc:
        raise _export_error(exc)

    headers = {
        "Content-Disposition": f'attachment; filename="{detail["filename"]}"',
        "ETag": f'"{detail["package_digest"]}"',
        "Accept-Ranges": "bytes",
        "X-Archive-Export": export_id,
        "X-Archive-Digest": detail["package_digest"],
    }
    total = len(archive)
    range_header = request.headers.get("range", "")
    if range_header.startswith("bytes="):
        raw_start, _, raw_end = range_header[6:].partition("-")
        try:
            start = int(raw_start) if raw_start else 0
            end = int(raw_end) if raw_end else total - 1
        except ValueError:
            start, end = 0, total - 1
        if 0 <= start <= end < total:
            chunk = archive[start:end + 1]
            headers["Content-Range"] = f"bytes {start}-{end}/{total}"
            headers["Content-Length"] = str(len(chunk))
            return Response(content=chunk, status_code=206,
                            media_type=detail["media_type"], headers=headers)

    headers["Content-Length"] = str(total)
    return Response(content=archive, media_type=detail["media_type"],
                    headers=headers)


@router.post("/data_archive/exports/{export_id}/verify")
async def verify_archive_export(
    export_id: str,
    request: Request,
) -> dict[str, Any]:
    """核对收到的压缩包：文件清单、摘要、归档版本/来源与批次行。"""
    archive_bytes = await request.body()
    if not archive_bytes:
        raise HTTPException(status_code=422, detail="未收到归档包内容")
    try:
        return archive_export_service().verify(export_id, archive_bytes)
    except Exception as exc:
        raise _export_error(exc)


@router.post("/data_archive/batches/{batch_id}/recheck")
def recheck_batch(
    batch_id: str,
    payload: BatchRecheckPayload,
) -> dict[str, Any]:
    """补采完成后提交批次行：重新校验通过后该批次才可再次归档导出。"""
    try:
        return archive_export_service().complete_recheck(
            batch_id, payload.rows, operator=payload.operator.strip())
    except Exception as exc:
        raise _export_error(exc)


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)
