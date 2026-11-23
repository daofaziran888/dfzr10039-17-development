"""归档导出测试：发布门槛、固定下载、校验拒收、批次存储同步与补采后复现。"""
from __future__ import annotations

import io
import json
import zipfile

import pytest

from app.models import digest_rows
from app.services.archive_export import (
    MANIFEST_NAME, ROWS_NAME, ArchiveExportError, archive_export,
)
from app.store import store

REQUESTER = "RECV-20261123"

TOUCHED_TABLES = [
    "batch_store", "batch_rows:B12", "batch_rows:B13",
    "archive_export", "archive_export_package", "export_task",
]


@pytest.fixture(autouse=True)
def isolated_tables():
    """归档与会改状态的批次表在用例间互相隔离。"""
    snapshot = {
        name: [dict(row) for row in store.rows(name)]
        for name in TOUCHED_TABLES
    }
    yield
    for name, rows in snapshot.items():
        store.rows(name).clear()
        store.rows(name).extend(rows)


def _b12_rows() -> list[dict[str, str]]:
    return [dict(row) for row in store.rows("batch_rows:B12")]


def _b13_rows() -> list[dict[str, str]]:
    return [dict(row) for row in store.rows("batch_rows:B13")]


def _b13_rechecked_rows() -> list[dict[str, Any]]:
    """B13 补采完成后的行：字段齐全。"""
    return [
        {"source_url": "https://news.example.com/b13/article/201",
         "extract_rule_id": "CONT-CRAW-JOB-B13",
         "data": {"title": "商圈客流回升", "author": "商业频道",
                  "body_hash": "sha256:4b6d1f83", "publish_date": "2026-11-22"}},
        {"source_url": "https://news.example.com/b13/article/202",
         "extract_rule_id": "CONT-CRAW-JOB-B13",
         "data": {"title": "补采完成页面", "author": "商业频道",
                  "body_hash": "sha256:a1c2e904", "publish_date": "2026-11-23"}},
    ]


def _batch(batch_id: str) -> dict:
    return next(row for row in store.rows("batch_store") if row["批次编号"] == batch_id)


def _publish(batches: list[str] | None = None) -> dict:
    return archive_export.publish(
        requester=REQUESTER, batch_ids=batches or ["B12", "B13"], note="接收核查")


def test_only_complete_batches_are_published() -> None:
    package = _publish(["B12", "B13"])

    assert [item["batch_id"] for item in package["published_batches"]] == ["B12"]
    blocked = {item["batch_id"]: item for item in package["blocked_batches"]}
    assert "B13" in blocked
    assert "校验失败" in blocked["B13"]["reason"]

    _, raw = archive_export.download(package["export_id"])
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        manifest = json.loads(zf.read(MANIFEST_NAME))
        csv_text = zf.read(ROWS_NAME).decode()
    assert [item["batch_id"] for item in manifest["published"]] == ["B12"]
    assert "B13" not in csv_text
    assert len(csv_text.strip().splitlines()) == 4  # 表头 + B12 三行


def test_recheck_rejects_still_incomplete_rows() -> None:
    """补采结果仍缺字段时，不能把 B13 标记为完成。"""
    with pytest.raises(ArchiveExportError) as exc:
        archive_export.complete_recheck("B13", _b13_rows(), operator="ops")
    assert exc.value.status_code == 422
    assert "字段为空" in exc.value.message
    assert _batch("B13")["status"] == "校验失败"


def test_all_blocked_batch_list_is_rejected() -> None:
    with pytest.raises(ArchiveExportError) as exc:
        archive_export.publish(requester=REQUESTER, batch_ids=["B13"])
    assert exc.value.status_code == 409
    assert "B13" in exc.value.message


def test_download_is_stable_and_scoped_to_requester() -> None:
    package = _publish(["B12"])
    first = archive_export.download(package["export_id"])[1]
    second = archive_export.download(package["export_id"])[1]

    assert first == second, "重复下载必须得到同一份内容"
    assert package["package_digest"] == digest_of(first)

    with pytest.raises(ArchiveExportError) as exc:
        archive_export.download(package["export_id"], requester="OTHER")
    assert exc.value.status_code == 403


def test_verify_accepts_package_and_syncs_batch_store() -> None:
    package = _publish(["B12", "B13"])
    _, raw = archive_export.download(package["export_id"])

    result = archive_export.verify(package["export_id"], raw)

    assert result["result"] == "通过", result["failures"]
    batch = _batch("B12")
    assert batch["归档版本"] == f"1.0/{package['export_id']}"
    assert batch["归档来源"] == f"data_archive:{package['export_id']}"
    assert batch["归档摘要"] == package["published_batches"][0]["content_digest"]
    assert "通过" in batch["校验结果"]
    assert _batch("B13")["归档版本"] == ""
    assert _batch("B13")["status"] == "校验失败"

    export_rows = [row for row in store.rows("export_task")
                   if row["记录编号"] == package["export_id"]]
    assert export_rows and export_rows[0]["status"] == "已完成"


def test_batch_store_recovers_rows_from_package() -> None:
    """批次存储按包内版本、来源和摘要恢复内容，并登记一致的核对结果。"""
    package = _publish(["B12"])
    _, raw = archive_export.download(package["export_id"])

    result = archive_export.verify(package["export_id"], raw)
    synced = result["batch_sync"]["batches"][0]

    assert synced["batch_id"] == "B12"
    assert synced["版本"] == f"1.0/{package['export_id']}"
    assert synced["来源"] == f"data_archive:{package['export_id']}"
    assert synced["恢复摘要"] == synced["内容摘要"]
    assert synced["摘要一致"] is True
    assert synced["归档版本"] == synced["版本"]
    assert synced["归档来源"] == synced["来源"]
    assert synced["校验结果"].startswith("通过")


def test_verify_rejects_tampered_row() -> None:
    package = _publish(["B12"])
    _, raw = archive_export.download(package["export_id"])

    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        files = {name: zf.read(name) for name in zf.namelist()}
    csv_text = files[ROWS_NAME].decode()
    lines = csv_text.splitlines()
    lines[1] = lines[1].replace("城市轨道交通新线开通", "被篡改的标题")
    files[ROWS_NAME] = ("\n".join(lines) + "\n").encode()

    tampered = io.BytesIO()
    with zipfile.ZipFile(tampered, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, payload in files.items():
            zf.writestr(name, payload)

    result = archive_export.verify(package["export_id"], tampered.getvalue())

    assert result["result"] == "拒收"
    failed = [item["检查项"] for item in result["checks"] if item["结果"] == "拒收"]
    assert "批次行文件摘要" in failed
    # 拒收结果要留痕，批次存储不得被标记为通过
    assert _batch("B12")["校验结果"].startswith("拒收")
    assert any("摘要" in failure for failure in result["failures"])


def test_published_package_survives_later_batches() -> None:
    first = _publish(["B12"])
    _, original = archive_export.download(first["export_id"])
    original_manifest = archive_export.public_view(
        archive_export._find_export(first["export_id"]), include_manifest=True)["manifest"]

    archive_export.complete_recheck("B13", _b13_rechecked_rows(), operator="ops")
    second = _publish(["B12", "B13"])

    assert [item["batch_id"] for item in second["published_batches"]] == ["B12", "B13"]
    # 原已发布包内容与适用范围保持不变
    _, again = archive_export.download(first["export_id"])
    assert again == original
    assert archive_export.verify(first["export_id"], again)["result"] == "通过"
    assert archive_export.public_view(
        archive_export._find_export(first["export_id"]), include_manifest=True
    )["manifest"]["publishable_batches"] == original_manifest["publishable_batches"] == ["B12"]


def test_request_state_tracks_pending_scope() -> None:
    _publish(["B12", "B13"])
    state = archive_export.request_state(REQUESTER)

    pending = {item["batch_id"]: item for item in state["pending_batches"]}
    assert "B13" in pending
    assert pending["B13"]["current_status"] == "校验失败"
    assert pending["B13"]["ready_for_export"] is False
    assert state["exported"]

    archive_export.complete_recheck("B13", _b13_rechecked_rows(), operator="ops")
    state = archive_export.request_state(REQUESTER)
    pending = {item["batch_id"]: item for item in state["pending_batches"]}
    assert pending["B13"]["ready_for_export"] is True


class _FakeRequest:
    def __init__(self, headers: dict[str, str] | None = None) -> None:
        self.headers = headers or {}


def test_download_supports_range_resume() -> None:
    """下载中断后按 Range 续传，拼回来必须仍是同一份已确认数据。"""
    from app.routers.resource import download_archive_export

    package = _publish(["B12"])
    full = download_archive_export(package["export_id"], _FakeRequest(), requester="")
    total = len(full.body)

    head = download_archive_export(
        package["export_id"], _FakeRequest({"range": "bytes=0-99"}), requester="")
    tail = download_archive_export(
        package["export_id"], _FakeRequest({"range": f"bytes=100-{total - 1}"}),
        requester="")

    assert head.status_code == 206 and tail.status_code == 206
    assert head.body + tail.body == full.body
    assert head.headers["content-range"] == f"bytes 0-99/{total}"
    assert head.headers["etag"] == f'"{package["package_digest"]}"'


def digest_of(raw: bytes) -> str:
    from app.models import digest_bytes

    return digest_bytes(raw)
