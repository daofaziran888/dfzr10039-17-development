"""归档导出：把已发布批次按归档清单打成交付包，并留下可回查的校验结果。

业务边界：
- 只有批次存储中「校验通过」且行数据仍可复现的批次才允许发布；
  校验失败 / 补采中的批次会在清单里标成 blocked，不会出现在压缩包中。
- 发布过的包一旦封存就不再改写：manifest、data.csv、checksums.sha256 与
  压缩包字节都按导出编号登记，重复下载返回同一份内容。
- 发布 / 下载 / 校验 / 同步批次存储都会登记导出台账，台账支持按发起方
  （requester）查询，未完成的范围据此可追踪。
"""
from __future__ import annotations

import csv
import io
import json
import zipfile
from datetime import datetime, timezone
from typing import Any

from app.models import canonical_rows, digest_bytes, digest_rows
from app.services.batch_store import BatchStoreService
from app.store import store

PACKAGE_VERSION = "1.0"
MANIFEST_NAME = "manifest.json"
ROWS_NAME = "data.csv"
CHECKSUM_NAME = "checksums.sha256"
REQUIRED_SCHEMA_VERSION = "v1"

# 归档包内的批次行字段（批次存储的列 + 行内字段）
ROW_META_FIELDS = ["source_url", "extract_rule_id", "data"]

# 接收方可以直接核对的边界：压缩包必须完全由这三份文件组成
EXPECTED_FILES = sorted([MANIFEST_NAME, ROWS_NAME, CHECKSUM_NAME])


class ArchiveExportError(Exception):
    """归档导出业务错误：带 HTTP 状态码和可读原因。"""

    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code


# ---------------------------------------------------------------------------
# 批次行存取：归档包里的 data.csv 是唯一可交付形态
# ---------------------------------------------------------------------------

def batch_rows(batch_id: str) -> list[dict[str, str]]:
    """读取批次的已封存行数据，返回可直接写入 CSV 的规整行。"""
    rows = store.rows(f"batch_rows:{batch_id}")
    return canonical_rows(rows)


def _csv_bytes(rows: list[dict[str, str]]) -> bytes:
    output = io.StringIO()
    writer = csv.DictWriter(
        output, fieldnames=ROW_META_FIELDS, lineterminator="\n",
        extrasaction="ignore",
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return output.getvalue().encode("utf-8")


def _csv_to_records(raw: bytes) -> list[dict[str, str]]:
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8")))
    missing = [field for field in ROW_META_FIELDS if field not in (reader.fieldnames or [])]
    if missing:
        raise ArchiveExportError(f"归档包的批次行缺少列：{'、'.join(missing)}", 422)
    records: list[dict[str, str]] = []
    for item in reader:
        record = {field: str(item.get(field) or "") for field in ROW_META_FIELDS}
        try:
            json.loads(record["data"])
        except (TypeError, ValueError):
            raise ArchiveExportError(
                f"批次行 {record['source_url'] or '-'} 的 data 不是合法 JSON", 422)
        records.append(record)
    return records


def _batch_public_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "batch_id": str(row.get("批次编号", "")),
        "source_job": str(row.get("来源作业", "")),
        "row_count": int(row.get("数据行数") or 0),
        "status": str(row.get("status", "")),
        "storage_path": str(row.get("存储路径", "")),
        "schema_version": str(row.get("Schema版本", "")),
        "checksum": str(row.get("校验摘要", "")),
        "archived_at": str(row.get("归档时间", "")),
        "archived_version": str(row.get("归档版本", "")),
        "archived_source": str(row.get("归档来源", "")),
        "archive_digest": str(row.get("归档摘要", "")),
    }


# ---------------------------------------------------------------------------
# 归档导出服务
# ---------------------------------------------------------------------------

class ArchiveExportService:
    LEDGER = "archive_export"

    # -- 台账 -------------------------------------------------------------
    def _ledger(self) -> list[dict[str, Any]]:
        return store.rows(self.LEDGER)

    def _exports(self) -> list[dict[str, Any]]:
        return store.rows("archive_export_package")

    def _find_export(self, export_id: str) -> dict[str, Any] | None:
        for item in self._exports():
            if item["export_id"] == export_id:
                return item
        return None

    def _next_export_id(self) -> str:
        prefix = f"ARC-{datetime.now(timezone.utc).strftime('%Y%m%d')}-"
        used = [
            int(item["export_id"].rsplit("-", 1)[-1])
            for item in self._exports()
            if item["export_id"].startswith(prefix)
            and item["export_id"].rsplit("-", 1)[-1].isdigit()
        ]
        return f"{prefix}{max(used, default=0) + 1:04d}"

    def _log(self, entry: dict[str, Any]) -> None:
        rows = self._ledger()
        audit = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "时间": datetime.now(timezone.utc).isoformat(),
            **entry,
        }
        rows.append(audit)

    # -- 清单 -------------------------------------------------------------
    def _candidates(self, batch_ids: list[str] | None) -> list[dict[str, Any]]:
        rows = BatchStoreService().list_entries(page=1, size=1_000_000)[0]
        if batch_ids:
            wanted = {item.strip() for item in batch_ids if item.strip()}
            found = {str(row.get("批次编号")) for row in rows}
            unknown = sorted(wanted - found)
            if unknown:
                raise ArchiveExportError(f"批次存储中不存在批次：{'、'.join(unknown)}", 404)
            rows = [row for row in rows if str(row.get("批次编号")) in wanted]
        return rows

    def build_manifest(
        self,
        *,
        requester: str,
        batch_ids: list[str] | None,
        note: str,
    ) -> dict[str, Any]:
        candidates = self._candidates(batch_ids)
        if not candidates:
            raise ArchiveExportError("归档清单为空：批次存储中没有可归档的批次", 409)

        requested: list[dict[str, Any]] = []
        for row in candidates:
            batch_id = str(row.get("批次编号", ""))
            status = str(row.get("status", ""))
            reason = ""
            rows = batch_rows(batch_id)
            if status != "校验通过":
                reason = f"批次状态为「{status}」，仅完整可校验批次允许发布"
            elif not rows:
                reason = "批次行数据不可复现，拒绝发布"
            elif int(row.get("数据行数") or 0) != len(rows):
                reason = (
                    f"登记行数 {row.get('数据行数')} 与实际行数 {len(rows)} 不一致"
                )
            elif str(row.get("Schema版本", "")) != REQUIRED_SCHEMA_VERSION:
                reason = f"Schema版本 {row.get('Schema版本')} 不受支持"

            entry = {
                **_batch_public_row(row),
                "rows": rows,
                "publishable": not reason,
                "reason": reason,
                "content_digest": digest_rows(rows) if rows else "",
            }
            if reason:
                entry["status"] = status
            requested.append(entry)

        publishable = [item for item in requested if item["publishable"]]
        if not publishable:
            reasons = "；".join(
                f"{item['batch_id']}：{item['reason']}" for item in requested)
            raise ArchiveExportError(f"归档清单中没有完整可校验的批次。{reasons}", 409)

        return {
            "package_version": PACKAGE_VERSION,
            "requester": requester,
            "note": note,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": REQUIRED_SCHEMA_VERSION,
            "source": "data_archive",
            "requested": requested,
            "publishable_batches": [item["batch_id"] for item in publishable],
            "blocked_batches": [
                item["batch_id"] for item in requested if not item["publishable"]
            ],
        }

    # -- 发布 -------------------------------------------------------------
    def _render_package(self, manifest: dict[str, Any], export_id: str) -> tuple[bytes, dict[str, str]]:
        published = [
            item for item in manifest["requested"] if item["publishable"]
        ]
        package_manifest = {
            **manifest,
            "export_id": export_id,
            "requested": [
                {key: value for key, value in item.items()
                 if key not in ("rows", "content_digest", "publishable")}
                for item in manifest["requested"]
            ],
            "published": [
                {
                    **{key: value for key, value in item.items()
                       if key not in ("rows", "publishable")},
                    "archived_version": f"{PACKAGE_VERSION}/{export_id}",
                    "archived_source": f"data_archive:{export_id}",
                }
                for item in published
            ],
        }
        data_bytes = _csv_bytes([
            row for item in published for row in item["rows"]
        ])
        all_rows = [row for item in published for row in item["rows"]]
        package_manifest["aggregate_digest"] = digest_rows(all_rows)

        manifest_bytes = json.dumps(
            package_manifest, sort_keys=True, ensure_ascii=False, indent=2,
        ).encode("utf-8")
        # checksums.sha256 存文件字节摘要，接收方可以直接 sha256sum -c 核对；
        # 批次内容的语义摘要放在 manifest 的 published[].content_digest。
        checksums = {
            ROWS_NAME: digest_bytes(data_bytes),
            MANIFEST_NAME: digest_bytes(manifest_bytes),
        }
        checksum_text = "".join(
            f"{value}  {name}\n" for name, value in sorted(checksums.items()))

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for name, payload in (
                (MANIFEST_NAME, manifest_bytes),
                (ROWS_NAME, data_bytes),
                (CHECKSUM_NAME, checksum_text.encode("utf-8")),
            ):
                info = zipfile.ZipInfo(name, date_time=(2026, 11, 23, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                zf.writestr(info, payload)
        package_manifest["checksums"] = checksums
        return buffer.getvalue(), checksums

    def publish(self, *, requester: str, batch_ids: list[str] | None,
                note: str = "") -> dict[str, Any]:
        manifest = self.build_manifest(
            requester=requester, batch_ids=batch_ids, note=note)
        export_id = self._next_export_id()
        archive_bytes, checksums = self._render_package(manifest, export_id)
        published = [item for item in manifest["requested"] if item["publishable"]]

        detail = {
            "export_id": export_id,
            "requester": requester,
            "note": note,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "package_version": PACKAGE_VERSION,
            "source": "data_archive",
            "filename": f"{export_id}.zip",
            "media_type": "application/zip",
            "manifest": manifest,
            "checksums": checksums,
            "package_digest": digest_bytes(archive_bytes),
            "package_size": len(archive_bytes),
            "archive": archive_bytes,
            "verifications": [],
            "syncs": [],
        }
        self._exports().append(detail)

        for item in published:
            batch = self._batch_row(item["batch_id"])
            if batch is None:
                continue
            batch["校验摘要"] = item["content_digest"]
            batch["归档版本"] = f"{PACKAGE_VERSION}/{export_id}"
            batch["归档来源"] = f"data_archive:{export_id}"
            batch["归档摘要"] = item["content_digest"]
            batch["归档导出"] = export_id
            batch["归档时间"] = detail["published_at"]
            batch["校验结果"] = "待接收方校验"
            batch["pending"] = False

        # 发布完成即回写批次存储清单，接收方拿包时清单里已有版本/来源/摘要
        batch_sync = self._sync_batch_store(detail, operation="发布归档包")
        self._record_export_task(detail)
        self._log({
            "操作": "发布归档包",
            "导出编号": export_id,
            "发起批次": requester,
            "接收批次": ",".join(item["batch_id"] for item in published),
            "结果": "成功",
            "说明": batch_sync["message"],
            "包摘要": detail["package_digest"],
            "包大小": detail["package_size"],
        })
        return self.public_view(detail, include_manifest=True)

    def _batch_row(self, batch_id: str) -> dict[str, Any] | None:
        for row in store.rows("batch_store"):
            if str(row.get("批次编号")) == batch_id:
                return row
        return None

    # -- 下载 -------------------------------------------------------------
    def download(self, export_id: str, *, requester: str = "") -> tuple[dict[str, Any], bytes]:
        detail = self._find_export(export_id)
        if detail is None:
            raise ArchiveExportError(f"归档包 {export_id} 不存在", 404)
        if requester and detail["requester"] != requester:
            raise ArchiveExportError(
                f"归档包 {export_id} 属于发起批次 {detail['requester']}，"
                f"不能按 {requester} 重新获取", 403)
        detail["download_count"] = int(detail.get("download_count", 0)) + 1
        self._log({
            "操作": "下载归档包",
            "导出编号": export_id,
            "发起批次": detail["requester"],
            "接收批次": ",".join(
                item["batch_id"] for item in detail["manifest"]["requested"]
                if item["publishable"]),
            "结果": "成功",
            "说明": f"第 {detail['download_count']} 次下载，返回已确认的同一份内容",
            "包摘要": detail["package_digest"],
            "包大小": detail["package_size"],
        })
        return detail, detail["archive"]

    # -- 校验 -------------------------------------------------------------
    def verify(self, export_id: str, archive_bytes: bytes) -> dict[str, Any]:
        """核对收到的压缩包：文件清单、包摘要、逐文件摘要、批次行摘要。"""
        detail = self._find_export(export_id)
        if detail is None:
            raise ArchiveExportError(f"归档包 {export_id} 不存在", 404)

        checks: list[dict[str, Any]] = []

        def check(name: str, ok: bool, expected: Any, actual: Any) -> None:
            checks.append({
                "检查项": name,
                "结果": "通过" if ok else "拒收",
                "期望": expected,
                "实际": actual,
            })

        try:
            zf = zipfile.ZipFile(io.BytesIO(archive_bytes))
        except zipfile.BadZipFile:
            result = self._finish_verify(detail, [{
                "检查项": "压缩包结构", "结果": "拒收",
                "期望": "可解压的 zip", "实际": "无法解压",
            }], ["压缩包无法解压"])
            return result

        with zf:
            names = sorted(zf.namelist())
            check("文件清单", names == EXPECTED_FILES, EXPECTED_FILES, names)

            package_digest = digest_bytes(archive_bytes)
            check("压缩包摘要", package_digest == detail["package_digest"],
                  detail["package_digest"], package_digest)

            if MANIFEST_NAME not in names or ROWS_NAME not in names:
                missing = [
                    name for name in (MANIFEST_NAME, ROWS_NAME) if name not in names]
                return self._finish_verify(detail, checks + [{
                    "检查项": "必需文件", "结果": "拒收",
                    "期望": f"{MANIFEST_NAME}、{ROWS_NAME}",
                    "实际": f"缺少 {'、'.join(missing)}",
                }], [f"归档包缺少 {'、'.join(missing)}"])

            manifest_bytes = zf.read(MANIFEST_NAME)
            rows_bytes = zf.read(ROWS_NAME)
            checksum_bytes = zf.read(CHECKSUM_NAME) if CHECKSUM_NAME in names else b""

            try:
                _csv_records = _csv_to_records(rows_bytes)
                csv_error = ""
            except (ArchiveExportError, UnicodeDecodeError, csv.Error) as exc:
                _csv_records = []
                csv_error = str(exc)
            check("批次行可解析", not csv_error, "UTF-8 CSV，data 为 JSON", csv_error or "可解析")
            check("批次行文件摘要",
                  digest_bytes(rows_bytes) == detail["checksums"][ROWS_NAME],
                  detail["checksums"][ROWS_NAME], digest_bytes(rows_bytes))
            check("manifest文件摘要",
                  digest_bytes(manifest_bytes) == detail["checksums"][MANIFEST_NAME],
                  detail["checksums"][MANIFEST_NAME], digest_bytes(manifest_bytes))

            expected_checksum_text = "".join(
                f"{value}  {name}\n"
                for name, value in sorted(detail["checksums"].items()))
            check("校验单文件", checksum_bytes.decode("utf-8", "replace")
                  == expected_checksum_text,
                  expected_checksum_text.strip(), checksum_bytes.decode("utf-8", "replace").strip())

            try:
                manifest = json.loads(manifest_bytes)
                if not isinstance(manifest, dict):
                    manifest = {}
            except ValueError:
                manifest = {}
            check("归档版本", manifest.get("package_version") == PACKAGE_VERSION,
                  PACKAGE_VERSION, manifest.get("package_version"))
            check("归档来源", manifest.get("source") == "data_archive",
                  "data_archive", manifest.get("source"))
            check("发起批次", manifest.get("requester") == detail["requester"],
                  detail["requester"], manifest.get("requester"))
            published_ids = [item["batch_id"] for item in manifest.get("published", [])]
            expected_ids = [item["batch_id"] for item in detail["manifest"]["requested"]
                            if item["publishable"]]
            check("已发布批次", published_ids == expected_ids,
                  expected_ids, published_ids)
            aggregate_digest = digest_rows(_csv_records)
            check("批次行内容摘要",
                  manifest.get("aggregate_digest") == aggregate_digest,
                  aggregate_digest, manifest.get("aggregate_digest"))

            # 逐批次核对：manifest 登记的版本、来源、摘要要和发布时一致；
            # 再按登记行数从 data.csv 切片重算行级摘要，防止单行被篡改。
            published_entries = {
                item.get("batch_id"): item for item in manifest.get("published", [])
            }
            requested = list(manifest.get("requested", []))
            offset = 0
            for item in detail["manifest"]["requested"]:
                if item["publishable"]:
                    entry = published_entries.get(item["batch_id"], {})
                    check(f"{item['batch_id']} 内容摘要",
                          entry.get("content_digest") == item["content_digest"],
                          item["content_digest"], entry.get("content_digest"))
                    check(f"{item['batch_id']} 归档版本",
                          entry.get("archived_version") == f"{PACKAGE_VERSION}/{export_id}",
                          f"{PACKAGE_VERSION}/{export_id}", entry.get("archived_version"))
                    check(f"{item['batch_id']} 归档来源",
                          entry.get("archived_source") == f"data_archive:{export_id}",
                          f"data_archive:{export_id}", entry.get("archived_source"))
                    count = int(item["row_count"])
                    slice_rows = _csv_records[offset:offset + count]
                    offset += count
                    slice_digest = digest_rows(slice_rows)
                    check(f"{item['batch_id']} 行级摘要",
                          slice_digest == item["content_digest"],
                          item["content_digest"], slice_digest)
                else:
                    requested_entry = next(
                        (r for r in requested if r.get("batch_id") == item["batch_id"]), {})
                    check(f"{item['batch_id']} 发布拦截",
                          requested_entry.get("status") == item["status"],
                          f"{item['status']}：{item['reason']}",
                          f"{requested_entry.get('status')}：{requested_entry.get('reason')}")

        failures = [item["检查项"] for item in checks if item["结果"] == "拒收"]
        return self._finish_verify(
            detail, checks, [f"{name} 与发布时登记不一致" for name in failures])

    def _finish_verify(self, detail: dict[str, Any], checks: list[dict[str, Any]],
                       failures: list[str]) -> dict[str, Any]:
        export_id = detail["export_id"]
        ok = not failures
        record = {
            "verified_at": datetime.now(timezone.utc).isoformat(),
            "result": "通过" if ok else "拒收",
            "checks": checks,
            "failures": failures,
        }
        detail["verifications"].append(record)

        for item in detail["manifest"]["requested"]:
            if not item["publishable"]:
                continue
            batch = self._batch_row(item["batch_id"])
            if batch is None:
                continue
            if ok:
                batch["校验结果"] = (
                    f"通过（{export_id}，{record['verified_at']}）")
                batch["归档摘要"] = item["content_digest"]
            else:
                batch["校验结果"] = (
                    f"拒收（{export_id}，{record['verified_at']}）："
                    f"{'；'.join(failures)}")

        sync = self._sync_batch_store(detail, operation="接收校验")
        self._log({
            "操作": "校验归档包",
            "导出编号": export_id,
            "发起批次": detail["requester"],
            "接收批次": ",".join(
                item["batch_id"] for item in detail["manifest"]["requested"]
                if item["publishable"]),
            "结果": record["result"],
            "说明": "全部检查通过" if ok else "；".join(failures),
            "包摘要": detail["package_digest"],
            "包大小": detail["package_size"],
        })
        return {
            **record,
            "export_id": export_id,
            "batch_sync": sync,
        }

    # -- 批次存储清单同步 ---------------------------------------------------
    def _recover_from_package(self, detail: dict[str, Any]) -> dict[str, list[dict[str, str]]]:
        """从已封存的包内恢复批次行，供批次存储按包内容核对。"""
        with zipfile.ZipFile(io.BytesIO(detail["archive"])) as zf:
            manifest = json.loads(zf.read(MANIFEST_NAME))
            records = _csv_to_records(zf.read(ROWS_NAME))
        recovered: dict[str, list[dict[str, str]]] = {}
        offset = 0
        for entry in manifest.get("published", []):
            count = int(entry.get("row_count") or 0)
            recovered[entry["batch_id"]] = records[offset:offset + count]
            offset += count
        return recovered

    def _sync_batch_store(self, detail: dict[str, Any], *, operation: str) -> dict[str, Any]:
        published = [
            item for item in detail["manifest"]["requested"] if item["publishable"]
        ]
        rows = {str(row.get("批次编号")): row for row in store.rows("batch_store")}
        recovered_rows = self._recover_from_package(detail)
        recovered: list[dict[str, Any]] = []
        for item in published:
            live = rows.get(item["batch_id"])
            restored = digest_rows(recovered_rows.get(item["batch_id"], []))
            recovered.append({
                "batch_id": item["batch_id"],
                "版本": f"{PACKAGE_VERSION}/{detail['export_id']}",
                "来源": f"data_archive:{detail['export_id']}",
                "归档版本": live.get("归档版本", "") if live else "",
                "归档来源": live.get("归档来源", "") if live else "",
                "内容摘要": item["content_digest"],
                "恢复摘要": restored,
                "摘要一致": restored == item["content_digest"],
                "登记摘要": live.get("归档摘要", "") if live else "",
                "校验结果": live.get("校验结果", "") if live else "",
            })
        record = {
            "synced_at": datetime.now(timezone.utc).isoformat(),
            "operation": operation,
            "export_id": detail["export_id"],
            "package_version": PACKAGE_VERSION,
            "source": "data_archive",
            "package_digest": detail["package_digest"],
            "package_size": detail["package_size"],
            "batches": recovered,
            "message": (
                f"批次存储清单已同步 {len(recovered)} 个批次"
                f"（版本 {PACKAGE_VERSION}，来源 data_archive:{detail['export_id']}）"
                if recovered else "没有可同步的已发布批次"),
        }
        detail["syncs"].append(record)
        detail["last_synced_at"] = record["synced_at"]
        detail["last_verify_result"] = detail["verifications"][-1]["result"] \
            if detail["verifications"] else ""
        detail["last_verify_at"] = detail["verifications"][-1]["verified_at"] \
            if detail["verifications"] else ""
        self._record_export_task(detail)
        return record

    def _record_export_task(self, detail: dict[str, Any]) -> None:
        """导出任务台账：待接收校验时是导出中，校验通过才算已完成。"""
        rows = store.rows("export_task")
        file_path = f"/archive/exports/{detail['filename']}"
        last_result = detail["verifications"][-1]["result"] if detail["verifications"] else ""
        status = "已完成" if last_result == "通过" else (
            "导出失败" if last_result == "拒收" else "导出中")
        row_count = str(sum(
            item["row_count"] for item in detail["manifest"]["requested"]
            if item["publishable"]))
        for row in rows:
            if str(row.get("记录编号")) == detail["export_id"]:
                row["status"] = status
                row["行数限制"] = row_count
                row["pending"] = status == "导出中"
                row["abnormal"] = status == "导出失败"
                return
        rows.append({
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "记录编号": detail["export_id"],
            "数据来源": "data_archive",
            "导出格式": "ZIP",
            "行数限制": str(sum(
                item["row_count"] for item in detail["manifest"]["requested"]
                if item["publishable"])),
            "字段筛选": "批次行全字段",
            "排序条件": "按批次清单顺序",
            "文件路径": file_path,
            "导出时间": detail["published_at"],
            "status": status,
            "pending": status == "导出中",
            "abnormal": status == "导出失败",
        })

    # -- 查询 -------------------------------------------------------------
    def public_view(self, detail: dict[str, Any], *, include_manifest: bool = False,
                    requester: str = "") -> dict[str, Any]:
        manifest = detail["manifest"]
        published = [item for item in manifest["requested"] if item["publishable"]]
        view = {
            "export_id": detail["export_id"],
            "requester": detail["requester"],
            "note": detail["note"],
            "published_at": detail["published_at"],
            "package_version": detail["package_version"],
            "source": detail["source"],
            "filename": detail["filename"],
            "media_type": detail["media_type"],
            "package_digest": detail["package_digest"],
            "package_size": detail["package_size"],
            "checksums": detail["checksums"],
            "published_batches": [
                {
                    "batch_id": item["batch_id"],
                    "source_job": item["source_job"],
                    "row_count": item["row_count"],
                    "schema_version": item["schema_version"],
                    "content_digest": item["content_digest"],
                }
                for item in published
            ],
            "blocked_batches": [
                {
                    "batch_id": item["batch_id"],
                    "status": item["status"],
                    "reason": item["reason"],
                }
                for item in manifest["requested"] if not item["publishable"]
            ],
            "download_url": f"/api/data_archive/exports/{detail['export_id']}/download",
            "download_count": int(detail.get("download_count", 0)),
            "verifications": detail["verifications"],
            "last_verify_result": detail.get("last_verify_result", ""),
            "last_verify_at": detail.get("last_verify_at", ""),
            "last_synced_at": detail.get("last_synced_at", ""),
        }
        if include_manifest:
            view["manifest"] = detail["manifest"]
        return view

    def list_exports(self, *, requester: str = "") -> list[dict[str, Any]]:
        items = self._exports()
        if requester:
            items = [item for item in items if item["requester"] == requester]
        return [self.public_view(detail) for detail in items]

    def request_state(self, requester: str) -> dict[str, Any]:
        """按发起批次查询归档导出进度：已发布、待校验、被拦截范围。"""
        if not requester:
            raise ArchiveExportError("必须指定发起批次 requester", 400)
        items = [item for item in self._exports() if item["requester"] == requester]
        export_ids = {item["export_id"] for item in items}
        blocked: dict[str, dict[str, Any]] = {}
        for item in items:
            for entry in item["manifest"]["requested"]:
                if not entry["publishable"]:
                    blocked[entry["batch_id"]] = {
                        "batch_id": entry["batch_id"],
                        "status": entry["status"],
                        "reason": entry["reason"],
                    }
        # 尚未完成的范围按当前批次状态刷新，方便发起方判断能否再次申请
        for batch_id, entry in blocked.items():
            live = self._batch_row(batch_id)
            if live is not None:
                entry["current_status"] = str(live.get("status", ""))
                entry["ready_for_export"] = (
                    entry["current_status"] == "校验通过"
                    and bool(batch_rows(batch_id))
                )
        return {
            "requester": requester,
            "exported": [self.public_view(item) for item in items],
            "verified": [
                self.public_view(item) for item in items
                if item["verifications"] and item["verifications"][-1]["result"] == "通过"
            ],
            "pending_batches": list(blocked.values()),
            "export_count": len(export_ids),
        }

    def complete_recheck(self, batch_id: str, rows: list[dict[str, Any]],
                         *, operator: str = "") -> dict[str, Any]:
        """补采完成后重新封存批次行并重新校验；之后的申请可再次归档该批次。"""
        if not rows:
            raise ArchiveExportError("补采结果为空，不能标记批次完成", 422)
        batch = self._batch_row(batch_id)
        if batch is None:
            raise ArchiveExportError(f"批次存储中不存在批次：{batch_id}", 404)

        normalized: list[dict[str, str]] = []
        problems: list[str] = []
        for index, raw in enumerate(rows, start=1):
            source_url = str(raw.get("source_url") or raw.get("来源URL") or "").strip()
            data = raw.get("data", raw.get("字段"))
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except ValueError:
                    data = None
            if not source_url or not isinstance(data, dict) or not data:
                problems.append(f"第 {index} 行缺少来源或字段数据")
                continue
            empty = [key for key, value in data.items()
                     if value is None or str(value).strip() == ""]
            if empty:
                problems.append(f"第 {index} 行字段为空：{'、'.join(sorted(empty))}")
                continue
            normalized.append({
                "source_url": source_url,
                "extract_rule_id": str(raw.get("extract_rule_id")
                                       or raw.get("解析规则") or f"CONT-{batch_id}"),
                "data": json.dumps(data, ensure_ascii=False, sort_keys=True),
            })
        if problems:
            raise ArchiveExportError(
                f"补采结果仍不可校验：{'；'.join(problems)}", 422)

        rows_table = store.rows(f"batch_rows:{batch_id}")
        rows_table.clear()
        rows_table.extend(dict(row) for row in normalized)
        digest = digest_rows(normalized)
        batch["数据行数"] = len(normalized)
        batch["校验摘要"] = digest
        batch["status"] = "校验通过"
        batch["pending"] = False
        batch["abnormal"] = False
        batch["校验结果"] = f"补采后重新校验通过（{operator or '系统'}）"
        self._log({
            "操作": "补采完成重新校验",
            "导出编号": "",
            "发起批次": operator,
            "接收批次": batch_id,
            "结果": "成功",
            "说明": f"行数 {len(normalized)}，摘要 {digest}",
            "包摘要": digest,
            "包大小": 0,
        })
        return {
            "batch_id": batch_id,
            "row_count": len(normalized),
            "content_digest": digest,
            "status": batch["status"],
        }

    def ledger(self, *, requester: str = "") -> list[dict[str, Any]]:
        rows = self._ledger()
        if requester:
            rows = [row for row in rows if row.get("发起批次") == requester]
        return list(reversed(rows))


archive_export = ArchiveExportService()
