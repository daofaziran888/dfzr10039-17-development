import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

export type ArchiveExportPackage = {
  export_id: string
  requester: string
  published_at: string
  package_version: string
  source: string
  filename: string
  package_digest: string
  package_size: number
  checksums: Record<string, string>
  published_batches: {
    batch_id: string
    source_job: string
    row_count: number
    content_digest: string
  }[]
  blocked_batches: { batch_id: string; status: string; reason: string }[]
  download_url: string
  download_count: number
  last_verify_result: string
  last_synced_at: string
}

export type ArchiveVerifyResult = {
  export_id: string
  result: '通过' | '拒收'
  verified_at: string
  failures: string[]
  checks: { 检查项: string; 结果: string; 期望: unknown; 实际: unknown }[]
  batch_sync: { batches: { batch_id: string; 归档版本: string; 归档来源: string }[] }
}

export async function publishArchiveExport(
  requester: string,
  batches: string[],
  note = '',
): Promise<ArchiveExportPackage> {
  const response = await request('/api/data_archive/exports', {
    method: 'POST',
    body: JSON.stringify({ requester, batches, note }),
  })
  const payload = await response.json()
  if (!response.ok) {
    throw new Error(payload?.detail ?? '归档打包失败')
  }
  return payload as ArchiveExportPackage
}

export async function fetchArchiveExport(exportId: string): Promise<ArchiveExportPackage> {
  const response = await request(`/api/data_archive/exports/${encodeURIComponent(exportId)}`)
  if (!response.ok) throw new Error('归档包信息读取失败')
  return (await response.json()) as ArchiveExportPackage
}

export async function downloadArchivePackage(
  exportId: string,
  filename?: string,
): Promise<void> {
  const response = await request(`/api/data_archive/exports/${encodeURIComponent(exportId)}/download`)
  if (!response.ok) throw new Error('归档包下载失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = filename ?? `${exportId}.zip`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function verifyArchivePackage(
  exportId: string,
  file: File,
): Promise<ArchiveVerifyResult> {
  const response = await fetch(`/api/data_archive/exports/${encodeURIComponent(exportId)}/verify`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/zip' },
    body: await file.arrayBuffer(),
  })
  const payload = await response.json()
  if (!response.ok) throw new Error(payload?.detail ?? '归档包校验失败')
  return payload as ArchiveVerifyResult
}
