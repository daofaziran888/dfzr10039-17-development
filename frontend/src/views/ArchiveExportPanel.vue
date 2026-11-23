<template>
  <section class="archive-export" data-testid="archive-export">
    <header class="page-head">
      <div>
        <h3>归档导出</h3>
        <p class="page-desc">
          按批次清单打包归档，只发布完整可校验的批次；已发布的包内容与适用范围固定不变。
        </p>
      </div>
    </header>

    <form class="filter-bar" @submit.prevent="publish">
      <label class="filter-item">
        <span>发起批次</span>
        <input
          v-model="requester"
          data-testid="archive-requester"
          placeholder="例如 RECV-20261123"
          required
        />
      </label>
      <label class="filter-item">
        <span>批次清单（逗号分隔）</span>
        <input
          v-model="batches"
          data-testid="archive-batches"
          placeholder="例如 B12,B13"
          required
        />
      </label>
      <button class="btn primary" data-testid="archive-publish" type="submit" :disabled="busy">
        {{ busy ? '打包中…' : '打包归档导出' }}
      </button>
    </form>

    <p v-if="message" class="error-text" data-testid="archive-message">{{ message }}</p>

    <article v-if="current" class="archive-package" data-testid="archive-package">
      <header class="archive-head">
        <strong>{{ current.export_id }}</strong>
        <span class="archive-meta">
          版本 {{ current.package_version }} · 来源 {{ current.source }} ·
          包摘要 {{ current.package_digest.slice(0, 16) }}…
        </span>
        <button class="btn" data-testid="archive-download" type="button" @click="download">
          下载归档包
        </button>
      </header>

      <table class="data-table">
        <thead>
          <tr><th>批次</th><th>来源作业</th><th>行数</th><th>内容摘要</th><th>状态</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in current.published_batches" :key="item.batch_id">
            <td>{{ item.batch_id }}</td>
            <td>{{ item.source_job }}</td>
            <td>{{ item.row_count }}</td>
            <td>{{ item.content_digest.slice(0, 16) }}…</td>
            <td>已发布</td>
          </tr>
          <tr v-for="item in current.blocked_batches" :key="item.batch_id">
            <td>{{ item.batch_id }}</td>
            <td>—</td>
            <td>—</td>
            <td>—</td>
            <td class="error-text">拦截：{{ item.reason }}</td>
          </tr>
        </tbody>
      </table>

      <div class="filter-bar">
        <label class="filter-item">
          <span>选择收到的归档包核对</span>
          <input data-testid="archive-verify-file" type="file" accept=".zip" @change="onFile" />
        </label>
        <button
          class="btn"
          data-testid="archive-verify"
          type="button"
          :disabled="!file || busy"
          @click="verify"
        >
          校验完整性
        </button>
      </div>

      <div v-if="verification" data-testid="archive-verify-result">
        <p :class="verification.result === '通过' ? 'ok-text' : 'error-text'">
          校验结果：{{ verification.result }}
          <span v-if="verification.failures.length">
            ｜{{ verification.failures.join('；') }}
          </span>
        </p>
        <table class="data-table">
          <thead><tr><th>检查项</th><th>结果</th></tr></thead>
          <tbody>
            <tr v-for="check in verification.checks" :key="check.检查项">
              <td>{{ check.检查项 }}</td>
              <td :class="check.结果 === '通过' ? '' : 'error-text'">{{ check.结果 }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </article>
  </section>
</template>

<script setup lang="ts">
import { ref } from 'vue'

import {
  downloadArchivePackage,
  publishArchiveExport,
  verifyArchivePackage,
} from '@/api/backend-service'
import type { ArchiveExportPackage, ArchiveVerifyResult } from '@/api/backend-service'

const requester = ref('')
const batches = ref('')
const busy = ref(false)
const message = ref('')
const current = ref<ArchiveExportPackage | null>(null)
const verification = ref<ArchiveVerifyResult | null>(null)
const file = ref<File | null>(null)

function onFile(event: Event) {
  const target = event.target as HTMLInputElement
  file.value = target.files?.[0] ?? null
  verification.value = null
}

async function publish() {
  busy.value = true
  message.value = ''
  verification.value = null
  try {
    current.value = await publishArchiveExport(
      requester.value.trim(),
      batches.value.split(/[,，\s]+/).filter(Boolean),
    )
  } catch (error) {
    message.value = error instanceof Error ? error.message : '归档打包失败'
  } finally {
    busy.value = false
  }
}

async function download() {
  if (!current.value) return
  message.value = ''
  try {
    await downloadArchivePackage(current.value.export_id, current.value.filename)
  } catch (error) {
    message.value = error instanceof Error ? error.message : '归档包下载失败'
  }
}

async function verify() {
  if (!current.value || !file.value) return
  busy.value = true
  message.value = ''
  try {
    verification.value = await verifyArchivePackage(current.value.export_id, file.value)
  } catch (error) {
    message.value = error instanceof Error ? error.message : '归档包校验失败'
  } finally {
    busy.value = false
  }
}
</script>
