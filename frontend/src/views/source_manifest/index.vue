<template>
  <section class="page" data-module="source_manifest">
    <header class="page-head">
      <div>
        <h2>采集源接入清单</h2>
        <p class="page-desc">
          读取采集源接入配置并校验接入边界，加工可交接的接入清单批次；每条来源同时保留连通状态与关联种子数量，
          发布后同步种子管理本批接入记录，支持导入、导出与打包回查。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-build" type="button" @click="openBuild">加工接入清单</button>
        <button class="btn" data-testid="open-import" type="button" @click="openImport">导入清单</button>
        <button class="btn" data-testid="refresh-archives" type="button" @click="reload">刷新交接记录</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card" data-testid="stat-total">
        <span class="stat-label">接入批次总数</span>
        <strong class="stat-value">{{ total }}</strong>
      </article>
      <article class="stat-card" data-testid="stat-processing">
        <span class="stat-label">加工中批次</span>
        <strong class="stat-value">{{ processingCount }}</strong>
      </article>
      <article class="stat-card" data-testid="stat-published">
        <span class="stat-label">已发布批次</span>
        <strong class="stat-value">{{ publishedCount }}</strong>
      </article>
      <article class="stat-card" data-testid="stat-archived">
        <span class="stat-label">已归档批次</span>
        <strong class="stat-value">{{ archivedCount }}</strong>
      </article>
    </div>

    <table class="data-table" data-testid="batch-table">
      <thead>
        <tr>
          <th v-for="column in batchColumns" :key="column">{{ column }}</th>
          <th>批次操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in batches" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in batchColumns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button class="link" data-testid="view" type="button" @click="viewBatch(Number(row.id))">查看明细</button>
            <button
              v-if="row.status === '加工中'"
              class="link"
              type="button"
              :data-testid="`publish-${row.id}`"
              @click="publishBatch(Number(row.id))"
            >发布批次</button>
            <button
              v-if="row.status === '加工中'"
              class="link"
              type="button"
              :data-testid="`discard-${row.id}`"
              @click="discardBatch(Number(row.id))"
            >废弃</button>
            <template v-if="row.status !== '加工中'">
              <button class="link" type="button" :data-testid="`export-${row.id}`" @click="exportBatch(Number(row.id))">导出CSV</button>
              <button class="link" type="button" :data-testid="`download-${row.id}`" @click="downloadBatch(Number(row.id))">打包下载</button>
            </template>
          </td>
        </tr>
        <tr v-if="!batches.length">
          <td :colspan="batchColumns.length + 1" class="empty-state">暂无接入清单批次，可先加工或导入</td>
        </tr>
      </tbody>
    </table>

    <!-- 加工弹窗 -->
    <div v-if="showBuild" class="modal-backdrop" data-testid="build-modal">
      <form class="modal" @submit.prevent="submitBuild">
        <h3>加工采集源接入清单</h3>
        <p class="page-desc">留空采集源ID则读取全部已登记采集源；边界校验不通过将整体回滚，不产生半套清单。</p>
        <label class="create-field">
          <span>采集源ID（逗号分隔，可留空）</span>
          <input v-model="buildIds" data-testid="build-source-ids" placeholder="例如 1,2" />
        </label>
        <label class="create-field">
          <span>交接备注</span>
          <input v-model="buildRemark" data-testid="build-remark" placeholder="本批交接说明" />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showBuild = false">取消</button>
          <button class="btn primary" data-testid="submit-build" type="submit" :disabled="submitting">
            {{ submitting ? '加工中…' : '开始加工' }}
          </button>
        </div>
      </form>
    </div>

    <!-- 导入弹窗 -->
    <div v-if="showImport" class="modal-backdrop" data-testid="import-modal">
      <form class="modal" @submit.prevent="submitImport">
        <h3>导入采集源接入清单</h3>
        <label class="create-field">
          <span>导入格式</span>
          <select v-model="importFormat" data-testid="import-format">
            <option value="json">JSON</option>
            <option value="csv">CSV（表头需含来源标识）</option>
          </select>
        </label>
        <label class="create-field">
          <span>导入内容</span>
          <textarea v-model="importContent" data-testid="import-content" rows="5"
            placeholder='JSON: ["SRC-ELEC-001"] 或 CSV: 来源标识,备注&#10;SRC-ELEC-001,导入'></textarea>
        </label>
        <label class="create-field">
          <span>冲突仲裁策略（同源再次导入时必选）</span>
          <select v-model="importStrategy" data-testid="import-strategy">
            <option value="">不指定（冲突时拒绝）</option>
            <option value="keep_binding">沿用绑定：保留原种子顺序，仅补写连通差异</option>
            <option value="rebuild">重建批次：新种子集合为唯一入口，旧种子转历史</option>
          </select>
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showImport = false">取消</button>
          <button class="btn primary" data-testid="submit-import" type="submit" :disabled="submitting">
            {{ submitting ? '导入中…' : '开始导入' }}
          </button>
        </div>
      </form>
    </div>

    <!-- 批次明细抽屉 -->
    <div v-if="detail" class="modal-backdrop" data-testid="detail-modal" @click.self="detail = null">
      <div class="modal modal-wide">
        <h3>批次明细 · {{ detail.批次编号 }}（{{ detail.status }}）</h3>
        <p class="page-desc">
          冲突策略：{{ detail.冲突策略 || '无' }} ｜ 导入格式：{{ detail.导入格式 }} ｜ 发布时间：{{ detail.published_at || '未发布' }}
        </p>
        <table class="data-table">
          <thead>
            <tr>
              <th v-for="column in itemColumns" :key="column">{{ column }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in detail.items" :key="item.来源标识" :data-testid="`detail-item-${item.来源标识}`">
              <td v-for="column in itemColumns" :key="column">
                <template v-if="column === '种子编号序列'">{{ (item[column] || []).join(' | ') }}</template>
                <template v-else-if="column === '连通状态'">
                  <span :data-testid="`conn-${item.来源标识}`">{{ item[column] }}</span>
                </template>
                <template v-else>{{ item[column] ?? '—' }}</template>
              </td>
            </tr>
          </tbody>
        </table>

        <h4>种子管理本批接入记录（按来源标识同步）</h4>
        <table class="data-table" data-testid="access-record-table">
          <thead>
            <tr><th>来源标识</th><th>种子集合版本</th><th>可用种子数量</th><th>可用种子</th></tr>
          </thead>
          <tbody>
            <tr v-for="rec in accessRecords" :key="rec.来源标识" :data-testid="`access-${rec.来源标识}`">
              <td>{{ rec.来源标识 }}</td>
              <td>{{ rec.种子集合版本 }}</td>
              <td :data-testid="`access-count-${rec.来源标识}`">{{ rec.可用种子数量 }}</td>
              <td>{{ (rec.可用种子 || []).join(' | ') }}</td>
            </tr>
            <tr v-if="!accessRecords.length">
              <td colspan="4" class="empty-state">批次发布后才会生成种子接入记录</td>
            </tr>
          </tbody>
        </table>

        <template v-if="detail.迁移记录 && detail.迁移记录.length">
          <h4>旧种子迁移记录（重建批次）</h4>
          <table class="data-table" data-testid="migration-table">
            <thead>
              <tr><th>来源标识</th><th>旧种子编号</th><th>迁移到批次</th><th>迁移时间</th></tr>
            </thead>
            <tbody>
              <tr v-for="m in detail.迁移记录" :key="m.旧种子编号" :data-testid="`migration-${m.旧种子编号}`">
                <td>{{ m.来源标识 }}</td>
                <td>{{ m.旧种子编号 }}</td>
                <td>{{ m.迁移到批次 }}</td>
                <td>{{ m.迁移时间 }}</td>
              </tr>
            </tbody>
          </table>
        </template>

        <div class="page-actions">
          <button class="btn" type="button" @click="detail = null">关闭</button>
          <button v-if="detail.status !== '加工中'" class="btn" type="button" @click="exportBatch(detail.id)">导出CSV</button>
          <button v-if="detail.status !== '加工中'" class="btn primary" type="button" @click="downloadBatch(detail.id)">打包下载</button>
        </div>
      </div>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 个接入批次，接入清单仅发布完整批次，中断自动回滚</span>
      <span v-if="errorMessage" class="error-text" data-testid="error-text">{{ errorMessage }}</span>
      <span v-if="okMessage" class="ok-text" data-testid="ok-text">{{ okMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

const ENDPOINT = '/api/source_manifest'
const SEED_ENDPOINT = '/api/seed_config'

type BatchRow = Record<string, string | number | null>
type Detail = Record<string, any>

const batchColumns = ['批次编号', 'status', '条目数', '冲突策略', '导入格式', 'created_at', 'published_at']
const itemColumns = ['来源标识', '源名称', '协议类型', '连通状态', '连通消息', '种子数量', '种子编号序列', '种子集合版本']

const batches = ref<BatchRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const okMessage = ref('')
const showBuild = ref(false)
const showImport = ref(false)
const submitting = ref(false)
const buildIds = ref('')
const buildRemark = ref('')
const importFormat = ref('json')
const importContent = ref('')
const importStrategy = ref('')
const detail = ref<Detail | null>(null)
const accessRecords = ref<Record<string, any>[]>([])

const processingCount = computed(() => batches.value.filter((b) => b.status === '加工中').length)
const publishedCount = computed(() => batches.value.filter((b) => b.status === '已发布').length)
const archivedCount = computed(() => batches.value.filter((b) => b.status === '已归档').length)

function openBuild() {
  buildIds.value = ''
  buildRemark.value = ''
  errorMessage.value = ''
  showBuild.value = true
}

function openImport() {
  importFormat.value = 'json'
  importContent.value = ''
  importStrategy.value = ''
  errorMessage.value = ''
  showImport.value = true
}

async function submitBuild() {
  submitting.value = true
  errorMessage.value = ''
  try {
    const ids = buildIds.value.split(',').map((s) => s.trim()).filter(Boolean).map(Number)
    const response = await request(`${ENDPOINT}/build`, {
      method: 'POST',
      body: JSON.stringify({ source_ids: ids, remark: buildRemark.value }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '加工失败')
    okMessage.value = payload.message
    showBuild.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '加工失败'
  } finally {
    submitting.value = false
  }
}

async function submitImport() {
  submitting.value = true
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/import`, {
      method: 'POST',
      body: JSON.stringify({
        format: importFormat.value,
        content: importContent.value,
        conflict_strategy: importStrategy.value || null,
      }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '导入失败')
    okMessage.value = payload.message
    showImport.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导入失败'
  } finally {
    submitting.value = false
  }
}

async function publishBatch(id: number) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${id}/publish`, { method: 'POST' })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '发布失败')
    okMessage.value = payload.message
    detail.value = null
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '发布失败'
  }
}

async function discardBatch(id: number) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${id}/discard`, { method: 'POST' })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '废弃失败')
    okMessage.value = payload.message
    detail.value = null
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '废弃失败'
  }
}

async function viewBatch(id: number) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${id}`)
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '明细读取失败')
    detail.value = payload.entry
    await loadAccessRecords()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '明细读取失败'
  }
}

async function loadAccessRecords() {
  accessRecords.value = []
  if (!detail.value || detail.value.status === '加工中') return
  try {
    const response = await request(`${SEED_ENDPOINT}/access/records`)
    const payload = await response.json()
    const records = payload.entry?.records || {}
    accessRecords.value = Object.values(records)
  } catch {
    accessRecords.value = []
  }
}

function triggerDownload(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = filename
  link.click()
  URL.revokeObjectURL(url)
}

async function exportBatch(id: number) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${id}/export`)
    if (!response.ok) throw new Error('导出失败')
    triggerDownload(await response.blob(), `source_manifest_${id}.csv`)
    okMessage.value = '清单CSV已导出，含连通状态、种子数量与来源标识'
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

async function downloadBatch(id: number) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${id}/download`)
    if (!response.ok) throw new Error('打包下载失败')
    triggerDownload(await response.blob(), `source_manifest_${id}.zip`)
    okMessage.value = '交接记录压缩包已下载，可离线回查'
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '打包下载失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/batches?page=1&size=100`)
    if (!response.ok) throw new Error('接入批次列表读取失败')
    const payload = await response.json()
    batches.value = payload.items ?? []
    total.value = payload.total ?? 0
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '接入批次列表读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.modal-wide {
  width: min(960px, 92vw);
  max-height: 86vh;
  overflow: auto;
}
.create-field textarea,
.create-field select {
  width: 100%;
  padding: 6px 8px;
  border: 1px solid var(--border);
  border-radius: 6px;
  font: inherit;
}
.ok-text {
  color: #15803d;
}
</style>
