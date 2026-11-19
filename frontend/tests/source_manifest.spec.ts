import { expect, test } from '@playwright/test'

/**
 * 采集源接入清单验收：
 * - 读取接入配置加工清单，连通状态与关联种子数量同时出现
 * - 仅发布完整批次，发布后种子管理同步本批接入记录
 * - 两个已有采集源连续导出，连通状态/种子数量/来源标识逐项对应
 * - 同源再次导入的冲突仲裁（沿用绑定 / 重建批次）
 * - 导出 CSV 与打包 zip 下载、归档回查
 */

const PAGE = '/source_manifest'

test.beforeEach(async ({ page }) => {
  await page.goto(PAGE)
  await expect(page.getByTestId('batch-table')).toBeVisible()
})

test('加工并发布完整批次：连通状态与种子数量同时写入，种子管理同步', async ({ page }) => {
  // 加工两个已有采集源（1、2）
  await page.getByTestId('open-build').click()
  await expect(page.getByTestId('build-modal')).toBeVisible()
  await page.getByTestId('build-source-ids').fill('1,2')
  await page.getByTestId('build-remark').fill('E2E交接批次')
  await page.getByTestId('submit-build').click()

  await expect(page.getByTestId('build-modal')).toHaveCount(0)
  const processingRow = page.locator('tbody tr[data-status="加工中"]').first()
  await expect(processingRow).toBeVisible()
  const batchId = (await processingRow.getAttribute('data-testid'))?.replace('row-', '') as string

  // 查看明细：两条来源的连通状态、种子数量、来源标识逐项对应
  await page.getByTestId('view').first().click()
  await expect(page.getByTestId('detail-modal')).toBeVisible()
  const item1 = page.getByTestId('detail-item-SRC-ELEC-001')
  await expect(item1).toContainText('SRC-ELEC-001')
  await expect(item1).toContainText('可达')
  await expect(item1).toContainText('SEED-E001')
  const item2 = page.getByTestId('detail-item-SRC-NEWS-002')
  await expect(item2).toContainText('SRC-NEWS-002')
  await expect(item2).toContainText('可达')
  await expect(item2).toContainText('SEED-N002')
  // 连通状态与种子数量成对存在
  await expect(page.getByTestId('conn-SRC-ELEC-001')).toHaveText('可达')
  await expect(page.getByTestId('conn-SRC-NEWS-002')).toHaveText('可达')
  await page.getByRole('button', { name: '关闭' }).click()

  // 发布
  await page.getByTestId(`publish-${batchId}`).click()
  await expect(page.locator(`tbody tr[data-testid="row-${batchId}"]`)).toHaveAttribute('data-status', '已发布')
  await expect(page.getByTestId('ok-text')).toContainText('完整发布')

  // 发布后明细可回查种子管理本批接入记录
  await page.getByTestId('view').first().click()
  const access1 = page.getByTestId('access-SRC-ELEC-001')
  await expect(access1).toContainText('SRC-ELEC-001')
  await expect(page.getByTestId('access-count-SRC-ELEC-001')).toHaveText('1')
  const access2 = page.getByTestId('access-SRC-NEWS-002')
  await expect(access2).toContainText('SEED-N002')
})

test('连续导出两个已有采集源：CSV 与 zip 下载且字段逐项对应', async ({ page }) => {
  // 先准备一个已发布批次
  await page.getByTestId('open-build').click()
  await page.getByTestId('build-source-ids').fill('1,2')
  await page.getByTestId('submit-build').click()
  const processingRow = page.locator('tbody tr[data-status="加工中"]').first()
  await expect(processingRow).toBeVisible()
  const batchId = (await processingRow.getAttribute('data-testid'))?.replace('row-', '') as string
  await page.getByTestId(`publish-${batchId}`).click()
  await expect(page.locator(`tbody tr[data-testid="row-${batchId}"]`)).toHaveAttribute('data-status', '已发布')

  // 导出 CSV
  const csvPromise = page.waitForEvent('download')
  await page.getByTestId(`export-${batchId}`).click()
  const csvDownload = await csvPromise
  expect(csvDownload.suggestedFilename()).toContain('source_manifest')
  expect(csvDownload.suggestedFilename()).toMatch(/\.csv$/)
  const csvPath = await csvDownload.path()
  const csvText = await import('node:fs/promises').then((fs) => fs.readFile(csvPath as string, 'utf-8'))
  expect(csvText).toContain('来源标识')
  expect(csvText).toContain('连通状态')
  expect(csvText).toContain('种子数量')
  expect(csvText).toContain('SRC-ELEC-001')
  expect(csvText).toContain('SRC-NEWS-002')
  // 两源逐项对应：可达 + 种子数 1
  const lines = csvText.replace(/^﻿/, '').trim().split('\n')
  const dataLines = lines.filter((l) => l.includes('SRC-ELEC-001') || l.includes('SRC-NEWS-002'))
  expect(dataLines).toHaveLength(2)
  for (const line of dataLines) {
    expect(line).toContain('可达')
    const cells = line.split(',')
    expect(cells[7]).toBe('1') // 种子数量列（批次编号,来源标识,源名称,协议类型,基础URL,连通状态,连通消息,种子数量）
  }

  // 打包下载 zip
  const zipPromise = page.waitForEvent('download')
  await page.getByTestId(`download-${batchId}`).click()
  const zipDownload = await zipPromise
  expect(zipDownload.suggestedFilename()).toMatch(/\.zip$/)
})

test('同源再次导入：未仲裁被拒绝，沿用绑定补写连通差异', async ({ page }) => {
  // 准备已发布批次（源1）
  await page.getByTestId('open-build').click()
  await page.getByTestId('build-source-ids').fill('1')
  await page.getByTestId('submit-build').click()
  const processingRow = page.locator('tbody tr[data-status="加工中"]').first()
  const batchId = (await processingRow.getAttribute('data-testid'))?.replace('row-', '') as string
  await page.getByTestId(`publish-${batchId}`).click()
  await expect(page.locator(`tbody tr[data-testid="row-${batchId}"]`)).toHaveAttribute('data-status', '已发布')

  // 不指定仲裁策略：冲突被拒绝并提示
  await page.getByTestId('open-import').click()
  await page.getByTestId('import-content').fill('["SRC-ELEC-001"]')
  await page.getByTestId('submit-import').click()
  await expect(page.getByTestId('error-text')).toContainText('数据版本冲突')
  await page.getByRole('button', { name: '取消' }).click()

  // 沿用绑定仲裁导入
  await page.getByTestId('open-import').click()
  await page.getByTestId('import-content').fill('["SRC-ELEC-001"]')
  await page.getByTestId('import-strategy').selectOption('keep_binding')
  await page.getByTestId('submit-import').click()
  await expect(page.getByTestId('import-modal')).toHaveCount(0)
  const newProcessing = page.locator('tbody tr[data-status="加工中"]').first()
  await expect(newProcessing).toBeVisible()
  const newId = (await newProcessing.getAttribute('data-testid'))?.replace('row-', '') as string

  // 明细中保留原种子顺序并带连通补写项
  await page.getByTestId('view').first().click()
  const item = page.getByTestId('detail-item-SRC-ELEC-001')
  await expect(item).toContainText('沿用绑定')
  await expect(item).toContainText('SEED-E001')
  await page.getByRole('button', { name: '关闭' }).click()

  // 发布后旧批次归档，仅一个已发布批次
  await page.getByTestId(`publish-${newId}`).click()
  await expect(page.locator(`tbody tr[data-testid="row-${newId}"]`)).toHaveAttribute('data-status', '已发布')
  await expect(page.locator(`tbody tr[data-testid="row-${batchId}"]`)).toHaveAttribute('data-status', '已归档')
})

test('加工中批次可废弃，不留半成品', async ({ page }) => {
  await page.getByTestId('open-build').click()
  await page.getByTestId('build-source-ids').fill('2')
  await page.getByTestId('submit-build').click()
  const processingRow = page.locator('tbody tr[data-status="加工中"]').first()
  await expect(processingRow).toBeVisible()
  const batchId = (await processingRow.getAttribute('data-testid'))?.replace('row-', '') as string
  await page.getByTestId(`discard-${batchId}`).click()
  await expect(page.locator(`tbody tr[data-testid="row-${batchId}"]`)).toHaveCount(0)
})
