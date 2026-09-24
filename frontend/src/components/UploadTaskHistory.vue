<script setup lang="ts">
/** 分页恢复上传任务，使用同一响应中的图片处理状态展示最终结果。 */
import { onMounted, onUnmounted, shallowRef } from 'vue'
import { api } from '../api'

interface UploadTask {
  task_id: string
  status: string
  message?: string
  upload?: { filename?: string; receipt_id?: string }
  result?: { filename?: string }
  error?: { error?: string; message?: string }
  processing?: { job_id: string; status: string; message?: string; error?: { error?: string; message?: string } }
}

const tasks = shallowRef<UploadTask[]>([])
const error = shallowRef('')
const retryable = new Set(['failed', 'blocked', 'unknown_execution'])
let timer: ReturnType<typeof setTimeout> | undefined
let disposed = false
let refreshing: Promise<void> | null = null
let refreshAgain = false

/** 遍历全部游标页，按接收身份保留最新尝试，恢复较早提交的活动任务。 */
async function readPages(): Promise<void> {
  if (timer) clearTimeout(timer)
  try {
    const collected = new Map<string, UploadTask>()
    let cursor: string | undefined
    do {
      const response = await api.tasks({ task_type: 'image_upload', limit: 100, cursor })
      if (disposed) return
      for (const task of response.items as UploadTask[]) {
        const key = task.upload?.receipt_id || task.task_id
        if (!collected.has(key)) collected.set(key, task)
      }
      cursor = response.next_cursor || undefined
    } while (cursor)
    tasks.value = [...collected.values()]
    error.value = ''
    if (tasks.value.some((task) => ['queued', 'running'].includes(task.status) || ['queued', 'running'].includes(task.processing?.status || ''))) {
      timer = setTimeout(() => { void refresh() }, 5000)
    }
  } catch (reason) {
    error.value = reason instanceof Error ? reason.message : String(reason)
    if (!disposed) timer = setTimeout(() => { void refresh() }, 10000)
  }
}

/** 合并同时发生的刷新请求，接收通知在当前查询结束后继续查询。 */
function refresh(): Promise<void> {
  if (refreshing) {
    refreshAgain = true
    return refreshing
  }
  refreshing = readPages().finally(() => {
    refreshing = null
    if (refreshAgain && !disposed) {
      refreshAgain = false
      void refresh()
    }
  })
  return refreshing
}

/** 展示接收任务与图片处理的真实终态及明确原因。 */
function status(task: UploadTask): string {
  if (task.status === 'failed') return task.error?.message || task.error?.error || '上传处理失败'
  const job = task.processing
  if (job?.status === 'succeeded') return '处理成功'
  if (job?.status === 'warning') return job.message || '处理完成，存在警告'
  if (job && retryable.has(job.status)) return job.error?.message || job.error?.error || job.message || job.status
  return job?.message || task.message || (task.status === 'queued' ? '等待处理' : '处理中')
}

/** 重试持久输入或最新图片 Job；后续刷新按图片身份读取新 revision。 */
async function retry(task: UploadTask): Promise<void> {
  try {
    if (task.status === 'failed') await api.retryTask(task.task_id)
    else if (task.processing) await api.retryProcessingJob(task.processing.job_id)
    await refresh()
  } catch (reason) {
    error.value = reason instanceof Error ? reason.message : String(reason)
  }
}

/** 服务端取消完成后重新读取状态，保留并发完成时的真实结果。 */
async function cancel(task: UploadTask): Promise<void> {
  try {
    await api.cancelTask(task.task_id)
    await refresh()
  } catch (reason) {
    error.value = reason instanceof Error ? reason.message : String(reason)
  }
}

defineExpose({ refresh })
onMounted(refresh)
onUnmounted(() => { disposed = true; if (timer) clearTimeout(timer) })
</script>

<template>
  <section aria-label="已接收上传任务">
    <button class="quiet" type="button" @click="refresh">刷新后台任务</button>
    <p v-if="error" role="alert">{{ error }}</p>
    <ul>
      <li v-for="task in tasks" :key="task.task_id">
        <strong>{{ task.upload?.filename || task.result?.filename || '上传图片' }}</strong>
        <span>{{ status(task) }}</span>
        <button v-if="task.status === 'failed' || retryable.has(task.processing?.status || '')" class="quiet" type="button" @click="retry(task)">重试</button>
        <button v-if="['queued', 'running'].includes(task.status)" class="quiet" type="button" @click="cancel(task)">取消后台任务</button>
      </li>
    </ul>
  </section>
</template>
