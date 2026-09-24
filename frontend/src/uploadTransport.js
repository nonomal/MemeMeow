/** 使用浏览器上传进度事件发送 multipart，保留服务端错误码和重试时间。 */
export function uploadTransport(url, body, { signal, onProgress, headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest()
    const abort = () => xhr.abort()
    xhr.open('POST', url)
    for (const [name, value] of Object.entries(headers)) xhr.setRequestHeader(name, value)
    xhr.responseType = 'json'
    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable) onProgress?.(event.loaded / event.total)
    }
    xhr.onloadend = () => signal?.removeEventListener('abort', abort)
    xhr.onabort = () => reject(new DOMException('上传传输已中止', 'AbortError'))
    xhr.onerror = () => reject(Object.assign(new Error('上传网络连接中断'), { code: 'upload_network_error' }))
    xhr.onload = () => {
      const data = xhr.response || {}
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(data)
        return
      }
      const detail = data.detail && typeof data.detail === 'object' ? data.detail : data
      const error = Object.assign(new Error(detail.message || (typeof data.detail === 'string' ? data.detail : '上传请求失败')), {
        code: detail.error || 'request_failed', status: xhr.status,
      })
      const retryAfter = xhr.getResponseHeader('retry-after')
      if (retryAfter) error.retryAfter = Number.isFinite(Number(retryAfter)) ? Math.max(0, Number(retryAfter)) : Math.max(0, (Date.parse(retryAfter) - Date.now()) / 1000)
      reject(error)
    }
    if (signal?.aborted) {
      reject(new DOMException('上传传输已中止', 'AbortError'))
      return
    }
    signal?.addEventListener('abort', abort, { once: true })
    xhr.send(body)
  })
}
