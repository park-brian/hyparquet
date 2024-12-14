import fs from 'fs'

/**
 * Read .parquet file into JSON
 *
 * @param {string} filePath
 * @returns {any}
 */
export function fileToJson(filePath) {
  const buffer = fs.readFileSync(filePath)
  return JSON.parse(buffer.toString())
}

/**
 * Make a DataReader from bytes
 *
 * @import {DataReader} from '../src/types.d.ts'
 * @param {number[]} bytes
 * @returns {DataReader}
 */
export function reader(bytes) {
  return { view: new DataView(new Uint8Array(bytes).buffer), offset: 0 }
}

/**
 * Concatenate array buffers into a single buffer.
 *
 * @param {ArrayBuffer[]} buffers
 * @returns {ArrayBuffer}
 */
export function concatenateArrayBuffers(buffers) {
  const bufferLength = buffers.reduce((acc, buffer) => acc + buffer.byteLength, 0)
  const buffer = new ArrayBuffer(bufferLength)
  const array = new Uint8Array(buffer)
  let offset = 0
  for (const buffer of buffers) {
    array.set(new Uint8Array(buffer), offset)
    offset += buffer.byteLength
  }
  return buffer
}

/**
 * Groups an array by a predicate function, returning an object with keys as the predicate result.
 *
 * @param {any[]} array
 * @param {(item: any) => string} predicate
 * @returns {Record<string, any[]>}
 */
export function groupBy(array, predicate) {
  return array.reduce((acc, el) => {
    const key = predicate(el)
    if (!acc[key]) acc[key] = []
    acc[key].push(el)
    return acc
  }, {})
}

/**
 * Creates an AsyncBuffer for efficiently reading large .parquet files
 *
 * @param {string} filePath - Path to the parquet file
 * @returns {import('../src/types.js').AsyncBuffer}
 */
export function largeFileToAsyncBuffer(filePath) {
  // Cache the file size to avoid repeated stat calls
  const byteLength = fs.statSync(filePath).size

  /**
   * Helper function to read a specific range from the file
   * @param {fs.promises.FileHandle} fileHandle - Open file handle
   * @param {[number, number | undefined] | null} range - [start, end] positions or null
   * @returns {Promise<ArrayBuffer>} The read buffer
   */
  async function readRange(fileHandle, range) {
    if (!range || !range.length) {
      return new ArrayBuffer(0)
    }

    const [start, end] = range
    const length = (end || byteLength) - start
    const buffer = new Uint8Array(length)
    await fileHandle.read(buffer, 0, length, start)
    return buffer.buffer
  }

  return {
    byteLength,

    async slice(start, end) {
      const fileHandle = await fs.promises.open(filePath, 'r')
      try {
        return await readRange(fileHandle, [start, end])
      } finally {
        await fileHandle.close()
      }
    },

    async sliceAll(ranges) {
      const fileHandle = await fs.promises.open(filePath, 'r')
      try {
        return await Promise.all(ranges.map((range) => readRange(fileHandle, range)))
      } finally {
        await fileHandle.close()
      }
    },
  }
}

/**
 * Wraps a remote file in an AsyncBuffer
 * @param {string} url
 * @returns {Promise<import('../src/types.js').AsyncBuffer>}
 */
export async function remoteFileToAsyncBuffer(url) {
  const response = await fetch(url, { method: 'HEAD', cache: 'no-store' })
  const byteLength = parseInt(response.headers.get('Content-Length') || '0', 10)

  return {
    byteLength,
    slice: async (start, end) => {
      const response = await fetch(url, {
        headers: { Range: `bytes=${start}-${end || byteLength}` },
        cache: 'no-store',
      })
      return await response.arrayBuffer()
    },
    sliceAll: async (ranges) => {
      const requests = chunk(ranges, 100).map((ranges) => getBytesFromRanges(url, ranges))
      const results = await Promise.all(requests)
      return results.flat()
    },
  }
}

/**
 * Chunks an array into smaller arrays of the specified size.
 *
 * @template T The type of elements in the array
 * @param {T[]} array
 * @param {number} chunkSize
 * @returns {T[][]}
 */
function chunk(array, chunkSize) {
  const chunks = []

  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize))
  }

  return chunks
}

/**
 * Retrieves byte ranges from a URL using the Fetch API.
 * Uses ASCII encoding for direct byte<->text position mapping.
 *
 * @param {string} url - Source URL
 * @param {(number[]|null)[]} ranges - Array of [start, end] pairs (end is exclusive)
 * @param {RequestInit} requestInit - Fetch API configuration
 * @param {boolean} [trustContentType=false] - Whether to trust multipart boundary from content-type header
 * @returns {Promise<ArrayBuffer[]>}
 */
async function getBytesFromRanges(url, ranges, requestInit = { cache: 'no-store' }, trustContentType = false) {
  const byteRanges = ranges.map(() => new ArrayBuffer(0))
  if (!ranges.some(Boolean)) return byteRanges

  const rangeHeader = ranges
    .filter(Boolean)
    .map(([start, end]) => `${start}-${end - 1}`)
    .join(',')

  const response = await fetch(url, {
    ...requestInit,
    headers: { ...requestInit.headers, range: `bytes=${rangeHeader}` },
  })

  const responseBuffer = await response.arrayBuffer()
  const responseText = new TextDecoder('ascii', { fatal: true }).decode(responseBuffer)

  let boundary = null

  if (trustContentType) {
    const contentType = response.headers.get('content-type') || ''
    boundary = contentType.includes('boundary=') ? contentType.split('boundary=')[1].trim() : null
  } else {
    const trimmed = responseText.trim()
    const boundaryMatch = trimmed.match(/^--([^\s]+)$/m)
    boundary = boundaryMatch?.index === 0 && trimmed.endsWith(`--${boundaryMatch[1]}--`) ? boundaryMatch[1] : null
  }

  if (!boundary) {
    ranges.forEach((range, i) => {
      if (range) byteRanges[i] = responseBuffer.slice(...range)
    })
    return byteRanges
  }

  const parts = responseText
    .split(`--${boundary}`)
    .filter((part) => part.trim() && !part.includes('--\r\n'))
    .map((part) => part.split('\r\n\r\n').map((p) => p.replace(/\r\n$/, '')))

  let position = 0
  for (const [headers, content] of parts) {
    const [, startStr, endStr] = headers.match(/content-range:.*bytes (\d+)-(\d+)/i) || []
    if (!startStr) continue
    const start = +startStr
    const end = +endStr + 1

    const contentOffset = responseText.indexOf(content, position + headers.length)
    const partBuffer = responseBuffer.slice(contentOffset, contentOffset + content.length)
    position = contentOffset + content.length

    ranges.forEach((range, i) => {
      if (!range) return
      if (start <= range[0] && range[1] <= end) {
        const offset = range[0] - start
        const length = range[1] - range[0]
        byteRanges[i] = partBuffer.slice(offset, offset + length)
      }
    })
  }

  return byteRanges
}
