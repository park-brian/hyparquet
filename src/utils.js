/**
 * Replace bigint, date, etc with legal JSON types.
 * When parsing parquet files, bigints are used to represent 64-bit integers.
 * However, JSON does not support bigints, so it's helpful to convert to numbers.
 *
 * @param {any} obj object to convert
 * @returns {unknown} converted object
 */
export function toJson(obj) {
  if (obj === undefined) return null
  if (typeof obj === 'bigint') return Number(obj)
  if (Array.isArray(obj)) return obj.map(toJson)
  if (obj instanceof Uint8Array) return Array.from(obj)
  if (obj instanceof Date) return obj.toISOString()
  if (obj instanceof Object) {
    /** @type {Record<string, unknown>} */
    const newObj = {}
    for (const key of Object.keys(obj)) {
      if (obj[key] === undefined) continue
      newObj[key] = toJson(obj[key])
    }
    return newObj
  }
  return obj
}

/**
 * Concatenate two arrays fast.
 *
 * @param {any[]} aaa first array
 * @param {DecodedArray} bbb second array
 */
export function concat(aaa, bbb) {
  const chunk = 10000
  for (let i = 0; i < bbb.length; i += chunk) {
    aaa.push(...bbb.slice(i, i + chunk))
  }
}

/**
 * Concatenate array buffers into a single buffer.
 *
 * @param {ArrayBuffer[]} buffers
 * @returns {ArrayBuffer}
 */
export function concatBuffers(...buffers) {
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
 * Calculate the extent of an array.
 *
 * @param {any[]} array
 * @returns {[any, any]}
 */
export function extent(array) {
  let min, max = array[0]
  for (const value of array) {
    if (value < min) min = value
    if (value > max) max = value
  }
  return [min, max]
}

/**
 * Chunks an array into smaller arrays of the specified size.
 *
 * @template T The type of elements in the array
 * @param {T[]} array
 * @param {number} chunkSize
 * @returns {T[][]}
 */
export function chunk(array, chunkSize) {
  const chunks = []

  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize))
  }

  return chunks
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
 * Deep equality comparison
 *
 * @param {any} a First object to compare
 * @param {any} b Second object to compare
 * @returns {boolean} true if objects are equal
 */
export function equals(a, b) {
  if (a === b) return true
  if (a instanceof Uint8Array && b instanceof Uint8Array) return equals(Array.from(a), Array.from(b))
  if (!a || !b || typeof a !== typeof b) return false
  return Array.isArray(a) && Array.isArray(b)
    ? a.length === b.length && a.every((v, i) => equals(v, b[i]))
    : typeof a === 'object' && Object.keys(a).length === Object.keys(b).length && Object.keys(a).every(k => equals(a[k], b[k]))
}

/**
 * Get the byte length of a URL using a HEAD request.
 * If requestInit is provided, it will be passed to fetch.
 *
 * @param {string} url
 * @param {RequestInit} [requestInit] fetch options
 * @returns {Promise<number>}
 */
export async function byteLengthFromUrl(url, requestInit) {
  return await fetch(url, { ...requestInit, method: 'HEAD' })
    .then(res => {
      if (!res.ok) throw new Error(`fetch head failed ${res.status}`)
      const length = res.headers.get('Content-Length')
      if (!length) throw new Error('missing content length')
      return parseInt(length)
    })
}

/**
 * Get byte ranges from a URL using a multi-range request.
 * Uses ASCII encoding for direct byte<->text position mapping.
 *
 * @param {string} url - Source URL
 * @param {(number[]|null)[]} ranges - Array of [start, end] pairs (end is exclusive)
 * @param {RequestInit} [requestInit] - Fetch API configuration
 * @param {boolean} [trustContentType=false] - Whether to trust multipart boundary from content-type header
 * @returns {Promise<ArrayBuffer[]>}
 */
export async function byteRangesFromUrl(url, ranges, requestInit = { cache: 'no-store' }, trustContentType = false) {
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

  if (!response.ok) throw new Error(`fetch failed ${response.status}`)

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

/**
 * Construct an AsyncBuffer for a URL.
 * If byteLength is not provided, will make a HEAD request to get the file size.
 * If requestInit is provided, it will be passed to fetch.
 *
 * @param {object} options
 * @param {string} options.url
 * @param {number} [options.byteLength]
 * @param {RequestInit} [options.requestInit]
 * @returns {Promise<AsyncBuffer>}
 */
export async function asyncBufferFromUrl({ url, byteLength, requestInit }) {
  if (!url) throw new Error('missing url')
  // byte length from HEAD request
  byteLength ||= await byteLengthFromUrl(url, requestInit)
  const init = requestInit || {}
  return {
    byteLength,
    async slice(start, end) {
      // fetch byte range from url
      const headers = new Headers(init.headers)
      const endStr = end === undefined ? '' : end - 1
      headers.set('Range', `bytes=${start}-${endStr}`)
      const res = await fetch(url, { ...init, headers })
      if (!res.ok || !res.body) throw new Error(`fetch failed ${res.status}`)
      return res.arrayBuffer()
    },
  }
}

/**
 * Construct an AsyncBuffer for a local file using node fs package.
 *
 * @param {string} filename
 * @returns {Promise<AsyncBuffer>}
 */
export async function asyncBufferFromFile(filename) {
  const fsPackage = 'fs' // webpack no include
  const fs = await import(fsPackage)
  const stat = await fs.promises.stat(filename)
  return {
    byteLength: stat.size,
    async slice(start, end) {
      // read file slice
      const readStream = fs.createReadStream(filename, { start, end })
      return await readStreamToArrayBuffer(readStream)
    },
  }
}

/**
 * Convert a node ReadStream to ArrayBuffer.
 *
 * @param {import('stream').Readable} input
 * @returns {Promise<ArrayBuffer>}
 */
function readStreamToArrayBuffer(input) {
  return new Promise((resolve, reject) => {
    /** @type {Buffer[]} */
    const chunks = []
    input.on('data', chunk => chunks.push(chunk))
    input.on('end', () => {
      const buffer = Buffer.concat(chunks)
      resolve(buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength))
    })
    input.on('error', reject)
  })
}

/**
 * Returns a cached layer on top of an AsyncBuffer. For caching slices of a file
 * that are read multiple times, possibly over a network.
 *
 * @param {AsyncBuffer} file file-like object to cache
 * @returns {AsyncBuffer} cached file-like object
 */
export function cachedAsyncBuffer({ byteLength, slice }) {
  const cache = new Map()
  return {
    byteLength,
    /**
     * @param {number} start
     * @param {number} [end]
     * @returns {Awaitable<ArrayBuffer>}
     */
    slice(start, end) {
      const key = cacheKey(start, end, byteLength)
      const cached = cache.get(key)
      if (cached) return cached
      // cache miss, read from file
      const promise = slice(start, end)
      cache.set(key, promise)
      return promise
    },
  }
}


/**
 * Returns canonical cache key for a byte range 'start,end'.
 * Normalize int-range and suffix-range requests to the same key.
 *
 * @import {AsyncBuffer, Awaitable, DecodedArray} from '../src/types.d.ts'
 * @param {number} start start byte of range
 * @param {number} [end] end byte of range, or undefined for suffix range
 * @param {number} [size] size of file, or undefined for suffix range
 * @returns {string}
 */
function cacheKey(start, end, size) {
  if (start < 0) {
    if (end !== undefined) throw new Error(`invalid suffix range [${start}, ${end}]`)
    if (size === undefined) return `${start},`
    return `${size + start},${size}`
  } else if (end !== undefined) {
    if (start > end) throw new Error(`invalid empty range [${start}, ${end}]`)
    return `${start},${end}`
  } else if (size === undefined) {
    return `${start},`
  } else {
    return `${start},${size}`
  }
}
