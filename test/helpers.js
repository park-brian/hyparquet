import fs from 'fs'

/**
 * Helper function to read .parquet file into ArrayBuffer
 *
 * @param {string} filePath
 * @returns {Promise<ArrayBuffer>}
 */
export async function readFileToArrayBuffer(filePath) {
  const buffer = await fs.promises.readFile(filePath)
  return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
}

/**
 * Wrap .parquet file in an AsyncBuffer
 *
 * @param {string} filePath
 * @returns {import('../src/types.js').AsyncBuffer}
 */
export function fileToAsyncBuffer(filePath) {
  return {
    byteLength: fs.statSync(filePath).size,
    slice: async (start, end) => (await readFileToArrayBuffer(filePath)).slice(start, end),
  }
}

/**
 * Wrap a very large .parquet file in an AsyncBuffer
 *
 * @param {string} filePath
 * @returns {import('../src/types.js').AsyncBuffer}
 */
export function largeFileToAsyncBuffer(filePath) {
  return {
    byteLength: fs.statSync(filePath).size,
    slice: async (start, end) => {
      const fileHandle = await fs.promises.open(filePath, 'r')
      const length = end - start
      const buffer = new Uint8Array(length)
      await fileHandle.read(buffer, 0, length, start)
      await fileHandle.close()
      return buffer.buffer
    },
    sliceBatch: async (ranges) => {
      const fileHandle = await fs.promises.open(filePath, 'r')
      const buffers = []
      for (const range of ranges) {
        if (!range) {
          buffers.push(new ArrayBuffer(0))
          continue
        }
        const [start, end] = range
        const length = end - start
        const buffer = new Uint8Array(length)
        await fileHandle.read(buffer, 0, length, start)
        buffers.push(buffer.buffer)
      }
      await fileHandle.close()
      return buffers
    },
  }
}

/**
 * Wraps a remote file in an AsyncBuffer
 * @param {string} url 
 * @returns {Promise<import('../src/types.js').AsyncBuffer>} 
 */
export async function remoteFileToAsyncBuffer(url) {
  return {
    byteLength: await getLength(url),
    slice: async (start, end) => {
      return await getBytes(url, start, end)
    },
    sliceBatch: async (ranges) => {
      const requests = chunk(ranges, 100).map(ranges => getBytesFromRanges(url, ranges))
      const results = await Promise.all(requests)
      return results.flat()
    },
  }
}

/**
 * Chunks an array into smaller arrays of the specified size.
 * 
 * @param {any[]} list 
 * @param {number} size 
 * @returns {any[][]}  
 */
function chunk(list, size) {
  return Array.from({ length: Math.ceil(list.length / size) }, (_, i) =>
    list.slice(i * size, i * size + size)
  )
}

/**
 * Returns the length of a remote file
 * 
 * @param {string} url 
 * @returns 
 */
export async function getLength(url) {
  const response = await fetch(url, { method: 'HEAD', cache: 'no-store' })
  return parseInt(response.headers.get('Content-Length'))
}


/**
 * Fetches a range of bytes from a remote file
 * 
 * 
 * @param {url} url 
 * @param {number} start 
 * @param {number} end 
 * @returns {Promise<ArrayBuffer>}
 */
export async function getBytes(url, start, end) {
  const response = await fetch(url, {
    headers: { Range: `bytes=${start}-${end - 1}` },
    cache: 'no-store',
  })
  return await response.arrayBuffer()
}

/**
 * Returns an ArrayBuffer[] from a multirange request
 *
 * @param {string} url
 * @param {(null | [number, number])[]} ranges
 * @returns {Promise<ArrayBuffer[]>}
 */
export async function getBytesFromRanges(url, ranges) {
  const arrayBuffers = new Array(ranges.length).fill(null).map(_ => new ArrayBuffer(0))
  if (!ranges.filter(Boolean).length) return arrayBuffers

  const headers = { range: 'bytes=' + ranges.filter(Boolean).map(([start, end]) => [start, end - 1].join('-')).join(',') }
  const getRangeIndex = (ranges, contentRange) => {
    const [_, start, end] = contentRange?.match(/bytes (\d+)-(\d+)\/(\d+)/)
    return ranges.findIndex(r => r && r[0] === parseInt(start) && r[1] === parseInt(end) + 1)
  }

  const response = await fetch(url, { headers, cache: 'no-store' })
  const contentType = response.headers.get('content-type')

  if (contentType?.includes('application/octet-stream')) {
    const contentRange = response.headers.get('content-range')
    const rangeIndex = getRangeIndex(ranges, contentRange)
    arrayBuffers[rangeIndex] = await response.arrayBuffer()
    return arrayBuffers
  }

  else if (contentType?.includes('multipart/byteranges')) {
    const boundary = '--' + contentType?.split('boundary=')[1]
    const responseArrayBuffer = await response.arrayBuffer()
    const responseText = new TextDecoder('ascii', { fatal: true }).decode(responseArrayBuffer) // use ascii since it maps nicely to Uint8Array
    const parts = responseText
      .split(boundary)
      .filter(part => !['', '--'].includes(part.trim()))
      .map(part => part.split('\r\n\r\n').map(p => p.replace(/\r\n$/, '')))

    let offset = 0
    for (const [header, body] of parts) {
      const parsedHeaders = header.split('\r\n').filter(Boolean).reduce((acc, line) => {
        const [key, value] = line.split(': ')
        return { ...acc, [key.toLowerCase()]: value }
      }, {})

      const bodyStartIndex = responseText.indexOf(body, offset)
      offset = bodyStartIndex + body.length
      const bodyArrayBuffer = responseArrayBuffer.slice(bodyStartIndex, offset)

      const rangeIndex = getRangeIndex(ranges, parsedHeaders['content-range'])
      arrayBuffers[rangeIndex] = bodyArrayBuffer
    }
  }

  return arrayBuffers
}


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
 * @param {number[]} bytes
 * @returns {import('../src/types.js').DataReader}
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