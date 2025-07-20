import { decodeDataPage, decodeDictionaryPage } from './column.js'
import { parquetMetadataAsync } from './metadata.js'
import { readColumnIndex, readOffsetIndex } from './indexes.js'
import { getSchemaPath } from './schema.js'
import { DEFAULT_PARSERS } from './convert.js'
import { parquetReadObjects } from './index.js'
import { concat } from './utils.js'

/**
 * @import {AsyncBuffer, FileMetaData, RowGroup, SchemaElement, ColumnMetaData, ParquetQueryFilter, ParquetReadOptions} from './types.js'
 */

/**
 * Query with page index optimization
 * Same API as parquetQuery but with page index pushdown
 *
 * @param {ParquetReadOptions & { filter?: ParquetQueryFilter, orderBy?: string, desc?: boolean, offset?: number, limit?: number }} options
 * @returns {Promise<Record<string, any>[]>}
 */
export async function parquetQuery(options) {

  const { file, filter, columns, orderBy, desc = false } = options
  if (!file || !(file.byteLength >= 0)) {
    throw new Error('parquet expected AsyncBuffer')
  }

  const metadata = options.metadata || await parquetMetadataAsync(file)

  // Support both APIs since users might use either style
  const offset = options.offset ?? options.rowStart ?? 0
  if (offset < 0) throw new Error('parquet rowStart must be positive')
  const limit = options.limit ?? (options.rowEnd !== undefined ? options.rowEnd - offset : undefined)

  // Get all columns from schema
  const allColumns = metadata.schema.slice(1).map(el => el.name)

  // Extract filter columns and validate
  const filterColumns = filter ? extractFilterColumns(filter) : []
  const outputColumns = columns || allColumns
  const requiredColumns = [...new Set([...outputColumns, ...filterColumns, ...orderBy ? [orderBy] : []].filter(Boolean))]

  // Validate columns exist
  if (filter) {
    const missingColumns = filterColumns.filter((col) => !allColumns.includes(col))
    if (missingColumns.length) {
      throw new Error(`parquet filter columns not found: ${missingColumns.join(', ')}`)
    }
  }
  if (orderBy && !allColumns.includes(orderBy)) {
    throw new Error(`parquet orderBy column not found: ${orderBy}`)
  }

  // Convert MongoDB-style filter to column predicates
  const predicates = filter ? createPredicates(filter) : new Map()

  // Execute page index query
  const filteredData = await executePageIndexQuery(file, metadata, predicates, requiredColumns)

  // Apply final filtering, sorting, and projection
  let rows = filteredData

  // Apply row-level filtering (for complex conditions that can't use page indexes)
  if (filter) {
    rows = rows.filter(row => matchesFilter(row, filter))
  }

  // Apply sorting if requested
  if (orderBy) {
    // Add index for stable sorting
    rows.forEach((row, i) => { row.__index__ = i })
    sortRows(rows, orderBy, desc)
    // Clean up __index__ fields after sorting only when filter is applied
    if (filter) {
      rows.forEach(row => delete row.__index__)
    }
  }

  // Apply offset and limit
  if (offset > 0 || limit !== undefined) {
    const start = offset
    const end = limit !== undefined ? offset + limit : undefined
    rows = rows.slice(start, end)
  }

  // Project to requested columns
  if (columns) {
    rows = rows.map(row => projectRow(row, columns))
  }

  return rows
}

/**
 * Extract filter columns from MongoDB-style filter
 * @param {ParquetQueryFilter} filter
 * @returns {string[]}
 */
function extractFilterColumns(filter) {
  const columns = new Set()
  function traverse(obj) {
    if (!obj || typeof obj !== 'object') return
    for (const [key, value] of Object.entries(obj)) {
      if (key.startsWith('$')) {
        if (Array.isArray(value)) {
          value.forEach(traverse)
        }
      } else {
        columns.add(key)
        if (typeof value === 'object' && value !== null) {
          traverse(value)
        }
      }
    }
  }
  traverse(filter)
  return Array.from(columns)
}

/**
 * Create range predicates from MongoDB filter
 * @param {ParquetQueryFilter} filter
 * @returns {Map<string, (min: any, max: any) => boolean>}
 */
function createPredicates(filter) {
  const predicates = new Map()
  function processFilter(f) {
    if (!f || typeof f !== 'object') return
    if (f.$and) {
      f.$and.forEach(processFilter)
    } else if (f.$or) {
      // For OR predicates, we need to extract all columns and create permissive predicates
      // The final row-level filtering will handle the exact OR logic
      f.$or.forEach(processFilter)
    } else {
      for (const [col, cond] of Object.entries(f)) {
        if (!col.startsWith('$')) {
          const pred = createRangePredicate(cond)
          if (pred) predicates.set(col, pred)
        }
      }
    }
  }
  processFilter(filter)
  return predicates
}

/**
 * Create range predicate from condition
 * @param {any} condition - filter condition (value or operators object)
 * @returns {((min: any, max: any) => boolean)|null} predicate function or null
 */
function createRangePredicate(condition) {
  // Handle direct value comparison
  if (typeof condition !== 'object' || condition === null) {
    // Skip statistics filtering for arrays - let row-level filtering handle it
    if (Array.isArray(condition)) return () => true
    return (min, max) => min <= condition && condition <= max
  }
  const { $eq, $gt, $gte, $lt, $lte, $in, $ne, $nin } = condition

  // Test if statistics range could contain values matching the condition
  return (min, max) => {
    if ($eq !== undefined) {
      // Skip statistics filtering for arrays - let row-level filtering handle it
      if (Array.isArray($eq)) return true
      return min <= $eq && $eq <= max
    }
    if ($ne !== undefined) {
      // For inequality, we can't exclude based on statistics alone
      return true
    }
    if ($in && Array.isArray($in)) {
      return $in.some((v) => min <= v && v <= max)
    }
    if ($nin && Array.isArray($nin)) {
      // For not-in, we can't exclude based on statistics alone
      return true
    }
    let possible = true
    if ($gt !== undefined) {
      possible = possible && max > $gt
    }
    if ($gte !== undefined) {
      possible = possible && max >= $gte
    }
    if ($lt !== undefined) {
      possible = possible && min < $lt
    }
    if ($lte !== undefined) {
      possible = possible && min <= $lte
    }
    return possible
  }
}

/**
 * Execute page index query
 * @param {AsyncBuffer & {sliceAll?: (ranges: ([number, number] | null)[]) => Promise<ArrayBuffer[]>}} file
 * @param {FileMetaData} metadata
 * @param {Map<string, (min: any, max: any) => boolean>} predicates
 * @param {string[]} outputColumns
 * @returns {Promise<Record<string, any>[]>}
 */
async function executePageIndexQuery(file, metadata, predicates, outputColumns) {

  if (predicates.size === 0) {
    // No predicates - fall back to full row group scan
    return await executeFullRowGroupScan(file, metadata.row_groups, outputColumns, metadata)
  }

  // Step 1: Filter row groups using statistics
  const matchingRowGroups = filterRowGroupsByStatistics(metadata, predicates)

  if (matchingRowGroups.length === 0) {
    return []
  }

  // Step 2: Build index location map
  const indexLocations = buildIndexLocationMap(matchingRowGroups, predicates, outputColumns, metadata)

  if (indexLocations.length === 0) {
    // Fall back to row group scan without page-level filtering
    return await executeFullRowGroupScan(file, matchingRowGroups, outputColumns, metadata)
  }

  // Step 3: Batch read indexes
  const [columnIndexData, offsetIndexData] = await Promise.all([
    sliceAll(file, indexLocations.map(loc => loc.columnIndex)),
    sliceAll(file, indexLocations.map(loc => loc.offsetIndex)),
  ])

  // Step 4: Process column indexes
  const columnPageData = processAllColumnIndexes(indexLocations, columnIndexData, offsetIndexData, metadata)

  // Step 5: Find matching pages
  const matchingPages = findPagesMatchingPredicates(columnPageData, predicates)

  if (matchingPages.length === 0) {
    return []
  }

  // Step 6: Read page data
  const pageData = await sliceAll(file, matchingPages.map(page => /** @type {[number, number]} */ [page.offset, page.offset + page.length]))

  // Step 7: Assemble rows from pages
  return assembleRowsFromPages(file, matchingPages, pageData, outputColumns, metadata)
}

/**
 * Filter row groups by statistics
 * @param {FileMetaData} metadata
 * @param {Map<string, (min: any, max: any) => boolean>} predicates
 * @returns {RowGroup[]}
 */
function filterRowGroupsByStatistics(metadata, predicates) {
  return metadata.row_groups.filter(group => {
    // Check if any predicate matches row group statistics
    for (const [columnName, predicate] of predicates) {
      const columnIdx = metadata.schema.findIndex(el => el.name === columnName) - 1
      if (columnIdx >= 0 && columnIdx < group.columns.length) {
        const columnMetadata = group.columns[columnIdx].meta_data
        if (columnMetadata && columnMetadata.statistics) {
          const { statistics } = columnMetadata
          // Use statistics.min and statistics.max (not min_value/max_value which may be undefined)
          if (statistics.min !== undefined && statistics.max !== undefined) {
            const matches = predicate(statistics.min, statistics.max)
            if (!matches) {
              return false // This row group doesn't match
            }
          }
        }
      }
    }
    return true // All predicates match or no statistics available
  })
}

/**
 * Build index location map for batch reading
 * @param {RowGroup[]} rowGroups
 * @param {Map<string, (min: any, max: any) => boolean>} predicates
 * @param {string[]} outputColumns
 * @param {FileMetaData} metadata
 * @returns {IndexLocation[]}
 */
function buildIndexLocationMap(rowGroups, predicates, outputColumns, metadata) {
  const locations = []

  rowGroups.forEach((rowGroup, rgIndex) => {
    const requiredColumns = new Set([...predicates.keys(), ...outputColumns])

    requiredColumns.forEach(columnName => {
      const columnIdx = metadata.schema.findIndex(el => el.name === columnName) - 1
      if (columnIdx >= 0 && columnIdx < rowGroup.columns.length) {
        const column = rowGroup.columns[columnIdx]

        // Check if this column has page indexes
        if (column.column_index_offset && column.offset_index_offset) {
          const columnIndexOffset = Number(column.column_index_offset)
          const columnIndexLength = Number(column.column_index_length)
          const offsetIndexOffset = Number(column.offset_index_offset)
          const offsetIndexLength = Number(column.offset_index_length)

          locations.push({
            rowGroup: rgIndex,
            column: columnIdx,
            columnName,
            columnIndex: [columnIndexOffset, columnIndexOffset + columnIndexLength],
            offsetIndex: [offsetIndexOffset, offsetIndexOffset + offsetIndexLength],
            columnMetadata: column.meta_data,
          })
        }
      }
    })
  })

  return locations
}

/**
 * Process all column indexes and build page metadata
 * @param {IndexLocation[]} indexLocations
 * @param {ArrayBuffer[]} columnIndexData
 * @param {ArrayBuffer[]} offsetIndexData
 * @param {FileMetaData} metadata
 * @returns {PageData[]}
 */
function processAllColumnIndexes(indexLocations, columnIndexData, offsetIndexData, metadata) {
  const columnPageData = []

  indexLocations.forEach((location, i) => {
    const columnIndexReader = { view: new DataView(columnIndexData[i]), offset: 0 }
    const offsetIndexReader = { view: new DataView(offsetIndexData[i]), offset: 0 }
    const schemaPath = getSchemaPath(metadata.schema, location.columnMetadata?.path_in_schema ?? [])
    const schema = schemaPath.at(-1)?.element || { name: '' }

    const columnIndex = readColumnIndex(columnIndexReader, schema)
    const offsetIndex = readOffsetIndex(offsetIndexReader)

    // Create page data for each page in this column
    offsetIndex.page_locations.forEach((pageLocation, j) => {
      columnPageData.push({
        rowGroup: location.rowGroup,
        column: location.column,
        columnName: location.columnName,
        page: j,
        offset: Number(pageLocation.offset),
        length: Number(pageLocation.compressed_page_size),
        minValue: columnIndex.min_values[j],
        maxValue: columnIndex.max_values[j],
        columnMetadata: location.columnMetadata,
      })
    })
  })

  return columnPageData
}

/**
 * Find pages matching all predicates
 * @param {PageData[]} columnPageData
 * @param {Map<string, (min: any, max: any) => boolean>} predicates
 * @returns {PageData[]}
 */
function findPagesMatchingPredicates(columnPageData, predicates) {
  // If no predicates, return all pages
  if (predicates.size === 0) {
    return columnPageData
  }

  // Group pages by row group and page index
  const pageGroups = new Map()
  columnPageData.forEach(page => {
    const key = `${page.rowGroup}-${page.page}`
    if (!pageGroups.has(key)) {
      pageGroups.set(key, { pages: [], matchesPredicates: true })
    }
    pageGroups.get(key).pages.push(page)
  })

  // For each page group, check if ALL predicate columns match
  pageGroups.forEach((group) => {
    for (const [columnName, predicate] of predicates) {
      const columnPage = group.pages.find(p => p.columnName === columnName)
      if (columnPage) {
        // If this column has a page and it doesn't match the predicate, exclude this page group
        if (!predicate(columnPage.minValue, columnPage.maxValue)) {
          group.matchesPredicates = false
          break
        }
      }
    }
  })

  // Collect all pages from groups that match predicates
  const matchingPages = []
  pageGroups.forEach(group => {
    if (group.matchesPredicates) {
      matchingPages.push(...group.pages)
    }
  })

  return matchingPages
}

/**
 * Assemble complete row objects from multiple column pages
 * @param {AsyncBuffer} file
 * @param {PageData[]} matchingPages
 * @param {ArrayBuffer[]} pageBuffers
 * @param {string[]} outputColumns
 * @param {FileMetaData} metadata
 * @returns {Promise<Record<string, any>[]>}
 */
async function assembleRowsFromPages(file, matchingPages, pageBuffers, outputColumns, metadata) {
  const results = []

  // Group pages by row group and column to optimize dictionary reading
  const columnGroups = new Map()
  matchingPages.forEach((page, index) => {
    const columnKey = `${page.rowGroup}-${page.columnName}`
    if (!columnGroups.has(columnKey)) {
      columnGroups.set(columnKey, {
        columnName: page.columnName,
        columnMetadata: page.columnMetadata,
        pages: [],
        pageIndices: [],
      })
    }
    columnGroups.get(columnKey).pages.push(page)
    columnGroups.get(columnKey).pageIndices.push(index)
  })

  // Prepare dictionary reads
  const dictionaryRanges = []
  const dictionaryColumns = []
  columnGroups.forEach((group) => {
    if (group.columnMetadata?.dictionary_page_offset) {
      dictionaryRanges.push([
        Number(group.columnMetadata.dictionary_page_offset),
        Number(group.columnMetadata.data_page_offset),
      ])
      dictionaryColumns.push(group)
    }
  })

  // Read dictionaries
  const dictionaries = new Map()
  if (dictionaryRanges.length > 0) {
    const dictionaryBuffers = await sliceAll(file, dictionaryRanges)
    dictionaryColumns.forEach((group, index) => {
      const columnDecoder = createColumnDecoder(group.columnName, metadata.schema, group.columnMetadata)
      const dictionary = decodeDictionaryPage(dictionaryBuffers[index], columnDecoder)
      dictionaries.set(`${group.pages[0].rowGroup}-${group.columnName}`, dictionary)
    })
  }

  // Process pages grouped by row group and page index
  const pageGroups = groupBy(matchingPages, p => `${p.rowGroup}-${p.page}`)

  for (const pages of Object.values(pageGroups)) {
    const columnData = new Map()

    // Process each column's pages
    pages.forEach(page => {
      const pageIndex = matchingPages.indexOf(page)
      const pageBuffer = pageBuffers[pageIndex]
      const columnDecoder = createColumnDecoder(page.columnName, metadata.schema, page.columnMetadata)

      // Get dictionary if available
      const dictionaryKey = `${page.rowGroup}-${page.columnName}`
      const dictionary = dictionaries.get(dictionaryKey)

      // Decode the page
      const pageData = decodeDataPage(pageBuffer, columnDecoder, dictionary)

      // Accumulate values for this column
      if (!columnData.has(page.columnName)) {
        columnData.set(page.columnName, [])
      }
      const columnValues = columnData.get(page.columnName)
      if (Array.isArray(pageData)) {
        concat(columnValues, pageData)
      } else {
        columnValues.push(pageData)
      }
    })

    // Assemble rows from column data
    const maxRows = Math.max(...Array.from(columnData.values()).map(data => data.length))
    for (let i = 0; i < maxRows; i++) {
      const row = {}
      for (const column of outputColumns) {
        const data = columnData.get(column)
        row[column] = data && i < data.length ? data[i] : null
      }
      results.push(row)
    }
  }

  return results
}

// ============================================================================
// Utility functions
// ============================================================================

/**
 * MongoDB filter matching
 * @param {Record<string, any>} row
 * @param {ParquetQueryFilter} filter
 * @returns {boolean}
 */
function matchesFilter(row, filter) {
  if (!filter || typeof filter !== 'object') return true

  if (filter.$and) {
    return filter.$and.every(f => matchesFilter(row, f))
  }
  if (filter.$or) {
    return filter.$or.some(f => matchesFilter(row, f))
  }
  if (filter.$nor) {
    return !filter.$nor.some(f => matchesFilter(row, f))
  }
  if (filter.$not) {
    return !matchesFilter(row, filter.$not)
  }

  // Check individual column conditions
  for (const [key, condition] of Object.entries(filter)) {
    if (!key.startsWith('$')) {
      const value = row[key]
      const matches = matchesCondition(value, condition)
      if (!matches) {
        return false
      }
    }
  }
  return true
}

/**
 * Condition matching
 * @param {any} value
 * @param {any} condition
 * @returns {boolean}
 */
function matchesCondition(value, condition) {
  if (typeof condition !== 'object' || condition === null) {
    return equals(value, condition)
  }

  const { $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $not } = condition

  // Handle single-value conditions that should return immediately
  if ($eq !== undefined) return equals(value, $eq)
  if ($ne !== undefined) return !equals(value, $ne)
  if ($in !== undefined) return Array.isArray($in) && $in.some(v => equals(value, v))
  if ($nin !== undefined) return Array.isArray($nin) && !$nin.some(v => equals(value, v))
  if ($not !== undefined) return !matchesCondition(value, $not)

  // For range conditions, check ALL conditions (don't return early)
  let result = true
  if ($gt !== undefined) result = result && value > $gt
  if ($gte !== undefined) result = result && value >= $gte
  if ($lt !== undefined) result = result && value < $lt
  if ($lte !== undefined) result = result && value <= $lte

  return result
}

/**
 * Equality comparison
 * @param {any} a
 * @param {any} b
 * @returns {boolean}
 */
function equals(a, b) {
  if (a === b) return true
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((val, i) => equals(val, b[i]))
  }
  return false
}

/**
 * Sort rows
 * @param {Record<string, any>[]} rows
 * @param {string} orderBy
 * @param {boolean} desc
 */
function sortRows(rows, orderBy, desc) {
  rows.sort((a, b) => {
    const aVal = a[orderBy]
    const bVal = b[orderBy]

    // Handle nulls
    if (aVal === null && bVal === null) return (a.__index__ || 0) - (b.__index__ || 0)
    if (aVal === null) return desc ? -1 : 1
    if (bVal === null) return desc ? 1 : -1

    // Compare values
    let cmp = 0
    if (aVal < bVal) cmp = -1
    else if (aVal > bVal) cmp = 1
    else cmp = (a.__index__ || 0) - (b.__index__ || 0) // stable sort

    return desc ? -cmp : cmp
  })
}

/**
 * Project row to specific columns
 * @param {Record<string, any>} row
 * @param {string[]} columns
 * @returns {Record<string, any>}
 */
function projectRow(row, columns) {
  const projected = {}
  for (const col of columns) {
    projected[col] = row[col]
  }
  return projected
}

/**
 * Batch reading with fallback
 * @param {AsyncBuffer & {sliceAll?: (ranges: ([number, number] | null)[]) => Promise<ArrayBuffer[]>}} file
 * @param {([number, number] | null)[]} ranges
 * @returns {Promise<ArrayBuffer[]>}
 */
function sliceAll(file, ranges) {
  return file.sliceAll?.(ranges) ??
    Promise.all(ranges.map(range => range ? file.slice(range[0], range[1]) : Promise.resolve(new ArrayBuffer(0))))
}

/**
 * Group array by key function
 * @param {any[]} array
 * @param {(item: any) => string} keyFn
 * @returns {Record<string, any[]>}
 */
function groupBy(array, keyFn) {
  return array.reduce((acc, item) => {
    const key = keyFn(item)
    if (!acc[key]) acc[key] = []
    acc[key].push(item)
    return acc
  }, {})
}


/**
 * Create a ColumnDecoder for reading pages
 * @param {string} columnName
 * @param {SchemaElement[]} schema
 * @param {import('./types.js').ColumnMetaData | undefined} columnMetadata
 * @returns {import('./types.js').ColumnDecoder}
 */
function createColumnDecoder(columnName, schema, columnMetadata) {
  if (!columnMetadata) {
    throw new Error(`Column metadata not found for column ${columnName}`)
  }

  const schemaPath = getSchemaPath(schema, columnMetadata.path_in_schema || [])
  const element = schemaPath[schemaPath.length - 1]?.element
  if (!element) {
    throw new Error(`Schema element not found for column ${columnName}`)
  }

  return {
    columnName,
    type: element.type || 'INT32',
    element,
    schemaPath,
    codec: columnMetadata.codec || 'UNCOMPRESSED',
    parsers: DEFAULT_PARSERS,
    compressors: {},
    utf8: true,
  }
}

/**
 * Execute full row group scan when page indexes aren't available
 * @param {AsyncBuffer} file
 * @param {import('./types.js').RowGroup[]} matchingRowGroups - Row groups to read
 * @param {string[]} outputColumns
 * @param {import('./types.js').FileMetaData} metadata
 * @returns {Promise<Record<string, any>[]>}
 */
async function executeFullRowGroupScan(file, matchingRowGroups, outputColumns, metadata) {
  // If no specific row groups, read all data
  if (matchingRowGroups.length === metadata.row_groups.length) {
    return await parquetReadObjects({
      file,
      metadata,
      columns: outputColumns.length === metadata.schema.length - 1 ? undefined : outputColumns,
    })
  }

  // Calculate row ranges for matching row groups
  const rowRanges = calculateRowRanges(matchingRowGroups, metadata.row_groups)
  // Read data from each row range and combine results
  const allResults = []
  for (const [rowStart, rowEnd] of rowRanges) {
    const rangeData = await parquetReadObjects({
      file,
      metadata,
      rowStart,
      rowEnd,
      columns: outputColumns.length === metadata.schema.length - 1 ? undefined : outputColumns,
    })
    allResults.push(...rangeData)
  }

  return allResults
}

/**
 * Calculate consecutive row ranges for matching row groups to minimize I/O operations
 * @param {import('./types.js').RowGroup[]} matchingRowGroups - Row groups to read
 * @param {import('./types.js').RowGroup[]} allRowGroups - All row groups for row offset calculation
 * @returns {[number, number][]} Array of [rowStart, rowEnd] ranges
 */
function calculateRowRanges(matchingRowGroups, allRowGroups) {
  // Create a set of matching row group indices for quick lookup
  const matchingIndices = new Set()
  allRowGroups.forEach((rowGroup, index) => {
    if (matchingRowGroups.includes(rowGroup)) {
      matchingIndices.add(index)
    }
  })

  // Calculate row ranges, merging consecutive groups
  const ranges = []
  let currentRangeStart = null
  let currentRowOffset = 0

  for (let i = 0; i < allRowGroups.length; i++) {
    const rowGroup = allRowGroups[i]
    const groupRows = Number(rowGroup.num_rows)
    if (matchingIndices.has(i)) {
      // This row group should be included
      if (currentRangeStart === null) {
        currentRangeStart = currentRowOffset
      }
    } else {
      // This row group should not be included
      if (currentRangeStart !== null) {
        // Close the current range
        ranges.push(/** @type {[number, number]} */ [currentRangeStart, currentRowOffset])
        currentRangeStart = null
      }
    }

    currentRowOffset += groupRows
  }

  // Close any remaining open range
  if (currentRangeStart !== null) {
    ranges.push(/** @type {[number, number]} */ [currentRangeStart, currentRowOffset])
  }

  return ranges
}

/**
 * @typedef {Object} IndexLocation
 * @property {number} rowGroup
 * @property {number} column
 * @property {string} columnName
 * @property {[number, number]} columnIndex
 * @property {[number, number]} offsetIndex
 * @property {import('./types.js').ColumnMetaData | undefined} columnMetadata
 */

/**
 * @typedef {Object} PageData
 * @property {number} rowGroup
 * @property {number} column
 * @property {string} columnName
 * @property {number} page
 * @property {number} offset
 * @property {number} length
 * @property {any} minValue
 * @property {any} maxValue
 * @property {import('./types.js').ColumnMetaData | undefined} columnMetadata
 */
