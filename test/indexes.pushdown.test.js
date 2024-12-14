import { describe, expect, it } from 'vitest'
import { readPage } from '../src/column.js'
import { parquetMetadataAsync } from '../src/hyparquet.js'
import { readColumnIndex, readOffsetIndex } from '../src/indexes.js'
import { getSchemaPath } from '../src/schema.js'
import { asyncBufferFromFile, concat } from '../src/utils.js'
import { concatenateArrayBuffers, groupBy, largeFileToAsyncBuffer } from './helpers.js'

describe('parquetReadIndices', () => {
  it('retrieves column index data and reads a single page', async () => {
    // given 'page_indices.parquet' we want to find the first page in the id column that could have values between 250 and 300 (inclusive)
    // note that this will not fetch the any additional pages that may also fulfill the predicate
    const file = await asyncBufferFromFile('test/files/page_indexes.parquet')
    const [columnName, minValue, maxValue] = ['id', 250, 300]
    const predicate = (min, max) => +min <= maxValue && minValue <= +max

    const metadata = await parquetMetadataAsync(file)
    const columnNameIndex = metadata.schema.findIndex(el => el.name === columnName) - 1

    // find the row group and column containing values that fulfill the predicate
    const groupIndex = metadata.row_groups
      .map(group => group.columns[columnNameIndex].meta_data?.statistics)
      .findIndex(g => predicate(g?.min_value, g?.max_value))
    const group = metadata.row_groups[groupIndex]
    const column = group.columns[columnNameIndex]

    // read the column index
    const columnIndexOffset = Number(column.column_index_offset)
    const columnIndexLength = Number(column.column_index_length)
    const columnIndexArrayBuffer = await file.slice(columnIndexOffset, columnIndexOffset + columnIndexLength)
    const columnIndexReader = { view: new DataView(columnIndexArrayBuffer), offset: 0 }
    const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema || [])
    const schema = schemaPath.at(-1)?.element || { name: '' }
    const columnIndex = readColumnIndex(columnIndexReader, schema)

    // read the offset index
    const offsetIndexOffset = Number(column.offset_index_offset)
    const offsetIndexLength = Number(column.offset_index_length)
    const offsetIndexArrayBuffer = await file.slice(offsetIndexOffset, offsetIndexOffset + offsetIndexLength)
    const offsetIndexReader = { view: new DataView(offsetIndexArrayBuffer), offset: 0 }
    const offsetIndex = readOffsetIndex(offsetIndexReader)

    // find the location of the first page containing values that fulfill the predicate
    const pageLocation = offsetIndex.page_locations.find((_, i) => predicate(columnIndex.min_values[i], columnIndex.max_values[i]))

    // set up a dataview containing the page data
    const pageOffset = Number(pageLocation.offset)
    const pageLength = Number(pageLocation.compressed_page_size)
    const pageArrayBuffer = await file.slice(pageOffset, pageOffset + pageLength)
    const pageReader = { view: new DataView(pageArrayBuffer), offset: 0 }

    // we used to retrieve page data using readColumn - we set rowLimit to 1 to read only the first page
    // however, since the introduction of rowData.length = rowLimit, we can no longer do this as it will truncate the data
    // so we now use readPage instead (basically just readColumn without a rowLimit and length checks)
    const pageData = readPage(pageReader, column.meta_data, schemaPath, {})

    // expect the page data to contain values that fulfill the predicate
    const filteredPageData = pageData.filter(x => predicate(x, x))
    expect(filteredPageData.length).toBeGreaterThan(0)
  })

  it('performs page-level predicate pushdown and retrieves data from adjacent columns', async () => {
    const file = await largeFileToAsyncBuffer('test/files/page_indexes.parquet')

    // define columns to read
    const columns = ['id', 'chromosome', 'position_grch37', 'position_grch38', 'function', 'type']

    // define the predicate
    const predicateColumn = 'id'
    const predicateValues = [3, 5, 10]
    const predicate = (min, max) => predicateValues.some(v => min <= v && v <= max)

    // read metadata
    const metadata = await parquetMetadataAsync(file)
    const findColumnIndex = name => metadata.schema.findIndex(el => el.name === name) - 1
    const columnNameIndexes = columns.map(findColumnIndex)
    const predicateColumnIndex = findColumnIndex(predicateColumn)
    const resultsPredicateColumnIndex = columns.indexOf(predicateColumn)

    // find row groups containing values that fulfill the predicate
    const rowGroups = metadata.row_groups.filter(group => {
      const { statistics } = group.columns[predicateColumnIndex].meta_data
      return predicate(statistics.min_value, statistics.max_value)
    })

    // find column index, offset index, and dictionary locations for each row group's columns
    const indexLocations = rowGroups.map((rowGroup) => columnNameIndexes.map(columnNameIndex => {
      const rowGroupIndex = metadata.row_groups.indexOf(rowGroup)
      const column = rowGroup.columns[columnNameIndex]
      const columnIndexOffset = Number(column.column_index_offset)
      const columnIndexLength = Number(column.column_index_length)
      const offsetIndexOffset = Number(column.offset_index_offset)
      const offsetIndexLength = Number(column.offset_index_length)
      const dictionaryOffset = Number(column.meta_data?.dictionary_page_offset)
      const dataPageOffset = Number(column.meta_data?.data_page_offset)

      return {
        rowGroup: rowGroupIndex,
        column: columnNameIndex,
        columnIndex: [columnIndexOffset, columnIndexOffset + columnIndexLength],
        offsetIndex: [offsetIndexOffset, offsetIndexOffset + offsetIndexLength],
        dictionary: dictionaryOffset ? [dictionaryOffset, dataPageOffset] : null,
      }
    })).flat()

    // fetch column index, offset index, and dictionary data for each column
    const [ columnIndexData, offsetIndexData, dictionaryData ] = await Promise.all([
      file.sliceAll(indexLocations.map(location => location.columnIndex)),
      file.sliceAll(indexLocations.map(location => location.offsetIndex)),
      file.sliceAll(indexLocations.map(location => location.dictionary)),
    ])

    // read column index and offset index data
    const columnPageData = indexLocations.map((location, i) => {
      const columnIndexReader = { view: new DataView(columnIndexData[i]), offset: 0 }
      const offsetIndexReader = { view: new DataView(offsetIndexData[i]), offset: 0 }
      const column = metadata.row_groups[location.rowGroup].columns[location.column]
      const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema ?? [])
      const schema = schemaPath.at(-1)?.element || { name: '' }

      const columnIndex = readColumnIndex(columnIndexReader, schema)
      const offsetIndex = readOffsetIndex(offsetIndexReader)

      return offsetIndex.page_locations.map((pageLocation, j) => ({
        rowGroup: location.rowGroup,
        column: location.column,
        page: j,
        dictionary: dictionaryData[i],
        offset: Number(pageLocation.offset),
        length: Number(pageLocation.compressed_page_size),
        minValue: columnIndex.min_values[j],
        maxValue: columnIndex.max_values[j],
      }))
    }).flat()

    // find pages containing values that fulfill the predicate
    const filteredColumnPageData = columnPageData.filter(p => p.column === predicateColumnIndex && predicate(p.minValue, p.maxValue))
    const rowGroupPages = groupBy(filteredColumnPageData, p => p.rowGroup)
    const pages = columnPageData.filter(page => rowGroupPages[page.rowGroup].find(rp => rp.page === page.page))
    // fetch and parse pages
    const pageData = await file.sliceAll(pages.map(page => [page.offset, page.offset + page.length]))
    const parsedPages = pages.map((page, i) => {
      const pageArrayBuffer = concatenateArrayBuffers([page.dictionary, pageData[i]])
      const pageReader = { view: new DataView(pageArrayBuffer), offset: 0 }
      const column = metadata.row_groups[page.rowGroup].columns[page.column]
      const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema || [])
      return { page, data: readPage(pageReader, column.meta_data, schemaPath, {}) }
    }).sort((a, b) => a.page.rowGroup - b.page.rowGroup || a.page.page - b.page.page || a.page.column - b.page.column)

    /** @type any[][] */
    const data = []

    // account for column order in each row group
    const columnOrderValues = Array.from(new Set(columnNameIndexes)).sort((a, b) => a - b)
    const columnOrder = columnNameIndexes.map(value => columnOrderValues.indexOf(value))

    for (let i = 0; i < parsedPages.length; i += columns.length) {
      const rowGroupData = new Array(parsedPages[i].data.length).fill(null).map(() => new Array(columns.length).fill(null))
      console.log(rowGroupData)
      for (let j = 0; j < columns.length; j++) {
        const columnData = parsedPages[i + j].data
        for (let k = 0; k < columnData.length; k++) {
          rowGroupData[k][columnOrder[j]] = columnData[k]
        }
      }
      concat(data, rowGroupData.filter(row => predicate(row[resultsPredicateColumnIndex], row[resultsPredicateColumnIndex])))
    }

    expect(data.length).toEqual(predicateValues.length)
  })

})
