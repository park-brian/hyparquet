import { describe, expect, it } from 'vitest'
import { readColumn } from '../src/column.js'
import { parquetMetadataAsync } from '../src/hyparquet.js'
import { readColumnIndex, readOffsetIndex } from '../src/indicies.js'
import { getSchemaPath } from '../src/schema.js'
import { fileToAsyncBuffer, largeFileToAsyncBuffer, remoteFileToAsyncBuffer, groupBy, concatenateArrayBuffers } from './helpers.js'

describe('parquetReadIndices', () => {
  it('retrieves column index data and reads a single page', async () => {
    // given 'page_indices.parquet' we want to find the first page in the id column that has values between 250 and 300 (inclusive)
    const file = fileToAsyncBuffer('test/files/page_indices.parquet')

    const [columnName, minValue, maxValue] = ['id', 250, 300]
    const predicate = (min, max) => !(maxValue < Number(min) || Number(max) < minValue)

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
    const columnIndex = readColumnIndex(columnIndexReader, metadata.schema[columnNameIndex + 1])

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

    // retrieve page data using readColumn - set rowLimit to 1 to read only the first page
    const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema)
    const pageData = readColumn(pageReader, 1, column.meta_data, schemaPath, {})

    // expect the page data to contain values that fulfill the predicate
    const filteredPageData = pageData.filter(x => predicate(x, x))
    expect(filteredPageData.length).toBeGreaterThan(0)
  })

  it('performs page-level predicate pushdown and retrieves data from adjacent columns', async () => {
    // given 'page_indices.parquet' we want to find the first page in the id column that has values between 250 and 300 (inclusive)
    const file = largeFileToAsyncBuffer('test/files/page_indices.parquet')
    // const file = await remoteFileToAsyncBuffer('http://localhost:8000/page_indices_large.parquet') // uncomment to test with large file

    // define columns to read
    const columns = ['id', 'chromosome', 'position_grch37', 'position_grch38', 'function', 'type']

    // define the predicate
    const predicateColumn = 'id'
    const predicateValues = [3, 5, 10]
    const predicate = (min, max) => predicateValues.some(v => min <= v && v <= max)

    // read metadata
    const metadata = await parquetMetadataAsync(file)
    const findColumnIndex = name => metadata.schema.findIndex(el => el.name === name) - 1
    const predicateColumnIndex = findColumnIndex(predicateColumn)
    const columnNameIndexes = columns.map(findColumnIndex)

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
      file.sliceBatch(indexLocations.map(location => location.columnIndex)),
      file.sliceBatch(indexLocations.map(location => location.offsetIndex)),
      file.sliceBatch(indexLocations.map(location => location.dictionary)),
    ])

    // read column index and offset index data
    const columnPageData = indexLocations.map((location, i) => {
      const columnIndexReader = { view: new DataView(columnIndexData[i]), offset: 0 }
      const offsetIndexReader = { view: new DataView(offsetIndexData[i]), offset: 0 }
      const columnIndex = readColumnIndex(columnIndexReader, metadata.schema[location.column + 1])
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
    const pageData = await file.sliceBatch(pages.map(page => [page.offset, page.offset + page.length]))
    const parsedPages = pages.map((page, i) => {
      const pageArrayBuffer = concatenateArrayBuffers([page.dictionary, pageData[i]])
      const pageReader = { view: new DataView(pageArrayBuffer), offset: 0 }
      const column = metadata.row_groups[page.rowGroup].columns[page.column]
      const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema)
      return { page, data: readColumn(pageReader, 1, column.meta_data, schemaPath, {}) }
    }).sort((a, b) => a.page.rowGroup - b.page.rowGroup || a.page.column - b.page.column || a.page.page - b.page.page)

    // determine the number of rows and columns in the combined page data
    const totalRows = parsedPages.reduce((acc, page) => acc + page.data.length, 0) / columns.length
    const totalColumns = columns.length
    const data = new Array(totalRows).fill(null).map(() => new Array(totalColumns).fill(null))

    // rearrange page data chunks into a 2D array
    for (let i = 0; i < parsedPages.length; i ++) {
      const currentPage = parsedPages[i]
      const previousPages = parsedPages.slice(0, i).filter(p => p.page.column === currentPage.page.column)
      const rowOffset = previousPages.reduce((acc, p) => acc + p.data.length, 0)
      const columnOffset = columnNameIndexes.indexOf(currentPage.page.column)
      for (let j = 0; j < currentPage.data.length; j++) {
        data[rowOffset + j][columnOffset] = currentPage.data[j]
      }
    }

    // find records that fulfill the predicate
    const resultsPredicateColumnIndex = columns.indexOf(predicateColumn)
    const filteredData = data.filter(row => predicate(row[resultsPredicateColumnIndex], row[resultsPredicateColumnIndex]))
    expect(filteredData.length).toEqual(predicateValues.length)
    expect(filteredData?.[0]?.length).toEqual(columns.length)
  })
})
