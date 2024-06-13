import { describe, expect, it } from 'vitest'
import { readColumn } from '../src/column.js'
import { parquetMetadataAsync } from '../src/hyparquet.js'
import { readColumnIndex, readOffsetIndex } from '../src/indicies.js'
import { getSchemaPath } from '../src/schema.js'
import { fileToAsyncBuffer } from './helpers.js'

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
    const columnIndex = readColumnIndex(columnIndexReader, column.meta_data)

    // read the offset index
    const offsetIndexOffset = Number(column.offset_index_offset)
    const offsetIndexLength = Number(column.offset_index_length)
    const offsetIndexArrayBuffer = await file.slice(offsetIndexOffset, offsetIndexOffset + offsetIndexLength)
    const offsetIndexReader = { view: new DataView(offsetIndexArrayBuffer), offset: 0 }
    const offsetIndex = readOffsetIndex(offsetIndexReader)

    // find the location of the first page containing values that fulfill the predicate
    const pageIndex = columnIndex.min_values.findIndex((_, i) => predicate(columnIndex.min_values[i], columnIndex.max_values[i]))
    const pageLocation = offsetIndex.page_locations[pageIndex]

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
})
