import { describe, expect, it } from 'vitest'
import { readColumn } from '../src/column.js'
import { parseDecimal } from '../src/convert.js'
import { parquetMetadataAsync } from '../src/hyparquet.js'
import { readColumnIndex, readOffsetIndex } from '../src/indicies.js'
import { getSchemaPath } from '../src/schema.js'
import { fileToAsyncBuffer } from './helpers.js'

describe('parquetReadIndices', () => {
  it('retrieves column index data and reads a single page', async () => {
    // given 'page_indices.parquet' we want to find the first page in the id column that has values between 250 and 300 (inclusive)
    const file = fileToAsyncBuffer('test/files/page_indices.parquet')
    const [columnName, minValue, maxValue] = ['id', 250, 300]

    const metadata = await parquetMetadataAsync(file)
    const columnNameIndex = metadata.schema.findIndex(el => el.name === columnName) - 1

    // determine which rowgroup and column contains our data
    const groupIndex = metadata.row_groups
      .map(group => group.columns[columnNameIndex].meta_data?.statistics)
      .findIndex(g => !(maxValue < Number(g?.min_value) || Number(g?.max_value) < minValue))
    const group = metadata.row_groups[groupIndex]
    const column = group.columns[columnNameIndex]

    // read the column index
    const columnIndexOffset = Number(column.column_index_offset)
    const columnIndexLength = Number(column.column_index_length)
    const columnIndexArrayBuffer = await file.slice(columnIndexOffset, columnIndexOffset + columnIndexLength)
    const columnIndexReader = { view: new DataView(columnIndexArrayBuffer), offset: 0 }
    const columnIndex = readColumnIndex(columnIndexReader, column)

    // read the offset index
    const offsetIndexOffset = Number(column.offset_index_offset)
    const offsetIndexLength = Number(column.offset_index_length)
    const offsetIndexArrayBuffer = await file.slice(offsetIndexOffset, offsetIndexOffset + offsetIndexLength)
    const offsetIndexReader = { view: new DataView(offsetIndexArrayBuffer), offset: 0 }
    const offsetIndex = readOffsetIndex(offsetIndexReader)

    // determine page value ranges
    const minValues = columnIndex.min_values.map(parseDecimal)
    const maxValues = columnIndex.max_values.map(parseDecimal)
    const pageValueRanges = minValues.map((_, i) => [minValues[i], maxValues[i]])

    // find the index of the first page that contains values intersecting the range
    const columnPageIndex = pageValueRanges.findIndex(([min, max]) => !(maxValue < min || max < minValue))

    // find out where the page is, based on the offset index
    const columnPageLocation = offsetIndex.page_locations[columnPageIndex]
    const columnPageOffset = Number(columnPageLocation.offset)
    const columnPageLength = Number(columnPageLocation.compressed_page_size)

    // set up a dataview containing the page data
    const pageArrayBuffer = await file.slice(columnPageOffset, columnPageOffset + columnPageLength)
    const pageReader = { view: new DataView(pageArrayBuffer), offset: 0 }

    // retrieve page data using readColumn - set rowLimit to 1 to read only the first page
    const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema || [])
    const columnData = readColumn(pageReader, 1, column.meta_data, schemaPath, {})

    // expect the page data to contain values within the specified range
    const filteredColumnData = columnData.filter(x => x >= minValue && x <= maxValue)
    expect(filteredColumnData.length).toBeGreaterThan(0)
  })
})
