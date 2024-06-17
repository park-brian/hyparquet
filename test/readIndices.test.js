import { describe, expect, it } from 'vitest'
import { readColumn } from '../src/column.js'
import { parquetMetadataAsync } from '../src/hyparquet.js'
import { readColumnIndex, readOffsetIndex } from '../src/indicies.js'
import { getSchemaPath } from '../src/schema.js'
import { concat } from '../src/utils.js'
import { concatenateArrayBuffers, fileToAsyncBuffer, groupBy, largeFileToAsyncBuffer, remoteFileToAsyncBuffer } from './helpers.js'

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
    const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema)
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

    // retrieve page data using readColumn - set rowLimit to 1 to read only the first page
    const pageData = readColumn(pageReader, 1, column.meta_data, schemaPath, {})

    // expect the page data to contain values that fulfill the predicate
    const filteredPageData = pageData.filter(x => predicate(x, x))
    expect(filteredPageData.length).toBeGreaterThan(0)
  })

  it('performs page-level predicate pushdown and retrieves data from adjacent columns', async () => {
    // given 'page_indices.parquet' we want to find the first page in the id column that has values between 250 and 300 (inclusive)
    const file = largeFileToAsyncBuffer('test/files/page_indices.parquet')
    // const file = await remoteFileToAsyncBuffer('http://localhost:8082/dbsnp.v14.parquet') // uncomment to test with large file

    // define columns to read
    const columns = ['id', 'chromosome', 'position_grch37', 'position_grch38', 'function', 'type']

    // define the predicate
    const predicateColumn = 'id'
    const predicateValues = [3, 5, 10]
    // const predicateValues = [6707874, 7599470, 71430382, 4538241, 7608524, 4234096, 1106639, 79626458, 78147778, 77984138, 62192037, 146871826, 62192027, 35305862, 4449174, 2002092, 6746151, 6754066, 7582691, 77062581, 78350525, 77433658, 62192024, 35061832, 62193068, 4073694, 28631456, 28605921, 10933568, 10933569, 12998967, 10755057, 7582582, 6733823, 71430385, 34762429, 6747623, 35350837, 6756901, 6740752, 7576068, 35359354, 34077392, 35249772, 143628759, 13408030, 741776, 35360790, 4425108, 744986, 10184669, 35235166, 62193073, 35320439, 34767042, 35070043, 141343442, 6715894, 4675882, 62192009, 113730074, 62192002, 35532480, 6721985, 62192013, 34293993, 4075957, 34647532, 62192003, 113047262, 4234097, 34519321, 78280775, 4675883, 4366942, 62192004, 35386923, 62192005, 62192001, 60611916, 62191982, 4073890, 4073889, 10171845, 145967151, 34337830, 66924973, 35964352, 66492541, 148541768, 4675886, 5001228, 62193122, 199568359, 62193123, 150400910, 62641586, 111792955, 150107870, 73010174, 111543205, 11552660, 35671465, 62193130, 55677174, 62193087, 2293760, 62193131, 62193129, 78536152, 11545301, 35301767, 1553620400, 35218641, 80241045, 3749158, 79248835, 28528975, 141906940, 114368048, 759314645, 41438146, 73014052, 4075467, 116047226, 35715487, 3892357, 34211035, 149204343, 4443030, 66668102, 7569717, 62193111, 62193116, 34980723, 75517014, 73011976, 62193115, 34346880, 139344622, 77940364, 143441946, 73011950, 760133, 73011977, 111941933, 142205045, 112910522, 11676038, 9973663, 73106166, 73011964, 201043890, 149870990, 7370843, 73014012, 73011956, 7577590, 34707996, 62193125, 4549127, 34339758, 76356158, 35039363, 36111671, 34071003, 62193093, 35670872, 62193128, 62193119, 60185457, 71351116, 35670545, 7420347, 3749157, 62193117, 2293759, 4413188, 78707837, 2293763, 2293761, 7421861, 5839829, 7419333, 6721711, 3749153, 3749154, 35449025, 76100726, 41414844, 62191168, 66492261, 34189210, 35214534, 16747, 62191174, 62191175, 35788187, 55829775, 185303382, 28548938, 28756669, 34761692, 112414038, 73113420, 28368848, 28501156, 7580133, 28425751, 28490306, 28896590, 28377719, 147705388, 28366438, 28570544, 28539662, 7595205, 67964063, 62191977, 62191169, 112247677, 66482163, 73016194, 2002091, 6713318, 7420202, 28559662, 28463046, 62191173, 7425592, 77563040, 113437814, 76381606, 74608291, 111295396, 62620233, 75740353, 35399295, 67727847, 74603454, 78145032, 78073278, 140839849, 150571294, 73014018, 28484736, 28520318, 73014070, 112111808, 10208011, 76379979, 775956358, 36065626, 68140142, 78620448, 114106326, 4444571, 114859158, 78008916, 4075279, 7573250, 4675885, 148543601, 139013602, 141344799, 147811780, 546752536, 115963514, 77111076, 1106416, 62191172, 62193121, 141016633, 376186280, 7560086, 141711339, 111957722, 112058555, 36072737, 577163648, 139276818, 139610220, 180716604, 34924235, 140549311, 74594321, 62193090, 7370514, 60946542, 142078977, 145807836, 55912101, 147210645, 3934496, 62191182, 146578303, 11899246, 34623950, 13011933, 66521038, 12993969, 34590332, 13015845, 143940595, 147835654, 11677068, 11677070, 62191312, 62192015, 6737028, 4973668, 12992932, 115230843, 115874667, 35985230, 12052582, 13018275, 140867577, 150044832, 35611507, 532321747, 141130385, 368652561, 116399465, 78538207, 3934504, 114826745, 150406548, 62192029, 148710832, 116596424, 138811002, 144379709, 78780160, 145451947, 6751010, 569665203, 146566663, 35344581, 28680420, 4428044, 147531749, 145401039, 80304737, 138277491, 73011960, 78423262, 568108685, 73011971, 111308487, 12998368, 139762431, 369447346, 79903379, 568027684, 4073891, 34336836, 7420143, 6437284, 189911709, 62191225, 79455001, 149148950, 541154253, 768710784, 144020262, 148537529, 62193127, 188879226, 542577031, 12988887, 33999075, 13024084, 6605274, 186009260, 139335428, 34222421, 112011439, 6747186, 58164922, 6727713, 112246762, 7585192, 111584532, 7581318, 903317307, 34076412, 62192202, 4973661, 62191961, 145834243, 114496196, 3749152, 150106715, 78358334, 7425282, 56043607, 10196834, 115923645, 6713471]
    const predicate = (min, max) => predicateValues.some(v => min <= v && v <= max)

    // read metadata
    const metadata = await parquetMetadataAsync(file)
    const findColumnIndex = name => metadata.schema.findIndex(el => el.name === name) - 1
    const predicateColumnIndex = findColumnIndex(predicateColumn)
    const columnNameIndexes = columns.map(findColumnIndex)
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
      file.sliceBatch(indexLocations.map(location => location.columnIndex)),
      file.sliceBatch(indexLocations.map(location => location.offsetIndex)),
      file.sliceBatch(indexLocations.map(location => location.dictionary)),
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
    const pageData = await file.sliceBatch(pages.map(page => [page.offset, page.offset + page.length]))
    const parsedPages = pages.map((page, i) => {
      const pageArrayBuffer = concatenateArrayBuffers([page.dictionary, pageData[i]])
      const pageReader = { view: new DataView(pageArrayBuffer), offset: 0 }
      const column = metadata.row_groups[page.rowGroup].columns[page.column]
      const schemaPath = getSchemaPath(metadata.schema, column.meta_data?.path_in_schema)
      return { page, data: readColumn(pageReader, 1, column.meta_data, schemaPath, {}) }
    }).sort((a, b) => a.page.rowGroup - b.page.rowGroup || a.page.page - b.page.page || a.page.column - b.page.column)

    /** @type any[][] */
    const data = []

    // account for column order in each row group
    const columnOrderValues = Array.from(new Set(columnNameIndexes)).sort((a, b) => a - b)
    const columnOrder = columnNameIndexes.map(value => columnOrderValues.indexOf(value))

    for (let i = 0; i < parsedPages.length; i += columns.length) {
      const rowGroupData = new Array(parsedPages[i].data.length).fill(null).map(() => new Array(columns.length).fill(null))
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
