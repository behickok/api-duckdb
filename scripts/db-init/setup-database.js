import duckdb from 'duckdb'
import path from 'path'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const dataDir = path.join(__dirname, '../../data')

const db = new duckdb.Database(':memory:')

function csv(name) {
  return path.join(dataDir, name).replace(/\\/g, '/')
}

function loadTable(table, file) {
  return `CREATE OR REPLACE TABLE ${table} AS SELECT * FROM read_csv_auto('${csv(file)}');`
}

async function main() {
  const tables = {
    INT_FRPAIR_RAW: 'frpair.csv',
    INT_FRPHOLD_RAW: 'frphold.csv',
    INT_FRPTRAN_RAW: 'frptran.csv',
    INT_FRPSEC_RAW: 'frpsec.csv',
    INT_FRPTCD_RAW:'frptcd.csv',
    INT_FRPSI1_RAW:'frpsi1.csv',
    INT_FRPINDX_RAW:'frpindx.csv',
    INT_FRPPRICE_RAW:'frpprice.csv',
    INT_FRPCTG_RAW:'frpctg.csv',
    INT_FRPAGG_RAW:'frpagg.csv'
  }
  const only = process.env.TABLE ? process.env.TABLE.toUpperCase() : null
  const statements = Object.entries(tables)
    .filter(([name]) => !only || only === name)
    .map(([name, file]) => loadTable(name, file))
    .join('\n')

  db.exec(statements)
  console.log('In-memory database initialized from CSV files.')
  db.close()
}

main().catch((err) => {
  console.error('Error initializing database', err)
  db.close()
})
