import { Database } from 'duckdb'
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const dataDir = path.resolve(__dirname, '../data')

function csv(name: string): string {
  return path.join(dataDir, name).replace(/\\/g, '/')
}

export function initializeDatabase(db: Database): void {
  const sql = `
    CREATE OR REPLACE TABLE sales AS
      SELECT * FROM read_csv_auto('${csv('sales.csv')}');

    CREATE OR REPLACE TABLE FRPAIR AS
      SELECT * FROM read_csv_auto('${csv('frpair.csv')}');

    CREATE OR REPLACE TABLE FRPSEC AS
      SELECT * FROM read_csv_auto('${csv('frpsec.csv')}');

    CREATE OR REPLACE TABLE FRPHOLD AS
      SELECT * FROM read_csv_auto('${csv('frphold.csv')}');

    CREATE OR REPLACE TABLE FRPTRAN AS
      SELECT * FROM read_csv_auto('${csv('frptran.csv')}');
  `

  db.exec(sql)
}

export function runMigrations(db: Database): void {
  const migrationsDir = path.resolve(__dirname, '../migrations')
  if (!fs.existsSync(migrationsDir)) return

  const files = fs
    .readdirSync(migrationsDir)
    .filter((f) => fs.statSync(path.join(migrationsDir, f)).isFile())
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))

  for (const file of files) {
    const sql = fs.readFileSync(path.join(migrationsDir, file), 'utf-8')
    db.exec(sql)
  }
}

export const QUERIES = {
  allSales: 'SELECT * FROM sales',
  salesSummary: `
    SELECT
      category,
      COUNT(*) as total_transactions,
      SUM(quantity) as total_quantity,
      SUM(total_price) as total_revenue
    FROM sales
    GROUP BY category
  `,
  dailySales: `
    SELECT
      date,
      COUNT(*) as transactions,
      SUM(total_price) as daily_revenue
    FROM sales
    GROUP BY date
    ORDER BY date
  `
}