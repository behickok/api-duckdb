// db.ts ---------------------------------------------------------------
import { DuckDBConnection } from '@duckdb/node-api';               // ✅ high-level API:contentReference[oaicite:0]{index=0}
import path from 'path';
import { fileURLToPath } from 'url';

const __filename  = fileURLToPath(import.meta.url);
const __dirname   = path.dirname(__filename);
const dataDir     = path.resolve(__dirname, '../scripts/db-init/data');

/** Initialise tables and seed starter rows.
 *  If you already have an open connection you can pass it in, otherwise
 *  we create an in-memory DB for you.
 */
export async function initializeDatabase(
  conn: DuckDBConnection | null = null
): Promise<DuckDBConnection> {
  const db = conn ?? await DuckDBConnection.create();             // async create() :contentReference[oaicite:1]{index=1}
  const sql = (s: string) => db.run(s);                            // helper → returns a Promise<Result>:contentReference[oaicite:2]{index=2}

  // Put everything in a single transaction for speed & atomicity
  await sql('BEGIN TRANSACTION');

  // --- sales ---------------------------------------------------------
  await sql(`
    CREATE TABLE IF NOT EXISTS sales(
      id          INTEGER,
      date        DATE,
      product     VARCHAR,
      category    VARCHAR,
      quantity    INTEGER,
      unit_price  DECIMAL(10,2),
      total_price DECIMAL(10,2)
    )`);

export function initializeDatabase(db: Database): void {
  const salesCsv = path.join(dataDir, 'sales.csv');
  const frpairCsv = path.join(dataDir, 'frpair.csv');
  const frpsecCsv = path.join(dataDir, 'frpsec.csv');
  const frpholdCsv = path.join(dataDir, 'frphold.csv');
  const frptranCsv = path.join(dataDir, 'frptran.csv');

    -- Insert sample data only if the table is empty
    INSERT INTO sales (id, date, product, category, quantity, unit_price, total_price)
    SELECT * FROM (VALUES
      (1, '2024-01-01', 'Laptop', 'Electronics', 5, 999.99, 4999.95),
      (2, '2024-01-02', 'Smartphone', 'Electronics', 10, 599.99, 5999.90),
      (3, '2024-01-03', 'T-shirt', 'Clothing', 20, 19.99, 399.80),
      (4, '2024-01-04', 'Book', 'Books', 15, 14.99, 224.85),
      (5, '2024-01-05', 'Headphones', 'Electronics', 8, 79.99, 639.92)
    ) AS new_data
    WHERE NOT EXISTS (SELECT 1 FROM sales);
  `)
}

// Pre-written queries you can feed straight to `connection.run()` or
// `connection.runAndReadAll()` etc.
export const QUERIES = {
  allSales : 'SELECT * FROM sales',
  salesSummary : `
      SELECT
        category,
        COUNT(*)           AS total_transactions,
        SUM(quantity)      AS total_quantity,
        SUM(total_price)   AS total_revenue
      FROM sales
      GROUP BY category`,
  dailySales : `
      SELECT
        date,
        COUNT(*)         AS transactions,
        SUM(total_price) AS daily_revenue
      FROM sales
      GROUP BY date
      ORDER BY date`
};
