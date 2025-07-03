import type { Database } from '@duckdb/node-api'
import path from 'path'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const dataDir = path.resolve(__dirname, '../scripts/db-init/data')

export function initializeDatabase(db: Database): void {
  const salesCsv = path.join(dataDir, 'sales.csv');
  const frpairCsv = path.join(dataDir, 'frpair.csv');
  const frpsecCsv = path.join(dataDir, 'frpsec.csv');
  const frpholdCsv = path.join(dataDir, 'frphold.csv');
  const frptranCsv = path.join(dataDir, 'frptran.csv');

  db.exec(`
    CREATE TABLE IF NOT EXISTS sales (
      id INTEGER PRIMARY KEY,
      date DATE,
      product VARCHAR,
      category VARCHAR,
      quantity INTEGER,
      unit_price DECIMAL(10, 2),
      total_price DECIMAL(10, 2)
    );
    COPY sales FROM '${salesCsv}' (HEADER TRUE);

    CREATE TABLE IF NOT EXISTS FRPAIR (
      ACCT VARCHAR(14) PRIMARY KEY,
      NAME VARCHAR(50),
      FYE INTEGER,
      ICPDATED DATE,
      ACTIVE VARCHAR(20)
    );
    COPY FRPAIR FROM '${frpairCsv}' (HEADER TRUE);

    CREATE TABLE IF NOT EXISTS FRPSEC (
      ID VARCHAR(255) PRIMARY KEY,
      NAMETKR VARCHAR(255),
      TICKER VARCHAR(50),
      CUSIP VARCHAR(9)
    );
    COPY FRPSEC FROM '${frpsecCsv}' (HEADER TRUE);

      CREATE TABLE IF NOT EXISTS FRPHOLD (
        AACCT VARCHAR(14),
        HID VARCHAR(255),
        ADATE VARCHAR(6),
        HDIRECT1 VARCHAR(255),
        HUNITS DOUBLE,
        HPRINCIPAL DOUBLE,
        HACCRUAL DOUBLE,
        PRIMARY KEY (AACCT, HID, ADATE)
      );
      COPY FRPHOLD FROM '${frpholdCsv}' (HEADER TRUE);

      CREATE TABLE IF NOT EXISTS FRPTRAN (
        AACCT VARCHAR(14),
        HID VARCHAR(255),
        ADATE VARCHAR(6),
        TDATE DATE,
        TCODE VARCHAR(255),
        TUNITS DOUBLE,
        TPRINCIPAL DOUBLE,
        TINCOME DOUBLE,
        FEE DOUBLE,
        PRIMARY KEY (AACCT, HID, TDATE, TCODE)
      );
      COPY FRPTRAN FROM '${frptranCsv}' (HEADER TRUE);
  `)
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