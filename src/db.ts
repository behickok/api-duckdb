import { Database } from 'duckdb'
import path from 'path'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)

const dataDir = path.resolve(__dirname, '../scripts/db-init/data')


export function initializeDatabase(db: Database): void {
  const salesCsv = path.join(dataDir, 'sales.csv');
  const frpairCsv = path.join(dataDir, 'frpair.csv');
  const frpsecCsv = path.join(dataDir, 'frpsec.csv');

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
    INSERT INTO FRPHOLD (AACCT, HID, ADATE, HDIRECT1, HUNITS, HPRINCIPAL, HACCRUAL)
    SELECT * FROM (VALUES
      ('ACC1001', 'SEC001', '202401', 'Equity', 100, 15000, 0.0),
      ('ACC1002', 'SEC002', '202401', 'Equity', 200, 25000, 0.0)
    ) AS new_data(AACCT, HID, ADATE, HDIRECT1, HUNITS, HPRINCIPAL, HACCRUAL)
    WHERE NOT EXISTS (SELECT 1 FROM FRPHOLD);

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
    INSERT INTO FRPTRAN (AACCT, HID, ADATE, TDATE, TCODE, TUNITS, TPRINCIPAL, TINCOME, FEE)
    SELECT * FROM (VALUES
      ('ACC1001', 'SEC001', '202401', '2024-01-15', 'BUY', 50, 7500, 0, 10.0),
      ('ACC1002', 'SEC002', '202401', '2024-01-20', 'BUY', 75, 18750, 0, 15.0)
    ) AS new_data(AACCT, HID, ADATE, TDATE, TCODE, TUNITS, TPRINCIPAL, TINCOME, FEE)
    WHERE NOT EXISTS (SELECT 1 FROM FRPTRAN);
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