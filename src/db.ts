// db.ts ---------------------------------------------------------------
import { DuckDBConnection } from '@duckdb/node-api';               // ✅ high-level API:contentReference[oaicite:0]{index=0}
import path from 'path';
import { fileURLToPath } from 'url';

<<<<<<< HEAD
import type { Database } from '@duckdb/node-api'
=======
const __filename  = fileURLToPath(import.meta.url);
const __dirname   = path.dirname(__filename);
const dataDir     = path.resolve(__dirname, '../scripts/db-init/data');
>>>>>>> 723ef31 (Maybe fixing?)

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

<<<<<<< HEAD
export function initializeDatabase(db: Database): void {
  const salesCsv = path.join(dataDir, 'sales.csv');
  const frpairCsv = path.join(dataDir, 'frpair.csv');
  const frpsecCsv = path.join(dataDir, 'frpsec.csv');
  const frpholdCsv = path.join(dataDir, 'frphold.csv');
  const frptranCsv = path.join(dataDir, 'frptran.csv');

=======
  await sql(`COPY sales FROM '${path.join(dataDir, 'sales.csv')}' (HEADER true)`);
>>>>>>> 723ef31 (Maybe fixing?)

  // --- frpair --------------------------------------------------------
  await sql(`
    CREATE TABLE IF NOT EXISTS frpair(
      acct      VARCHAR(14),
      name      VARCHAR(50),
      fye       INTEGER,
      icpdated  DATE,
      active    VARCHAR(20)
    )`);

  await sql(`COPY frpair FROM '${path.join(dataDir, 'frpair.csv')}' (HEADER true)`);

  // --- frpsec --------------------------------------------------------
  await sql(`
    CREATE TABLE IF NOT EXISTS frpsec(
      id       VARCHAR(255),
      nametkr  VARCHAR(255),
      ticker   VARCHAR(50),
      cusip    VARCHAR(9)
    )`);

<<<<<<< HEAD
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
=======
  await sql(`COPY frpsec FROM '${path.join(dataDir, 'frpsec.csv')}' (HEADER true)`);

  // --- frphold seed rows --------------------------------------------
  await sql(`
    CREATE TABLE IF NOT EXISTS frphold(
      aacct       VARCHAR(14),
      hid         VARCHAR(255),
      adate       VARCHAR(6),
      hdirect1    VARCHAR(255),
      hunits      DOUBLE,
      hprincipal  DOUBLE,
      haccrual    DOUBLE,
      PRIMARY KEY(aacct, hid, adate)
    )`);

  await sql(`
    INSERT INTO frphold
    SELECT * FROM (VALUES
      ('ACC1001','SEC001','202401','Equity',100,15000,0.0),
      ('ACC1002','SEC002','202401','Equity',200,25000,0.0)
    ) AS v(aacct,hid,adate,hdirect1,hunits,hprincipal,haccrual)
    WHERE NOT EXISTS (SELECT 1 FROM frphold)`);

  // --- frptran seed rows --------------------------------------------
  await sql(`
    CREATE TABLE IF NOT EXISTS frptran(
      aacct       VARCHAR(14),
      hid         VARCHAR(255),
      adate       VARCHAR(6),
      tdate       DATE,
      tcode       VARCHAR(255),
      tunits      DOUBLE,
      tprincipal  DOUBLE,
      tincome     DOUBLE,
      fee         DOUBLE,
      PRIMARY KEY(aacct, hid, tdate, tcode)
    )`);

  await sql(`
    INSERT INTO frptran
    SELECT * FROM (VALUES
      ('ACC1001','SEC001','202401','2024-01-15','BUY',50, 7500,0,10.0),
      ('ACC1002','SEC002','202401','2024-01-20','BUY',75,18750,0,15.0)
    ) AS v(aacct,hid,adate,tdate,tcode,tunits,tprincipal,tincome,fee)
    WHERE NOT EXISTS (SELECT 1 FROM frptran)`);

  await sql('COMMIT');                                              // end transaction
  return db;
>>>>>>> 723ef31 (Maybe fixing?)
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
