import { Database } from 'duckdb';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { promisify } from 'util';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const dataDir = path.resolve(__dirname, '../data');

/**
 * Generates a clean, forward-slash-based path for a given CSV file.
 * @param name The name of the CSV file.
 * @returns The resolved path to the CSV file.
 */
function csv(name: string): string {
  const filePath = path.join(dataDir, name).replace(/\\/g, '/');
  console.log(`[INFO] Resolving path for '${name}' to: ${filePath}`);
  return filePath;
}

/**
 * Initializes the database by creating tables from CSV files.
 * @param db The DuckDB database instance.
 */
export function initializeDatabase(db: Database): void {
  console.log('[INFO] Starting database initialization...');
  try {
    const tableCreationSQL = `
      CREATE OR REPLACE TABLE sales AS
        SELECT * FROM read_csv_auto('${csv('sales.csv')}');

      CREATE OR REPLACE TABLE INT_FRPAIR_RAW AS
        SELECT * FROM read_csv_auto('${csv('frpair.csv')}');

      CREATE OR REPLACE TABLE INT_FRPSEC_RAW AS
        SELECT * FROM read_csv_auto('${csv('frpsec.csv')}');

      CREATE OR REPLACE TABLE INT_FRPHOLD_RAW AS
        SELECT * FROM read_csv('${csv('frphold.csv')}', HEADER=TRUE, AUTO_DETECT=TRUE);

      CREATE OR REPLACE TABLE INT_FRPTRAN_RAW AS
        SELECT * FROM read_csv_auto('${csv('frptran.csv')}');

      CREATE OR REPLACE TABLE INT_FRPTCD_RAW AS
        SELECT * FROM read_csv_auto('${csv('frptcd.csv')}');

      CREATE OR REPLACE TABLE INT_FRPSI1_RAW AS
        SELECT * FROM read_csv_auto('${csv('frpsi1.csv')}');

      CREATE OR REPLACE TABLE INT_FRPINDX_RAW AS
        SELECT * FROM read_csv_auto('${csv('frpindx.csv')}');

      CREATE OR REPLACE TABLE INT_FRPPRICE_RAW AS
        SELECT * FROM read_csv_auto('${csv('frpprice.csv')}');

      CREATE OR REPLACE TABLE INT_FRPCTG_RAW AS
        SELECT * FROM read_csv_auto('${csv('frpctg.csv')}');

      CREATE OR REPLACE TABLE INT_FRPAGG_RAW AS
        SELECT * FROM read_csv_auto('${csv('frpagg.csv')}');
    `;

    db.exec(tableCreationSQL);
    console.log('[SUCCESS] Database initialization completed successfully.');
  } catch (error) {
    console.error('[ERROR] An error occurred during database initialization:', error);
    throw error; // Re-throw the error to allow higher-level handling
  }
}

/**
 * Executes SQL migration files found in the migrations directory.
 * This version is now ASYNCHRONOUS to correctly handle database operations.
 * @param db The DuckDB database instance.
 */
export async function runMigrations(db: Database): Promise<void> { // 1. Make the function async
  console.log('[INFO] Checking for and running migrations...');
  const migrationsDir = path.resolve(__dirname, '../migrations');

  if (!fs.existsSync(migrationsDir)) {
    console.log('[INFO] Migrations directory not found. Skipping migration step.');
    return;
  }

  // 2. Promisify db.exec to use it with async/await
  // This converts the callback-based function into a promise-based one.
  const exec = promisify(db.exec.bind(db));

  try {
    const files = fs
      .readdirSync(migrationsDir)
      .filter((f) => fs.statSync(path.join(migrationsDir, f)).isFile())
      .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));

    if (files.length === 0) {
      console.log('[INFO] No migration files to run.');
      return;
    }

    console.log(`[INFO] Found ${files.length} migration files.`);

    for (const file of files) {
      const filePath = path.join(migrationsDir, file);
      console.log(`[INFO] Running migration: ${file}`);
      const sql = fs.readFileSync(filePath, 'utf-8');
      
      // 3. Await the execution of the SQL command
      // The code will now pause here until the database confirms completion.
      await exec(sql);
      
      console.log(`[SUCCESS] Successfully ran migration: ${file}`);
    }

    console.log('[SUCCESS] All migrations completed.');
  } catch (error) {
    console.error('[ERROR] An error occurred during migrations:', error);
    throw error;
  }
}

/**
 * A collection of SQL queries.
 */
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
};