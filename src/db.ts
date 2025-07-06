import { Database } from 'duckdb';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { promisify } from 'util';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
/**
 * Deprecated initializer kept for backwards compatibility. Table creation
 * is now handled via SQL files in the `migrations` directory.
 */
export function initializeDatabase(_db: Database): void {
  console.log('[INFO] Database initialization is handled by migrations.');
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