import { Database } from 'duckdb';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { promisify } from 'util';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Executes SQL migration files found in the migrations directory.
 * @param db The DuckDB database instance.
 */
export async function runMigrations(db: Database): Promise<void> {
  console.log('[INFO] Checking for and running migrations...');
  const migrationsDir = path.resolve(__dirname, '../migrations');

  if (!fs.existsSync(migrationsDir)) {
    console.log('[INFO] Migrations directory not found. Skipping migration step.');
    return;
  }

  // Promisify db.exec so it can be awaited
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
    ORDER BY date  `,
  kbDocs: 'SELECT doc_id, source, content FROM kb_docs'
};
