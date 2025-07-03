
<<<<<<< HEAD
// server.ts -----------------------------------------------------------
import { Hono } from 'hono';
import { DuckDBInstance } from '@duckdb/node-api';          // ✅ Neo instance API:contentReference[oaicite:0]{index=0}
import { initializeDatabase } from './db';
import { setupRoutes } from './routes';

/* ------------------------------------------------------------------ *
 * 1️⃣  Create – and immediately connect to – an in-memory instance.
 * ------------------------------------------------------------------ */
const instance  = await DuckDBInstance.create(':memory:'); // in-memory DB:contentReference[oaicite:1]{index=1}
const db        = await instance.connect();                // returns DuckDBConnection:contentReference[oaicite:2]{index=2}
=======
const app = new Hono()
function openDb(path: string): Promise<duckdb.Database> {
  return new Promise((resolve, reject) => {
    const db = new duckdb.Database(path, err => {
      if (err) reject(err);
      else     resolve(db);          // ← only resolve once the file is open
    });
  });
}

const memoryDb  = await openDb(':memory:');
await initializeDatabase(memoryDb);   // <-- if this is async make sure to await

<<<<<<< HEAD
// Map API keys to database file paths. Connections are opened on demand
const dbMap: Record<string, string> = {
  'secret123-key': 'data/key.duckdb',
  'secret123-stis': 'data/stis.duckdb',
}
=======
const dbMap: Record<string, duckdb.Database> = {
  'secret123-key':  await openDb('data/key.duckdb'),
  'secret123-stis': await openDb('data/stis.duckdb'),
};

>>>>>>> 697cd33 (Addding tests)

setupRoutes(app, dbMap, memoryDb)
>>>>>>> parent of ba012c0 (Merge pull request #12 from behickok/codex/update-database-setup-with-custom-tables)


/* ------------------------------------------------------------------ *
 * 2️⃣  Bootstrap tables / seed data (from db.ts we rewrote earlier).
 * ------------------------------------------------------------------ */
await initializeDatabase(db);

/* ------------------------------------------------------------------ *
 * 3️⃣  Build & export the Hono app.
 * ------------------------------------------------------------------ */
const app = new Hono();
setupRoutes(app, db);

export default app;
