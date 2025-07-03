import { Hono } from 'hono'
import * as duckdb from 'duckdb'
import { initializeDatabase } from './db'
import { setupRoutes } from './routes'

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

export default app
