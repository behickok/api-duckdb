
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
