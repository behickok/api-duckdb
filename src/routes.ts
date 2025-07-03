
// routes.ts -----------------------------------------------------------
import { Hono, type Context } from 'hono';
import { DuckDBConnection } from '@duckdb/node-api';      // ✅ Neo connection
import { QUERIES } from './db';


/* ------------------------------------------------------------------ *
 * Domain models (unchanged)
 * ------------------------------------------------------------------ */
interface SalesRecord {
  id: number;
  date: string;
  product: string;
  category: string;
  quantity: number;
  unit_price: number;
  total_price: number;
}

interface SalesSummary {
  category: string;
  total_transactions: number;
  total_quantity: number;
  total_revenue: number;
}

interface DailySales {
  date: string;
  transactions: number;
  daily_revenue: number;
}

/* ------------------------------------------------------------------ *
 * Route registration
 * ------------------------------------------------------------------ */
export function setupRoutes(app: Hono, db: DuckDBConnection): void {
  app.get('/', c => c.text('DuckDB Analytics API'));

  // --- small helpers ------------------------------------------------
  const executeQuery = async <T>(sql: string): Promise<T[]> => {
    const reader = await db.runAndReadAll(sql);           // async Neo API:contentReference[oaicite:0]{index=0}
    return reader.getRowObjects() as T[];
  };

  const send = <T>(c: Context, data: T) => c.json(data);

  // --- predefined reports ------------------------------------------
  app.get('/sales',        async c => send(c, await executeQuery<SalesRecord>(QUERIES.allSales)));
  app.get('/sales/summary',async c => send(c, await executeQuery<SalesSummary>(QUERIES.salesSummary)));
  app.get('/sales/daily',  async c => send(c, await executeQuery<DailySales>(QUERIES.dailySales)));

  // --- ad-hoc SQL endpoint -----------------------------------------
  app.post('/query', async c => {
    let body: { query?: string };
    try       { body = await c.req.json(); }
    catch     { return c.json({ error: 'Invalid JSON' }, 400); }

    const { query } = body;
    if (!query) return c.json({ error: 'Query is required' }, 400);

    try       { return send(c, await executeQuery<any>(query)); }
    catch (e) { return c.json({ error: String(e) }, 400); }
  });
}
