import { Hono, type Context } from 'hono'
import { Database } from 'duckdb'
import { spawn } from 'child_process'
import { fileURLToPath } from 'url'
import path from 'path'
import { QUERIES } from './db'


interface SalesRecord {
  id: number
  date: string
  product: string
  category: string
  quantity: number
  unit_price: number
  total_price: number
}

interface SalesSummary {
  category: string
  total_transactions: number
  total_quantity: number
  total_revenue: number
}

interface DailySales {
  date: string
  transactions: number
  daily_revenue: number
}

// dbMap maps API keys to database file paths
export function setupRoutes(app: Hono, dbMap: Record<string, string>, defaultDb: Database): void {
  app.get('/', (c) => c.text('DuckDB Analytics API'))

  const __filename = fileURLToPath(import.meta.url)
  const __dirname = path.dirname(__filename)
  const setupScript = path.resolve(__dirname, '../scripts/db-init/setup-database.js')

  const runInit = (dbPath: string, table: string) =>
    new Promise<void>((resolve, reject) => {
      const child = spawn('bun', [setupScript], {
        env: { ...process.env, DUCKDB_PATH: dbPath, TABLE: table },
        stdio: 'inherit',
      })
      child.on('error', reject)
      child.on('exit', (code) => {
        code === 0 ? resolve() : reject(new Error(`init failed with code ${code}`))
      })
    })

  const executeQuery = async <T>(dbConn: Database, query: string): Promise<T[]> => {
    return new Promise((resolve, reject) => {
      dbConn.all(query, (err: Error | null, rows: any[]) => {
        if (err) reject(err)
        resolve(rows as T[])
      })
    })
  }

  const sendJsonResponse = <T>(c: Context, data: T) => {
    const jsonString = JSON.stringify(data, (_, value) => typeof value === 'bigint' ? value.toString() : value)
    return c.json(JSON.parse(jsonString))
  }

  app.get('/sales', async (c) => {
    const rows = await executeQuery<SalesRecord>(defaultDb, QUERIES.allSales)
    return sendJsonResponse(c, rows)
  })

  app.get('/sales/summary', async (c) => {
    const rows = await executeQuery<SalesSummary>(defaultDb, QUERIES.salesSummary)
    return sendJsonResponse(c, rows)
  })

  app.get('/sales/daily', async (c) => {
    const rows = await executeQuery<DailySales>(defaultDb, QUERIES.dailySales)
    return sendJsonResponse(c, rows)
  })

  app.post('/init', async (c) => {
    let body: { database?: string; table?: string }
    try {
      body = await c.req.json()
    } catch {
      return c.json({ error: 'Invalid JSON' }, 400)
    }

    const dbPath = body.database
    const table = body.table
    if (!dbPath || !table) {
      return c.json({ error: 'database and table are required' }, 400)
    }

    try {
      await runInit(dbPath, table)
      return c.json({ status: 'initialized', database: dbPath, table })
    } catch (err) {
      return c.json({ error: String(err) }, 500)
    }
  })

  app.post('/query', async (c) => {
    const key = c.req.header('x-api-key')
    const dbPath = key ? dbMap[key] : undefined
    if (!dbPath) {
      return c.json({ error: 'Unauthorized' }, 401)
    }

    let body: { query?: string }
    try {
      body = await c.req.json()
    } catch {
      return c.json({ error: 'Invalid JSON' }, 400)
    }

    if (!body.query) {
      return c.json({ error: 'Query is required' }, 400)
    }

    const dbConn = new Database(dbPath)
    try {
      const rows = await executeQuery<any>(dbConn, body.query)
      return sendJsonResponse(c, rows)
    } catch (err) {
      return c.json({ error: String(err) }, 400)
    } finally {
      dbConn.close(() => {})
    }
  })
}
