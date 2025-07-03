import { Hono, type Context } from 'hono'
import { Database } from '@duckdb/node-api'
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

export function setupRoutes(app: Hono, dbMap: Record<string, Database>, defaultDb: Database): void {
  app.get('/', (c) => c.text('DuckDB Analytics API'))

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

  app.post('/query', async (c) => {
    const key = c.req.header('x-api-key')
    const db = key ? dbMap[key] : undefined
    if (!db) {
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

    try {
      const rows = await executeQuery<any>(db, body.query)
      return sendJsonResponse(c, rows)
    } catch (err) {
      return c.json({ error: String(err) }, 400)
    }
  })
}
