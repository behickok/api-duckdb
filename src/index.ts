import { Hono } from 'hono'
import * as duckdb from 'duckdb'
import { initializeDatabase } from './db'
import { setupRoutes } from './routes'

const app = new Hono()

const memoryDb = new duckdb.Database(':memory:')
initializeDatabase(memoryDb)

// Map API keys to database file paths. Connections are opened on demand
const dbMap: Record<string, string> = {
  'secret123-key': 'data/key.duckdb',
  'secret123-stis': 'data/stis.duckdb',
}

setupRoutes(app, dbMap, memoryDb)

export default app
