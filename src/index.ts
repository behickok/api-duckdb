import { Hono } from 'hono'
import * as duckdb from '@duckdb/node-api'
import { initializeDatabase } from './db'
import { setupRoutes } from './routes'

const app = new Hono()

const memoryDb = new duckdb.Database(':memory:')
initializeDatabase(memoryDb)

const dbMap: Record<string, duckdb.Database> = {
  'secret123-key': new duckdb.Database('data/key.duckdb'),
  'secret123-stis': new duckdb.Database('data/stis.duckdb'),
}

setupRoutes(app, dbMap, memoryDb)

export default app
