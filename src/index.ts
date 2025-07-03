import { Hono } from 'hono'
import duckdb from '@duckdb/node-api'
import { initializeDatabase } from './db'
import { setupRoutes } from './routes'

const app = new Hono()
const db = new duckdb.Database(':memory:')

initializeDatabase(db)
setupRoutes(app, db)

export default app
