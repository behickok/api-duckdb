import { Hono } from 'hono'
import * as duckdb from 'duckdb'
import { runMigrations } from './db'
import { setupRoutes } from './routes'

const app = new Hono()
const db = new duckdb.Database(':memory:')

await runMigrations(db)
setupRoutes(app, db)

export default app
