# Hono + DuckDB

This is a sample analytical API built with Hono and DuckDB.

## Installation

To install dependencies:

```bash
bun install
```

To run:

```bash
bun start
```

## Database Setup

Use the migration script to create persistent databases:

```bash
# Example: create data/key.duckdb
DUCKDB_PATH=data/key.duckdb node scripts/db-init/setup-database.js
```

You can also run `npm run build` to create both `key` and `stis` databases.

Requests with API keys `secret123-key` or `secret123-stis` will execute against
`data/key.duckdb` and `data/stis.duckdb` respectively.

During Docker builds the script runs automatically via `npm run build` to
populate both databases.

## API Endpoints

- GET /: API info
- GET /sales: List all sales transactions
- GET /sales/daily: Get daily sales data
- GET /sales/summary: Get sales summary by category
- POST /query: Execute a custom SQL query (requires API key)

## Deploy

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template/i3i9G7?referralCode=jan)