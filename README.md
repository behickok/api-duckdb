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

You can initialize both `key` and `stis` databases at runtime by sending a
`POST` request to `/init`. The server will run the same setup script for both
databases.

Requests with API keys `secret123-key` or `secret123-stis` will execute against
`data/key.duckdb` and `data/stis.duckdb` respectively.

## API Endpoints

- GET /: API info
- GET /sales: List all sales transactions
- GET /sales/daily: Get daily sales data
- GET /sales/summary: Get sales summary by category
- POST /query: Execute a custom SQL query (requires API key)
- POST /init: Run database initialization scripts

## Deploy

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template/i3i9G7?referralCode=jan)