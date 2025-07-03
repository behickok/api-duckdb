# Hono + DuckDB

This is a sample analytical API built with Hono and DuckDB.

This project requires **Node.js 18 LTS** in order to use the DuckDB Node API.

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
DUCKDB_PATH=data/key.duckdb bun scripts/db-init/setup-database.js
```

You can create tables on demand by sending a `POST` request to `/init` with a
JSON body specifying the database file and table name:

```json
{ "database": "data/key.duckdb", "table": "FRPAIR" }
```
The server runs the setup script for the requested table only.

Requests with API keys `secret123-key` or `secret123-stis` will execute against
`data/key.duckdb` and `data/stis.duckdb` respectively.
Connections to these databases are opened on demand for each request.

## API Endpoints

- GET /: API info
- GET /sales: List all sales transactions
- GET /sales/daily: Get daily sales data
- GET /sales/summary: Get sales summary by category
- POST /query: Execute a custom SQL query (requires API key)
- POST /init: Initialize a single table by database and table name

## Deploy

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template/i3i9G7?referralCode=jan)