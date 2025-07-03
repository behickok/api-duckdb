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

The application now uses an in-memory DuckDB instance. On startup the database
is seeded with a small set of demo tables. The sample rows for tables such as
`sales`, `FRPAIR`, `FRPSEC`, `FRPCTG`, `FRPSI1`, and `FRPAGG` are stored in CSV
files under `scripts/db-init/data`. These CSV files are loaded using DuckDB's
`COPY` command during initialization.

This project uses the `@duckdb/node-api` package, which provides prebuilt
binaries for recent Node versions (including Node 24). You no longer need a
Python toolchain unless you want to compile DuckDB from source.

## API Endpoints

- GET /: API info
- GET /sales: List all sales transactions
- GET /sales/daily: Get daily sales data
- GET /sales/summary: Get sales summary by category
- POST /query: Execute a custom SQL query against the in-memory database

## Deploy

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template/i3i9G7?referralCode=jan)