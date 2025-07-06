# Hono + DuckDB

This is a sample analytical API built with Hono and DuckDB.

## Installation

This project expects **Node 18.x**. If you are using `nvm`, run:

```bash
nvm use
```

To install dependencies:

```bash
bun install
```

To run:

```bash
bun start
```

## Database Setup

The application uses an in-memory DuckDB instance. On startup a migration
script located at `migrations/0-create-tables.sql` loads the CSV files from the
`data/` directory and creates base tables like `sales`, `FRPAIR`, `FRPHOLD`,
`FRPTRAN` and `FRPSEC`. No external setup scripts or API keys are required.

Any SQL files placed in the `migrations/` directory are executed in order on
startup. Prefix files with a number (e.g. `1 test.txt`) to control execution
order. This makes it easy to add views or other objects that depend on the base
tables.

## API Endpoints

- GET /: API info
- GET /sales: List all sales transactions
- GET /sales/daily: Get daily sales data
- GET /sales/summary: Get sales summary by category
- POST /query: Execute a custom SQL query against the in-memory database

## Deploy

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template/i3i9G7?referralCode=jan)