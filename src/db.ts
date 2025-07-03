import { Database } from 'duckdb'

export function initializeDatabase(db: Database): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS sales (
      id INTEGER PRIMARY KEY,
      date DATE,
      product VARCHAR,
      category VARCHAR,
      quantity INTEGER,
      unit_price DECIMAL(10, 2),
      total_price DECIMAL(10, 2)
    );

    -- Insert sample data only if the table is empty
    INSERT INTO sales (id, date, product, category, quantity, unit_price, total_price)
    SELECT * FROM (VALUES
      (1, '2024-01-01', 'Laptop', 'Electronics', 5, 999.99, 4999.95),
      (2, '2024-01-02', 'Smartphone', 'Electronics', 10, 599.99, 5999.90),
      (3, '2024-01-03', 'T-shirt', 'Clothing', 20, 19.99, 399.80),
      (4, '2024-01-04', 'Book', 'Books', 15, 14.99, 224.85),
      (5, '2024-01-05', 'Headphones', 'Electronics', 8, 79.99, 639.92)
    ) AS new_data
    WHERE NOT EXISTS (SELECT 1 FROM sales);

    CREATE TABLE IF NOT EXISTS FRPAIR (
      ACCT VARCHAR(14) PRIMARY KEY,
      NAME VARCHAR(50),
      FYE INTEGER,
      ICPDATED DATE,
      ACTIVE VARCHAR(20)
    );
    INSERT INTO FRPAIR (ACCT, NAME, FYE, ICPDATED, ACTIVE)
    SELECT * FROM (VALUES
      ('ACC1001', 'Global Equity Fund', 1231, '2010-01-15', 'Open'),
      ('ACC1002', 'Fixed Income Trust', 1231, '2015-06-20', 'Open')
    ) AS new_data(ACCT, NAME, FYE, ICPDATED, ACTIVE)
    WHERE NOT EXISTS (SELECT 1 FROM FRPAIR);

    CREATE TABLE IF NOT EXISTS FRPSEC (
      ID VARCHAR(255) PRIMARY KEY,
      NAMETKR VARCHAR(255),
      TICKER VARCHAR(50),
      CUSIP VARCHAR(9)
    );
    INSERT INTO FRPSEC (ID, NAMETKR, TICKER, CUSIP)
    SELECT * FROM (VALUES
      ('SEC001', 'Apple Inc.', 'AAPL', '037833100'),
      ('SEC002', 'Microsoft Corp.', 'MSFT', '594918104')
    ) AS new_data(ID, NAMETKR, TICKER, CUSIP)
    WHERE NOT EXISTS (SELECT 1 FROM FRPSEC);

    CREATE TABLE IF NOT EXISTS FRPHOLD (
      AACCT VARCHAR(14),
      HID VARCHAR(255),
      ADATE VARCHAR(6),
      HDIRECT1 VARCHAR(255),
      HUNITS DOUBLE,
      HPRINCIPAL DOUBLE,
      HACCRUAL DOUBLE,
      PRIMARY KEY (AACCT, HID, ADATE)
    );
    INSERT INTO FRPHOLD (AACCT, HID, ADATE, HDIRECT1, HUNITS, HPRINCIPAL, HACCRUAL)
    SELECT * FROM (VALUES
      ('ACC1001', 'SEC001', '202401', 'Equity', 100, 15000, 0.0),
      ('ACC1002', 'SEC002', '202401', 'Equity', 200, 25000, 0.0)
    ) AS new_data(AACCT, HID, ADATE, HDIRECT1, HUNITS, HPRINCIPAL, HACCRUAL)
    WHERE NOT EXISTS (SELECT 1 FROM FRPHOLD);

    CREATE TABLE IF NOT EXISTS FRPTRAN (
      AACCT VARCHAR(14),
      HID VARCHAR(255),
      ADATE VARCHAR(6),
      TDATE DATE,
      TCODE VARCHAR(255),
      TUNITS DOUBLE,
      TPRINCIPAL DOUBLE,
      TINCOME DOUBLE,
      FEE DOUBLE,
      PRIMARY KEY (AACCT, HID, TDATE, TCODE)
    );
    INSERT INTO FRPTRAN (AACCT, HID, ADATE, TDATE, TCODE, TUNITS, TPRINCIPAL, TINCOME, FEE)
    SELECT * FROM (VALUES
      ('ACC1001', 'SEC001', '202401', '2024-01-15', 'BUY', 50, 7500, 0, 10.0),
      ('ACC1002', 'SEC002', '202401', '2024-01-20', 'BUY', 75, 18750, 0, 15.0)
    ) AS new_data(AACCT, HID, ADATE, TDATE, TCODE, TUNITS, TPRINCIPAL, TINCOME, FEE)
    WHERE NOT EXISTS (SELECT 1 FROM FRPTRAN);
  `)
}

export const QUERIES = {
  allSales: 'SELECT * FROM sales',
  salesSummary: `
    SELECT
      category,
      COUNT(*) as total_transactions,
      SUM(quantity) as total_quantity,
      SUM(total_price) as total_revenue
    FROM sales
    GROUP BY category
  `,
  dailySales: `
    SELECT
      date,
      COUNT(*) as transactions,
      SUM(total_price) as daily_revenue
    FROM sales
    GROUP BY date
    ORDER BY date
  `
}