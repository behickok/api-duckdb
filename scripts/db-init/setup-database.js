
import duckdb from "duckdb"

import path from 'path'
import { fileURLToPath } from 'url'
import fs from 'fs'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const defaultPath = path.join(__dirname, '../../db/my_app.duckdb')
const dbPath = path.resolve(process.env.DUCKDB_PATH || defaultPath)
const dbDir = path.dirname(dbPath)

console.log(`Target database file path: ${dbPath}`)

if (!fs.existsSync(dbDir)) {
  console.log(`Creating database directory at: ${dbDir}`)
  fs.mkdirSync(dbDir, { recursive: true })
}

const db = new duckdb.Database(dbPath)

function runStatement(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, err => {
      if (err) reject(err); else resolve();
    });
  });
}

function runQuery(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err); else resolve(rows);
    });
  });
}


async function main() {

  try {
    console.log('Initializing database schema and seeding data...')

    await runStatement( `CREATE SEQUENCE IF NOT EXISTS report_configurations_id_seq START 1;`)
    console.log('Sequences for IDs ensured.')

    await runStatement( `DROP TABLE IF EXISTS sample_data;`)
    console.log("Old 'sample_data' table dropped if it existed.")

    await runStatement( `

            CREATE TABLE IF NOT EXISTS report_configurations (
                id INTEGER PRIMARY KEY DEFAULT nextval('report_configurations_id_seq'),
                label VARCHAR(255) NOT NULL,
                query_template TEXT NOT NULL,
                column_definitions TEXT,
                parameter_definitions TEXT,
                crud_config TEXT,
                ai_prompt_template TEXT
            );
        `)
    console.log("'report_configurations' table schema ensured.")


    await runStatement( `

            CREATE TABLE IF NOT EXISTS agentic_workflows (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                target_script_path TEXT,
                handler_function_name VARCHAR(255),
                parameters_schema JSON,
                trigger_type VARCHAR(50) CHECK (trigger_type IN ('manual', 'cron')),
                cron_schedule VARCHAR(255),
                output_type VARCHAR(50) CHECK (output_type IN ('table', 'text_status', 'json')),
                created_at TIMESTAMP DEFAULT current_timestamp,
                updated_at TIMESTAMP DEFAULT current_timestamp
            );
        `)
    console.log("'agentic_workflows' table schema ensured.")

    const agenticWorkflowsCountResult = await runQuery( 'SELECT COUNT(*) FROM agentic_workflows')

    if (agenticWorkflowsCountResult && agenticWorkflowsCountResult.length > 0 && Number(agenticWorkflowsCountResult[0][0]) === 0) {
      const workflowsToSeed = [
        {
          id: 'wf_anomalies_perf',
          name: 'Performance Anomaly Detection',
          description: 'For account {account_id}, research underlying holdings and transactions for the last {months_lookback} months to look for potential anomalies and/or describe sources of underperformance.',
          target_script_path: 'scripts/workflows/performance_anomaly.js',
          handler_function_name: 'runPerformanceAnomalyCheck',
          parameters_schema: JSON.stringify({
            type: 'object',
            properties: {
              account_id: { type: 'string', description: 'Account ID (e.g., ACC1001)' },
              months_lookback: { type: 'integer', description: 'Number of months to look back', default: 3 }
            },
            required: ['account_id']
          }),
          trigger_type: 'manual',
          cron_schedule: null,
          output_type: 'text_status'
        },
        {
          id: 'wf_audit_review',
          name: 'Audit Review Automation',
          description: "Review outstanding audits. Accounts with no issues are set to 'final' with report date of {report_date}. Accounts with issues are set to 'under_review' with report date of {report_date_previous_month}.",
          target_script_path: 'scripts/workflows/audit_review.js',
          handler_function_name: 'runAuditReview',
          parameters_schema: JSON.stringify({
            type: 'object',
            properties: {
              report_date: { type: 'string', format: 'date', description: 'Typically current date (system)' },
              report_date_previous_month: { type: 'string', format: 'date', description: 'Typically previous month end (system)'}
            },
            system_provided: ['report_date', 'report_date_previous_month']
          }),
          trigger_type: 'cron',
          cron_schedule: '0 2 * * 1',
          output_type: 'text_status'
        },
        {
          id: 'wf_data_validation',
          name: 'Nightly Data Validation',
          description: "Review prices and benchmarks from last night's files. Identify any missing data, malformed data, or data that seems unreasonable.",
          target_script_path: 'scripts/workflows/data_validation.js',
          handler_function_name: 'runDataValidation',
          parameters_schema: JSON.stringify({ type: 'object', properties: {} }),
          trigger_type: 'cron',
          cron_schedule: '0 1 * * *',
          output_type: 'table'
        }
      ]
      for (const wf of workflowsToSeed) {

        await runStatement( `
                    INSERT INTO agentic_workflows (id, name, description, target_script_path, handler_function_name, parameters_schema, trigger_type, cron_schedule, output_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                `, [wf.id, wf.name, wf.description, wf.target_script_path, wf.handler_function_name, wf.parameters_schema, wf.trigger_type, wf.cron_schedule, wf.output_type])
      }
      console.log("'agentic_workflows' table seeded with sample data.")
    } else {
      console.log("'agentic_workflows' data already exists or an error occurred during count.")
    }

    await runStatement( `
            CREATE TABLE IF NOT EXISTS FRPAIR (
                ACCT VARCHAR(14) PRIMARY KEY,
                NAME VARCHAR(50),
                FYE INTEGER,
                ICPDATED DATE,
                ACTIVE VARCHAR(20)
            );
        `)
    console.log("'FRPAIR' table schema ensured.")

    await runStatement( `
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
        `)
    console.log("'FRPHOLD' table schema ensured.")

    await runStatement( `
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
        `)
    console.log("'FRPTRAN' table schema ensured.")

    await runStatement( `
            CREATE TABLE IF NOT EXISTS FRPSECTR (
                ACCT VARCHAR(14),
                HID VARCHAR(255),
                ADATE VARCHAR(6),
                SECTOR VARCHAR(255),
                UVR DOUBLE,
                MKT DOUBLE,
                PMKT DOUBLE,
                POS DOUBLE,
                NEG DOUBLE,
                PF DOUBLE,
                NF DOUBLE,
                INC DOUBLE,
                PRIMARY KEY (ACCT, HID, ADATE, SECTOR)
            );
        `)
    console.log("'FRPSECTR' table schema ensured.")


    await runStatement( `
            CREATE TABLE IF NOT EXISTS FRPCTG (
                SECTOR VARCHAR(255) PRIMARY KEY,
                CATEGORY VARCHAR(255)
            );
        `)
    console.log("'FRPCTG' table schema ensured.")

    await runStatement( `
            CREATE TABLE IF NOT EXISTS FRPSI1 (
                SIFLAG VARCHAR(255),
                SORI VARCHAR(255),
                SORINAME VARCHAR(255),
                PRIMARY KEY (SIFLAG, SORI)
            );
        `)
    console.log("'FRPSI1' table schema ensured.")


    await runStatement( `
            CREATE TABLE IF NOT EXISTS FRPSEC (
                ID VARCHAR(255) PRIMARY KEY,
                NAMETKR VARCHAR(255),
                TICKER VARCHAR(50),
                CUSIP VARCHAR(9)
            );
        `)
    console.log("'FRPSEC' table schema ensured.")


    await runStatement( `
            CREATE TABLE IF NOT EXISTS FRPPRICE (
                ID VARCHAR(255),
                SDATE DATE,
                SPRICE DOUBLE,
                PRIMARY KEY (ID, SDATE)
            );
        `)
    console.log("'FRPPRICE' table schema ensured.")

    await runStatement( `
            CREATE TABLE IF NOT EXISTS FRPAGG (
                AGG VARCHAR(14),
                ACCT VARCHAR(14),
                DTOVER__1 VARCHAR(11),
                DTOVER__2 VARCHAR(11),
                DTOVER__3 VARCHAR(11),
                DTOVER__4 VARCHAR(11),
                DTOVER__5 VARCHAR(11),
                DTOVER__6 VARCHAR(11),
                DTOVER__7 VARCHAR(11),
                DTOVER__8 VARCHAR(11),
                DTOVER__9 VARCHAR(11),
                DTOVER__10 VARCHAR(11),
                DTOVER__11 VARCHAR(11),
                DTOVER__12 VARCHAR(11),
                DTOVER__13 VARCHAR(11),
                DTOVER__14 VARCHAR(11),
                DTOVER__15 VARCHAR(11),
                DTOVER__16 VARCHAR(11),
                DTOVER__17 VARCHAR(11),
                DTOVER__18 VARCHAR(11),
                DTOVER__19 VARCHAR(11),
                DTOVER__20 VARCHAR(11),
                PRIMARY KEY (AGG, ACCT)
            );
        `)
    console.log("'FRPAGG' table schema ensured.")


    const reportsCountResult = await runQuery( 'SELECT COUNT(*) FROM report_configurations')
    if (reportsCountResult && reportsCountResult.length > 0 && Number(reportsCountResult[0][0]) > 0) {
      await runStatement( 'DELETE FROM report_configurations')
      console.log('Cleared existing report configurations.')
    }

    const freshReportsCountResult = await runQuery( 'SELECT COUNT(*) FROM report_configurations')

    if (freshReportsCountResult && freshReportsCountResult.length > 0 && Number(freshReportsCountResult[0][0]) === 0) {
      const newReports = [
        {
          id: 1,
          label: 'Account Demographics (FRPAIR)',
          query_template: 'SELECT ACCT, NAME, FYE, ICPDATED, ACTIVE FROM FRPAIR ORDER BY ACCT;',
          column_definitions: JSON.stringify([
            { code: 'ACCT', label: 'Account Number', type: 'text', hidden: false },
            { code: 'NAME', label: 'Account Name', type: 'text', hidden: false },
            { code: 'FYE', label: 'Fiscal Year End', type: 'number', hidden: false },
            { code: 'ICPDATED', label: 'Inception Date', type: 'date', hidden: false },
            { code: 'ACTIVE', label: 'Status', type: 'text', hidden: false }
          ]),
          parameter_definitions: '[]',
          crud_config: null,
          ai_prompt_template: 'Review the following account demographic data. Provide a brief summary of the accounts, noting their activity status and inception dates.\n\nData:\n{{Data}}'
        },
        {
          id: 2,
          label: 'Account Holdings (FRPHOLD)',
          query_template: `
                        SELECT H.AACCT, P.NAME as ACCOUNT_NAME, H.HID, SEC.NAMETKR as SECURITY_NAME, H.ADATE, H.HDIRECT1, H.HUNITS, H.HPRINCIPAL, H.HACCRUAL
                        FROM FRPHOLD H
                        JOIN FRPAIR P ON H.AACCT = P.ACCT
                        JOIN FRPSEC SEC ON H.HID = SEC.ID
                        {{whereClause}}
                        ORDER BY H.AACCT, H.ADATE, H.HID;
                    `,
          column_definitions: JSON.stringify([
            { code: 'AACCT', label: 'Account No.', type: 'text', hidden: false },
            { code: 'ACCOUNT_NAME', label: 'Account Name', type: 'text', hidden: false },
            { code: 'HID', label: 'Asset ID', type: 'text', hidden: false },
            { code: 'SECURITY_NAME', label: 'Security Name', type: 'text', hidden: false },
            { code: 'ADATE', label: 'Record Month (YYYYMM)', type: 'text', hidden: false },
            { code: 'HDIRECT1', label: 'Classification', type: 'text', hidden: false },
            { code: 'HUNITS', label: 'Units', type: 'number', hidden: false },
            { code: 'HPRINCIPAL', label: 'Principal Value', type: 'currency', hidden: false },
            { code: 'HACCRUAL', label: 'Accrual', type: 'currency', hidden: false }
          ]),
          parameter_definitions: JSON.stringify([
            { code: 'AACCT', label: 'Account Number', type: 'dropdown', optionsQuery: "SELECT DISTINCT ACCT AS value, NAME || ' (' || ACCT || ')' AS label FROM FRPAIR WHERE ACTIVE = 'Open' ORDER BY NAME;", defaultValue: "" },
            { code: 'ADATE', label: 'Record Month (YYYYMM)', type: 'dropdown', optionsQuery: "SELECT DISTINCT ADATE AS value, ADATE AS label FROM FRPHOLD ORDER BY ADATE DESC;", defaultValue: "" }
          ]),
          crud_config: null,
          ai_prompt_template: 'Analyze the holdings for the selected account and period. What are the major asset types and their values? Are there any concentrated positions?\n\nData:\n{{Data}}'
        },
        {
          id: 3,
          label: 'Account Transactions (FRPTRAN)',
          query_template: `
                        SELECT T.AACCT, P.NAME as ACCOUNT_NAME, T.HID, SEC.NAMETKR as SECURITY_NAME, T.ADATE, T.TDATE, T.TCODE, T.TUNITS, T.TPRINCIPAL, T.TINCOME, T.FEE
                        FROM FRPTRAN T
                        JOIN FRPAIR P ON T.AACCT = P.ACCT
                        JOIN FRPSEC SEC ON T.HID = SEC.ID
                        {{whereClause}}
                        ORDER BY T.AACCT, T.TDATE, T.HID;
                    `,
          column_definitions: JSON.stringify([
            { code: 'AACCT', label: 'Account No.', type: 'text' },
            { code: 'ACCOUNT_NAME', label: 'Account Name', type: 'text' },
            { code: 'HID', label: 'Asset ID', type: 'text' },
            { code: 'SECURITY_NAME', label: 'Security Name', type: 'text' },
            { code: 'ADATE', label: 'Record Month', type: 'text' },
            { code: 'TDATE', label: 'Transaction Date', type: 'date' },
            { code: 'TCODE', label: 'Type', type: 'text' },
            { code: 'TUNITS', label: 'Units', type: 'number' },
            { code: 'TPRINCIPAL', label: 'Principal', type: 'currency' },
            { code: 'TINCOME', label: 'Income', type: 'currency' },
            { code: 'FEE', label: 'Fee', type: 'currency' }
          ]),
          parameter_definitions: JSON.stringify([
            { code: 'AACCT', label: 'Account Number', type: 'dropdown', optionsQuery: "SELECT DISTINCT ACCT AS value, NAME || ' (' || ACCT || ')' AS label FROM FRPAIR WHERE ACTIVE = 'Open' ORDER BY NAME;", defaultValue: "" },
            { code: 'TDATE_START', label: 'Transaction Date From', type: 'date', defaultValue: "" },
            { code: 'TDATE_END', label: 'Transaction Date To', type: 'date', defaultValue: "" }
          ]),
          crud_config: null,
          ai_prompt_template: 'Review the transaction log for the selected account and date range. What types of transactions are most common? Are there any large or unusual transactions?\n\nData:\n{{Data}}'
        },
        {
          id: 4,
          label: 'Performance Sector Summary (FRPSECTR)',
          query_template: `
                        SELECT S.ACCT, P.NAME as ACCOUNT_NAME, S.HID, SEC.NAMETKR as SECURITY_NAME, S.ADATE, S.SECTOR, PSI.SORINAME as SECTOR_NAME, S.UVR, S.MKT, S.PMKT, S.POS, S.NEG, S.INC
                        FROM FRPSECTR S
                        JOIN FRPAIR P ON S.ACCT = P.ACCT
                        JOIN FRPSEC SEC ON S.HID = SEC.ID
                        LEFT JOIN FRPSI1 PSI ON S.SECTOR = PSI.SORI AND PSI.SIFLAG = 'SECTOR'
                        {{whereClause}}
                        ORDER BY S.ACCT, S.ADATE, S.SECTOR;
                    `,
          column_definitions: JSON.stringify([
            { code: 'ACCT', label: 'Account No.', type: 'text' },
            { code: 'ACCOUNT_NAME', label: 'Account Name', type: 'text' },
            { code: 'HID', label: 'Asset ID', type: 'text' },
            { code: 'SECURITY_NAME', label: 'Security Name', type: 'text' },
            { code: 'ADATE', label: 'Record Month', type: 'text' },
            { code: 'SECTOR', label: 'Sector ID', type: 'text' },
            { code: 'SECTOR_NAME', label: 'Sector Name', type: 'text' },
            { code: 'UVR', label: 'Unit Value Return', type: 'percentage', hidden:false, numberFormat: { minimumFractionDigits: 4, maximumFractionDigits: 4 } },
            { code: 'MKT', label: 'Ending Market Value', type: 'currency' },
            { code: 'PMKT', label: 'Beginning Market Value', type: 'currency' },
            { code: 'POS', label: 'Positive Flows', type: 'currency' },
            { code: 'NEG', label: 'Negative Flows', type: 'currency' },
            { code: 'INC', label: 'Income', type: 'currency' }
          ]),
          parameter_definitions: JSON.stringify([
            { code: 'ACCT', label: 'Account Number', type: 'dropdown', optionsQuery: "SELECT DISTINCT ACCT AS value, NAME || ' (' || ACCT || ')' AS label FROM FRPAIR ORDER BY NAME;", defaultValue: "" },
            { code: 'SECTOR', label: 'Sector', type: 'dropdown', optionsQuery: "SELECT DISTINCT SORI AS value, SORINAME || ' (' || SORI || ')' AS label FROM FRPSI1 WHERE SIFLAG = 'SECTOR' ORDER BY SORINAME;", defaultValue: "" },
            { code: 'ADATE_START', label: 'Record Month From (YYYYMM)', type: 'text', defaultValue: "" },
            { code: 'ADATE_END', label: 'Record Month To (YYYYMM)', type: 'text', defaultValue: "" }
          ]),
          crud_config: null,
          ai_prompt_template: 'Analyze the performance sector summary. Which sectors performed best/worst for the selected account and period? What were the key drivers (market value changes, flows, income)?\n\nData:\n{{Data}}'
        }
      ]


      const maxIdResult = await runQuery( 'SELECT MAX(id) FROM report_configurations')
      const nextId = (maxIdResult && maxIdResult[0] && maxIdResult[0][0] !== null) ? Number(maxIdResult[0][0]) + 1 : 1
      const maxInsertedId = newReports.reduce((max, r) => Math.max(max, r.id), 0)
      for (const report of newReports) {
          await runStatement(
                    'INSERT INTO report_configurations (id, label, query_template, column_definitions, parameter_definitions, crud_config, ai_prompt_template) VALUES (?, ?, ?, ?, ?, ?, ?)',
                    [report.id, report.label, report.query_template, report.column_definitions, report.parameter_definitions, report.crud_config, report.ai_prompt_template]
                )
      }
      console.log('New report configurations seeded.')
    } else {
      console.log('Report configurations already exist or no new reports to seed.')
    }

    function randomDate(start, end) {
      return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()))
    }

    function formatDate(date) {
      return date.toISOString().split('T')[0]
    }

    function formatADATE(date) {
      const year = date.getFullYear()
      const month = (date.getMonth() + 1).toString().padStart(2, '0')
      return `${year}${month}`
    }


    const frpairCountResult = await runQuery( 'SELECT COUNT(*) FROM FRPAIR')
    if (frpairCountResult && frpairCountResult.length > 0 && Number(frpairCountResult[0][0]) === 0) {
      const accounts = [
        { ACCT: 'ACC1001', NAME: 'Global Equity Fund', FYE: 1231, ICPDATED: formatDate(new Date(2010, 0, 15)), ACTIVE: 'Open' },
        { ACCT: 'ACC1002', NAME: 'Fixed Income Trust', FYE: 1231, ICPDATED: formatDate(new Date(2015, 5, 20)), ACTIVE: 'Open' },
        { ACCT: 'ACC1003', NAME: 'Emerging Markets Fund', FYE: 630, ICPDATED: formatDate(new Date(2018, 8, 10)), ACTIVE: 'Open' },
        { ACCT: 'ACC1004', NAME: 'Real Estate Investment', FYE: 1231, ICPDATED: formatDate(new Date(2012, 3, 5)), ACTIVE: 'Closed' },
        { ACCT: 'ACC1005', NAME: 'Balanced Portfolio', FYE: 930, ICPDATED: formatDate(new Date(2020, 1, 25)), ACTIVE: 'Open' },
      ]
      for (const acc of accounts) {

        await runStatement( 'INSERT INTO FRPAIR (ACCT, NAME, FYE, ICPDATED, ACTIVE) VALUES (?, ?, ?, ?, ?)',
                    [acc.ACCT, acc.NAME, acc.FYE, acc.ICPDATED, acc.ACTIVE])
      }
      console.log('FRPAIR table seeded.')
    } else {
      console.log('FRPAIR data already exists or an error occurred.')
    }


    const frpsecCountResult = await runQuery( 'SELECT COUNT(*) FROM FRPSEC')
    if (frpsecCountResult && frpsecCountResult.length > 0 && Number(frpsecCountResult[0][0]) === 0) {
      const securities = [
        { ID: 'SEC001', NAMETKR: 'Apple Inc.', TICKER: 'AAPL', CUSIP: '037833100' },
        { ID: 'SEC002', NAMETKR: 'Microsoft Corp.', TICKER: 'MSFT', CUSIP: '594918104' },
        { ID: 'SEC003', NAMETKR: 'US Treasury Bond 2.5% 2030', TICKER: 'USTB2030', CUSIP: '912828X39' },
        { ID: 'SEC004', NAMETKR: 'Vanguard Total Stock Market ETF', TICKER: 'VTI', CUSIP: '922908769' },
        { ID: 'SEC005', NAMETKR: 'Gold Spot', TICKER: 'XAUUSD', CUSIP: 'GOLDSPOTX' },
      ]
      for (const sec of securities) {

        await runStatement( 'INSERT INTO FRPSEC (ID, NAMETKR, TICKER, CUSIP) VALUES (?, ?, ?, ?)',
                    [sec.ID, sec.NAMETKR, sec.TICKER, sec.CUSIP])
      }
      console.log('FRPSEC table seeded.')
    } else {
      console.log('FRPSEC data already exists or an error occurred.')
    }

    const basePrices = { 'SEC001': 150, 'SEC002': 250, 'SEC003': 102, 'SEC004': 200, 'SEC005': 1800 }

    const frppriceCountResult = await runQuery( 'SELECT COUNT(*) FROM FRPPRICE')
    if (frppriceCountResult && frppriceCountResult.length > 0 && Number(frppriceCountResult[0][0]) === 0) {
      const prices = []
      const securityIds = ['SEC001', 'SEC002', 'SEC003', 'SEC004', 'SEC005']
      const startDate = new Date(2022, 0, 1)

      for (const secId of securityIds) {
        for (let i = 0; i < 24; i++) {
          const priceDate = new Date(startDate.getFullYear(), startDate.getMonth() + i, 1)
          const price = basePrices[secId] * (1 + (Math.random() - 0.45) * 0.1)
          prices.push({ ID: secId, SDATE: formatDate(priceDate), SPRICE: parseFloat(price.toFixed(2)) })
        }
      }
      for (const price of prices) {

        await runStatement( 'INSERT INTO FRPPRICE (ID, SDATE, SPRICE) VALUES (?, ?, ?)',
                    [price.ID, price.SDATE, price.SPRICE])
      }
      console.log('FRPPRICE table seeded.')
    } else {
      console.log('FRPPRICE data already exists or an error occurred.')
    }


    const frpholdCountResult = await runQuery( 'SELECT COUNT(*) FROM FRPHOLD')
    if (frpholdCountResult && frpholdCountResult.length > 0 && Number(frpholdCountResult[0][0]) === 0) {
      const holdings = []
      const accountIds = ['ACC1001', 'ACC1002', 'ACC1003', 'ACC1005']
      const securityIds = ['SEC001', 'SEC002', 'SEC003', 'SEC004', 'SEC005']
      const startDate = new Date(2023, 0, 1)

      for (const accId of accountIds) {
        for (const secId of securityIds.slice(0, Math.floor(Math.random() * 3) + 2)) {
          for (let i = 0; i < 12; i++) {
            const recordDate = new Date(startDate.getFullYear(), startDate.getMonth() + i, 1)
            const adate = formatADATE(recordDate)
            holdings.push({
              AACCT: accId,
              HID: secId,
              ADATE: adate,
              HDIRECT1: (Math.random() > 0.7) ? 'Equity' : 'Fixed Income',
              HUNITS: parseFloat((Math.random() * 1000 + 50).toFixed(4)),
              HPRINCIPAL: parseFloat((Math.random() * 100000 + 5000).toFixed(2)),
              HACCRUAL: parseFloat((Math.random() * 100).toFixed(2))
            })
          }
        }
      }
      for (const hold of holdings) {

        await runStatement( 'INSERT INTO FRPHOLD (AACCT, HID, ADATE, HDIRECT1, HUNITS, HPRINCIPAL, HACCRUAL) VALUES (?, ?, ?, ?, ?, ?, ?)',
                    [hold.AACCT, hold.HID, hold.ADATE, hold.HDIRECT1, hold.HUNITS, hold.HPRINCIPAL, hold.HACCRUAL])
      }
      console.log('FRPHOLD table seeded.')
    } else {
      console.log('FRPHOLD data already exists or an error occurred.')
    }

    const frptranCountResult = await runQuery( 'SELECT COUNT(*) FROM FRPTRAN')
    if (frptranCountResult && frptranCountResult.length > 0 && Number(frptranCountResult[0][0]) === 0) {
      const transactions = []
      const accountIds = ['ACC1001', 'ACC1002', 'ACC1003', 'ACC1005']
      const securityIds = ['SEC001', 'SEC002', 'SEC003', 'SEC004']
      const transactionTypes = ['BUY', 'SELL', 'DIV', 'INT']
      const startDate = new Date(2023, 0, 1)

      for (const accId of accountIds) {
        for (let i = 0; i < 20; i++) {
          const tDate = randomDate(startDate, new Date(2023, 11, 31))
          const aDate = formatADATE(tDate)
          const secId = securityIds[Math.floor(Math.random() * securityIds.length)]
          const tCode = transactionTypes[Math.floor(Math.random() * transactionTypes.length)]
          let units = parseFloat((Math.random() * 200 + 10).toFixed(4))
          let principal = parseFloat((units * (basePrices[secId] || 100) * (1 + (Math.random() - 0.5)*0.02)).toFixed(2))
          let income = 0
          let fee = parseFloat((principal * 0.001).toFixed(2))

          if (tCode === 'SELL') units = -units
          if (tCode === 'DIV' || tCode === 'INT') {
            units = 0
            income = parseFloat((principal * 0.02).toFixed(2))
            principal = 0
          }

          transactions.push({
            AACCT: accId, HID: secId, ADATE: aDate, TDATE: formatDate(tDate),
            TCODE: tCode, TUNITS: units, TPRINCIPAL: principal, TINCOME: income, FEE: fee
          })
        }
      }
      for (const tran of transactions) {

        await runStatement(
                    'INSERT INTO FRPTRAN (AACCT, HID, ADATE, TDATE, TCODE, TUNITS, TPRINCIPAL, TINCOME, FEE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    [tran.AACCT, tran.HID, tran.ADATE, tran.TDATE, tran.TCODE, tran.TUNITS, tran.TPRINCIPAL, tran.TINCOME, tran.FEE])
      }
      console.log('FRPTRAN table seeded.')
    } else {
      console.log('FRPTRAN data already exists or an error occurred.')
    }

    const frpsectorCountResult = await runQuery( 'SELECT COUNT(*) FROM FRPSECTR')
    if (frpsectorCountResult && frpsectorCountResult.length > 0 && Number(frpsectorCountResult[0][0]) === 0) {
      const performanceData = []
      const accountIds = ['ACC1001', 'ACC1002', 'ACC1003', 'ACC1005']
      const securityIds = ['SEC001', 'SEC002', 'SEC003', 'SEC004', 'SEC005']
      const sectors = ['US_EQUITY_LARGE', 'US_EQUITY_SMALL', 'INTL_EQUITY_DEV', 'FIXED_INCOME_CORP', 'FIXED_INCOME_GOVT', 'REAL_ESTATE', 'COMMODITIES']
      const startDate = new Date(2023, 0, 1)

      for (const accId of accountIds) {
        for (const secId of securityIds.slice(0, Math.floor(Math.random() * 2)+1)) {
          for (const sector of sectors.slice(0, Math.floor(Math.random() * 2) + 1)) {
            for (let i = 0; i < 12; i++) {
              const recordDate = new Date(startDate.getFullYear(), startDate.getMonth() + i, 1)
              const adate = formatADATE(recordDate)
              const pmkt = parseFloat((Math.random() * 50000 + 10000).toFixed(2))
              const pos = parseFloat((Math.random() * 5000).toFixed(2))
              const neg = parseFloat((Math.random() * 3000).toFixed(2))
              const inc = parseFloat((Math.random() * 500).toFixed(2))
              const mkt = pmkt + pos - neg + inc + (pmkt * (Math.random() * 0.1 - 0.04))

              performanceData.push({
                ACCT: accId, HID: secId, ADATE: adate, SECTOR: sector,
                UVR: parseFloat((Math.random() * 0.05 - 0.02).toFixed(4)),
                MKT: parseFloat(mkt.toFixed(2)),
                PMKT: pmkt,
                POS: pos, NEG: neg,
                PF: parseFloat(Math.random().toFixed(4)), NF: parseFloat(Math.random().toFixed(4)),
                INC: inc
              })
            }
          }
        }
      }
      for (const perf of performanceData) {

        await runStatement(
                    'INSERT INTO FRPSECTR (ACCT, HID, ADATE, SECTOR, UVR, MKT, PMKT, POS, NEG, PF, NF, INC) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    [perf.ACCT, perf.HID, perf.ADATE, perf.SECTOR, perf.UVR, perf.MKT, perf.PMKT, perf.POS, perf.NEG, perf.PF, perf.NF, perf.INC])
      }
      console.log('FRPSECTR table seeded.')
    } else {
      console.log('FRPSECTR data already exists or an error occurred.')
    }

    const frpctgCountResult = await runQuery( 'SELECT COUNT(*) FROM FRPCTG')
    if (frpctgCountResult && frpctgCountResult.length > 0 && Number(frpctgCountResult[0][0]) === 0) {
      const classifications = [
        { SECTOR: 'US_EQUITY_LARGE', CATEGORY: 'US_EQUITY' },
        { SECTOR: 'US_EQUITY_SMALL', CATEGORY: 'US_EQUITY' },
        { SECTOR: 'INTL_EQUITY_DEV', CATEGORY: 'INTL_EQUITY' },
        { SECTOR: 'FIXED_INCOME_CORP', CATEGORY: 'FIXED_INCOME' },
        { SECTOR: 'FIXED_INCOME_GOVT', CATEGORY: 'FIXED_INCOME' },
        { SECTOR: 'US_EQUITY', CATEGORY: 'EQUITY' },
        { SECTOR: 'INTL_EQUITY', CATEGORY: 'EQUITY' },
        { SECTOR: 'EQUITY', CATEGORY: 'TOTAL_FUND' },
        { SECTOR: 'FIXED_INCOME', CATEGORY: 'TOTAL_FUND' },
        { SECTOR: 'REAL_ESTATE', CATEGORY: 'ALTERNATIVES' },
        { SECTOR: 'COMMODITIES', CATEGORY: 'ALTERNATIVES' },
        { SECTOR: 'ALTERNATIVES', CATEGORY: 'TOTAL_FUND' },
      ]
      for (const clas of classifications) {

        await runStatement( 'INSERT INTO FRPCTG (SECTOR, CATEGORY) VALUES (?, ?)', [clas.SECTOR, clas.CATEGORY])

      }
      console.log('FRPCTG table seeded.')
    } else {
      console.log('FRPCTG data already exists or an error occurred.')
    }

    const frpsi1CountResult = await runQuery( 'SELECT COUNT(*) FROM FRPSI1')

    if (frpsi1CountResult && frpsi1CountResult.length > 0 && Number(frpsi1CountResult[0][0]) === 0) {
      const descriptions = [
        { SIFLAG: 'SECTOR', SORI: 'US_EQUITY_LARGE', SORINAME: 'US Large Cap Equity' },
        { SIFLAG: 'SECTOR', SORI: 'US_EQUITY_SMALL', SORINAME: 'US Small Cap Equity' },
        { SIFLAG: 'SECTOR', SORI: 'INTL_EQUITY_DEV', SORINAME: 'International Developed Equity' },
        { SIFLAG: 'SECTOR', SORI: 'FIXED_INCOME_CORP', SORINAME: 'Corporate Fixed Income' },
        { SIFLAG: 'SECTOR', SORI: 'FIXED_INCOME_GOVT', SORINAME: 'Government Fixed Income' },
        { SIFLAG: 'SECTOR', SORI: 'REAL_ESTATE', SORINAME: 'Real Estate Holdings' },
        { SIFLAG: 'SECTOR', SORI: 'COMMODITIES', SORINAME: 'Commodities Direct' },
        { SIFLAG: 'CATEGORY', SORI: 'US_EQUITY', SORINAME: 'US Equity Composite' },
        { SIFLAG: 'CATEGORY', SORI: 'INTL_EQUITY', SORINAME: 'International Equity Composite' },
        { SIFLAG: 'CATEGORY', SORI: 'EQUITY', SORINAME: 'Total Equity' },
        { SIFLAG: 'CATEGORY', SORI: 'FIXED_INCOME', SORINAME: 'Total Fixed Income' },
        { SIFLAG: 'CATEGORY', SORI: 'ALTERNATIVES', SORINAME: 'Alternative Investments' },
        { SIFLAG: 'CATEGORY', SORI: 'TOTAL_FUND', SORINAME: 'Total Fund Composite' },
        { SIFLAG: 'INDEX', SORI: 'SP500', SORINAME: 'S&P 500 Index' },
        { SIFLAG: 'INDEX', SORI: 'AGG_BOND', SORINAME: 'Bloomberg Barclays Aggregate Bond Index' },
      ]
      for (const desc of descriptions) {

        await runStatement( 'INSERT INTO FRPSI1 (SIFLAG, SORI, SORINAME) VALUES (?, ?, ?)', [desc.SIFLAG, desc.SORI, desc.SORINAME])

      }
      console.log('FRPSI1 table seeded.')
    } else {
      console.log('FRPSI1 data already exists or an error occurred.')
    }


    const frpaggCountResult = await runQuery( 'SELECT COUNT(*) FROM FRPAGG')

    if (frpaggCountResult && frpaggCountResult.length > 0 && Number(frpaggCountResult[0][0]) === 0) {
      const aggregations = [
        { AGG: 'AGG_TOTAL_EQUITY', ACCT: 'ACC1001', DTOVER__1: '201001 999912' },
        { AGG: 'AGG_TOTAL_EQUITY', ACCT: 'ACC1003', DTOVER__1: '201809 999912' },
        { AGG: 'AGG_FIXED_INCOME', ACCT: 'ACC1002', DTOVER__1: '201506 999912' },
        { AGG: 'AGG_BALANCED', ACCT: 'ACC1005', DTOVER__1: '202002 202306', DTOVER__2: '202310 999912' },
        { AGG: 'MASTER_FUND_A', ACCT: 'AGG_TOTAL_EQUITY', DTOVER__1: '201001 999912'},
        { AGG: 'MASTER_FUND_A', ACCT: 'AGG_FIXED_INCOME', DTOVER__1: '201506 999912'},
      ]
      for (const agg of aggregations) {
        const params = [agg.AGG, agg.ACCT]
        let dtoverValues = []
        for(let i=1; i<=20; ++i) {
          dtoverValues.push(agg[`DTOVER__${i}`] || null)
        }
        params.push(...dtoverValues)
        const valuePlaceholders = dtoverValues.map(() => '?').join(', ')

        await runStatement(

                    `INSERT INTO FRPAGG (AGG, ACCT, ${Array.from({length: 20}, (_, i) => `DTOVER__${i+1}`).join(', ')}) VALUES (?, ?, ${valuePlaceholders})`,
                    params)
      }
      console.log('FRPAGG table seeded.')
    } else {
      console.log('FRPAGG data already exists or an error occurred.')
    }

    console.log('Database initialization script completed successfully with new data model and seeding.')

    console.log('\n--- Verifying Table Counts ---')

    const tablesToVerify = ['FRPAIR', 'FRPHOLD', 'FRPTRAN', 'FRPSECTR', 'FRPCTG', 'FRPSI1', 'FRPSEC', 'FRPPRICE', 'FRPAGG', 'report_configurations', 'agentic_workflows']
    for (const tableName of tablesToVerify) {
      try {
        const result = await runQuery( `SELECT COUNT(*) FROM ${tableName}`)

        if (result && result.length > 0 && result[0] && typeof result[0][0] === 'bigint') {
          console.log(`Count for ${tableName}: ${result[0][0]}`)
        } else {
          console.log(`Could not retrieve valid count for ${tableName}. Result:`, result)
        }
      } catch (e) {
        console.error(`Error counting ${tableName}:`, e.message)
      }
    }
    console.log('--- Verification Counts End ---')

  } catch (err) {
    console.error("Error during database initialization:", err);
    process.exitCode = 1;
  } finally {
    db.close(err => {
      if (err) console.error("Error closing database", err);
      else console.log("Database connection closed.");
    });
    process.exit(process.exitCode || 0);

  }
}

main()
