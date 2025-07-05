Agent Guide for the Financial Reporting Data Model
This document outlines the structure, relationships, and query logic for a financial reporting database. It is intended to guide LLMs in understanding how to retrieve and calculate information accurately.

Core Concepts
Account (ACCT/AACCT): The central entity representing a client's portfolio. This is the primary key for joining demographic, holding, and transaction data.

Security (HID): The unique identifier for a financial instrument (e.g., a stock or bond).

Hierarchy & Aggregation: The model supports two levels of grouping:

Account Grouping (FRPAGG): Combines multiple accounts into a single entity (e.g., a family or a composite) using an AGG ID. This is crucial for GIPS compliance and household-level reporting.

Sector Roll-up (FRPCTG): Defines the parent-child relationships between security sectors. This allows for calculating performance and exposure at different category levels (e.g., rolling "UK Small Cap Equities" up into "Equities").

Point-in-Time Data: Holdings data (FRPHOLD) is a month-end snapshot. To get a complete picture, it must be used with transaction data (FRPTRAN), which represents the "flows" between snapshots.

Unit Value Returns: Performance data for benchmarks (FRPINDX) is stored as a unit value index. To calculate the return for a specific period, use the formula: (Ending Value / Starting Value) - 1.

Table Schemas & Relationships
FRPAIR - Account Demographics
Purpose: The master table for account information.

Key Columns:

ACCT: Primary Key.

NAME: The full name of the account.

INCEPTION_DATE: The start date of the account.

Common Use Cases: Retrieve the name or other static details for a given account ID.

FRPHOLD - Holdings
Purpose: Stores month-end units for each security in every account.

Primary Keys: AACCT, HID, ADATE.

Foreign Keys:

AACCT links to FRPAIR.ACCT.

HID links to FRPSEC.ID.

HDIRECT1 links to FRPSI1.ID for sector classification.

Common Use Cases: Get the number of units of a security held by an account at a specific month's end. This is the starting point for any market value calculations.

FRPTRAN - Transactions
Purpose: Records all financial activities (buys, sells, dividends, fees, etc.).

Primary Keys: AACCT, HID, TDATE, TSEQ.

Foreign Keys:

AACCT links to FRPAIR.ACCT.

HID links to FRPSEC.ID.

TCODE links to FRPTCD.ID.

Common Use Cases: Analyze portfolio changes. The TCODE is critical for understanding the impact of a transaction on units and cash.

FRPAGG - Account Hierarchy & Composites
Purpose: Groups accounts for reporting.

Key Columns:

ACCT: The account ID.

AGG: The ID of the group or composite the account belongs to.

DTOVER##: Date override columns. These must be checked for time-sensitive reporting like GIPS, as they define when an account was part of a composite. If null, the account is considered part of the aggregate for its entire life.

Common Use Cases: Identify all accounts belonging to a family or a specific investment strategy composite for a given period.

FRPINDX - Benchmark Returns
Purpose: Stores benchmark performance data.

Key Columns:

INDX: The benchmark identifier.

Foreign Keys:

INDX links to FRPSI1.SORI where siflag = 'I'.

Common Use Cases: Retrieve performance history for a benchmark to compare against an account's performance.

Supporting Tables
FRPSEC: Security master file. Contains details about each HID. A daily price source from this table or another is required for mid-month market value calculations.

FRPTCD: Transaction code master. Defines the financial impact of each TCODE.

FRPSI1: Sector and index master. Provides labels and metadata for sectors (siflag='S') and benchmarks (siflag='I').

FRPCTG: The sector hierarchy definition table. Used to "roll up" detailed sectors into broader categories.

Querying Patterns & Logic
To Calculate Market Value
For Month-End:

Join FRPHOLD and FRPSEC on HID.

For a given AACCT and ADATE, the calculation is FRPHOLD.H_UNITS * FRPSEC.PRICE.

For a Mid-Month Date:

Get the units from the previous month-end in FRPHOLD.

Sum the T_UNITS from all transactions in FRPTRAN that occurred between the month-end date and the desired mid-month date.

Calculate Current Units = (Previous Month-End Units + Sum of Transactional Units).

Multiply Current Units by the price for the desired date (requires a daily price source).

To Report on a Group of Accounts
Query FRPAGG using the AGG ID (e.g., AGGSMITH) to get a list of all associated ACCT values.

When performing historical analysis (e.g., for a composite), always filter the FRPAGG records based on the DTOVER date fields to ensure accounts are only included for the correct time periods.

Use the resulting list of ACCT values to query the FRPHOLD and FRPTRAN tables.

To Report by Sector or Category
Start with a position from FRPHOLD. The HDIRECT1 column gives you the most granular sector ID.

Use this ID to look up the hierarchy in FRPCTG. This table allows you to traverse up the tree to find parent sectors (e.g., EQ, GEQ).

Use the IDs from FRPCTG to query FRPSI1 to get the human-readable labels for each category level (e.g., "Small Cap Equities", "Global Equities").

Aggregate market values for all positions that roll up into the desired category.